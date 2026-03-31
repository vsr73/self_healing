import json
from datetime import datetime, timezone

from pipeline.config import KAFKA_SETTINGS
from pipeline.db import execute_mysql_ddl, fetch_mysql_table_schema, insert_mysql_row
from pipeline.drift import SchemaChangeTracker
from pipeline.kafka_utils import build_consumer, build_producer
from pipeline.llm_repair import generate_consumer_repair
from pipeline.metadata_store import (
    apply_schema_dag_to_graph,
    apply_schema_update_plan,
    build_producer_metadata_from_graph,
    load_graph,
    save_graph,
    save_producer_metadata,
)
from pipeline.schema import detect_and_fix_drift, extract_table_schema
from sync_metadata_from_mysql import sync_metadata_graph


def build_log_entry(raw_event, normalized_event, cleaned_event, drift_report):
    return {
        "event_type": "DATA_REPAIR",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "raw_event": raw_event,
        "normalized_event": normalized_event,
        "cleaned_event": cleaned_event,
        "details": drift_report,
    }


def build_pause_log(change_event):
    return {
        "event_type": "CONSUMER_PAUSED_FOR_DRIFT",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "change_id": change_event.get("change_id"),
        "table": change_event.get("table"),
        "reason": "Consumer paused normal processing to repair schema drift.",
    }


def build_producer_metadata_seen_log(log_event):
    return {
        "event_type": "CONSUMER_SAW_PRODUCER_METADATA",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "table": log_event.get("table"),
        "changes": log_event.get("changes", []),
    }


def build_llm_debug_log(change_event, repair_plan):
    return {
        "event_type": "LLM_REPAIR_DEBUG",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "change_id": change_event.get("change_id"),
        "consumer_saw": change_event,
        "llm_prompt": repair_plan.get("_debug_prompt", ""),
        "llm_sent": {
            key: value
            for key, value in repair_plan.items()
            if key != "_debug_prompt"
        },
    }


def build_consumer_ddl_log(change_event, repair_plan):
    return {
        "event_type": "CONSUMER_DDL_PLAN",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "change_id": change_event.get("change_id"),
        "table": change_event.get("table"),
        "change_event": change_event,
        "repair_plan": {
            key: value
            for key, value in repair_plan.items()
            if key != "_debug_prompt"
        },
    }


def build_ddl_execution_log(change_event, repair_plan, success, error=None):
    return {
        "event_type": "DDL_EXECUTED" if success else "DDL_FAILED",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "change_id": change_event.get("change_id"),
        "table": change_event.get("table"),
        "ddl_statements": repair_plan.get("ddl_statements", []),
        "summary": repair_plan.get("summary"),
        "error": error,
    }


def build_metadata_update_plan(change_event, repair_plan, raw_event=None, cleaned_event=None):
    return {
        "table": change_event["table"],
        "summary": repair_plan["summary"],
        "dag": repair_plan.get("dag", []),
    }


def build_storage_row(graph, table_name, cleaned_event):
    return dict(cleaned_event)


def summarize_event(event):
    return {
        "keys": list(event.keys()),
        "row_id": event.get("id", event.get("event_id", event.get("event_name", ""))),
    }


def ddl_already_applied(change_event, repair_plan):
    operation = change_event.get("operation", {})
    if operation.get("type") != "rename_column":
        return False, None

    table_name = change_event.get("table")
    previous_name = operation.get("from")
    current_name = operation.get("to")
    live_schema = fetch_mysql_table_schema(table_name)

    if current_name in live_schema and previous_name not in live_schema:
        return True, (
            f"Skipped DDL because live table already has `{current_name}` and no longer has "
            f"`{previous_name}`."
        )

    return False, None


def repair_schema_change(change_event, producer, logs_topic):
    graph = load_graph()
    repair_plan = generate_consumer_repair(change_event, graph)

    print("\n[Consumer Saw Drift]")
    print(json.dumps(change_event, indent=2))
    print("\n[LLM Prompt]")
    print(repair_plan.get("_debug_prompt", ""))
    print("\n[LLM DAG]")
    print(json.dumps(repair_plan.get("dag", []), indent=2))
    print("\n[LLM Repair Output]")
    print(
        json.dumps(
            {key: value for key, value in repair_plan.items() if key != "_debug_prompt"},
            indent=2,
        )
    )

    producer.send(logs_topic, value=build_llm_debug_log(change_event, repair_plan))
    producer.send(logs_topic, value=build_consumer_ddl_log(change_event, repair_plan))

    print("\n[Generated DDL]")
    for ddl in repair_plan.get("ddl_statements", []):
        print(ddl)

    ddl_success = False
    ddl_error = None
    try:
        skipped, skip_reason = ddl_already_applied(change_event, repair_plan)
        if skipped:
            ddl_success = True
            ddl_error = skip_reason
        else:
            for ddl in repair_plan.get("ddl_statements", []):
                if ddl.strip().startswith("--"):
                    continue
                execute_mysql_ddl(ddl)
            ddl_success = True
    except Exception as exc:
        ddl_error = str(exc)

    producer.send(
        logs_topic,
        value=build_ddl_execution_log(
            change_event,
            repair_plan,
            success=ddl_success,
            error=ddl_error,
        ),
    )

    if not ddl_success:
        print(f"[DDL Failed] {ddl_error}")
        raise RuntimeError(f"Schema repair failed: {ddl_error}")

    if ddl_error:
        print(f"[DDL Skipped] {ddl_error}")
    else:
        print("[DDL Applied] MySQL schema updated successfully.")

    updated_graph = apply_schema_dag_to_graph(graph, repair_plan.get("dag", []))
    updated_graph = apply_schema_update_plan(
        updated_graph,
        build_metadata_update_plan(change_event, repair_plan),
    )
    save_graph(updated_graph)
    graph = sync_metadata_graph()
    save_producer_metadata(
        build_producer_metadata_from_graph(graph),
        "producer_metadata.json",
    )
    producer.flush()
    return graph


def main():
    raw_topic = KAFKA_SETTINGS["topics"]["raw"]
    clean_topic = KAFKA_SETTINGS["topics"]["clean"]
    logs_topic = KAFKA_SETTINGS["topics"]["logs"]

    tracker = SchemaChangeTracker()
    consumer = build_consumer([raw_topic, logs_topic])
    producer = build_producer()

    print(f"Consumer listening on topics: {raw_topic}, {logs_topic}")

    try:
        for message in consumer:
            if message.topic == logs_topic:
                log_event = message.value
                if log_event.get("event_type") == "PRODUCER_METADATA_UPDATED":
                    print("\n[Producer Metadata Updated]")
                    print(json.dumps(log_event, indent=2))
                    seen_log = build_producer_metadata_seen_log(log_event)
                    producer.send(logs_topic, value=seen_log)
                    producer.flush()
                    print("Consumer acknowledged producer metadata update.")
                    continue

                if log_event.get("event_type") == "SCHEMA_CHANGE":
                    tracker.apply_change_log(log_event)
                    print(
                        "\n[Schema Change Received]\n"
                        + json.dumps(log_event, indent=2)
                    )
                    pause_log = build_pause_log(log_event)
                    producer.send(logs_topic, value=pause_log)
                    producer.flush()
                    print("Consumer paused normal row processing for schema repair.")
                    repair_schema_change(log_event, producer, logs_topic)
                    tracker = SchemaChangeTracker()
                continue

            raw_event = message.value
            graph = load_graph()
            schema = extract_table_schema(graph, "stock_events")
            tracker.hydrate_from_graph(graph, "stock_events")
            print(f"\n[Data Received] {json.dumps(summarize_event(raw_event))}")

            normalized_event, parsed_changes = tracker.normalize_event(raw_event)
            cleaned_event, drift_report, has_drift = detect_and_fix_drift(
                normalized_event, schema
            )
            drift_report["parsed_logs"] = parsed_changes

            storage_row = build_storage_row(graph, "stock_events", cleaned_event)
            try:
                insert_mysql_row("stock_events", storage_row)
                print(f"[Data Sent To DB] {json.dumps(summarize_event(storage_row))}")
            except Exception as exc:
                error_log = {
                    "event_type": "DB_INSERT_FAILED",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "table": "stock_events",
                    "row": storage_row,
                    "error": str(exc),
                }
                producer.send(logs_topic, value=error_log)
                print(f"[DB Insert Failed] {exc}")

            producer.send(clean_topic, value=cleaned_event)
            print(f"[Data Sent To Kafka] {json.dumps(summarize_event(cleaned_event))}")

            if has_drift:
                log_entry = build_log_entry(
                    raw_event, normalized_event, cleaned_event, drift_report
                )
                producer.send(logs_topic, value=log_entry)
                print(
                    "[Data Repair Logged] "
                    + json.dumps(
                        {
                            "missing_fields": drift_report.get("missing_fields", []),
                            "extra_fields": [item["field"] for item in drift_report.get("extra_fields", [])],
                            "parsed_logs": drift_report.get("parsed_logs", []),
                        }
                    )
                )

            producer.flush()
    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
