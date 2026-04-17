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
    producer_metadata_path_for_table,
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


def _execute_ddl_atomic(repair_plan):
    """Execute every DDL statement from the repair plan.

    Each statement is tried independently so that already-applied changes
    (idempotency) do not abort the rest of the batch.  Returns (success, errors).
    """
    idempotent_phrases = (
        "already exists",
        "doesn't exist",
        "unknown column",
        "can't drop",
        "duplicate column",
        "check that column/key exists",
    )
    errors = []
    skipped = []
    for ddl in repair_plan.get("ddl_statements", []):
        stmt = ddl.strip()
        if not stmt or stmt.startswith("--"):
            continue
        try:
            execute_mysql_ddl(stmt)
            print(f"  [DDL OK] {stmt}")
        except Exception as exc:
            err = str(exc).lower()
            if any(phrase in err for phrase in idempotent_phrases):
                skipped.append(stmt)
                print(f"  [DDL Skip - already applied] {stmt}")
            else:
                errors.append(str(exc))
                print(f"  [DDL Error] {exc}")

    return (len(errors) == 0), errors, skipped


def repair_schema_change(change_event, producer, logs_topic):
    """Single LLM call → all DDL executed atomically → DAG + metadata updated."""
    graph = load_graph()
    repair_plan = generate_consumer_repair(change_event, graph)

    ops_count = len(
        change_event.get("operations") or
        ([change_event.get("operation")] if change_event.get("operation") else [])
    )
    print(f"\n[Schema Repair] table={change_event.get('table')} ops={ops_count}")
    print(json.dumps(change_event, indent=2))
    print("\n[LLM DAG]")
    print(json.dumps(repair_plan.get("dag", []), indent=2))
    print("\n[LLM DDL]")
    for ddl in repair_plan.get("ddl_statements", []):
        print(f"  {ddl}")

    producer.send(logs_topic, value=build_llm_debug_log(change_event, repair_plan))
    producer.send(logs_topic, value=build_consumer_ddl_log(change_event, repair_plan))

    # --- Execute all DDL atomically (per-statement idempotency) ---
    ddl_success, ddl_errors, ddl_skipped = _execute_ddl_atomic(repair_plan)

    combined_error = "; ".join(ddl_errors) if ddl_errors else (
        f"Skipped (already applied): {ddl_skipped}" if ddl_skipped else None
    )
    producer.send(
        logs_topic,
        value=build_ddl_execution_log(
            change_event,
            repair_plan,
            success=ddl_success,
            error=combined_error,
        ),
    )

    if not ddl_success:
        print(f"[DDL Failed] {combined_error}")
        raise RuntimeError(f"Schema repair failed: {combined_error}")

    if ddl_skipped and not ddl_errors:
        print(f"[DDL] {len(ddl_skipped)} statement(s) already applied, rest OK.")
    else:
        print(f"[DDL Applied] {len(repair_plan.get('ddl_statements', []))} statement(s) executed.")

    # --- Apply full DAG to metadata graph in one pass ---
    updated_graph = apply_schema_dag_to_graph(graph, repair_plan.get("dag", []))
    updated_graph = apply_schema_update_plan(
        updated_graph,
        build_metadata_update_plan(change_event, repair_plan),
    )
    save_graph(updated_graph)
    print(f"[Metadata] DAG applied — {len(repair_plan.get('dag', []))} node(s) updated.")

    graph = sync_metadata_graph()
    repaired_table = change_event.get("table", "stock_events")
    save_producer_metadata(
        build_producer_metadata_from_graph(graph, table_name=repaired_table),
        str(producer_metadata_path_for_table(repaired_table)),
    )
    print(f"[Metadata] producer_metadata_{repaired_table}.json synced.")
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
            # Route to the correct table; strip the routing field before processing
            table_name = raw_event.pop("_table", "stock_events")

            graph = load_graph()
            schema = extract_table_schema(graph, table_name)
            tracker.hydrate_from_graph(graph, table_name)
            print(f"\n[{table_name}] [Data Received] {json.dumps(summarize_event(raw_event))}")

            normalized_event, parsed_changes = tracker.normalize_event(raw_event)
            cleaned_event, drift_report, has_drift = detect_and_fix_drift(
                normalized_event, schema
            )
            drift_report["parsed_logs"] = parsed_changes

            storage_row = build_storage_row(graph, table_name, cleaned_event)
            try:
                insert_mysql_row(table_name, storage_row)
                print(f"[{table_name}] [Data Sent To DB] {json.dumps(summarize_event(storage_row))}")
            except Exception as exc:
                error_log = {
                    "event_type": "DB_INSERT_FAILED",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "table": table_name,
                    "row": storage_row,
                    "error": str(exc),
                }
                producer.send(logs_topic, value=error_log)
                print(f"[{table_name}] [DB Insert Failed] {exc}")

            producer.send(clean_topic, value=cleaned_event)
            print(f"[{table_name}] [Data Sent To Kafka] {json.dumps(summarize_event(cleaned_event))}")

            if has_drift:
                log_entry = build_log_entry(
                    raw_event, normalized_event, cleaned_event, drift_report
                )
                producer.send(logs_topic, value=log_entry)
                print(
                    f"[{table_name}] [Data Repair Logged] "
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
