from datetime import datetime, timezone

from pipeline.config import KAFKA_SETTINGS
from pipeline.kafka_utils import build_consumer, build_producer
from pipeline.llm_repair import generate_schema_update_plan
from pipeline.metadata_store import apply_schema_update_plan, load_graph, save_graph


def build_metadata_update_log(change_event, plan):
    return {
        "event_type": "METADATA_GRAPH_UPDATED",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "table": change_event["table"],
        "change_id": change_event.get("change_id"),
        "summary": plan.get("summary", ""),
        "plan": plan,
    }


def main():
    logs_topic = KAFKA_SETTINGS["topics"]["logs"]
    consumer = build_consumer(logs_topic)
    producer = build_producer()

    print(f"Metadata repair consumer listening on topic: {logs_topic}")

    try:
        for message in consumer:
            event = message.value
            if event.get("event_type") != "SCHEMA_CHANGE":
                continue

            graph = load_graph()
            plan = generate_schema_update_plan(event, graph)
            updated_graph = apply_schema_update_plan(graph, plan)
            save_graph(updated_graph)

            update_log = build_metadata_update_log(event, plan)
            producer.send(logs_topic, value=update_log)
            producer.flush()

            print(f"Updated metadata_graph.json for change {event.get('change_id')}")
            print(f"Plan summary: {plan.get('summary', '')}")
    except KeyboardInterrupt:
        print("\nMetadata repair consumer stopped by user.")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
