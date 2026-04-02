import argparse
import time

from pipeline.config import KAFKA_SETTINGS, PIPELINE_DEFAULTS, SCHEMA_DRIFT_SETTINGS
from pipeline.drift import ManualRenameDriftSimulator
from pipeline.generator import StockEventGenerator
from pipeline.kafka_utils import build_producer
from pipeline.metadata_store import ensure_producer_metadata, load_graph, save_producer_metadata
from pipeline.schema import extract_table_schema


def main():
    parser = argparse.ArgumentParser(
        description="Generate stock events and publish them to Kafka."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Number of events to publish. Use 0 for continuous streaming.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=PIPELINE_DEFAULTS["generator_interval_seconds"],
        help="Seconds to wait between events.",
    )
    parser.add_argument(
        "--drift",
        action="store_true",
        default=SCHEMA_DRIFT_SETTINGS["enabled"],
        help="Enable schema drift simulation.",
    )
    args = parser.parse_args()

    producer = build_producer()
    generator = StockEventGenerator()
    raw_topic = KAFKA_SETTINGS["topics"]["raw"]
    logs_topic = KAFKA_SETTINGS["topics"]["logs"]

    drift_simulator = None
    producer_metadata = None

    sent = 0
    try:
        while True:
            graph = load_graph()
            live_schema = extract_table_schema(graph, SCHEMA_DRIFT_SETTINGS["table_name"])
            producer_metadata = ensure_producer_metadata(graph)

            if args.drift:
                drift_simulator = ManualRenameDriftSimulator(
                    graph_schema=live_schema,
                    table_name=SCHEMA_DRIFT_SETTINGS["table_name"],
                    current_names=producer_metadata.get("last_applied_names", {}),
                    target_names=producer_metadata.get(
                        "next_source_names",
                        producer_metadata.get("last_applied_names", {}),
                    ),
                    pending_additions=producer_metadata.get("pending_additions", {}),
                )
            else:
                drift_simulator = None

            event_fields = (
                list(live_schema.keys())
                if producer_metadata
                else None
            )
            base_event = generator.generate_event(event_fields)
            change_logs = drift_simulator.sync_changes() if drift_simulator else []
            event = (
                drift_simulator.apply_to_event(base_event)
                if drift_simulator
                else base_event
            )

            for change_log in change_logs:
                producer.send(logs_topic, value=change_log)
                print(f"Published schema change log: {change_log}")
            producer.send(raw_topic, value=event)

            producer.flush()
            if drift_simulator:
                producer_metadata["last_applied_names"] = dict(drift_simulator.current_names)
                producer_metadata["source_schema"] = {
                    drift_simulator.current_names[field]: live_schema[field]
                    for field in live_schema
                }
                producer_metadata["source_to_graph"] = {
                    drift_simulator.current_names[field]: field
                    for field in live_schema
                }
                producer_metadata["pending_additions"] = {}
                save_producer_metadata(producer_metadata)
            sent += 1
            print(
                f"Produced event {sent} | sent keys: {list(event.keys())} | "
                f"event_id: {event.get('event_id', event.get('id', 'n/a'))}"
            )

            if args.count and sent >= args.count:
                break

            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nProducer stopped by user.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
