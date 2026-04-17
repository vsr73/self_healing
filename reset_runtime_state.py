import json
from datetime import datetime, timezone
from pathlib import Path

from pipeline.config import SCHEMA_DRIFT_SETTINGS
from pipeline.metadata_store import (
    reset_consumer_state,
    save_graph,
)
from pipeline.schema import extract_table_schema
from sync_metadata_from_mysql import sync_metadata_graph

BASE_DIR = Path(__file__).resolve().parent


def _write_json(path, payload):
    with Path(path).open("w") as handle:
        json.dump(payload, handle, indent=4)


def _reset_legacy_files():
    _write_json(BASE_DIR / "manual_drift_config.json", {"renames": {}})
    (BASE_DIR / "drift_events.jsonl").write_text("")
    (BASE_DIR / "generated_data.jsonl").write_text("")


def _reset_producer_metadata(graph):
    """Reset per-table producer metadata files for all managed tables."""
    results = {}
    for table_name in SCHEMA_DRIFT_SETTINGS["managed_tables"]:
        live_schema = extract_table_schema(graph, table_name)
        metadata = {
            "table": table_name,
            "source_schema": dict(live_schema),
            "source_to_graph": {field: field for field in live_schema},
            "next_source_names": {field: field for field in live_schema},
            "last_applied_names": {field: field for field in live_schema},
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
        _write_json(BASE_DIR / f"producer_metadata_{table_name}.json", metadata)
        results[table_name] = metadata
    return results


def main():
    graph = sync_metadata_graph()
    graph = reset_consumer_state(graph)
    save_graph(graph)

    all_metadata = _reset_producer_metadata(graph)
    _reset_legacy_files()

    print(
        json.dumps(
            {
                "status": "reset_complete",
                "managed_tables": SCHEMA_DRIFT_SETTINGS["managed_tables"],
                "per_table": {
                    tbl: {
                        "live_columns": [
                            node["id"].split(".")[-1]
                            for node in graph["nodes"]
                            if node.get("type") == "column"
                            and f".{tbl}." in node.get("id", "")
                        ],
                        "producer_source_columns": sorted(
                            all_metadata[tbl].get("source_schema", {}).keys()
                        ),
                    }
                    for tbl in SCHEMA_DRIFT_SETTINGS["managed_tables"]
                },
                "legacy_files_reset": [
                    "manual_drift_config.json",
                    "drift_events.jsonl",
                    "generated_data.jsonl",
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
