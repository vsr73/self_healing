
# ============================================================
# metadata_updater.py  —  Legacy metadata patch utility
# (kept for backward compatibility)
# schema_repair_engine.py is the primary repair path now.
# ============================================================

import json
import os
from datetime import datetime

_HERE         = os.path.dirname(os.path.abspath(__file__))
METADATA_FILE = os.path.join(_HERE, "..", "metadata_graph.json")


def load_metadata():
    with open(METADATA_FILE) as f:
        return json.load(f)


def save_metadata(graph):
    graph["last_updated"] = datetime.utcnow().isoformat()
    with open(METADATA_FILE, "w") as f:
        json.dump(graph, f, indent=4)
    print("Metadata graph saved.")


def patch_metadata(metadata, logs):
    """
    Simple rule-based metadata patcher.
    Handles RENAME COLUMN statements from schema_change_log.
    """
    for log in logs:
        print("Processing schema log:", log)
        log_upper = log.upper()

        if "RENAME COLUMN" in log_upper:
            parts      = log.split()
            # ALTER TABLE <table> RENAME COLUMN <old> TO <new>
            try:
                table      = parts[2]
                old_column = parts[5]
                new_column = parts[7]
                old_id     = f"{table}.{old_column}"
                new_id     = f"{table}.{new_column}"

                for node in metadata["nodes"]:
                    if node["id"] == old_id:
                        print(f"Renaming: {old_id} → {new_id}")
                        node["id"]    = new_id
                        node["table"] = table

                for node in metadata["nodes"]:
                    for edge in node.get("edges", []):
                        if edge["target"] == old_id:
                            edge["target"] = new_id
            except IndexError:
                print(f"  ⚠️  Could not parse RENAME COLUMN: {log}")

    return metadata


def update_metadata(cursor, error):
    print("\n----- Metadata Repair Triggered -----")
    metadata    = load_metadata()
    last_update = metadata["last_updated"]
    print("Last metadata update:", last_update)

    cursor.execute("""
        SELECT executed_query
        FROM schema_change_log
        WHERE event_time > %s
        ORDER BY event_time
    """, (last_update,))
    logs = [r[0] for r in cursor.fetchall()]

    if not logs:
        print("No schema changes detected after metadata timestamp.")
        return metadata

    print("\nSchema changes detected:")
    for log in logs:
        print("•", log)

    print("\nError that triggered repair:")
    print(error)

    updated_metadata = patch_metadata(metadata, logs)
    save_metadata(updated_metadata)
    print("Metadata successfully repaired.\n")
    return updated_metadata
