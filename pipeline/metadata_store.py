import json
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from pipeline.config import CANONICAL_SCHEMA, SCHEMA_DRIFT_SETTINGS, get_managed_tables


def metadata_path(path="metadata_graph.json"):
    return Path(path)


def producer_metadata_path(path="producer_metadata.json"):
    """Legacy single-file path resolver. Used by callers that pass an explicit path."""
    return Path(path)


def producer_metadata_path_for_table(table_name, base_dir=None):
    """Return the per-table producer metadata file path."""
    directory = Path(base_dir) if base_dir else Path(".")
    return directory / f"producer_metadata_{table_name}.json"


def load_graph(path="metadata_graph.json"):
    graph_path = metadata_path(path)
    with graph_path.open() as handle:
        graph = json.load(handle)
    initialize_graph_extensions(graph)
    return graph


def load_producer_metadata(path="producer_metadata.json"):
    producer_path = Path(path)
    with producer_path.open() as handle:
        return json.load(handle)


def save_producer_metadata(metadata, path="producer_metadata.json"):
    metadata["last_updated"] = datetime.now(timezone.utc).isoformat()
    producer_path = Path(path)
    with producer_path.open("w") as handle:
        json.dump(metadata, handle, indent=4)


def build_producer_metadata_from_graph(graph, table_name="stock_events"):
    live_schema = {}
    for node in graph.get("nodes", []):
        if node.get("type") != "column":
            continue
        parts = node.get("id", "").split(".")
        if len(parts) >= 3 and parts[-2] == table_name:
            live_schema[parts[-1]] = node.get("datatype")

    return {
        "table": table_name,
        "source_schema": dict(live_schema),
        "source_to_graph": {field: field for field in live_schema},
        "next_source_names": {field: field for field in live_schema},
        "last_applied_names": {field: field for field in live_schema},
    }


def ensure_producer_metadata(
    graph=None,
    path=None,
    table_name="stock_events",
    base_dir=None,
):
    """Load, validate, and if necessary create producer metadata for a single table.

    When *path* is None the per-table file ``producer_metadata_{table_name}.json``
    is used.  If that file does not exist yet but the legacy ``producer_metadata.json``
    was written for the same table, its contents are migrated to the new file
    automatically.  Callers that still pass an explicit *path* string keep the
    old behaviour unchanged.
    """
    schema = dict(CANONICAL_SCHEMA)
    schema_fields = list(schema.keys())

    # --- resolve file path ---
    if path is None:
        new_path = producer_metadata_path_for_table(table_name, base_dir)
        legacy_path = Path(base_dir or ".") / "producer_metadata.json"
        if not new_path.exists() and legacy_path.exists():
            try:
                old_data = json.loads(legacy_path.read_text())
                if old_data.get("table") == table_name:
                    new_path.write_text(json.dumps(old_data, indent=4))
            except (json.JSONDecodeError, OSError):
                pass
        producer_path = new_path
    else:
        producer_path = Path(path)

    live_graph_columns = {}
    if graph is not None:
        for node in graph.get("nodes", []):
            if node.get("type") != "column":
                continue
            parts = node.get("id", "").split(".")
            if len(parts) >= 3 and parts[-2] == table_name:
                live_graph_columns[parts[-1]] = node.get("datatype")

    if producer_path.exists():
        metadata = load_producer_metadata(str(producer_path))
        metadata.setdefault("table", table_name)
        source_to_graph = metadata.get("source_to_graph", metadata.get("source_to_canonical", {}))
        source_schema = metadata.get("source_schema", {})
        last_applied_names = metadata.get("last_applied_names", {})

        invalid_mapping = (
            "current_names" in metadata
            or "schema" in metadata
            or not source_to_graph
            or not source_schema
            or set(source_schema.keys()) != set(source_to_graph.keys())
            or any(not graph_field for graph_field in source_to_graph.values())
        )

        if invalid_mapping:
            if last_applied_names and all(name for name in last_applied_names.values()):
                metadata["source_schema"] = {
                    last_applied_names.get(field, field): schema[field]
                    for field in schema_fields
                    if field in schema
                }
                metadata["source_to_graph"] = {
                    last_applied_names.get(field, field): field
                    for field in schema_fields
                    if field in schema
                }
            else:
                bootstrap_schema = live_graph_columns or schema
                metadata["source_schema"] = {
                    field: bootstrap_schema.get(field, schema.get(field, "varchar"))
                    for field in bootstrap_schema
                }
                metadata["source_to_graph"] = {
                    field: field for field in metadata["source_schema"]
                }

            metadata.pop("current_names", None)
            metadata.pop("schema", None)

        metadata.setdefault("source_schema", {field: schema[field] for field in schema_fields if field in schema})
        metadata.setdefault("source_to_graph", {field: field for field in schema_fields if field in schema})
        if "last_applied_names" not in metadata:
            metadata["last_applied_names"] = {
                graph_field: source_name
                for source_name, graph_field in metadata.get("source_to_graph", {}).items()
            }
        metadata.setdefault(
            "next_source_names",
            dict(metadata.get("last_applied_names", {})),
        )
        metadata.pop("source_to_canonical", None)
        save_producer_metadata(metadata, str(producer_path))
        return metadata

    metadata = {
        "table": table_name,
        "source_schema": {field: schema[field] for field in schema_fields if field in schema},
        "source_to_graph": {field: field for field in schema_fields if field in schema},
        "next_source_names": {field: field for field in schema_fields if field in schema},
        "last_applied_names": {field: field for field in schema_fields if field in schema},
    }
    save_producer_metadata(metadata, str(producer_path))
    return metadata


def save_graph(graph, path="metadata_graph.json"):
    graph["last_updated"] = datetime.now(timezone.utc).isoformat()
    graph_path = metadata_path(path)
    with graph_path.open("w") as handle:
        json.dump(graph, handle, indent=4)


def initialize_graph_extensions(graph):
    """Ensure consumer_state uses the per-table structure.

    Runs the one-time migration from the legacy flat ``metadata_events`` list
    to the nested ``consumer_state.tables.<table>.metadata_events`` layout.
    The migration is idempotent: after the first ``save_graph`` call the flat
    key is gone and this function becomes a no-op.
    """
    state = graph.setdefault("consumer_state", {})

    # Case A: very old top-level metadata_events key
    top_level_events = graph.pop("metadata_events", None)
    if top_level_events:
        state.setdefault("metadata_events", top_level_events)

    # Case B: flat metadata_events directly on consumer_state (current single-table format)
    flat_events = state.pop("metadata_events", None)

    tables = state.setdefault("tables", {})

    if flat_events is not None:
        grouped = defaultdict(list)
        for ev in flat_events:
            grouped[ev.get("table", "stock_events")].append(ev)
        for tbl, evs in grouped.items():
            tbl_state = tables.setdefault(tbl, {})
            existing = tbl_state.setdefault("metadata_events", [])
            existing_keys = {(e.get("timestamp"), e.get("summary")) for e in existing}
            for ev in evs:
                key = (ev.get("timestamp"), ev.get("summary"))
                if key not in existing_keys:
                    existing.append(ev)
                    existing_keys.add(key)

    # Ensure every managed table has a default sub-state entry
    for tbl in get_managed_tables():
        tbl_state = tables.setdefault(tbl, {})
        tbl_state.setdefault("metadata_events", [])
        tbl_state.setdefault("aliases", {})
        tbl_state.setdefault("drift_status", "healthy")

    state.setdefault("dependencies", [])
    return graph


def get_consumer_state(graph):
    initialize_graph_extensions(graph)
    return graph["consumer_state"]


def get_table_state(graph, table_name):
    """Return the per-table consumer state dict, initializing defaults if absent."""
    initialize_graph_extensions(graph)
    return graph["consumer_state"]["tables"].setdefault(
        table_name,
        {"metadata_events": [], "aliases": {}, "drift_status": "healthy"},
    )


def reset_consumer_state(graph):
    initialize_graph_extensions(graph)
    tables = {}
    for tbl in get_managed_tables():
        tables[tbl] = {
            "metadata_events": [],
            "aliases": {},
            "drift_status": "healthy",
        }
    graph["consumer_state"] = {
        "tables": tables,
        "dependencies": [],
    }
    return graph


def _find_column_node(graph, table_name, canonical_field):
    suffix = f".{table_name}.{canonical_field}"
    for node in graph.get("nodes", []):
        if node.get("type") == "column" and node.get("id", "").endswith(suffix):
            return node
    return None


def _find_table_node(graph, table_name):
    suffix = f".{table_name}"
    for node in graph.get("nodes", []):
        if node.get("type") == "table" and node.get("id", "").endswith(suffix):
            return node
    return None


def apply_schema_dag_to_graph(graph, dag):
    updated_graph = deepcopy(graph)
    node_map = {node["id"]: node for node in updated_graph.get("nodes", [])}
    source_name = (
        updated_graph.get("sources", [{}])[0].get("name", "mysql_stock")
        if updated_graph.get("sources")
        else "mysql_stock"
    )

    def rename_column(table, old_column, new_column):
        old_id = f"{source_name}.{table}.{old_column}"
        new_id = f"{source_name}.{table}.{new_column}"
        if old_id not in node_map:
            return
        node = node_map[old_id]
        node["id"] = new_id
        node_map[new_id] = node
        del node_map[old_id]
        for candidate in updated_graph["nodes"]:
            for edge in candidate.get("edges", []):
                if edge.get("target") == old_id:
                    edge["target"] = new_id

    def add_column(table, column, datatype="unknown"):
        table_id = f"{source_name}.{table}"
        column_id = f"{table_id}.{column}"
        if column_id in node_map:
            return
        node = {
            "id": column_id,
            "type": "column",
            "edges": [],
            "datatype": datatype,
            "nullable": "YES",
        }
        updated_graph["nodes"].append(node)
        node_map[column_id] = node
        table_node = _find_table_node(updated_graph, table)
        if table_node is not None:
            table_node.setdefault("edges", []).append(
                {"relation": "HAS_COLUMN", "target": column_id}
            )

    def drop_column(table, column):
        column_id = f"{source_name}.{table}.{column}"
        node = node_map.get(column_id)
        if node is None:
            return
        updated_graph["nodes"].remove(node)
        del node_map[column_id]
        for candidate in updated_graph["nodes"]:
            candidate["edges"] = [
                edge for edge in candidate.get("edges", []) if edge.get("target") != column_id
            ]

    def create_table(table):
        table_id = f"{source_name}.{table}"
        if table_id in node_map:
            return
        table_node = {"id": table_id, "type": "table", "edges": []}
        updated_graph["nodes"].append(table_node)
        node_map[table_id] = table_node
        for node in updated_graph["nodes"]:
            if node.get("type") == "database":
                node.setdefault("edges", []).append(
                    {"relation": "HAS_TABLE", "target": table_id}
                )
                break

    def drop_table(table):
        table_prefix = f"{source_name}.{table}"
        updated_graph["nodes"] = [
            node for node in updated_graph["nodes"] if not node.get("id", "").startswith(table_prefix)
        ]
        for node in updated_graph["nodes"]:
            node["edges"] = [
                edge
                for edge in node.get("edges", [])
                if not edge.get("target", "").startswith(table_prefix)
            ]

    for step in dag:
        operation = step.get("operation")
        if operation == "RENAME_COLUMN":
            rename_column(
                step["table"],
                step.get("old_column", step.get("previous_name")),
                step.get("new_column", step.get("current_name")),
            )
        elif operation == "ADD_COLUMN":
            add_column(step["table"], step["column"], step.get("datatype", "unknown"))
        elif operation == "DROP_COLUMN":
            drop_column(step["table"], step["column"])
        elif operation == "CREATE_TABLE":
            create_table(step["table"])
        elif operation == "DROP_TABLE":
            drop_table(step["table"])

    return updated_graph


def apply_schema_update_plan(graph, plan):
    initialize_graph_extensions(graph)
    updated_graph = deepcopy(graph)
    table_name = plan["table"]
    tbl_state = get_table_state(updated_graph, table_name)

    tbl_state["metadata_events"].append(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "table": table_name,
            "summary": plan.get("summary", ""),
            "dag": plan.get("dag", []),
        }
    )

    return updated_graph
