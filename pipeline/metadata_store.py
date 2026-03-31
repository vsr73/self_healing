import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from pipeline.config import CANONICAL_SCHEMA


def metadata_path(path="metadata_graph.json"):
    return Path(path)


def producer_metadata_path(path="producer_metadata.json"):
    return Path(path)


def load_graph(path="metadata_graph.json"):
    graph_path = metadata_path(path)
    with graph_path.open() as handle:
        graph = json.load(handle)
    initialize_graph_extensions(graph)
    return graph


def load_producer_metadata(path="producer_metadata.json"):
    producer_path = producer_metadata_path(path)
    with producer_path.open() as handle:
        return json.load(handle)


def save_producer_metadata(metadata, path="producer_metadata.json"):
    metadata["last_updated"] = datetime.now(timezone.utc).isoformat()
    producer_path = producer_metadata_path(path)
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


def ensure_producer_metadata(graph=None, path="producer_metadata.json", table_name="stock_events"):
    producer_path = producer_metadata_path(path)
    schema = dict(CANONICAL_SCHEMA)
    schema_fields = list(schema.keys())
    live_graph_columns = {}
    if graph is not None:
        for node in graph.get("nodes", []):
            if node.get("type") != "column":
                continue
            parts = node.get("id", "").split(".")
            if len(parts) >= 3 and parts[-2] == table_name:
                live_graph_columns[parts[-1]] = node.get("datatype")

    if producer_path.exists():
        metadata = load_producer_metadata(path)
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
                }
                metadata["source_to_graph"] = {
                    last_applied_names.get(field, field): field
                    for field in schema_fields
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

        metadata.setdefault("source_schema", {field: schema[field] for field in schema_fields})
        metadata.setdefault("source_to_graph", {field: field for field in schema_fields})
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
        save_producer_metadata(metadata, path)
        return metadata

    metadata = {
        "table": table_name,
        "source_schema": {field: schema[field] for field in schema_fields},
        "source_to_graph": {field: field for field in schema_fields},
        "next_source_names": {field: field for field in schema_fields},
        "last_applied_names": {field: field for field in schema_fields},
    }
    save_producer_metadata(metadata, path)
    return metadata


def save_graph(graph, path="metadata_graph.json"):
    graph["last_updated"] = datetime.now(timezone.utc).isoformat()
    graph_path = metadata_path(path)
    with graph_path.open("w") as handle:
        json.dump(graph, handle, indent=4)


def initialize_graph_extensions(graph):
    state = graph.setdefault("consumer_state", {})
    metadata_events = graph.pop("metadata_events", [])
    state.setdefault("metadata_events", metadata_events)

    return graph


def get_consumer_state(graph):
    initialize_graph_extensions(graph)
    return graph["consumer_state"]


def reset_consumer_state(graph):
    initialize_graph_extensions(graph)
    graph["consumer_state"] = {
        "metadata_events": [],
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
    state = get_consumer_state(updated_graph)

    state["metadata_events"].append(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "table": plan["table"],
            "summary": plan.get("summary", ""),
            "dag": plan.get("dag", []),
        }
    )

    return updated_graph
