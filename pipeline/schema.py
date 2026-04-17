import json
from pathlib import Path


TYPE_COMPATIBILITY = {
    "varchar": str,
    "char": str,
    "text": str,
    "float": float,
    "double": float,
    "decimal": float,
    "int": int,
    "integer": int,
    "bigint": int,
}


def load_metadata_graph(path="metadata_graph.json"):
    graph_path = Path(path)
    if not graph_path.exists():
        raise FileNotFoundError(
            f"Metadata graph not found at {graph_path}. Run metadata_builder.py first."
        )
    with graph_path.open() as handle:
        return json.load(handle)


def extract_table_schema(graph, table_name):
    schema = {}
    for node in graph.get("nodes", []):
        if node.get("type") != "column":
            continue
        node_id = node.get("id", "")
        parts = node_id.split(".")
        if len(parts) < 3:
            continue
        if parts[-2] != table_name:
            continue
        schema[parts[-1]] = node.get("datatype")
    return schema


def _consumer_state(graph):
    return graph.get("consumer_state", {})


def get_alias_map(graph, table_name):
    alias_map = {}

    for node in graph.get("nodes", []):
        if node.get("type") != "column":
            continue
        node_id = node.get("id", "")
        parts = node_id.split(".")
        if len(parts) < 3 or parts[-2] != table_name:
            continue
        canonical = parts[-1]
        alias_map[canonical] = canonical
        for alias in node.get("aliases", []):
            alias_map[alias] = canonical

    tables = _consumer_state(graph).get("tables", {})
    tbl_aliases = tables.get(table_name, {}).get("aliases", {})
    for alias, canonical in tbl_aliases.items():
        alias_map[alias] = canonical

    return alias_map


def normalize_event_keys(event, graph, table_name):
    alias_map = get_alias_map(graph, table_name)
    normalized = {}
    renamed_fields = []

    for field, value in event.items():
        canonical_field = alias_map.get(field, field)
        normalized[canonical_field] = value
        if canonical_field != field:
            renamed_fields.append({"from": field, "to": canonical_field})

    return normalized, renamed_fields


def coerce_value(value, datatype):
    if value is None:
        return None
    normalized = (datatype or "").lower()
    try:
        if normalized in {"varchar", "char", "text"}:
            return str(value)
        if normalized in {"float", "double", "decimal"}:
            return float(value)
        if normalized in {"int", "integer", "bigint"}:
            return int(float(value))
    except (TypeError, ValueError):
        return value
    return value


def default_value_for_type(datatype):
    normalized = (datatype or "").lower()
    if normalized in {"float", "double", "decimal"}:
        return 0.0
    if normalized in {"int", "integer", "bigint"}:
        return 0
    return ""


def detect_and_fix_drift(event, schema):
    cleaned_event = {}
    drift_report = {
        "renamed_fields": [],
        "missing_fields": [],
        "extra_fields": [],
        "type_mismatches": [],
    }

    for field, datatype in schema.items():
        if field not in event:
            cleaned_event[field] = default_value_for_type(datatype)
            drift_report["missing_fields"].append(field)
            continue

        original_value = event[field]
        coerced_value = coerce_value(original_value, datatype)
        cleaned_event[field] = coerced_value

        expected_type = TYPE_COMPATIBILITY.get((datatype or "").lower())
        if expected_type and original_value is not None and not isinstance(
            original_value, expected_type
        ):
            if type(coerced_value) is expected_type:
                drift_report["type_mismatches"].append(
                    {
                        "field": field,
                        "from_type": type(original_value).__name__,
                        "to_type": expected_type.__name__,
                    }
                )

    for field, value in event.items():
        if field not in schema:
            drift_report["extra_fields"].append({"field": field, "value": value})

    has_drift = any(drift_report.values())
    return cleaned_event, drift_report, has_drift
