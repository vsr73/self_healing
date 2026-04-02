import json
import uuid
from datetime import datetime, timezone
from pathlib import Path


class ManualRenameDriftSimulator:
    def __init__(
        self,
        graph_schema,
        table_name,
        config_path=None,
        current_names=None,
        target_names=None,
        pending_additions=None,
    ):
        self.graph_schema = dict(graph_schema)
        self.table_name = table_name
        self.config_path = Path(config_path) if config_path else None
        self.current_names = {field: field for field in graph_schema}
        self.target_names = target_names or {}
        self.pending_additions = dict(pending_additions or {})
        if current_names:
            for graph_field, current_name in current_names.items():
                if graph_field in self.current_names and current_name:
                    self.current_names[graph_field] = current_name

    def _load_requested_renames(self):
        if self.target_names:
            return {
                graph_field: drifted_field
                for graph_field, drifted_field in self.target_names.items()
                if graph_field in self.graph_schema and drifted_field
            }

        if self.config_path is None or not self.config_path.exists():
            return {}

        with self.config_path.open() as handle:
            payload = json.load(handle)

        renames = payload.get("renames", payload)
        sanitized = {}
        for graph_field, drifted_field in renames.items():
            if graph_field not in self.graph_schema:
                continue
            if not drifted_field:
                continue
            sanitized[graph_field] = drifted_field
        return sanitized

    def current_schema_snapshot(self):
        schema = {}
        for graph_field, datatype in self.graph_schema.items():
            current_name = self.current_names[graph_field]
            schema[current_name] = datatype
        schema.update(self.pending_additions)
        return schema

    def sync_changes(self):
        requested_renames = self._load_requested_renames()
        change_logs = []

        for graph_field in self.graph_schema:
            target_name = requested_renames.get(graph_field, graph_field)
            current_name = self.current_names[graph_field]
            if target_name == current_name:
                continue

            before_schema = self.current_schema_snapshot()
            self.current_names[graph_field] = target_name
            change_logs.append(
                {
                    "event_type": "SCHEMA_CHANGE",
                    "change_id": str(uuid.uuid4()),
                    "table": self.table_name,
                    "operation": {
                        "type": "rename_column",
                        "graph_field": graph_field,
                        "from": current_name,
                        "to": target_name,
                    },
                    "before_schema": before_schema,
                    "after_schema": self.current_schema_snapshot(),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )

        for field_name, datatype in self.pending_additions.items():
            if field_name in self.current_schema_snapshot():
                continue
            before_schema = self.current_schema_snapshot()
            change_logs.append(
                {
                    "event_type": "SCHEMA_CHANGE",
                    "change_id": str(uuid.uuid4()),
                    "table": self.table_name,
                    "operation": {
                        "type": "add_column",
                        "field": field_name,
                        "datatype": datatype,
                    },
                    "before_schema": before_schema,
                    "after_schema": {**before_schema, field_name: datatype},
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )

        return change_logs

    def _default_value_for_addition(self, field_name, datatype, source_event):
        normalized_name = field_name.lower()
        normalized_type = (datatype or "").lower()
        if normalized_name.endswith("flag"):
            return "Y"
        if normalized_name.endswith("code"):
            return "AUTO"
        if normalized_type in {"int", "integer", "bigint"}:
            return 1
        if normalized_type in {"float", "double", "decimal"}:
            return 1.0
        return f"{field_name}_value"

    def apply_to_event(self, graph_event):
        drifted_event = {}

        for graph_field, value in graph_event.items():
            field_name = self.current_names[graph_field]
            drifted_event[field_name] = value

        for field_name, datatype in self.pending_additions.items():
            if field_name not in drifted_event:
                drifted_event[field_name] = self._default_value_for_addition(
                    field_name,
                    datatype,
                    graph_event,
                )

        return drifted_event


class SchemaChangeTracker:
    def __init__(self):
        self.alias_map = {}
        self.removed_fields = set()
        self.type_overrides = {}
        self.observed_additions = {}
        self.change_history = []

    def hydrate_from_graph(self, graph, table_name):
        self.alias_map = {}

    def apply_change_log(self, change_log):
        operation = change_log["operation"]
        operation_type = operation["type"]

        if operation_type == "rename_column":
            graph_field = operation.get("graph_field", operation.get("canonical_field", operation["from"]))
            self.alias_map[operation["to"]] = graph_field
        elif operation_type == "add_column":
            self.alias_map[operation["field"]] = operation["field"]

        self.change_history.append(change_log)

    def normalize_event(self, event):
        normalized_event = {}
        parsed_changes = []

        for field, value in event.items():
            canonical_field = self.alias_map.get(field, field)
            normalized_event[canonical_field] = value
            if canonical_field != field:
                parsed_changes.append(
                    {
                        "type": "rename_column",
                        "from": field,
                        "to": canonical_field,
                    }
                )

        return normalized_event, parsed_changes
