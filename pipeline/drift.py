import json
import uuid
from datetime import datetime, timezone
from pathlib import Path


class ManualRenameDriftSimulator:
    def __init__(
        self,
        canonical_schema,
        table_name,
        config_path=None,
        current_names=None,
        target_names=None,
    ):
        self.canonical_schema = dict(canonical_schema)
        self.table_name = table_name
        self.config_path = Path(config_path) if config_path else None
        self.current_names = {field: field for field in canonical_schema}
        self.target_names = target_names or {}
        if current_names:
            for canonical_field, current_name in current_names.items():
                if canonical_field in self.current_names and current_name:
                    self.current_names[canonical_field] = current_name

    def _load_requested_renames(self):
        if self.target_names:
            return {
                canonical_field: drifted_field
                for canonical_field, drifted_field in self.target_names.items()
                if canonical_field in self.canonical_schema and drifted_field
            }

        if self.config_path is None or not self.config_path.exists():
            return {}

        with self.config_path.open() as handle:
            payload = json.load(handle)

        renames = payload.get("renames", payload)
        sanitized = {}
        for canonical_field, drifted_field in renames.items():
            if canonical_field not in self.canonical_schema:
                continue
            if not drifted_field:
                continue
            sanitized[canonical_field] = drifted_field
        return sanitized

    def current_schema_snapshot(self):
        schema = {}
        for canonical_field, datatype in self.canonical_schema.items():
            current_name = self.current_names[canonical_field]
            schema[current_name] = datatype
        return schema

    def sync_renames(self):
        requested_renames = self._load_requested_renames()
        change_logs = []

        for canonical_field in self.canonical_schema:
            target_name = requested_renames.get(canonical_field, canonical_field)
            current_name = self.current_names[canonical_field]
            if target_name == current_name:
                continue

            before_schema = self.current_schema_snapshot()
            self.current_names[canonical_field] = target_name
            change_logs.append(
                {
                    "event_type": "SCHEMA_CHANGE",
                    "change_id": str(uuid.uuid4()),
                    "table": self.table_name,
                    "operation": {
                        "type": "rename_column",
                        "canonical_field": canonical_field,
                        "from": current_name,
                        "to": target_name,
                    },
                    "before_schema": before_schema,
                    "after_schema": self.current_schema_snapshot(),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )

        return change_logs

    def apply_to_event(self, canonical_event):
        drifted_event = {}

        for canonical_field, value in canonical_event.items():
            field_name = self.current_names[canonical_field]
            drifted_event[field_name] = value

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
