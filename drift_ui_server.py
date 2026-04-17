import json
import re
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from pipeline.config import KAFKA_SETTINGS, get_managed_tables, save_managed_tables
from pipeline.db import execute_mysql_ddl
from pipeline.drift import ManualRenameDriftSimulator
from pipeline.generator import StockEventGenerator
from pipeline.kafka_utils import build_producer
from pipeline.metadata_store import (
    ensure_producer_metadata,
    save_producer_metadata,
)
from pipeline.schema import extract_table_schema, load_metadata_graph
from sync_metadata_from_mysql import sync_metadata_graph


BASE_DIR = Path(__file__).resolve().parent
UI_DIR = BASE_DIR / "ui"

VALID_DATATYPES = {
    "varchar": "VARCHAR(255)",
    "text": "TEXT",
    "int": "INT",
    "integer": "INT",
    "bigint": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "decimal": "DECIMAL(10,2)",
}

_TABLE_NAME_RE = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]*$')
_COL_NAME_RE   = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]*$')


def _resolve_table_name(query_string=""):
    """Parse ?table=xxx; default to first managed table."""
    managed = get_managed_tables()
    if query_string:
        for part in query_string.lstrip("?").split("&"):
            if part.startswith("table="):
                candidate = part[len("table="):]
                if candidate in managed:
                    return candidate
    return managed[0] if managed else "stock_events"


def _meta_path_for_table(table_name):
    per_table = BASE_DIR / f"producer_metadata_{table_name}.json"
    if per_table.exists():
        return per_table
    legacy = BASE_DIR / "producer_metadata.json"
    return legacy if legacy.exists() else per_table


def load_graph():
    return load_metadata_graph(BASE_DIR / "metadata_graph.json")


def load_ui_state(table_name):
    graph = load_graph()
    meta_path = _meta_path_for_table(table_name)
    producer_metadata = ensure_producer_metadata(
        graph, path=str(meta_path), table_name=table_name
    )
    live_schema = extract_table_schema(graph, table_name)
    next_source_names = producer_metadata.get(
        "next_source_names",
        producer_metadata.get("last_applied_names", {}),
    )
    source_columns = sorted(producer_metadata.get("source_schema", {}).keys())

    return {
        "table": table_name,
        "managed_tables": get_managed_tables(),
        "columns": [
            {
                "name": name,
                "datatype": datatype,
                "source_name": next_source_names.get(name, name),
            }
            for name, datatype in live_schema.items()
        ],
        "source_columns": source_columns,
        "pending_additions": producer_metadata.get("pending_additions", {}),
        "last_updated": producer_metadata.get("last_updated", ""),
    }


def build_producer_metadata_log(previous_names, new_names, table_name):
    changes = []
    all_fields = sorted(set(previous_names) | set(new_names))
    for field in all_fields:
        old_name = previous_names.get(field, field)
        new_name = new_names.get(field, field)
        if old_name == new_name:
            continue
        changes.append(
            {
                "table_column": field,
                "previous_name": old_name,
                "current_name": new_name,
            }
        )
    return {
        "event_type": "PRODUCER_METADATA_UPDATED",
        "source": "drift_ui",
        "table": table_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "changes": changes,
        "producer_metadata": {"next_source_names": new_names},
    }


def build_addition_log(additions, table_name):
    return {
        "event_type": "PRODUCER_METADATA_UPDATED",
        "source": "drift_ui",
        "table": table_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "changes": [
            {
                "table_column": name,
                "previous_name": None,
                "current_name": name,
                "change_type": "add_column",
                "datatype": datatype,
            }
            for name, datatype in additions.items()
        ],
        "producer_metadata": {"pending_additions": additions},
    }


def publish_log_event(event):
    producer = build_producer()
    try:
        producer.send(KAFKA_SETTINGS["topics"]["logs"], value=event)
        producer.flush()
    finally:
        producer.close()


def generate_ui_sample_data(table_name):
    meta_path = _meta_path_for_table(table_name)
    producer_metadata = ensure_producer_metadata(
        load_graph(), path=str(meta_path), table_name=table_name
    )
    source_to_graph = producer_metadata.get("source_to_graph", {})
    schema = {
        graph_field: producer_metadata.get("source_schema", {}).get(source_name)
        for source_name, graph_field in source_to_graph.items()
    }
    current_names = {
        graph_field: source_name
        for source_name, graph_field in source_to_graph.items()
    }
    generator = StockEventGenerator()
    simulator = ManualRenameDriftSimulator(
        graph_schema=schema,
        table_name=table_name,
        current_names=current_names,
        target_names=current_names,
        pending_additions=producer_metadata.get("pending_additions", {}),
    )
    simulator.sync_changes()
    graph_event = generator.generate_event()
    source_event = simulator.apply_to_event(graph_event)
    return {
        "event_type": "UI_DATA_PREVIEW",
        "source": "drift_ui",
        "table": table_name,
        "graph_event": graph_event,
        "source_event": source_event,
    }


def handle_create_table(payload):
    """Validate input, create the MySQL table, sync graph, register as managed."""
    table_name = (payload.get("table_name") or "").strip()
    columns = payload.get("columns") or []

    if not _TABLE_NAME_RE.match(table_name):
        return {"error": "Table name must start with a letter and contain only letters, numbers, or underscores."}, HTTPStatus.BAD_REQUEST

    if table_name in get_managed_tables():
        return {"error": f"Table '{table_name}' is already managed."}, HTTPStatus.CONFLICT

    if not columns:
        return {"error": "At least one column is required."}, HTTPStatus.BAD_REQUEST

    col_defs = []
    schema_def = {}
    seen_names = set()
    for col in columns:
        name = (col.get("name") or "").strip()
        dtype = (col.get("datatype") or "varchar").lower().strip()
        if not _COL_NAME_RE.match(name):
            return {"error": f"Invalid column name: '{name}'"}, HTTPStatus.BAD_REQUEST
        if name in seen_names:
            return {"error": f"Duplicate column name: '{name}'"}, HTTPStatus.BAD_REQUEST
        seen_names.add(name)
        if dtype not in VALID_DATATYPES:
            dtype = "varchar"
        col_defs.append(f"  `{name}` {VALID_DATATYPES[dtype]}")
        schema_def[name] = dtype

    ddl = (
        f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n"
        + ",\n".join(col_defs)
        + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    )

    try:
        execute_mysql_ddl(ddl)
    except Exception as exc:
        return {"error": f"MySQL error: {exc}"}, HTTPStatus.INTERNAL_SERVER_ERROR

    # Rebuild graph from live MySQL so the new table appears
    graph = sync_metadata_graph()

    # Bootstrap producer metadata for the new table
    meta_path = BASE_DIR / f"producer_metadata_{table_name}.json"
    ensure_producer_metadata(graph, path=str(meta_path), table_name=table_name)

    # Register in managed_tables.json
    managed = get_managed_tables()
    if table_name not in managed:
        managed.append(table_name)
        save_managed_tables(managed)

    return {
        "table": table_name,
        "columns": schema_def,
        "ddl": ddl,
        "managed_tables": get_managed_tables(),
    }, HTTPStatus.OK


class DriftUIHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload, status=HTTPStatus.OK):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, file_path):
        content = file_path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _read_json_body(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8")), None
        except json.JSONDecodeError:
            return None, "Invalid JSON payload."

    def log_message(self, fmt, *args):  # suppress default access log noise
        pass

    def do_GET(self):
        parsed = urlparse(self.path)
        table_name = _resolve_table_name(parsed.query)

        if parsed.path in {"/", "/index.html"}:
            self._send_html(UI_DIR / "drift_manager.html")
            return

        if parsed.path == "/api/tables":
            self._send_json({"managed_tables": get_managed_tables()})
            return

        if parsed.path == "/api/schema":
            self._send_json(load_ui_state(table_name))
            return

        if parsed.path == "/api/producer-metadata":
            meta_path = _meta_path_for_table(table_name)
            self._send_json(
                ensure_producer_metadata(load_graph(), path=str(meta_path), table_name=table_name)
            )
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self):
        parsed = urlparse(self.path)
        table_name = _resolve_table_name(parsed.query)

        # ── create new table ──────────────────────────────────────────────────
        if parsed.path == "/api/create-table":
            payload, err = self._read_json_body()
            if err:
                self._send_json({"error": err}, HTTPStatus.BAD_REQUEST)
                return
            result, status = handle_create_table(payload)
            self._send_json(result, status)
            return

        # ── generate sample data ──────────────────────────────────────────────
        if parsed.path == "/api/generate-data":
            self._send_json(generate_ui_sample_data(table_name))
            return

        if parsed.path != "/api/producer-metadata":
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        # ── update producer metadata (renames + additions) ────────────────────
        payload, err = self._read_json_body()
        if err:
            self._send_json({"error": err}, HTTPStatus.BAD_REQUEST)
            return

        graph = load_graph()
        meta_path = _meta_path_for_table(table_name)
        producer_metadata = ensure_producer_metadata(
            graph, path=str(meta_path), table_name=table_name
        )
        schema = extract_table_schema(graph, table_name)
        previous_names = dict(
            producer_metadata.get(
                "next_source_names",
                producer_metadata.get("last_applied_names", {}),
            )
        )
        renames   = payload.get("renames", {})
        additions = payload.get("additions", {})

        if not isinstance(renames, dict):
            self._send_json({"error": "`renames` must be an object."}, HTTPStatus.BAD_REQUEST)
            return
        if not isinstance(additions, dict):
            self._send_json({"error": "`additions` must be an object."}, HTTPStatus.BAD_REQUEST)
            return

        sanitized = {}
        for table_column, drifted_name in renames.items():
            if table_column not in schema:
                continue
            if not isinstance(drifted_name, str):
                continue
            drifted_name = drifted_name.strip()
            if not drifted_name or drifted_name == table_column:
                continue
            sanitized[table_column] = drifted_name

        new_names = {
            table_column: sanitized.get(
                table_column,
                previous_names.get(table_column, table_column),
            )
            for table_column in schema
        }
        sanitized_additions = {}
        for column_name, datatype in additions.items():
            if not isinstance(column_name, str) or not isinstance(datatype, str):
                continue
            column_name = column_name.strip()
            datatype = datatype.strip().lower()
            if not column_name or column_name in schema:
                continue
            if datatype not in VALID_DATATYPES:
                datatype = "varchar"
            sanitized_additions[column_name] = datatype

        producer_metadata["next_source_names"] = dict(new_names)
        producer_metadata["pending_additions"] = sanitized_additions
        save_producer_metadata(producer_metadata, str(meta_path))
        metadata_log = build_producer_metadata_log(previous_names, new_names, table_name)
        if metadata_log["changes"]:
            publish_log_event(metadata_log)
        if sanitized_additions:
            publish_log_event(build_addition_log(sanitized_additions, table_name))
        self._send_json(producer_metadata)


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 8000), DriftUIHandler)
    print("Drift UI running at http://127.0.0.1:8000")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nDrift UI stopped.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
