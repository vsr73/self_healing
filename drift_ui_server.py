import json
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from pipeline.drift import ManualRenameDriftSimulator
from pipeline.generator import StockEventGenerator
from pipeline.kafka_utils import build_producer
from pipeline.metadata_store import (
    ensure_producer_metadata,
    save_producer_metadata,
)
from pipeline.config import KAFKA_SETTINGS
from pipeline.schema import extract_table_schema, load_metadata_graph


BASE_DIR = Path(__file__).resolve().parent
UI_DIR = BASE_DIR / "ui"
TABLE_NAME = "stock_events"


def load_graph():
    return load_metadata_graph(BASE_DIR / "metadata_graph.json")


def load_ui_state():
    graph = load_graph()
    producer_metadata = ensure_producer_metadata(graph, BASE_DIR / "producer_metadata.json")
    live_schema = extract_table_schema(graph, TABLE_NAME)
    next_source_names = producer_metadata.get(
        "next_source_names",
        producer_metadata.get("last_applied_names", {}),
    )
    source_columns = sorted(producer_metadata.get("source_schema", {}).keys())

    return {
        "table": TABLE_NAME,
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


def build_producer_metadata_log(previous_names, new_names):
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
        "table": TABLE_NAME,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "changes": changes,
        "producer_metadata": {
            "next_source_names": new_names,
        },
    }


def build_addition_log(additions):
    return {
        "event_type": "PRODUCER_METADATA_UPDATED",
        "source": "drift_ui",
        "table": TABLE_NAME,
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
        "producer_metadata": {
            "pending_additions": additions,
        },
    }


def publish_log_event(event):
    producer = build_producer()
    try:
        producer.send(KAFKA_SETTINGS["topics"]["logs"], value=event)
        producer.flush()
    finally:
        producer.close()


def generate_ui_sample_data():
    producer_metadata = ensure_producer_metadata(load_graph(), BASE_DIR / "producer_metadata.json")
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
        table_name=TABLE_NAME,
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
        "table": TABLE_NAME,
        "graph_event": graph_event,
        "source_event": source_event,
    }


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

    def do_GET(self):
        if self.path in {"/", "/index.html"}:
            self._send_html(UI_DIR / "drift_manager.html")
            return

        if self.path == "/api/schema":
            self._send_json(load_ui_state())
            return

        if self.path == "/api/producer-metadata":
            self._send_json(
                ensure_producer_metadata(load_graph(), BASE_DIR / "producer_metadata.json")
            )
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self):
        if self.path == "/api/generate-data":
            self._send_json(generate_ui_sample_data())
            return

        if self.path != "/api/producer-metadata":
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length)

        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError:
            self._send_json({"error": "Invalid JSON payload."}, status=HTTPStatus.BAD_REQUEST)
            return

        graph = load_graph()
        producer_metadata = ensure_producer_metadata(graph, BASE_DIR / "producer_metadata.json")
        schema = extract_table_schema(graph, TABLE_NAME)
        previous_names = dict(
            producer_metadata.get(
                "next_source_names",
                producer_metadata.get("last_applied_names", {}),
            )
        )
        renames = payload.get("renames", {})
        additions = payload.get("additions", {})

        if not isinstance(renames, dict):
            self._send_json(
                {"error": "`renames` must be an object."},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        if not isinstance(additions, dict):
            self._send_json(
                {"error": "`additions` must be an object."},
                status=HTTPStatus.BAD_REQUEST,
            )
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
            if datatype not in {"varchar", "text", "int", "integer", "bigint", "float", "double", "decimal"}:
                datatype = "varchar"
            sanitized_additions[column_name] = datatype

        producer_metadata["next_source_names"] = dict(new_names)
        producer_metadata["pending_additions"] = sanitized_additions
        save_producer_metadata(producer_metadata, BASE_DIR / "producer_metadata.json")
        metadata_log = build_producer_metadata_log(
            previous_names,
            new_names,
        )
        if metadata_log["changes"]:
            publish_log_event(metadata_log)
        if sanitized_additions:
            publish_log_event(build_addition_log(sanitized_additions))
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
