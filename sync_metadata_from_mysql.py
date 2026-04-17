import json
from datetime import datetime, timezone
from pathlib import Path

from pipeline.config import DATA_SOURCES
from pipeline.db import connect_mysql
from pipeline.metadata_store import get_consumer_state, load_graph, save_graph


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_PATH = BASE_DIR / "metadata_graph.json"


def _build_live_graph():
    graph = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "sources": [],
        "nodes": [],
    }
    node_map = {}

    def create_node(node_id, node_type, **attrs):
        if node_id in node_map:
            return node_map[node_id]
        node = {"id": node_id, "type": node_type, "edges": []}
        node.update(attrs)
        graph["nodes"].append(node)
        node_map[node_id] = node
        return node

    for source_name, source in DATA_SOURCES.items():
        if source["type"] != "mysql":
            continue

        connection = connect_mysql(source_name)
        try:
            with connection.cursor() as cursor:
                database = source["connection"]["database"]
                graph["sources"].append(
                    {
                        "name": source_name,
                        "type": source["type"],
                        "database": database,
                    }
                )

                db_node = create_node(
                    f"{source_name}.{database}",
                    "database",
                    database=database,
                )

                cursor.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = DATABASE()
                    """
                )
                tables = [row[0] for row in cursor.fetchall()]

                for table_name in tables:
                    table_node_id = f"{source_name}.{table_name}"
                    create_node(table_node_id, "table", database=database)
                    db_node["edges"].append(
                        {"relation": "HAS_TABLE", "target": table_node_id}
                    )

                cursor.execute(
                    """
                    SELECT table_name, column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                    ORDER BY table_name, ordinal_position
                    """
                )
                for table_name, column_name, datatype, nullable in cursor.fetchall():
                    table_node_id = f"{source_name}.{table_name}"
                    column_node_id = f"{table_node_id}.{column_name}"
                    create_node(
                        column_node_id,
                        "column",
                        datatype=datatype,
                        nullable=nullable,
                    )
                    node_map[table_node_id]["edges"].append(
                        {"relation": "HAS_COLUMN", "target": column_node_id}
                    )
        finally:
            connection.close()

    return graph


def sync_metadata_graph():
    live_graph = _build_live_graph()
    try:
        previous_graph = load_graph(OUTPUT_PATH)
    except FileNotFoundError:
        save_graph(live_graph, OUTPUT_PATH)
        return live_graph

    # initialize_graph_extensions (called inside get_consumer_state) already
    # migrates any legacy flat metadata_events list to the per-table structure.
    previous_state = get_consumer_state(previous_graph)
    tables = previous_state.get("tables", {})
    if tables:
        live_graph.setdefault("consumer_state", {})
        live_graph["consumer_state"]["tables"] = tables
        live_graph["consumer_state"]["dependencies"] = previous_state.get("dependencies", [])

    save_graph(live_graph, OUTPUT_PATH)
    return live_graph


def main():
    graph = sync_metadata_graph()
    print(json.dumps({"last_updated": graph["last_updated"], "nodes": len(graph["nodes"])}, indent=2))


if __name__ == "__main__":
    main()
