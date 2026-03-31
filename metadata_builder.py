# ============================================================
# metadata_builder.py
# Builds metadata_graph.json from all configured DB sources
# ============================================================

import json
import sys
import os
from datetime import datetime

# Add pipeline/ to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "pipeline"))

import pymysql
try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

from config import DATA_SOURCES


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def adapt_timestamp(ts=None):
    if ts is None:
        ts = datetime.now()
    return ts.strftime("%Y-%m-%d %H:%M:%S")


graph = {
    "last_updated": adapt_timestamp(),
    "sources": [],
    "nodes": []
}

node_map = {}


def create_node(node_id, node_type, **attrs):
    if node_id in node_map:
        return
    node = {
        "id": node_id,
        "type": node_type,
        "edges": []
    }
    node.update(attrs)
    graph["nodes"].append(node)
    node_map[node_id] = node


def connect(source):
    cfg = source["connection"]

    if source["type"] == "postgres":
        if not HAS_PSYCOPG2:
            raise ImportError("Install psycopg2-binary")

        return psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            dbname=cfg["database"],
            user=cfg["user"],
            password=cfg["password"]
        )

    elif source["type"] == "mysql":
        return pymysql.connect(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            user=cfg["user"],
            password=cfg["password"]
        )

    else:
        raise ValueError(f"Unknown source type: {source['type']}")


# ─────────────────────────────────────────────────────────────
# Main Extraction Loop
# ─────────────────────────────────────────────────────────────

for source_name, source in DATA_SOURCES.items():

    print(f"\n🔍 Extracting metadata from: {source_name}")

    try:
        conn = connect(source)
        cursor = conn.cursor()
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        continue

    db = source["connection"]["database"]

    graph["sources"].append({
        "name": source_name,
        "type": source["type"],
        "database": db
    })

    # Create DB node
    db_node = f"{source_name}.{db}"
    create_node(db_node, "database", database=db)

    # ── Fetch Tables ─────────────────────────
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
    """)

    tables = cursor.fetchall()
    print(f"  Found {len(tables)} table(s)")

    for (table,) in tables:
        table_node = f"{source_name}.{table}"

        create_node(table_node, "table", database=db)

        node_map[db_node]["edges"].append({
            "relation": "HAS_TABLE",
            "target": table_node
        })

    # ── Fetch Columns ────────────────────────
    cursor.execute("""
        SELECT table_name, column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
    """)

    columns = cursor.fetchall()
    print(f"  Found {len(columns)} column(s)")

    for table, col, dtype, nullable in columns:

        table_node = f"{source_name}.{table}"
        col_node = f"{table_node}.{col}"

        create_node(
            col_node,
            "column",
            datatype=dtype,
            nullable=nullable
        )

        if table_node in node_map:
            node_map[table_node]["edges"].append({
                "relation": "HAS_COLUMN",
                "target": col_node
            })

    cursor.close()
    conn.close()

    print(f"✅ Done extracting from {source_name}")


# ─────────────────────────────────────────────────────────────
# Save Graph
# ─────────────────────────────────────────────────────────────

output_path = os.path.join(os.path.dirname(__file__), "metadata_graph.json")

with open(output_path, "w") as f:
    json.dump(graph, f, indent=4)

print("\n📦 metadata_graph.json created!")