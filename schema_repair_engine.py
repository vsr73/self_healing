
# ============================================================
# schema_repair_engine.py
# Detects schema drift from MySQL logs → builds repair DAG
# → applies changes to metadata_graph.json
# ============================================================

from google import genai
import config
import json
import pymysql
import re
from datetime import datetime
import os

# Resolve metadata path relative to THIS file's location
_HERE = os.path.dirname(os.path.abspath(__file__))
METADATA_FILE = os.path.join(_HERE, "..", "metadata_graph.json")


# ── Timestamp helpers ────────────────────────────────────────

def adapt_timestamp(ts):
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)
    return ts.strftime("%Y-%m-%d %H:%M:%S")


# ── Metadata I/O ─────────────────────────────────────────────

def load_metadata():
    with open(METADATA_FILE) as f:
        return json.load(f)


def save_metadata(graph):
    graph["last_updated"] = adapt_timestamp(datetime.now())
    with open(METADATA_FILE, "w") as f:
        json.dump(graph, f, indent=4)
    print("✅ Metadata graph saved")


# ── SQL log normalisation ────────────────────────────────────

def normalize_logs(raw_logs):
    cleaned = []
    for log in raw_logs:
        if isinstance(log, bytes):
            log = log.decode()
        log = log.strip()
        if not log:
            continue
        if log.upper().startswith(("SELECT", "SET", "SHOW", "USE", "COMMIT")):
            continue
        parts = [p.strip() for p in log.split(";") if p.strip()]
        cleaned.extend(parts)

    cleaned = list(dict.fromkeys(cleaned))
    ddl = ("ALTER", "CREATE", "DROP")
    return [l for l in cleaned if l.upper().startswith(ddl)]


# ── Fetch DDL logs from MySQL general_log ────────────────────

def fetch_mysql_logs(source, last_update):
    conn = pymysql.connect(**source["connection"])
    cursor = conn.cursor()
    log_cfg = source["logs"]
    query = f"""
    SELECT {log_cfg['query_column']}
    FROM {log_cfg['table']}
    WHERE {log_cfg['time_column']} > %s
    ORDER BY {log_cfg['time_column']} ASC
    """
    cursor.execute(query, (last_update,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return normalize_logs([r[0] for r in rows])


# ── Gemini: SQL logs → structured DAG ───────────────────────

def generate_metadata_dag(schema_logs):
    client = genai.Client(api_key=config.GEMINI_API_KEY)
    logs = "\n".join(schema_logs)

    prompt = f"""You are a database schema change extractor.

Given the following SQL DDL statements, extract every schema operation.

DDL logs:
{logs}

Return ONLY a valid JSON array. Each element is one of:
[
  {{"operation":"RENAME_COLUMN","table":"customers","old_column":"a","new_column":"b"}},
  {{"operation":"ADD_COLUMN","table":"customers","column":"c","datatype":"varchar"}},
  {{"operation":"DROP_COLUMN","table":"customers","column":"c"}},
  {{"operation":"CREATE_TABLE","table":"table_name"}},
  {{"operation":"DROP_TABLE","table":"table_name"}},
  {{"operation":"RENAME_TABLE","old_table":"a","new_table":"b"}}
]

Return ONLY the JSON array — no explanation, no markdown fences."""

    response = client.models.generate_content(
        model="gemini-2.5-flash",          # ✅ fixed model name
        contents=prompt
    )

    txt = response.text.replace("```json", "").replace("```", "").strip()
    match = re.search(r"\[.*\]", txt, re.DOTALL)
    if not match:
        print("⚠️  Gemini returned no JSON:", txt)
        return []
    return json.loads(match.group())


# ── Apply DAG to metadata graph ──────────────────────────────

def apply_dag(metadata_graph, dag):
    nodes    = metadata_graph["nodes"]
    node_map = {n["id"]: n for n in nodes}
    source   = "mysql_local"
    updated  = False

    def rename_column(table, old, new):
        old_id = f"{source}.{table}.{old}"
        new_id = f"{source}.{table}.{new}"
        if old_id not in node_map:
            print(f"  ⚠️  Column not found in graph: {old_id}")
            return
        node = node_map[old_id]
        node["id"] = new_id
        node_map[new_id] = node
        del node_map[old_id]
        for n in nodes:
            for edge in n.get("edges", []):
                if edge["target"] == old_id:
                    edge["target"] = new_id
        print(f"  ✅ Renamed column: {old} → {new}")

    def add_column(table, column, datatype="unknown"):
        table_id  = f"{source}.{table}"
        column_id = f"{table_id}.{column}"
        if column_id in node_map:
            return
        node = {"id": column_id, "type": "column", "datatype": datatype, "edges": []}
        nodes.append(node)
        node_map[column_id] = node
        if table_id in node_map:
            node_map[table_id]["edges"].append(
                {"relation": "HAS_COLUMN", "target": column_id}
            )
        print(f"  ✅ Added column: {column}")

    def drop_column(table, column):
        column_id = f"{source}.{table}.{column}"
        if column_id not in node_map:
            return
        nodes.remove(node_map[column_id])
        del node_map[column_id]
        for n in nodes:
            n["edges"] = [e for e in n.get("edges", []) if e["target"] != column_id]
        print(f"  ✅ Dropped column: {column}")

    def create_table(table):
        table_id = f"{source}.{table}"
        if table_id in node_map:
            return
        node = {"id": table_id, "type": "table", "edges": []}
        nodes.append(node)
        node_map[table_id] = node
        print(f"  ✅ Created table: {table}")

    def drop_table(table):
        table_id = f"{source}.{table}"
        for node in list(nodes):
            if node["id"].startswith(table_id):
                nodes.remove(node)
        print(f"  ✅ Dropped table: {table}")

    for step in dag:
        op     = step.get("operation")
        column = step.get("column") or step.get("column_name")

        if op == "ADD_COLUMN":
            add_column(step["table"], column, step.get("datatype", "unknown"))
        elif op == "DROP_COLUMN":
            drop_column(step["table"], column)
        elif op == "RENAME_COLUMN":
            rename_column(step["table"], step["old_column"], step["new_column"])
        elif op == "CREATE_TABLE":
            create_table(step["table"])
        elif op == "DROP_TABLE":
            drop_table(step["table"])
        elif op == "RENAME_TABLE":
            print(f"  ℹ️  RENAME_TABLE not yet implemented: {step}")
        updated = True

    return metadata_graph, updated


# ── Entry point ──────────────────────────────────────────────

if __name__ == "__main__":
    metadata    = load_metadata()
    last_update = adapt_timestamp(metadata["last_updated"])
    print("📋 Metadata last updated:", last_update)

    source = config.DATA_SOURCES["mysql_local"]
    logs   = fetch_mysql_logs(source, last_update)

    if not logs:
        print("✅ No schema drift detected")
        save_metadata(metadata)     # refresh the timestamp
        exit()

    print(f"\n🔍 Found {len(logs)} DDL statement(s):")
    for l in logs:
        print(" -", l)

    dag = generate_metadata_dag(logs)
    print("\n🔧 Generated DAG:", json.dumps(dag, indent=2))

    metadata, updated = apply_dag(metadata, dag)
    save_metadata(metadata)
    print("\n✅ Schema repair completed")
