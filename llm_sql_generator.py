
# ============================================================
# llm_sql_generator.py
# Converts a natural language user query → SQL using Gemini
# ============================================================

from google import genai
import config
import json
import re


def generate_sql(user_query, metadata_graph, source_name):
    """
    Args:
        user_query    : plain English question e.g. "Top 5 customers by spending"
        metadata_graph: the loaded metadata_graph.json dict
        source_name   : key in config.DATA_SOURCES e.g. "mysql_local"

    Returns:
        A clean SQL string ready to execute
    """

    client = genai.Client(api_key=config.GEMINI_API_KEY)

    # Build a compact schema summary from the metadata graph
    schema_summary = _build_schema_summary(metadata_graph, source_name)

    prompt = f"""You are an expert SQL generation engine for MySQL.

Target database source: {source_name}

Database schema (extracted from metadata graph):
{schema_summary}

User question: {user_query}

Rules:
- Return ONLY valid MySQL SQL — no explanation, no markdown, no backtick fences
- Use table and column names EXACTLY as shown in the schema
- Use backticks around table/column names
- Limit results to 100 rows unless the user specifies otherwise

SQL:"""

    response = client.models.generate_content(
        model="gemini-2.5-flash",          # ✅ fixed: gemini-3-flash-preview doesn't exist
        contents=prompt
    )

    sql = response.text.strip()

    # Strip any accidental markdown fences
    sql = re.sub(r"```sql", "", sql, flags=re.IGNORECASE)
    sql = sql.replace("```", "").strip()

    return sql


def _build_schema_summary(metadata_graph, source_name):
    """
    Converts the node graph into a readable schema text like:
        TABLE customers: customer_id (int), name (varchar), email (varchar)
        TABLE orders: order_id (int), customer_id (int), amount (float)
    """
    nodes = metadata_graph.get("nodes", [])
    node_map = {n["id"]: n for n in nodes}

    tables = [n for n in nodes if n["type"] == "table"
              and n["id"].startswith(source_name)]

    lines = []
    for table_node in tables:
        table_name = table_node["id"].split(".")[-1]
        col_edges = [e for e in table_node.get("edges", [])
                     if e["relation"] == "HAS_COLUMN"]
        cols = []
        for edge in col_edges:
            col_node = node_map.get(edge["target"])
            if col_node:
                col_name = col_node["id"].split(".")[-1]
                dtype    = col_node.get("datatype", "unknown")
                cols.append(f"{col_name} ({dtype})")
        lines.append(f"TABLE `{table_name}`: {', '.join(cols)}")

    return "\n".join(lines) if lines else json.dumps(metadata_graph, indent=2)
