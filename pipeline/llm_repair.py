import json

from pipeline.config import LLM_SETTINGS
from pipeline.schema import extract_table_schema


def build_consumer_prompt(change_event, graph):
    table_name = change_event["table"]
    return f"""
You are a database schema change extractor and repair planner.
The consumer received a Kafka schema drift log for a MySQL source table.
You must produce both:
1. a structured schema repair DAG
2. executable MySQL DDL statements

Rules:
- Return ONLY valid JSON.
- Use the Kafka schema change log as the source of truth.
- The `dag` must be a JSON array whose elements are one of:
  {{
    "operation": "RENAME_COLUMN",
    "table": "stock_events",
    "previous_name": "price",
    "current_name": "renamed_price",
    "graph_field": "price"
  }}
  {{
    "operation": "ADD_COLUMN",
    "table": "stock_events",
    "column": "new_col",
    "datatype": "varchar"
  }}
  {{
    "operation": "DROP_COLUMN",
    "table": "stock_events",
    "column": "old_col",
    "graph_field": "old_col"
  }}
  {{
    "operation": "CREATE_TABLE",
    "table": "new_table"
  }}
  {{
    "operation": "DROP_TABLE",
    "table": "old_table"
  }}
  {{
    "operation": "RENAME_TABLE",
    "old_table": "a",
    "new_table": "b"
  }}
- `ddl_statements` must be a JSON array of MySQL DDL strings.
- `alias_mappings` must map current source names to stable graph field names where applicable.
- `summary` must be short and practical.

Return a JSON object with exactly these top-level keys:
{{
  "table": "stock_events",
  "graph_field": "price",
  "previous_name": "price",
  "current_name": "renamed_price",
  "datatype": "float",
  "alias_mappings": {{"renamed_price": "price"}},
  "dag": [...],
  "ddl_statements": ["ALTER TABLE stock_events RENAME COLUMN price TO renamed_price;"],
  "summary": "..."
}}

Metadata graph schema for the table:
{json.dumps(extract_table_schema(graph, table_name), indent=2)}

Kafka schema change log received by the consumer:
{json.dumps(change_event, indent=2)}
""".strip()


def _extract_json(text):
    text = text.strip()
    if text.startswith("```"):
        parts = text.split("```")
        for chunk in parts:
            chunk = chunk.strip()
            if chunk.startswith("{") and chunk.endswith("}"):
                return chunk
            if "\n" in chunk:
                maybe_json = chunk.split("\n", 1)[1].strip()
                if maybe_json.startswith("{") and maybe_json.endswith("}"):
                    return maybe_json
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return text[start : end + 1]
    raise ValueError("No JSON object found in LLM response.")


def _gemini_consumer_repair(change_event, graph):
    if not LLM_SETTINGS["api_key"]:
        raise RuntimeError("Gemini API key is not configured.")

    try:
        from google import genai
    except ImportError as exc:
        raise RuntimeError("google-genai is not installed.") from exc

    prompt = build_consumer_prompt(change_event, graph)

    client = genai.Client(api_key=LLM_SETTINGS["api_key"])
    response = client.models.generate_content(
        model=LLM_SETTINGS["model"],
        contents=prompt,
    )
    raw_text = response.text or ""
    return json.loads(_extract_json(raw_text))


def generate_consumer_repair(change_event, graph):
    result = _gemini_consumer_repair(change_event, graph)
    result["_debug_prompt"] = build_consumer_prompt(change_event, graph)
    return result
