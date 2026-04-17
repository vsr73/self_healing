import json

from pipeline.config import LLM_SETTINGS
from pipeline.schema import extract_table_schema


def _get_operations(change_event):
    """Return the list of operations from either the batched or legacy format."""
    ops = change_event.get("operations")
    if ops:
        return ops
    single = change_event.get("operation")
    return [single] if single else []


def build_consumer_prompt(change_event, graph):
    table_name = change_event["table"]
    operations = _get_operations(change_event)
    schema_json = json.dumps(extract_table_schema(graph, table_name), indent=2)
    ops_json = json.dumps(operations, indent=2)

    return f"""
You are a database schema repair planner for a self-healing data pipeline.
The consumer received a Kafka schema drift log for MySQL table `{table_name}`.
There may be ONE or MULTIPLE operations — you must handle ALL of them in a single response.

RULES:
- Return ONLY valid JSON. No markdown fences, no explanation, no extra keys.
- Your `dag` array must contain one entry for EVERY operation listed below.
- Your `ddl_statements` array must contain one executable MySQL DDL string per operation.
- `alias_mappings` maps every current source name to its stable graph field name.
- `summary` describes all changes in one sentence.

Allowed `dag` element shapes:
  {{"operation":"RENAME_COLUMN","table":"{table_name}","previous_name":"old","current_name":"new","graph_field":"old"}}
  {{"operation":"ADD_COLUMN","table":"{table_name}","column":"col","datatype":"varchar"}}
  {{"operation":"DROP_COLUMN","table":"{table_name}","column":"col","graph_field":"col"}}
  {{"operation":"CREATE_TABLE","table":"tbl"}}
  {{"operation":"DROP_TABLE","table":"tbl"}}

Return exactly this JSON structure (all keys required, arrays may have multiple elements):
{{
  "table": "{table_name}",
  "alias_mappings": {{}},
  "dag": [],
  "ddl_statements": [],
  "summary": ""
}}

Current metadata graph schema for `{table_name}`:
{schema_json}

Operations to apply — handle ALL of them:
{ops_json}

Full Kafka change event:
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
