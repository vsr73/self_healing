
# ============================================================
# test.py  —  Quick smoke test for the Gemini LLM connection
# Run: cd pipeline && python test.py
# ============================================================

import json
import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from google import genai
import config

client = genai.Client(api_key=config.GEMINI_API_KEY)

# ── Test 1: Schema DAG extraction ────────────────────────────
print("\n===== TEST 1: Schema DAG extraction =====\n")

logs = [
    "ALTER TABLE customers RENAME COLUMN full_name TO naam",
    "ALTER TABLE orders ADD COLUMN discount FLOAT"
]

logs_text = "\n".join(logs)

prompt = f"""You are a database metadata engine.

Extract schema operations from the following SQL logs.

Logs:
{logs_text}

Return ONLY JSON like this:

[
  {{
    "operation": "RENAME_COLUMN",
    "table": "customers",
    "old_column": "full_name",
    "new_column": "naam"
  }},
  {{
    "operation": "ADD_COLUMN",
    "table": "orders",
    "column": "discount",
    "datatype": "float"
  }}
]"""

print("Prompt sent to Gemini:")
print(prompt)

response = client.models.generate_content(
    model="gemini-2.0-flash",       # ✅ fixed model name
    contents=prompt
)

print("\nRaw LLM response:")
print(response.text)

txt   = response.text.replace("```json", "").replace("```", "").strip()
match = re.search(r"\[.*\]", txt, re.DOTALL)

if match:
    try:
        data = json.loads(match.group())
        print("\n✅ Parsed JSON successfully:")
        print(json.dumps(data, indent=2))
    except Exception as e:
        print("\n❌ JSON parse failed:", e)
else:
    print("\n❌ No JSON array found in response")


# ── Test 2: SQL generation ────────────────────────────────────
print("\n===== TEST 2: SQL generation =====\n")

dummy_metadata = {
    "nodes": [
        {
            "id": "mysql_local.customers",
            "type": "table",
            "edges": [
                {"relation": "HAS_COLUMN", "target": "mysql_local.customers.customer_id"},
                {"relation": "HAS_COLUMN", "target": "mysql_local.customers.naam"},
                {"relation": "HAS_COLUMN", "target": "mysql_local.customers.email"}
            ]
        },
        {"id": "mysql_local.customers.customer_id", "type": "column", "datatype": "int"},
        {"id": "mysql_local.customers.naam",        "type": "column", "datatype": "varchar"},
        {"id": "mysql_local.customers.email",       "type": "column", "datatype": "varchar"}
    ]
}

from llm_sql_generator import generate_sql
sql = generate_sql("Show me all customers", dummy_metadata, "mysql_local")
print("Generated SQL:", sql)
print("\n✅ All tests complete")
