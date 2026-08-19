
# ============================================================
# main.py  —  Self-Healing Pipeline Entry Point
# ============================================================
# Run from inside the pipeline/ folder:
#   cd pipeline && python main.py
# ============================================================

import json
import sys
import os
import pymysql
from datetime import datetime

# Make sure imports work regardless of cwd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from llm_sql_generator import generate_sql

METADATA_FILE = os.path.join(os.path.dirname(__file__), "..", "metadata_graph.json")
LOG_FILE      = os.path.join(os.path.dirname(__file__), "pipeline_logs.json")
KB_FILE       = os.path.join(os.path.dirname(__file__), "..", "knowledge_base.json")


# ── Monitor / Logger ─────────────────────────────────────────

def monitor_event(event_type, payload):
    log = {
        "timestamp": datetime.utcnow().isoformat(),
        "event":     event_type,
        "payload":   str(payload)
    }
    print(f"\n[{event_type}] {payload}")
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(log) + "\n")


# ── Analyzer Agent ────────────────────────────────────────────

def analyze_error(error):
    err = str(error).lower()
    if "unknown column" in err:
        return "SCHEMA_DRIFT_COLUMN"
    elif "doesn't exist" in err or "table" in err:
        return "SCHEMA_DRIFT_TABLE"
    elif "syntax" in err:
        return "SQL_ERROR"
    elif "access denied" in err or "connect" in err:
        return "CONNECTION_ERROR"
    else:
        return "UNKNOWN"


# ── Planner Agent ─────────────────────────────────────────────

def plan_action(issue):
    plans = {
        "SCHEMA_DRIFT_COLUMN": ["REPAIR_METADATA", "REGENERATE_SQL"],
        "SCHEMA_DRIFT_TABLE":  ["REPAIR_METADATA", "REGENERATE_SQL"],
        "SQL_ERROR":           ["REGENERATE_SQL"],
        "CONNECTION_ERROR":    ["ESCALATE"],
        "UNKNOWN":             ["ESCALATE"],
    }
    return plans.get(issue, ["ESCALATE"])


# ── Knowledge Base ────────────────────────────────────────────

def update_knowledge_base(issue, plan):
    try:
        with open(KB_FILE) as f:
            kb = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        kb = []

    kb.append({
        "issue":     issue,
        "solution":  plan,
        "timestamp": datetime.utcnow().isoformat()
    })

    with open(KB_FILE, "w") as f:
        json.dump(kb, f, indent=2)
    print("[KB] Knowledge base updated")


# ── Metadata loader ───────────────────────────────────────────

def load_metadata():
    with open(METADATA_FILE) as f:
        return json.load(f)


# ── Query Executor ────────────────────────────────────────────

def run_query(sql):
    source = config.DATA_SOURCES["mysql_local"]
    conn   = pymysql.connect(**source["connection"])
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows
    finally:
        cursor.close()
        conn.close()


# ── Schema Repair (calls schema_repair_engine as a module) ────

def run_schema_repair():
    monitor_event("ACTION", "Running schema repair engine")
    # Import and call directly — avoids subprocess path issues
    import schema_repair_engine as sre
    metadata    = sre.load_metadata()
    last_update = sre.adapt_timestamp(metadata["last_updated"])
    source      = config.DATA_SOURCES["mysql_local"]

    try:
        logs = sre.fetch_mysql_logs(source, last_update)
    except Exception as e:
        print(f"  ⚠️  Could not read MySQL general_log: {e}")
        print("  ℹ️  Enable general log: SET GLOBAL general_log = 'ON';")
        return

    if not logs:
        print("  ✅ No schema drift found in logs")
        sre.save_metadata(metadata)
        return

    print(f"  🔍 Found DDL statements: {len(logs)}")
    dag = sre.generate_metadata_dag(logs)
    print(f"  🔧 DAG: {dag}")
    metadata, _ = sre.apply_dag(metadata, dag)
    sre.save_metadata(metadata)


# ── Pretty print rows ─────────────────────────────────────────

def print_results(rows, sql):
    print("\n" + "="*60)
    print("RESULTS")
    print("="*60)
    if not rows:
        print("(no rows returned)")
    else:
        for i, row in enumerate(rows, 1):
            print(f"  {i:>3}. {row}")
    print(f"\nTotal: {len(rows)} row(s)")


# ── MAIN PIPELINE ─────────────────────────────────────────────

if __name__ == "__main__":

    print("\n" + "="*60)
    print("  🚀 SELF-HEALING DATA PIPELINE")
    print("="*60)

    monitor_event("START", "Pipeline started")

    metadata    = load_metadata()
    source_name = "mysql_local"

    user_query = input("\n💬 Enter your query (natural language): ").strip()
    if not user_query:
        print("No query entered. Exiting.")
        sys.exit(0)

    monitor_event("USER_QUERY", user_query)

    sql  = None
    rows = []

    # ── ATTEMPT 1: Generate SQL and run ──────────────────────
    try:
        print("\n[SQL GENERATOR] Generating SQL from LLM...")
        sql = generate_sql(user_query, metadata, source_name)
        print(f"\n[SQL]\n{sql}\n")

        rows = run_query(sql)
        monitor_event("SUCCESS", "Query executed successfully")
        print_results(rows, sql)

    except Exception as e:
        monitor_event("FAILURE", str(e))
        print(f"\n❌ Error: {e}")

        # ── ANALYZE ──────────────────────────────────────────
        issue = analyze_error(e)
        print(f"\n[ANALYZE] Detected issue: {issue}")

        # ── PLAN ─────────────────────────────────────────────
        plans = plan_action(issue)
        print(f"[PLAN] Actions to try: {plans}")

        recovered = False

        for plan in plans:
            print(f"\n[EXECUTE] Running: {plan}")

            try:
                if plan == "REPAIR_METADATA":
                    run_schema_repair()
                    metadata = load_metadata()      # reload fresh graph

                elif plan == "REGENERATE_SQL":
                    print("[SQL GENERATOR] Re-generating SQL with updated metadata...")
                    sql = generate_sql(user_query, metadata, source_name)
                    print(f"\n[SQL]\n{sql}\n")

                elif plan == "ESCALATE":
                    print("⚠️  [ESCALATE] Manual intervention required.")
                    print("   Check your database connection and schema.")
                    break

                # Retry execution after each repair step
                if sql:
                    print("[RETRY] Re-executing SQL...")
                    rows = run_query(sql)
                    monitor_event("RECOVERY_SUCCESS", f"Recovered using: {plan}")
                    update_knowledge_base(issue, plan)
                    print_results(rows, sql)
                    recovered = True
                    break

            except Exception as retry_error:
                monitor_event("RETRY_FAILED", str(retry_error))
                print(f"  ❌ Retry failed: {retry_error}")

        if not recovered:
            print("\n❌ Pipeline could not auto-recover. Check logs.")

    print("\n" + "="*60)
    print("  ✅ Pipeline run complete")
    print("="*60)
