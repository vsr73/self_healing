
# ============================================================
# agents/schema_drift.py
# Thin wrapper that calls schema_repair_engine and returns
# a list of issues for the orchestrator context
# ============================================================

import sys
import os

# Make sure the parent pipeline/ dir is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import config
import schema_repair_engine as sre


class SchemaDriftAgent:

    def run(self, ctx: dict) -> dict:
        issues = []

        try:
            metadata    = sre.load_metadata()
            last_update = sre.adapt_timestamp(metadata["last_updated"])
            source      = config.DATA_SOURCES["mysql_local"]

            logs = sre.fetch_mysql_logs(source, last_update)

            if not logs:
                print("  [SCHEMA DRIFT] No DDL changes detected")
                return {"issues": [], "dag": []}

            print(f"  [SCHEMA DRIFT] Found {len(logs)} DDL statement(s)")
            dag = sre.generate_metadata_dag(logs)

            # Each DAG step is itself a detected schema issue
            for step in dag:
                issues.append({
                    "type":   "SCHEMA_DRIFT",
                    "detail": step
                })

            metadata, _ = sre.apply_dag(metadata, dag)
            sre.save_metadata(metadata)

            return {"issues": issues, "dag": dag}

        except Exception as e:
            print(f"  ⚠️  [SCHEMA DRIFT] Error: {e}")
            print("      (MySQL general_log may not be enabled)")
            return {"issues": [], "error": str(e)}
