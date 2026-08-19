
# ============================================================
# agents/repair.py
# Coordinates all repair actions based on detected issues.
# Schema repairs are handled by schema_repair_engine.
# Data repairs are handled by holoclean.
# ============================================================

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class RepairAgent:

    def run(self, ctx: dict) -> list:
        issues  = ctx.get("issues", [])
        repairs = []

        schema_issues = [i for i in issues if i.get("type") == "SCHEMA_DRIFT"]
        data_issues   = [i for i in issues if i.get("type") != "SCHEMA_DRIFT"]

        if schema_issues:
            print(f"  [REPAIR] {len(schema_issues)} schema issue(s) — already fixed by SchemaDriftAgent")
            for i in schema_issues:
                repairs.append({
                    "type":   "SCHEMA_REPAIR",
                    "detail": i.get("detail"),
                    "status": "applied"
                })

        if data_issues:
            print(f"  [REPAIR] {len(data_issues)} data issue(s) queued for HoloClean")
            for i in data_issues:
                repairs.append({
                    "type":   "DATA_REPAIR",
                    "issue":  i,
                    "status": "queued"
                })

        return repairs
