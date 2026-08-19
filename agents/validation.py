
# ============================================================
# agents/validation.py
# Runs post-repair validation checks on the database.
# Uses lightweight built-in checks (no great_expectations needed).
# Optionally uses great_expectations if installed.
# ============================================================

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pymysql
import config


class ValidationAgent:

    def run(self, ctx: dict) -> list:
        results = []

        try:
            source = config.DATA_SOURCES["mysql_local"]
            conn   = pymysql.connect(**source["connection"])
            cursor = conn.cursor()

            # Collect tables that had issues OR default to all tables
            issue_tables = list({
                i.get("table") for i in ctx.get("issues", []) if i.get("table")
            })

            # If no specific tables, validate all tables in the DB
            if not issue_tables:
                cursor.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = DATABASE()
                """)
                issue_tables = [r[0] for r in cursor.fetchall()]

            for table in issue_tables:
                result = self._validate_table(cursor, table)
                results.append(result)
                status = "✅ PASS" if result["passed"] else "❌ FAIL"
                print(f"  [VALIDATION] {table}: {status} — {result['summary']}")

            cursor.close()
            conn.close()

        except Exception as e:
            print(f"  ⚠️  [VALIDATION] Error: {e}")
            results.append({"error": str(e), "passed": False})

        return results

    def _validate_table(self, cursor, table: str) -> dict:
        checks  = []
        passed  = True

        # ── Check 1: Table exists and has rows ────────────────
        try:
            cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
            row_count = cursor.fetchone()[0]
            ok = row_count > 0
            checks.append({"check": "row_count_gt_0", "passed": ok, "value": row_count})
            if not ok:
                passed = False
        except Exception as e:
            checks.append({"check": "row_count_gt_0", "passed": False, "error": str(e)})
            passed = False

        # ── Check 2: No column is 100% null ──────────────────
        try:
            cursor.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = %s
            """, (table,))
            columns = [r[0] for r in cursor.fetchall()]

            for col in columns:
                cursor.execute(f"""
                    SELECT
                        COUNT(*) as total,
                        SUM(CASE WHEN `{col}` IS NULL THEN 1 ELSE 0 END) as nulls
                    FROM `{table}`
                """)
                total, nulls = cursor.fetchone()
                if total and nulls == total:
                    checks.append({
                        "check":  f"not_all_null:{col}",
                        "passed": False,
                        "value":  "100% null"
                    })
                    passed = False
        except Exception as e:
            checks.append({"check": "null_check", "passed": False, "error": str(e)})

        # ── Check 3: Primary key column has no nulls ─────────
        try:
            cursor.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = DATABASE()
                AND table_name = %s
                AND column_key = 'PRI'
                LIMIT 1
            """, (table,))
            pk_row = cursor.fetchone()
            if pk_row:
                pk_col = pk_row[0]
                cursor.execute(f"""
                    SELECT COUNT(*) FROM `{table}`
                    WHERE `{pk_col}` IS NULL
                """)
                pk_nulls = cursor.fetchone()[0]
                ok = pk_nulls == 0
                checks.append({
                    "check":  f"pk_not_null:{pk_col}",
                    "passed": ok,
                    "value":  pk_nulls
                })
                if not ok:
                    passed = False
        except Exception:
            pass

        total_checks  = len(checks)
        passed_checks = sum(1 for c in checks if c.get("passed"))
        summary       = f"{passed_checks}/{total_checks} checks passed"

        return {
            "table":   table,
            "passed":  passed,
            "summary": summary,
            "checks":  checks
        }
