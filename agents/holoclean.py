
# ============================================================
# agents/holoclean.py
# HoloClean-inspired data cleaning:
#   Stage 1 — Rule-based: email regex, category normalisation
#   Stage 2 — Statistical: null imputation with column mean
#   Stage 3 — LLM fallback for ambiguous values
# ============================================================

import re
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pymysql
import config

EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')

# Add any normalisation maps relevant to your data here
CATEGORY_MAPS = {
    "gender": {
        "m": "Male", "f": "Female",
        "male": "Male", "female": "Female",
        "M": "Male", "F": "Female"
    },
    "status": {
        "active": "Active", "inactive": "Inactive",
        "pend": "Pending", "pending": "Pending"
    },
}


class HoloCleanEngine:

    def run(self, ctx: dict) -> list:
        repairs = []

        try:
            source = config.DATA_SOURCES["mysql_local"]
            conn   = pymysql.connect(**source["connection"])
            cursor = conn.cursor()

            # ── Stage 1a: Email validation ────────────────────
            repairs.extend(self._fix_emails(cursor))

            # ── Stage 1b: Category normalisation ─────────────
            repairs.extend(self._normalise_categories(cursor))

            # ── Stage 2: Null imputation for drift issues ─────
            repairs.extend(self._impute_nulls(cursor, ctx.get("issues", [])))

            conn.commit()
            cursor.close()
            conn.close()

        except Exception as e:
            print(f"  ⚠️  [HOLOCLEAN] Error: {e}")

        if repairs:
            print(f"  [HOLOCLEAN] Applied {len(repairs)} repair(s)")
        else:
            print("  [HOLOCLEAN] No cleaning needed")

        return repairs

    # ── Email fixer ───────────────────────────────────────────

    def _fix_emails(self, cursor) -> list:
        repairs = []
        try:
            cursor.execute(
                "SELECT customer_id, email FROM customers WHERE email IS NOT NULL"
            )
            rows = cursor.fetchall()
        except Exception:
            return repairs      # table might not have email column

        for row_id, email in rows:
            if not EMAIL_RE.match(str(email)):
                fixed = self._llm_fix_email(email)
                if fixed:
                    cursor.execute(
                        "UPDATE customers SET email=%s WHERE customer_id=%s",
                        (fixed, row_id)
                    )
                    repairs.append({
                        "rule":   "EMAIL_FIX",
                        "row_id": row_id,
                        "old":    email,
                        "new":    fixed
                    })
        return repairs

    def _llm_fix_email(self, bad_email: str):
        """Ask Gemini to correct a malformed email address."""
        try:
            from google import genai
            client = genai.Client(api_key=config.GEMINI_API_KEY)
            prompt = (
                f"Fix this malformed email: '{bad_email}'. "
                "Return ONLY the corrected email or the word INVALID."
            )
            resp   = client.models.generate_content(
                model="gemini-2.0-flash", contents=prompt
            )
            result = resp.text.strip()
            if result == "INVALID" or not EMAIL_RE.match(result):
                return None
            return result
        except Exception:
            return None

    # ── Category normaliser ───────────────────────────────────

    def _normalise_categories(self, cursor) -> list:
        repairs = []
        for col, mapping in CATEGORY_MAPS.items():
            try:
                cursor.execute(f"SELECT DISTINCT `{col}` FROM customers")
                values = [r[0] for r in cursor.fetchall() if r[0] is not None]
                for val in values:
                    normalised = mapping.get(str(val).strip())
                    if normalised and normalised != val:
                        cursor.execute(
                            f"UPDATE customers SET `{col}`=%s WHERE `{col}`=%s",
                            (normalised, val)
                        )
                        repairs.append({
                            "rule":   "CATEGORY_NORM",
                            "column": col,
                            "old":    val,
                            "new":    normalised
                        })
            except Exception:
                pass            # column may not exist on this table
        return repairs

    # ── Null imputer ──────────────────────────────────────────

    def _impute_nulls(self, cursor, issues: list) -> list:
        repairs = []
        for issue in issues:
            if issue.get("type") != "NULL_SPIKE":
                continue
            table  = issue.get("table")
            column = issue.get("column")
            try:
                cursor.execute(
                    f"SELECT AVG(`{column}`) FROM `{table}` WHERE `{column}` IS NOT NULL"
                )
                mean_val = cursor.fetchone()[0]
                if mean_val is None:
                    continue
                mean_val = round(float(mean_val), 4)
                cursor.execute(
                    f"UPDATE `{table}` SET `{column}`=%s WHERE `{column}` IS NULL",
                    (mean_val,)
                )
                repairs.append({
                    "rule":          "NULL_IMPUTE_MEAN",
                    "table":         table,
                    "column":        column,
                    "imputed_value": mean_val
                })
            except Exception:
                pass
        return repairs
