
# ============================================================
# agents/data_drift.py
# Statistical drift detection: null spikes, mean shift,
# cardinality drops — compared to baseline stored in graph
# ============================================================

import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pymysql
import config


class DataDriftAgent:

    # How much change triggers an alert
    THRESHOLDS = {
        "null_rate_delta":  0.10,   # 10% more nulls
        "mean_delta_pct":   0.20,   # 20% shift in mean
        "cardinality_drop": 0.30,   # 30% fewer distinct values
    }

    def run(self, ctx: dict) -> dict:
        issues = []

        try:
            source = config.DATA_SOURCES["mysql_local"]
            conn   = pymysql.connect(**source["connection"])
            cursor = conn.cursor()

            with open(ctx["metadata_path"]) as f:
                graph = json.load(f)

            # Build baseline map from graph nodes that have stats
            baselines = {
                n["id"]: n.get("stats", {})
                for n in graph["nodes"]
                if n["type"] == "column"
            }

            # Get all numeric columns in the live database
            cursor.execute("""
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                AND data_type IN ('int','bigint','float','double','decimal',
                                  'tinyint','smallint','mediumint')
            """)
            numeric_cols = cursor.fetchall()

            for table, col in numeric_cols:
                col_id = f"mysql_local.{table}.{col}"

                try:
                    cursor.execute(f"""
                        SELECT
                            COUNT(*) as total,
                            SUM(CASE WHEN `{col}` IS NULL THEN 1 ELSE 0 END) as nulls,
                            AVG(`{col}`) as mean_val,
                            COUNT(DISTINCT `{col}`) as cardinality
                        FROM `{table}`
                    """)
                    row = cursor.fetchone()
                except Exception:
                    continue

                if not row or row[0] == 0:
                    continue

                total, nulls, mean_val, card = row
                null_rate = float(nulls or 0) / total
                mean_val  = float(mean_val or 0)

                current = {
                    "null_rate":   null_rate,
                    "mean":        mean_val,
                    "cardinality": int(card or 0)
                }

                baseline = baselines.get(col_id, {})

                if not baseline:
                    # First time seeing this column — store baseline, no alert
                    self._store_baseline(graph, col_id, current)
                    continue

                # ── Null rate spike ───────────────────────────────
                null_delta = null_rate - baseline.get("null_rate", 0)
                if null_delta > self.THRESHOLDS["null_rate_delta"]:
                    issues.append({
                        "type":     "NULL_SPIKE",
                        "table":    table,
                        "column":   col,
                        "delta":    round(null_delta, 3),
                        "severity": "HIGH" if null_delta > 0.3 else "MEDIUM"
                    })

                # ── Mean shift ───────────────────────────────────
                base_mean = baseline.get("mean", 0)
                if base_mean != 0:
                    mean_pct = abs(mean_val - base_mean) / abs(base_mean)
                    if mean_pct > self.THRESHOLDS["mean_delta_pct"]:
                        issues.append({
                            "type":           "DISTRIBUTION_SHIFT",
                            "table":          table,
                            "column":         col,
                            "mean_delta_pct": round(mean_pct, 3),
                            "severity":       "MEDIUM"
                        })

                # ── Cardinality drop ─────────────────────────────
                base_card = baseline.get("cardinality", 0)
                if base_card > 0:
                    card_drop = (base_card - current["cardinality"]) / base_card
                    if card_drop > self.THRESHOLDS["cardinality_drop"]:
                        issues.append({
                            "type":     "CARDINALITY_DROP",
                            "table":    table,
                            "column":   col,
                            "severity": "LOW"
                        })

                # Update baseline with current snapshot
                self._store_baseline(graph, col_id, current)

            cursor.close()
            conn.close()

            # Persist updated baselines
            with open(ctx["metadata_path"], "w") as f:
                json.dump(graph, f, indent=4)

            if issues:
                print(f"  [DATA DRIFT] {len(issues)} issue(s) detected")
            else:
                print("  [DATA DRIFT] No drift detected")

            return {"issues": issues}

        except Exception as e:
            print(f"  ⚠️  [DATA DRIFT] Error: {e}")
            return {"issues": [], "error": str(e)}

    def _store_baseline(self, graph, col_id, stats):
        for node in graph["nodes"]:
            if node["id"] == col_id:
                node["stats"] = stats
                return
