
# ============================================================
# agents/planner.py
# Reads metadata freshness and decides which agents to run
# ============================================================

import json
from datetime import datetime


class PlannerAgent:
    """
    Checks how old the metadata graph is and returns a plan dict
    telling the orchestrator which agents should run this cycle.
    """

    def run(self, ctx: dict) -> dict:

        with open(ctx["metadata_path"]) as f:
            graph = json.load(f)

        last_updated_raw = graph.get("last_updated", "2000-01-01 00:00:00")

        # Handle both "YYYY-MM-DD HH:MM:SS" and ISO format
        try:
            last_updated = datetime.fromisoformat(last_updated_raw.replace(" ", "T"))
        except ValueError:
            last_updated = datetime(2000, 1, 1)

        age_minutes = (datetime.now() - last_updated).total_seconds() / 60

        plan = {
            "run_schema_drift": age_minutes > 2,    # run if graph > 2 min old
            "run_data_drift":   age_minutes > 5,    # run if graph > 5 min old
            "run_holoclean":    age_minutes > 10,   # run if graph > 10 min old
            "metadata_age_minutes": round(age_minutes, 1),
            "reason": f"Metadata is {round(age_minutes, 1)} minute(s) old"
        }

        print(f"  [PLANNER] {plan['reason']}")
        print(f"  [PLANNER] Plan → {plan}")
        return plan
