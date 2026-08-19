
# ============================================================
# agents/logging_agent.py
# Appends structured JSONL entries to pipeline_logs.json
# ============================================================

import json
import os
from datetime import datetime

_HERE    = os.path.dirname(os.path.abspath(__file__))
LOG_PATH = os.path.join(_HERE, "..", "pipeline_logs.json")


class LoggingAgent:

    def record(self, event: str, payload):
        entry = {
            "timestamp": datetime.now().isoformat(),
            "event":     event,
            "payload":   payload if isinstance(payload, str) else json.dumps(payload, default=str)
        }
        with open(LOG_PATH, "a") as f:
            f.write(json.dumps(entry) + "\n")
        print(f"  [LOG] {event}")
