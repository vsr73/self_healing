
# ============================================================
# agents/root_cause.py
# Correlates pipeline logs + metadata + detected issues
# and asks Gemini to explain root causes in plain English
# ============================================================

import json
import re
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import config

_HERE    = os.path.dirname(os.path.abspath(__file__))
LOG_PATH = os.path.join(_HERE, "..", "pipeline_logs.json")


class RootCauseAgent:

    def run(self, ctx: dict) -> list:
        issues = ctx.get("issues", [])
        if not issues:
            return [{"summary": "No issues to analyse"}]

        logs = self._load_recent_logs(limit=30)

        with open(ctx["metadata_path"]) as f:
            graph = json.load(f)

        prompt = f"""You are a senior data engineer performing root cause analysis.

Recent pipeline log events (last 30):
{json.dumps(logs, indent=2)}

Detected data/schema issues this run:
{json.dumps(issues, indent=2)}

Metadata graph last updated: {graph.get('last_updated')}

For each issue, explain:
1. What happened (in plain English)
2. The most likely cause (schema change, upstream data problem, config error, etc.)
3. When it likely started based on the logs
4. Confidence level: HIGH / MEDIUM / LOW

Return ONLY a JSON array — no markdown, no explanation:
[
  {{
    "issue": "...",
    "root_cause": "...",
    "timeline": "...",
    "confidence": "HIGH|MEDIUM|LOW",
    "recommended_action": "..."
  }}
]"""

        try:
            from google import genai
            client   = genai.Client(api_key=config.GEMINI_API_KEY)
            response = client.models.generate_content(
                model="gemini-2.0-flash",
                contents=prompt
            )
            txt   = response.text.replace("```json", "").replace("```", "").strip()
            match = re.search(r"\[.*\]", txt, re.DOTALL)
            if match:
                result = json.loads(match.group())
                print(f"  [ROOT CAUSE] {len(result)} root cause(s) identified")
                return result
        except Exception as e:
            print(f"  ⚠️  [ROOT CAUSE] LLM error: {e}")

        return [{"issue": str(issues), "root_cause": "LLM unavailable"}]

    def _load_recent_logs(self, limit=30) -> list:
        events = []
        try:
            with open(LOG_PATH) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        events.append(json.loads(line))
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        return events[-limit:]
