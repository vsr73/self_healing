
# ============================================================
# orchestrator.py  —  Full Agentic Pipeline Orchestrator
# ============================================================
# Runs all agents in sequence and returns a full context dict.
# Can be called standalone OR imported by an Airflow DAG.
#
# Run directly:
#   cd pipeline && python orchestrator.py
# ============================================================

import json
import sys
import os
from datetime import datetime

# Ensure pipeline/ is on sys.path regardless of cwd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from agents.planner       import PlannerAgent
from agents.schema_drift  import SchemaDriftAgent
from agents.data_drift    import DataDriftAgent
from agents.repair        import RepairAgent
from agents.holoclean     import HoloCleanEngine
from agents.root_cause    import RootCauseAgent
from agents.validation    import ValidationAgent
from agents.logging_agent import LoggingAgent

_HERE         = os.path.dirname(os.path.abspath(__file__))
METADATA_PATH = os.path.join(_HERE, "..", "metadata_graph.json")


def run_pipeline() -> dict:

    log = LoggingAgent()

    # ── Shared context — flows through every agent ────────────
    ctx = {
        "timestamp":          datetime.now().isoformat(),
        "sources":            config.DATA_SOURCES,
        "metadata_path":      METADATA_PATH,
        "issues":             [],
        "repairs":            [],
        "validation_results": []
    }

    print("\n" + "="*60)
    print("  🚀  ORCHESTRATOR — Agentic Self-Healing Pipeline")
    print("="*60)

    log.record("PIPELINE_START", ctx["timestamp"])

    # ── Step 1: Planner decides what to run ──────────────────
    print("\n[1/7] PLANNER")
    plan        = PlannerAgent().run(ctx)
    ctx["plan"] = plan
    log.record("PLAN", plan)

    # ── Step 2: Schema drift detection ───────────────────────
    print("\n[2/7] SCHEMA DRIFT AGENT")
    if plan.get("run_schema_drift"):
        result = SchemaDriftAgent().run(ctx)
        ctx["issues"].extend(result.get("issues", []))
        log.record("SCHEMA_DRIFT", result)
    else:
        print("  ⏭  Skipped (metadata is fresh)")

    # ── Step 3: Data drift detection ─────────────────────────
    print("\n[3/7] DATA DRIFT AGENT")
    if plan.get("run_data_drift"):
        result = DataDriftAgent().run(ctx)
        ctx["issues"].extend(result.get("issues", []))
        log.record("DATA_DRIFT", result)
    else:
        print("  ⏭  Skipped (metadata is fresh)")

    # ── Step 4: Root cause analysis ──────────────────────────
    print("\n[4/7] ROOT CAUSE AGENT")
    if ctx["issues"]:
        rca            = RootCauseAgent().run(ctx)
        ctx["root_cause"] = rca
        log.record("ROOT_CAUSE", rca)
        print(f"  Root causes found: {len(rca)}")
    else:
        print("  ⏭  No issues detected — skipping RCA")

    # ── Step 5: Repair ────────────────────────────────────────
    print("\n[5/7] REPAIR AGENT")
    if ctx["issues"]:
        repairs        = RepairAgent().run(ctx)
        ctx["repairs"] = repairs
        log.record("REPAIRS", repairs)
    else:
        print("  ⏭  No repairs needed")

    # ── Step 6: HoloClean data cleaning ──────────────────────
    print("\n[6/7] HOLOCLEAN ENGINE")
    if plan.get("run_holoclean"):
        cleaned        = HoloCleanEngine().run(ctx)
        ctx["cleaned"] = cleaned
        log.record("HOLOCLEAN", cleaned)
    else:
        print("  ⏭  Skipped (not yet due)")

    # ── Step 7: Validation ────────────────────────────────────
    print("\n[7/7] VALIDATION AGENT")
    validation            = ValidationAgent().run(ctx)
    ctx["validation_results"] = validation
    log.record("VALIDATION", validation)

    # ── Summary ───────────────────────────────────────────────
    log.record("PIPELINE_END", "success")

    print("\n" + "="*60)
    print("  ✅  PIPELINE COMPLETE")
    print("="*60)
    print(f"  Issues detected : {len(ctx['issues'])}")
    print(f"  Repairs applied : {len(ctx['repairs'])}")
    all_passed = all(v.get("passed") for v in validation if "passed" in v)
    print(f"  Validation      : {'✅ ALL PASS' if all_passed else '❌ SOME FAILED'}")
    print(f"  Plan used       : {plan.get('reason')}")
    print("="*60)

    return ctx


# ── Run standalone ────────────────────────────────────────────

if __name__ == "__main__":
    result = run_pipeline()
    print("\n📋 Full context keys:", list(result.keys()))
