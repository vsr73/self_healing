"""
LLM Schema Repair — Evaluation Framework
=========================================
Measures repair accuracy of the Gemini-based schema repair pipeline
across 15 drift scenarios spanning EASY → HARD → EDGE categories.

Evaluation dimensions per test case:
  [S] Structure     — response has all required keys
  [D] DDL           — statements mention correct column names + op type
  [G] DAG           — entries have correct op / previous / current names
  [F] graph_field   — canonical field identifier correctly assigned
  [E] End-to-End    — after DAG applied, events normalize correctly

Run:
    python test_llm_repair.py
    python test_llm_repair.py --verbose     (show LLM output per case)
    python test_llm_repair.py --tc TC04     (run single test case)
"""

import argparse
import json
import sys
import time
import traceback
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pipeline.llm_repair import generate_consumer_repair
from pipeline.metadata_store import apply_schema_dag_to_graph
from pipeline.schema import get_alias_map, extract_table_schema

# ── ANSI colours ──────────────────────────────────────────────────────────────
GRN  = "\033[92m"
RED  = "\033[91m"
YLW  = "\033[93m"
BLU  = "\033[94m"
DIM  = "\033[2m"
BOLD = "\033[1m"
RST  = "\033[0m"

PASS    = f"{GRN}✓ PASS   {RST}"
PARTIAL = f"{YLW}~ PARTIAL{RST}"
FAIL    = f"{RED}✗ FAIL   {RST}"
ERR     = f"{RED}⚡ ERROR  {RST}"

# ── Canonical stock_events graph (pre-drift baseline) ─────────────────────────
COLS = {
    "id":       "varchar",
    "symbol":   "varchar",
    "price":    "float",
    "volume":   "int",
    "ts":       "bigint",
    "exchange": "varchar",
    "currency": "varchar",
}

def _make_graph(extra_renames=None):
    """Build a fresh metadata graph, optionally with pre-applied renames
    so we can simulate chain-rename scenarios (TC08)."""
    col_names = list(COLS.keys())
    if extra_renames:
        col_names = [extra_renames.get(c, c) for c in col_names]

    nodes = [
        {
            "id": "mysql_stock.stock_pipeline",
            "type": "database",
            "edges": [{"relation": "HAS_TABLE", "target": "mysql_stock.stock_events"}],
            "database": "stock_pipeline",
        },
        {
            "id": "mysql_stock.stock_events",
            "type": "table",
            "edges": [
                {"relation": "HAS_COLUMN", "target": f"mysql_stock.stock_events.{c}"}
                for c in col_names
            ],
            "database": "stock_pipeline",
        },
    ]
    original_cols = list(COLS.keys())
    for i, col in enumerate(col_names):
        nodes.append({
            "id": f"mysql_stock.stock_events.{col}",
            "type": "column",
            "edges": [],
            "datatype": COLS[original_cols[i]],
            "nullable": "NO" if original_cols[i] == "id" else "YES",
        })

    return {
        "sources": [{"name": "mysql_stock", "type": "mysql", "database": "stock_pipeline"}],
        "nodes": nodes,
        "consumer_state": {"tables": {}, "dependencies": []},
    }


def _make_op(op_type, **kwargs):
    return {"type": op_type, **kwargs}


def _rename_op(graph_field, from_name, to_name):
    return _make_op("rename_column", graph_field=graph_field, **{"from": from_name, "to": to_name})


def _add_op(field, datatype):
    return _make_op("add_column", field=field, datatype=datatype)


def _change_event(table, operations, graph=None):
    if graph is None:
        graph = _make_graph()
    schema = extract_table_schema(graph, table)
    return {
        "event_type": "SCHEMA_CHANGE",
        "change_id": "test-evaluation",
        "table": table,
        "operations": operations,
        "before_schema": dict(schema),
        "after_schema": {},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ── Test case definitions ─────────────────────────────────────────────────────
#
# Each dict:
#   id           str   — TC01 … TC15
#   category     str   — EASY / MEDIUM / HARD / EDGE
#   name         str   — human description
#   graph        dict  — graph state before this change arrives
#   event        dict  — SCHEMA_CHANGE event consumer receives
#   expect_dag   list  — [{operation, previous_name/column, current_name, graph_field?}]
#   expect_ddl_keywords  list[list[str]] — per-statement: all must appear (case-insensitive)
#   test_input   dict  — drifted event to normalize after repair
#   test_output  dict  — expected canonical event after normalization
#

def build_test_cases():
    TABLE = "stock_events"
    g = _make_graph()

    cases = []

    # ── EASY ──────────────────────────────────────────────────────────────────

    # TC01 — single rename
    cases.append(dict(
        id="TC01", category="EASY",
        name="Single rename: price → price_usd",
        graph=deepcopy(g),
        event=_change_event(TABLE, [_rename_op("price", "price", "price_usd")], g),
        expect_dag=[
            dict(operation="RENAME_COLUMN", previous_name="price",
                 current_name="price_usd", graph_field="price"),
        ],
        expect_ddl_keywords=[["price", "price_usd"]],
        test_input ={"price_usd": 1.5, "symbol": "AAPL", "volume": 100,
                     "ts": 1700000000, "exchange": "NYSE", "currency": "USD", "id": "x1"},
        test_output={"price":     1.5, "symbol": "AAPL", "volume": 100,
                     "ts": 1700000000, "exchange": "NYSE", "currency": "USD", "id": "x1"},
    ))

    # TC02 — single add column
    cases.append(dict(
        id="TC02", category="EASY",
        name="Single add column: +region varchar",
        graph=deepcopy(g),
        event=_change_event(TABLE, [_add_op("region", "varchar")], g),
        expect_dag=[
            dict(operation="ADD_COLUMN", column="region"),
        ],
        expect_ddl_keywords=[["add", "region"]],
        test_input ={"id": "x2", "symbol": "AAPL", "price": 1.5, "volume": 100,
                     "ts": 1700000000, "exchange": "NYSE", "currency": "USD", "region": "US"},
        test_output={"id": "x2", "symbol": "AAPL", "price": 1.5, "volume": 100,
                     "ts": 1700000000, "exchange": "NYSE", "currency": "USD", "region": "US"},
    ))

    # TC03 — rename non-primary column
    cases.append(dict(
        id="TC03", category="EASY",
        name="Single rename: symbol → ticker",
        graph=deepcopy(g),
        event=_change_event(TABLE, [_rename_op("symbol", "symbol", "ticker")], g),
        expect_dag=[
            dict(operation="RENAME_COLUMN", previous_name="symbol",
                 current_name="ticker", graph_field="symbol"),
        ],
        expect_ddl_keywords=[["symbol", "ticker"]],
        test_input ={"id": "x3", "ticker": "MSFT", "price": 2.0, "volume": 50,
                     "ts": 1700000001, "exchange": "NASDAQ", "currency": "USD"},
        test_output={"id": "x3", "symbol": "MSFT", "price": 2.0, "volume": 50,
                     "ts": 1700000001, "exchange": "NASDAQ", "currency": "USD"},
    ))

    # ── MEDIUM ─────────────────────────────────────────────────────────────────

    # TC04 — batch: 2 renames together
    cases.append(dict(
        id="TC04", category="MEDIUM",
        name="Batch rename: price → price_usd  AND  symbol → ticker",
        graph=deepcopy(g),
        event=_change_event(TABLE, [
            _rename_op("price",  "price",  "price_usd"),
            _rename_op("symbol", "symbol", "ticker"),
        ], g),
        expect_dag=[
            dict(operation="RENAME_COLUMN", previous_name="price",
                 current_name="price_usd", graph_field="price"),
            dict(operation="RENAME_COLUMN", previous_name="symbol",
                 current_name="ticker",    graph_field="symbol"),
        ],
        expect_ddl_keywords=[["price", "price_usd"], ["symbol", "ticker"]],
        test_input ={"id": "x4", "ticker": "AAPL", "price_usd": 3.0, "volume": 200,
                     "ts": 1700000002, "exchange": "NYSE", "currency": "USD"},
        test_output={"id": "x4", "symbol": "AAPL", "price":     3.0, "volume": 200,
                     "ts": 1700000002, "exchange": "NYSE", "currency": "USD"},
    ))

    # TC05 — batch rename + add column
    cases.append(dict(
        id="TC05", category="MEDIUM",
        name="Batch: price → cost  AND  +region varchar",
        graph=deepcopy(g),
        event=_change_event(TABLE, [
            _rename_op("price", "price", "cost"),
            _add_op("region", "varchar"),
        ], g),
        expect_dag=[
            dict(operation="RENAME_COLUMN", previous_name="price",
                 current_name="cost", graph_field="price"),
            dict(operation="ADD_COLUMN", column="region"),
        ],
        expect_ddl_keywords=[["price", "cost"], ["add", "region"]],
        test_input ={"id": "x5", "symbol": "GOOG", "cost": 100.0, "volume": 10,
                     "ts": 1700000003, "exchange": "NASDAQ", "currency": "USD", "region": "APAC"},
        test_output={"id": "x5", "symbol": "GOOG", "price": 100.0, "volume": 10,
                     "ts": 1700000003, "exchange": "NASDAQ", "currency": "USD", "region": "APAC"},
    ))

    # TC06 — rename to close-to-reserved-word (timestamp is MySQL data type name)
    cases.append(dict(
        id="TC06", category="MEDIUM",
        name="Rename ts → event_timestamp (long descriptive name)",
        graph=deepcopy(g),
        event=_change_event(TABLE, [_rename_op("ts", "ts", "event_timestamp")], g),
        expect_dag=[
            dict(operation="RENAME_COLUMN", previous_name="ts",
                 current_name="event_timestamp", graph_field="ts"),
        ],
        expect_ddl_keywords=[["ts", "event_timestamp"]],
        test_input ={"id": "x6", "symbol": "AMZN", "price": 5.0, "volume": 30,
                     "event_timestamp": 1700000004, "exchange": "NYSE", "currency": "USD"},
        test_output={"id": "x6", "symbol": "AMZN", "price": 5.0, "volume": 30,
                     "ts": 1700000004,              "exchange": "NYSE", "currency": "USD"},
    ))

    # TC07 — rename to compound snake_case
    cases.append(dict(
        id="TC07", category="MEDIUM",
        name="Rename exchange → exchange_code  AND  currency → base_currency",
        graph=deepcopy(g),
        event=_change_event(TABLE, [
            _rename_op("exchange", "exchange", "exchange_code"),
            _rename_op("currency", "currency", "base_currency"),
        ], g),
        expect_dag=[
            dict(operation="RENAME_COLUMN", previous_name="exchange",
                 current_name="exchange_code",  graph_field="exchange"),
            dict(operation="RENAME_COLUMN", previous_name="currency",
                 current_name="base_currency", graph_field="currency"),
        ],
        expect_ddl_keywords=[["exchange", "exchange_code"], ["currency", "base_currency"]],
        test_input ={"id": "x7", "symbol": "META", "price": 7.0, "volume": 40,
                     "ts": 1700000005, "exchange_code": "LSE", "base_currency": "GBP"},
        test_output={"id": "x7", "symbol": "META", "price": 7.0, "volume": 40,
                     "ts": 1700000005, "exchange": "LSE",         "currency": "GBP"},
    ))

    # ── HARD ───────────────────────────────────────────────────────────────────

    # TC08 — chain rename: graph already has price_renamed from previous repair
    g_chain = _make_graph(extra_renames={"price": "price_renamed"})
    cases.append(dict(
        id="TC08", category="HARD",
        name="Chain rename: price_renamed → price_final  (graph already shows price_renamed)",
        graph=deepcopy(g_chain),
        event=_change_event(TABLE,
            [_rename_op("price_renamed", "price_renamed", "price_final")], g_chain),
        expect_dag=[
            dict(operation="RENAME_COLUMN", previous_name="price_renamed",
                 current_name="price_final", graph_field="price_renamed"),
        ],
        expect_ddl_keywords=[["price_renamed", "price_final"]],
        test_input ={"id": "x8", "symbol": "TSLA", "price_final": 9.0, "volume": 60,
                     "ts": 1700000006, "exchange": "NYSE", "currency": "USD"},
        test_output={"id": "x8", "symbol": "TSLA", "price_renamed": 9.0, "volume": 60,
                     "ts": 1700000006, "exchange": "NYSE", "currency": "USD"},
    ))

    # TC09 — 3 renames simultaneously (large batch)
    cases.append(dict(
        id="TC09", category="HARD",
        name="Batch 3 renames: price→price_usd, volume→qty, ts→trade_time",
        graph=deepcopy(g),
        event=_change_event(TABLE, [
            _rename_op("price",  "price",  "price_usd"),
            _rename_op("volume", "volume", "qty"),
            _rename_op("ts",     "ts",     "trade_time"),
        ], g),
        expect_dag=[
            dict(operation="RENAME_COLUMN", previous_name="price",
                 current_name="price_usd",  graph_field="price"),
            dict(operation="RENAME_COLUMN", previous_name="volume",
                 current_name="qty",        graph_field="volume"),
            dict(operation="RENAME_COLUMN", previous_name="ts",
                 current_name="trade_time", graph_field="ts"),
        ],
        expect_ddl_keywords=[
            ["price", "price_usd"],
            ["volume", "qty"],
            ["ts", "trade_time"],
        ],
        test_input ={"id": "x9", "symbol": "NVDA", "price_usd": 11.0, "qty": 80,
                     "trade_time": 1700000007, "exchange": "NYSE", "currency": "USD"},
        test_output={"id": "x9", "symbol": "NVDA", "price":     11.0, "volume": 80,
                     "ts": 1700000007,            "exchange": "NYSE", "currency": "USD"},
    ))

    # TC10 — rename id to a reserved-ish word
    cases.append(dict(
        id="TC10", category="HARD",
        name="Rename id → trade_id  (primary key column rename)",
        graph=deepcopy(g),
        event=_change_event(TABLE, [_rename_op("id", "id", "trade_id")], g),
        expect_dag=[
            dict(operation="RENAME_COLUMN", previous_name="id",
                 current_name="trade_id", graph_field="id"),
        ],
        expect_ddl_keywords=[["id", "trade_id"]],
        test_input ={"trade_id": "x10", "symbol": "AMD", "price": 13.0, "volume": 90,
                     "ts": 1700000008, "exchange": "NASDAQ", "currency": "USD"},
        test_output={"id":       "x10", "symbol": "AMD", "price": 13.0, "volume": 90,
                     "ts": 1700000008, "exchange": "NASDAQ", "currency": "USD"},
    ))

    # TC11 — 4 ops: 3 renames + 1 add
    cases.append(dict(
        id="TC11", category="HARD",
        name="Batch 4 ops: 3 renames + 1 add column",
        graph=deepcopy(g),
        event=_change_event(TABLE, [
            _rename_op("price",    "price",    "price_usd"),
            _rename_op("symbol",   "symbol",   "ticker"),
            _rename_op("exchange", "exchange", "market"),
            _add_op("trade_flag", "varchar"),
        ], g),
        expect_dag=[
            dict(operation="RENAME_COLUMN", previous_name="price",
                 current_name="price_usd", graph_field="price"),
            dict(operation="RENAME_COLUMN", previous_name="symbol",
                 current_name="ticker",    graph_field="symbol"),
            dict(operation="RENAME_COLUMN", previous_name="exchange",
                 current_name="market",    graph_field="exchange"),
            dict(operation="ADD_COLUMN",    column="trade_flag"),
        ],
        expect_ddl_keywords=[
            ["price", "price_usd"],
            ["symbol", "ticker"],
            ["exchange", "market"],
            ["add", "trade_flag"],
        ],
        test_input ={"id": "x11", "ticker": "COIN", "price_usd": 15.0, "volume": 110,
                     "ts": 1700000009, "market": "CRYPTO", "currency": "USD",
                     "trade_flag": "Y"},
        test_output={"id": "x11", "symbol": "COIN", "price":     15.0, "volume": 110,
                     "ts": 1700000009, "exchange": "CRYPTO", "currency": "USD",
                     "trade_flag": "Y"},
    ))

    # ── EDGE CASES ─────────────────────────────────────────────────────────────

    # TC12 — add 3 columns at once (no renames)
    cases.append(dict(
        id="TC12", category="EDGE",
        name="Add 3 columns at once: +session_id, +risk_score, +region",
        graph=deepcopy(g),
        event=_change_event(TABLE, [
            _add_op("session_id", "varchar"),
            _add_op("risk_score", "float"),
            _add_op("region",     "varchar"),
        ], g),
        expect_dag=[
            dict(operation="ADD_COLUMN", column="session_id"),
            dict(operation="ADD_COLUMN", column="risk_score"),
            dict(operation="ADD_COLUMN", column="region"),
        ],
        expect_ddl_keywords=[
            ["add", "session_id"],
            ["add", "risk_score"],
            ["add", "region"],
        ],
        test_input ={"id": "x12", "symbol": "SQ", "price": 17.0, "volume": 130,
                     "ts": 1700000010, "exchange": "NYSE", "currency": "USD",
                     "session_id": "s1", "risk_score": 0.3, "region": "US"},
        test_output={"id": "x12", "symbol": "SQ", "price": 17.0, "volume": 130,
                     "ts": 1700000010, "exchange": "NYSE", "currency": "USD",
                     "session_id": "s1", "risk_score": 0.3, "region": "US"},
    ))

    # TC13 — rename to very similar but different name (typo-like drift)
    cases.append(dict(
        id="TC13", category="EDGE",
        name="Rename volume → volm (abbreviation drift, easy to confuse)",
        graph=deepcopy(g),
        event=_change_event(TABLE, [_rename_op("volume", "volume", "volm")], g),
        expect_dag=[
            dict(operation="RENAME_COLUMN", previous_name="volume",
                 current_name="volm", graph_field="volume"),
        ],
        expect_ddl_keywords=[["volume", "volm"]],
        test_input ={"id": "x13", "symbol": "LYFT", "price": 19.0, "volm": 150,
                     "ts": 1700000011, "exchange": "NASDAQ", "currency": "USD"},
        test_output={"id": "x13", "symbol": "LYFT", "price": 19.0, "volume": 150,
                     "ts": 1700000011, "exchange": "NASDAQ", "currency": "USD"},
    ))

    # TC14 — collision rename: volume → price (price already exists — impossible without swap)
    # Expected: LLM should fail or generate invalid DDL. We mark FAIL as the "correct" outcome.
    cases.append(dict(
        id="TC14", category="EDGE",
        name="Impossible rename: volume → price  (target column already exists)",
        graph=deepcopy(g),
        event=_change_event(TABLE, [_rename_op("volume", "volume", "price")], g),
        expect_dag=[
            dict(operation="RENAME_COLUMN", previous_name="volume",
                 current_name="price", graph_field="volume"),
        ],
        expect_ddl_keywords=[["volume", "price"]],
        # This rename is semantically invalid — we expect the LLM to attempt it
        # but MySQL would reject it. Pass if LLM at least generates the DAG correctly;
        # the DDL check is relaxed since the operation itself is contradictory.
        test_input ={"id": "x14", "symbol": "UBER", "price": 21.0, "volume": 170,
                     "ts": 1700000012, "exchange": "NYSE", "currency": "USD"},
        test_output={"id": "x14", "symbol": "UBER", "price": 21.0, "volume": 170,
                     "ts": 1700000012, "exchange": "NYSE", "currency": "USD"},
        is_adversarial=True,
    ))

    # TC15 — full table overhaul: rename all 7 columns
    cases.append(dict(
        id="TC15", category="EDGE",
        name="Stress: rename ALL 7 columns simultaneously",
        graph=deepcopy(g),
        event=_change_event(TABLE, [
            _rename_op("id",       "id",       "trade_id"),
            _rename_op("symbol",   "symbol",   "ticker"),
            _rename_op("price",    "price",    "price_usd"),
            _rename_op("volume",   "volume",   "qty"),
            _rename_op("ts",       "ts",       "trade_time"),
            _rename_op("exchange", "exchange", "market"),
            _rename_op("currency", "currency", "base_currency"),
        ], g),
        expect_dag=[
            dict(operation="RENAME_COLUMN", previous_name="id",       current_name="trade_id",      graph_field="id"),
            dict(operation="RENAME_COLUMN", previous_name="symbol",   current_name="ticker",        graph_field="symbol"),
            dict(operation="RENAME_COLUMN", previous_name="price",    current_name="price_usd",     graph_field="price"),
            dict(operation="RENAME_COLUMN", previous_name="volume",   current_name="qty",           graph_field="volume"),
            dict(operation="RENAME_COLUMN", previous_name="ts",       current_name="trade_time",    graph_field="ts"),
            dict(operation="RENAME_COLUMN", previous_name="exchange", current_name="market",        graph_field="exchange"),
            dict(operation="RENAME_COLUMN", previous_name="currency", current_name="base_currency", graph_field="currency"),
        ],
        expect_ddl_keywords=[
            ["id", "trade_id"], ["symbol", "ticker"], ["price", "price_usd"],
            ["volume", "qty"],  ["ts", "trade_time"], ["exchange", "market"],
            ["currency", "base_currency"],
        ],
        test_input ={"trade_id": "x15", "ticker": "BRK", "price_usd": 23.0, "qty": 190,
                     "trade_time": 1700000013, "market": "NYSE", "base_currency": "USD"},
        test_output={"id":       "x15", "symbol": "BRK", "price":     23.0, "volume": 190,
                     "ts": 1700000013,        "exchange": "NYSE", "currency": "USD"},
    ))

    return cases


# ── Evaluation helpers ─────────────────────────────────────────────────────────

def check_structure(repair_plan):
    required = {"dag", "ddl_statements", "summary", "table"}
    missing  = required - set(repair_plan.keys())
    ok = not missing
    note = "" if ok else f"missing keys: {missing}"
    return ok, note


def check_ddl(repair_plan, expect_ddl_keywords):
    """Each inner list of keywords must all appear (case-insensitive) in at least one DDL stmt."""
    stmts = [s.lower() for s in repair_plan.get("ddl_statements", [])
             if s.strip() and not s.strip().startswith("--")]
    if not stmts:
        return False, "no DDL statements generated"

    full_ddl = " ".join(stmts)
    misses = []
    for kw_group in expect_ddl_keywords:
        found = any(all(kw.lower() in stmt for kw in kw_group) for stmt in stmts) or \
                all(kw.lower() in full_ddl for kw in kw_group)
        if not found:
            misses.append(kw_group)

    ok = not misses
    note = "" if ok else f"DDL missing: {misses}"
    return ok, note


def check_dag(repair_plan, expect_dag):
    actual_dag = repair_plan.get("dag", [])
    if len(actual_dag) != len(expect_dag):
        return False, f"DAG length {len(actual_dag)} ≠ expected {len(expect_dag)}"

    errors = []
    for i, (actual, expected) in enumerate(zip(actual_dag, expect_dag)):
        step_errors = []
        if actual.get("operation") != expected.get("operation"):
            step_errors.append(
                f"op={actual.get('operation')!r} expected={expected.get('operation')!r}"
            )
        if "previous_name" in expected:
            if actual.get("previous_name") != expected["previous_name"]:
                step_errors.append(
                    f"prev={actual.get('previous_name')!r} expected={expected['previous_name']!r}"
                )
        if "current_name" in expected:
            if actual.get("current_name") != expected["current_name"]:
                step_errors.append(
                    f"curr={actual.get('current_name')!r} expected={expected['current_name']!r}"
                )
        if "column" in expected:
            col_actual = actual.get("column") or actual.get("field")
            if col_actual != expected["column"]:
                step_errors.append(
                    f"column={col_actual!r} expected={expected['column']!r}"
                )
        if step_errors:
            errors.append(f"DAG[{i}]: {'; '.join(step_errors)}")

    ok = not errors
    note = " | ".join(errors) if errors else ""
    return ok, note


def check_graph_field(repair_plan, expect_dag):
    """graph_field must match the canonical field identifier for rename ops."""
    actual_dag = repair_plan.get("dag", [])
    errors = []
    for i, (actual, expected) in enumerate(zip(actual_dag, expect_dag)):
        if "graph_field" not in expected:
            continue
        gf_actual = actual.get("graph_field")
        gf_expect = expected["graph_field"]
        if gf_actual != gf_expect:
            errors.append(f"DAG[{i}] graph_field={gf_actual!r} expected={gf_expect!r}")

    ok = not errors
    note = " | ".join(errors) if errors else ""
    return ok, note


def check_e2e(graph, repair_plan, test_input, test_output, table):
    """Full end-to-end normalization check.

    1. Apply LLM's DAG to a graph copy.
    2. Inject LLM's alias_mappings into consumer_state so get_alias_map can read them.
    3. Normalize the drifted test_input through the alias map.
    4. Compare against expected canonical test_output.
    """
    try:
        # Step 1 — apply DAG (renames graph node IDs)
        updated_graph = apply_schema_dag_to_graph(graph, repair_plan.get("dag", []))

        # Step 2 — store LLM's alias_mappings in consumer_state so get_alias_map picks them up
        alias_mappings = repair_plan.get("alias_mappings") or {}
        if alias_mappings:
            cs = updated_graph.setdefault("consumer_state", {})
            tbl_state = cs.setdefault("tables", {}).setdefault(table, {})
            tbl_state.setdefault("aliases", {}).update(alias_mappings)

        # Step 3 — build alias map and normalize
        alias_map  = get_alias_map(updated_graph, table)
        normalized = {alias_map.get(f, f): v for f, v in test_input.items()}

        # Step 4 — compare (extra keys from added columns are ignored)
        mismatches = {
            k: {"got": normalized.get(k), "expected": v}
            for k, v in test_output.items()
            if normalized.get(k) != v
        }
        ok   = not mismatches
        note = "" if ok else f"mismatch: {mismatches}"
        return ok, note
    except Exception as exc:
        return False, f"exception: {exc}"


# ── Test runner ───────────────────────────────────────────────────────────────

def run_case(tc, verbose=False):
    is_adversarial = tc.get("is_adversarial", False)
    result = {
        "id":       tc["id"],
        "category": tc["category"],
        "name":     tc["name"],
        "verdict":  None,
        "checks":   {},
        "llm_time": None,
        "error":    None,
        "repair":   None,
    }

    # Retry with exponential backoff for rate-limit (429) errors
    max_retries = 5
    for attempt in range(max_retries):
        try:
            t0 = time.time()
            repair_plan = generate_consumer_repair(tc["event"], tc["graph"])
            result["llm_time"] = round(time.time() - t0, 2)
            result["repair"]   = repair_plan
            break
        except Exception as exc:
            err_str = str(exc)
            is_rate_limit = "429" in err_str or "RESOURCE_EXHAUSTED" in err_str
            if is_rate_limit and attempt < max_retries - 1:
                wait = 15 * (attempt + 1)   # 15s, 30s, 45s, 60s
                print(f"\r  [rate limit] waiting {wait}s before retry {attempt+2}/{max_retries}…",
                      end="", flush=True)
                time.sleep(wait)
                continue
            result["verdict"] = "ERROR"
            result["error"]   = err_str
            return result

    if verbose:
        print(f"\n{DIM}── LLM output for {tc['id']} ──{RST}")
        clean = {k: v for k, v in repair_plan.items() if k != "_debug_prompt"}
        print(json.dumps(clean, indent=2))

    s_ok, s_note = check_structure(repair_plan)
    d_ok, d_note = check_ddl(repair_plan, tc["expect_ddl_keywords"])
    g_ok, g_note = check_dag(repair_plan, tc["expect_dag"])
    f_ok, f_note = check_graph_field(repair_plan, tc["expect_dag"])
    e_ok, e_note = check_e2e(
        tc["graph"], repair_plan, tc["test_input"], tc["test_output"], tc["event"]["table"]
    )

    result["checks"] = {
        "S_structure":  (s_ok, s_note),
        "D_ddl":        (d_ok, d_note),
        "G_dag":        (g_ok, g_note),
        "F_graph_field":(f_ok, f_note),
        "E_e2e":        (e_ok, e_note),
    }

    passes = sum(1 for ok, _ in result["checks"].values() if ok)
    total  = len(result["checks"])

    if is_adversarial:
        # Adversarial: we only care that LLM attempted something (structure + dag present)
        result["verdict"] = "PASS" if (s_ok and g_ok) else "FAIL"
    elif passes == total:
        result["verdict"] = "PASS"
    elif passes >= 3:
        result["verdict"] = "PARTIAL"
    else:
        result["verdict"] = "FAIL"

    return result


def verdict_label(v):
    return {"PASS": PASS, "PARTIAL": PARTIAL, "FAIL": FAIL, "ERROR": ERR}.get(v, v)


def print_report(results):
    cats   = ["EASY", "MEDIUM", "HARD", "EDGE"]
    totals = {c: {"pass": 0, "partial": 0, "fail": 0, "error": 0, "total": 0} for c in cats}
    all_pass = all_total = 0

    # ── header ─────────────────────────────────────────────────────────────────
    w = 72
    print("\n" + "═" * w)
    print(f"  {BOLD}LLM Schema Repair — Evaluation Report{RST}")
    print(f"  Model: gemini-2.5-flash  │  Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("═" * w)

    for r in results:
        cat = r["category"]
        v   = r["verdict"]
        lbl = verdict_label(v)
        t   = f"{r['llm_time']}s" if r["llm_time"] else "—"
        name_trunc = r["name"][:50]
        print(f"  {r['id']} [{cat:<6}] {lbl}  {name_trunc:<51} {DIM}{t}{RST}")

        # Per-check detail
        if r["verdict"] in ("PARTIAL", "FAIL", "ERROR") or True:
            for ck_name, (ok, note) in r.get("checks", {}).items():
                tick = f"{GRN}✓{RST}" if ok else f"{RED}✗{RST}"
                note_str = f" {DIM}{note}{RST}" if note else ""
                print(f"            [{tick}] {ck_name}{note_str}")
        if r.get("error"):
            print(f"            {RED}Error: {r['error']}{RST}")

        # Tallies
        totals[cat]["total"] += 1
        all_total += 1
        if v == "PASS":
            totals[cat]["pass"] += 1
            all_pass += 1
        elif v == "PARTIAL":
            totals[cat]["partial"] += 1
        elif v == "FAIL":
            totals[cat]["fail"] += 1
        else:
            totals[cat]["error"] += 1

    # ── per-category summary ───────────────────────────────────────────────────
    print("─" * w)
    print(f"  {BOLD}Per-Category Results{RST}")
    for cat in cats:
        t = totals[cat]
        if t["total"] == 0:
            continue
        pct = round(100 * t["pass"] / t["total"])
        bar_filled = int(pct / 5)
        bar = f"{GRN}{'█' * bar_filled}{DIM}{'░' * (20 - bar_filled)}{RST}"
        print(
            f"  {cat:<8} [{bar}] "
            f"{GRN}{t['pass']} pass{RST} "
            f"{YLW}{t['partial']} partial{RST} "
            f"{RED}{t['fail']} fail{RST} "
            f"/ {t['total']}  ({pct}%)"
        )

    # ── overall ────────────────────────────────────────────────────────────────
    overall_pct = round(100 * all_pass / all_total) if all_total else 0
    partial_count = sum(t["partial"] for t in totals.values())
    fail_count    = sum(t["fail"]    for t in totals.values())
    error_count   = sum(t["error"]   for t in totals.values())

    print("═" * w)
    print(f"  {BOLD}Overall:  {all_pass}/{all_total} PASS  "
          f"{partial_count} PARTIAL  {fail_count} FAIL  {error_count} ERROR  "
          f"→  {overall_pct}% full accuracy{RST}")

    # ── check-level breakdown ──────────────────────────────────────────────────
    check_keys = ["S_structure", "D_ddl", "G_dag", "F_graph_field", "E_e2e"]
    check_labels = {
        "S_structure":   "[S] Structure    ",
        "D_ddl":         "[D] DDL correct  ",
        "G_dag":         "[G] DAG correct  ",
        "F_graph_field": "[F] graph_field  ",
        "E_e2e":         "[E] End-to-End   ",
    }
    print(f"\n  {BOLD}Check-Level Accuracy{RST}")
    for ck in check_keys:
        ck_pass  = sum(1 for r in results if r.get("checks", {}).get(ck, (False,))[0])
        ck_total = sum(1 for r in results if ck in r.get("checks", {}))
        if ck_total == 0:
            continue
        pct = round(100 * ck_pass / ck_total)
        bar_filled = int(pct / 5)
        bar = f"{GRN}{'█' * bar_filled}{DIM}{'░' * (20 - bar_filled)}{RST}"
        print(f"  {check_labels[ck]} [{bar}] {ck_pass}/{ck_total}  ({pct}%)")

    print("═" * w + "\n")


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="LLM repair evaluation")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Print full LLM output for each test case")
    parser.add_argument("--tc", metavar="ID",
                        help="Run a single test case, e.g. --tc TC04")
    parser.add_argument("--save", metavar="FILE",
                        help="Save results JSON to file")
    args = parser.parse_args()

    cases = build_test_cases()
    if args.tc:
        cases = [c for c in cases if c["id"].upper() == args.tc.upper()]
        if not cases:
            print(f"Test case '{args.tc}' not found.")
            sys.exit(1)

    results = []
    print(f"\nRunning {len(cases)} test case(s)…  "
          f"{DIM}(each makes one Gemini API call){RST}\n")

    for i, tc in enumerate(cases, 1):
        print(f"  {DIM}[{i}/{len(cases)}] {tc['id']} — {tc['name'][:55]}{RST}", end="", flush=True)
        r = run_case(tc, verbose=args.verbose)
        label = {"PASS": f" {GRN}✓{RST}", "PARTIAL": f" {YLW}~{RST}",
                 "FAIL": f" {RED}✗{RST}", "ERROR": f" {RED}⚡{RST}"}.get(r["verdict"], "")
        print(f"\r  [{i}/{len(cases)}] {tc['id']}{label} — {tc['name'][:55]}")
        results.append(r)
        time.sleep(0.5)   # gentle rate-limit buffer

    print_report(results)

    if args.save:
        serializable = []
        for r in results:
            row = {k: v for k, v in r.items() if k not in ("repair",)}
            if r.get("repair"):
                row["repair_summary"] = r["repair"].get("summary", "")
                row["repair_dag_len"] = len(r["repair"].get("dag", []))
                row["repair_ddl_len"] = len(r["repair"].get("ddl_statements", []))
            serializable.append(row)
        Path(args.save).write_text(json.dumps(serializable, indent=2))
        print(f"Results saved to {args.save}")


if __name__ == "__main__":
    main()
