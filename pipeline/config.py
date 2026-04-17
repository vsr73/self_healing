# ============================================================
# pipeline/config.py
# Central configuration for data sources, Kafka, and LLM settings
# ============================================================

import json as _json
import os
from pathlib import Path as _Path

# managed_tables.json sits next to this package (project root)
_MANAGED_TABLES_PATH = _Path(__file__).resolve().parent.parent / "managed_tables.json"


def get_managed_tables():
    """Return the current managed-tables list from managed_tables.json.

    Hot-reloadable: reads the file every call so producer/consumer loop
    picks up newly created tables without a restart.
    """
    if _MANAGED_TABLES_PATH.exists():
        try:
            return _json.loads(_MANAGED_TABLES_PATH.read_text()).get(
                "managed_tables", ["stock_events"]
            )
        except Exception:
            pass
    return list(SCHEMA_DRIFT_SETTINGS.get("managed_tables", ["stock_events"]))


def save_managed_tables(tables):
    """Persist an updated list of managed tables to managed_tables.json."""
    _MANAGED_TABLES_PATH.write_text(
        _json.dumps({"managed_tables": list(tables)}, indent=4)
    )

DATA_SOURCES = {
    "mysql_stock": {
        "type": "mysql",
        "connection": {
            "host": "localhost",
            "port": 3306,
            "database": "stock_pipeline",
            "user": "root",
            "password": "root",
        },
    }
}

KAFKA_SETTINGS = {
    "bootstrap_servers": ["localhost:9092"],
    "topics": {
        "raw": "stock_raw_v3",
        "clean": "stock_clean_v3",
        "logs": "schema_logs_v3",
    },
    "consumer_group": "stock_pipeline_consumer_v3",
    "auto_offset_reset": "latest",
}

PIPELINE_DEFAULTS = {
    "generator_interval_seconds": 1.0,
    "default_exchange": "NASDAQ",
    "default_currency": "USD",
}

TABLE_SCHEMAS = {
    "stock_events": {
        "event_id": "varchar",
        "symbol": "varchar",
        "price": "float",
        "volume": "int",
        "timestamp": "bigint",
        "exchange": "varchar",
        "currency": "varchar",
    },
    # Add more tables here, e.g.:
    # "user_profiles": {"user_id": "varchar", "full_name": "varchar", ...},
}

# Backward-compat alias — existing imports of CANONICAL_SCHEMA continue to work
CANONICAL_SCHEMA = TABLE_SCHEMAS["stock_events"]

SCHEMA_DRIFT_SETTINGS = {
    "enabled": True,
    "managed_tables": ["stock_events"],
    # Legacy shim — remove once all callers use managed_tables
    "table_name": "stock_events",
}

LLM_SETTINGS = {
    "provider": "gemini",
    "model": "gemini-1.5-flash",
    "api_key": os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "AQ.Ab8RN6Lonfn4Z8EjGspvwPYyG08JbQfFi5PE6w-1XagNcon-cQ",
    "temperature": 0.1,
}
