# ============================================================
# pipeline/config.py
# Central configuration for data sources, Kafka, and LLM settings
# ============================================================

import os

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

CANONICAL_SCHEMA = {
    "event_id": "varchar",
    "symbol": "varchar",
    "price": "float",
    "volume": "int",
    "timestamp": "bigint",
    "exchange": "varchar",
    "currency": "varchar",
}

SCHEMA_DRIFT_SETTINGS = {
    "enabled": True,
    "table_name": "stock_events",
}

LLM_SETTINGS = {
    "provider": "gemini",
    "model": "gemini-2.5-flash",
    "api_key": os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "AIzaSyBPacATSRX0cVbJPN7c8_inqmcg-OiSU2w",
    "temperature": 0.1,
}
