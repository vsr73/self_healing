
# ============================================================
# config.py  —  Central configuration for the pipeline
# ============================================================
# ⚠️  EDIT ONLY THIS FILE to change DB credentials / API keys

GEMINI_API_KEY = "YOUR_KEY_HERE"

DATA_SOURCES = {

    "mysql_local": {

        "type": "mysql",

        "connection": {
            "host": "localhost",
            "port": 3306,
            "database": "pipeline_source_mysql",
            "user": "root",
            "password": "root"
        },

        "logs": {
            "table": "mysql.general_log",
            "time_column": "event_time",
            "query_column": "argument"
        }
    }

    # ── Uncomment to add PostgreSQL cloud source ──────────────
    # "postgres_cloud": {
    #     "type": "postgres",
    #     "connection": {
    #         "host": "34.100.232.158",
    #         "port": 5432,
    #         "database": "postgres",
    #         "user": "postgres",
    #         "password": "Pipeline@123"
    #     },
    #     "logs": {
    #         "table": "schema_change_log",
    #         "time_column": "event_time",
    #         "query_column": "executed_query"
    #     }
    # },
}
