
# ============================================================
# query_executor.py  —  Simple query runner against local MySQL
# (updated to use config.py instead of hardcoded cloud IP)
# ============================================================

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pymysql
import config


def execute(query: str, source_name: str = "mysql_local"):
    """
    Execute a SQL query against the configured data source.

    Args:
        query       : SQL string to run
        source_name : key in config.DATA_SOURCES (default: mysql_local)

    Returns:
        List of result rows (tuples)
    """
    source = config.DATA_SOURCES[source_name]
    conn   = pymysql.connect(**source["connection"])
    print(f"Connected to {source_name} ({source['connection']['database']})")

    cursor = conn.cursor()
    rows   = []

    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        print(f"Total rows: {len(rows)}")
        for row in rows:
            print(row)
    except Exception as e:
        print("SQL Execution Error:", e)
        raise
    finally:
        cursor.close()
        conn.close()

    return rows


# ── Quick test ─────────────────────────────────────────────

if __name__ == "__main__":
    test_sql = "SELECT * FROM customers LIMIT 5"
    print(f"\nRunning: {test_sql}\n")
    execute(test_sql)
