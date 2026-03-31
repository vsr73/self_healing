import pymysql

from pipeline.config import DATA_SOURCES


def get_mysql_source(source_name="mysql_stock"):
    source = DATA_SOURCES[source_name]
    if source["type"] != "mysql":
        raise ValueError(f"Source {source_name} is not a MySQL source.")
    return source


def connect_mysql(source_name="mysql_stock"):
    source = get_mysql_source(source_name)
    cfg = source["connection"]
    return pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
        autocommit=True,
    )


def execute_mysql_ddl(ddl, source_name="mysql_stock"):
    connection = connect_mysql(source_name)
    try:
        with connection.cursor() as cursor:
            cursor.execute(ddl)
    finally:
        connection.close()


def fetch_mysql_table_schema(table_name, source_name="mysql_stock"):
    connection = connect_mysql(source_name)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = %s
                ORDER BY ordinal_position
                """,
                (table_name,),
            )
            return {column_name: datatype for column_name, datatype in cursor.fetchall()}
    finally:
        connection.close()


def insert_mysql_row(table_name, row, source_name="mysql_stock"):
    if not row:
        return

    columns = list(row.keys())
    placeholders = ", ".join(["%s"] * len(columns))
    column_sql = ", ".join(columns)
    sql = f"INSERT INTO {table_name} ({column_sql}) VALUES ({placeholders})"

    connection = connect_mysql(source_name)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, [row[column] for column in columns])
    finally:
        connection.close()
