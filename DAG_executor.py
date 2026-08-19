
# ============================================================
# DAG_executor.py  —  Utility: apply a repair DAG to metadata
# (kept for backward compatibility; schema_repair_engine
#  uses apply_dag directly now)
# ============================================================


def apply_dag_to_metadata(metadata_graph: dict, dag: list) -> dict:
    """
    Apply a list of schema change operations to the metadata graph.

    Supported operations:
      ADD_TABLE, ADD_COLUMN, DROP_COLUMN, RENAME_COLUMN, ALTER_COLUMN_TYPE
    """
    nodes    = metadata_graph["nodes"]
    node_map = {n["id"]: n for n in nodes}

    for step in dag:

        op       = step.get("operation")
        table    = step.get("table")
        column   = step.get("column")
        datatype = step.get("datatype")

        if op == "ADD_TABLE":
            if table and table not in node_map:
                node = {"id": table, "type": "table", "edges": []}
                nodes.append(node)
                node_map[table] = node

        elif op == "ADD_COLUMN":
            column_id = f"{table}.{column}"
            if column_id not in node_map:
                node = {
                    "id":       column_id,
                    "type":     "column",
                    "edges":    [],
                    "table":    table,
                    "datatype": datatype,
                    "nullable": "YES"
                }
                nodes.append(node)
                node_map[column_id] = node
                if table in node_map:
                    node_map[table]["edges"].append({
                        "relation": "HAS_COLUMN",
                        "target":   column_id
                    })

        elif op == "DROP_COLUMN":
            column_id = f"{table}.{column}"
            if column_id in node_map:
                nodes.remove(node_map[column_id])
                del node_map[column_id]

        elif op == "RENAME_COLUMN":
            new_name  = step.get("new_name") or step.get("new_column")
            old_id    = f"{table}.{column}"
            new_id    = f"{table}.{new_name}"
            if old_id in node_map:
                node        = node_map[old_id]
                node["id"]  = new_id
                node_map[new_id] = node
                del node_map[old_id]

        elif op == "ALTER_COLUMN_TYPE":
            column_id = f"{table}.{column}"
            if column_id in node_map:
                node_map[column_id]["datatype"] = datatype

    return metadata_graph
