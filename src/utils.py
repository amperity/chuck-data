"""
Utility functions for Chuck TUI.
"""

import time
import logging


def build_query_params(params):
    """
    Build URL query parameters string from a dictionary.

    Args:
        params: Dictionary of parameter names and values

    Returns:
        URL query string starting with '?' if parameters exist, empty string otherwise
    """
    if not params:
        return ""

    # Filter out None values
    filtered_params = {k: v for k, v in params.items() if v is not None}

    # Convert boolean values to strings
    for key, value in filtered_params.items():
        if isinstance(value, bool):
            filtered_params[key] = "true" if value else "false"
        else:
            filtered_params[key] = str(value)

    # Build query string
    query_string = "&".join(
        [f"{key}={value}" for key, value in filtered_params.items()]
    )
    return f"?{query_string}" if query_string else ""


def execute_sql_statement(
    client, warehouse_id, sql_text, catalog=None, wait_timeout="5s"
):
    """
    Execute SQL statement and poll for results.

    Args:
        client: DatabricksAPIClient instance
        warehouse_id: ID of the SQL warehouse
        sql_text: SQL query text
        catalog: Optional catalog name
        wait_timeout: Wait timeout value

    Returns:
        SQL statement result data
    """
    # Prepare request data
    data = {
        "on_wait_timeout": "CONTINUE",
        "statement": sql_text,
        "wait_timeout": wait_timeout,
        "warehouse_id": warehouse_id,
    }

    if catalog:
        data["catalog"] = catalog

    logging.debug(f"Executing SQL: {sql_text}")

    # Submit the SQL statement
    response = client.post("/api/2.0/sql/statements", data)
    statement_id = response.get("statement_id")

    # Poll until complete
    state = None
    while True:
        status = client.get(f"/api/2.0/sql/statements/{statement_id}")
        state = status.get("status", {}).get("state", status.get("state"))
        if state not in ["PENDING", "RUNNING"]:
            break
        time.sleep(1)

    # Check result
    if state != "SUCCEEDED":
        error = status.get("status", {}).get("error", status.get("error", {}))
        message = error.get("message", "Unknown error")
        raise ValueError(f"SQL statement failed: {message}")

    return status.get("result", {})
