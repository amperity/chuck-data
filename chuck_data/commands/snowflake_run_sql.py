"""
Command for executing SQL queries directly on Snowflake.
"""

import logging
from typing import Optional, Any

from chuck_data.clients.snowflake import SnowflakeAPIClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.commands.base import CommandResult
from chuck_data.config import get_active_database


def handle_command(
    client: Optional[SnowflakeAPIClient], **kwargs: Any
) -> CommandResult:
    """
    Execute a SQL query on the connected Snowflake account.

    Args:
        client: SnowflakeAPIClient instance
        **kwargs:
            query (str): SQL query to execute
            database (str, optional): Database context (uses active database if not provided)
    """
    if not client:
        return CommandResult(
            False,
            message="No Snowflake client available. Please run /setup first.",
        )

    query = kwargs.get("query")
    if not query:
        return CommandResult(False, message="query parameter is required.")

    database = kwargs.get("database") or get_active_database()

    try:
        result = client.execute_sql(sql=query, database=database)

        records = []
        columns = []
        if result.get("result"):
            rows = result["result"].get("Records", [])
            # Rows from SnowflakeAPIClient are dicts (DictCursor)
            if rows:
                columns = list(rows[0].keys()) if rows else []
                records = rows

        return CommandResult(
            True,
            data={
                "statement_id": result.get("statement_id"),
                "status": result.get("status"),
                "columns": columns,
                "records": records,
                "row_count": len(records),
                "query": query,
            },
            message=f"Query executed successfully. {len(records)} row(s) returned.",
        )
    except Exception as e:
        logging.error(f"Error executing Snowflake SQL: {e}")
        return CommandResult(False, message=f"Query failed: {str(e)}", error=e)


DEFINITION = CommandDefinition(
    name="snowflake_run_sql",
    description="Execute a SQL query on the connected Snowflake account and return results.",
    handler=handle_command,
    parameters={
        "query": {
            "type": "string",
            "description": "SQL query to execute",
        },
        "database": {
            "type": "string",
            "description": "Database context for the query (uses active database if not provided).",
        },
    },
    required_params=["query"],
    tui_aliases=["/run-sql", "/sql", "/sf-sql"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="condensed",
    condensed_action="Running SQL on Snowflake:",
    usage_hint="Usage: /run-sql <query>",
    provider="snowflake",
)
