"""
Command for listing schemas in a Snowflake database.
"""

import logging
from typing import Optional, Any

from chuck_data.clients.snowflake import SnowflakeAPIClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.commands.base import CommandResult
from chuck_data.config import get_active_database, get_active_schema


def handle_command(
    client: Optional[SnowflakeAPIClient], **kwargs: Any
) -> CommandResult:
    """
    List schemas in a Snowflake database.

    Args:
        client: SnowflakeAPIClient instance
        **kwargs:
            display (bool): Whether to display the table (default: False)
            database (str): Database to list schemas from (uses active database if not provided)
    """
    if not client:
        return CommandResult(
            False,
            message="No Snowflake client available. Please run /setup first.",
        )

    display = kwargs.get("display", False)
    database = kwargs.get("database") or get_active_database()
    current_schema = get_active_schema() or "not selected"

    if not database:
        return CommandResult(
            False,
            message="No database specified and no active database set. Use /select-database first.",
        )

    try:
        result = client.list_schemas(database=database)
        schemas = result.get("schemas", [])

        if not schemas:
            return CommandResult(
                True,
                message=f"No schemas found in database '{database}'.",
                data={
                    "schemas": [],
                    "total_count": 0,
                    "display": display,
                    "current_schema": current_schema,
                    "database": database,
                },
            )

        formatted = [{"name": s["name"], "database": database} for s in schemas]
        return CommandResult(
            True,
            data={
                "schemas": formatted,
                "total_count": len(formatted),
                "display": display,
                "current_schema": current_schema,
                "database": database,
            },
            message=f"Found {len(formatted)} schema(s) in database '{database}'.",
        )
    except Exception as e:
        logging.error(f"Error listing Snowflake schemas: {e}")
        return CommandResult(
            False, message=f"Failed to list schemas: {str(e)}", error=e
        )


DEFINITION = CommandDefinition(
    name="snowflake_list_schemas",
    description="Lists all schemas in the active Snowflake database. Use display=true to show the table.",
    handler=handle_command,
    parameters={
        "display": {
            "type": "boolean",
            "description": "Whether to display the schema table (default: false).",
        },
        "database": {
            "type": "string",
            "description": "Database name to list schemas from (uses active database if not provided).",
        },
    },
    required_params=[],
    tui_aliases=["/list-schemas", "/schemas", "/sf-schemas"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="conditional",
    display_condition=lambda result: result.get("display", False),
    condensed_action="Listing Snowflake schemas",
    usage_hint="Usage: /list-schemas [--display true|false] [--database <name>]",
    provider="snowflake",
)
