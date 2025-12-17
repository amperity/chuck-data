"""
Command for listing Redshift schemas.
"""

from typing import Optional, Any
from chuck_data.clients.redshift import RedshiftAPIClient
from chuck_data.commands.base import CommandResult
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import get_active_database, get_active_schema
import logging


def handle_command(client: Optional[RedshiftAPIClient], **kwargs: Any) -> CommandResult:
    """
    List schemas in a Redshift database.

    Args:
        client: RedshiftAPIClient instance for API calls
        **kwargs: Command parameters
            - display: bool, whether to display the table (default: False)
            - database: str, database name to list schemas from (optional, uses active database if not provided)

    Returns:
        CommandResult with list of schemas if successful
    """
    if not client:
        return CommandResult(
            False,
            message="No Redshift client available. Please set up your Redshift connection first.",
        )

    # Check if display should be shown (default to False for agent calls)
    display = kwargs.get("display", False)
    database = kwargs.get("database")

    # Get current schema and database for context
    current_schema = get_active_schema()
    current_database = get_active_database()

    # If database not provided, use active database
    if not database:
        database = current_database

    if not database:
        return CommandResult(
            False,
            message="No database specified and no active database set. Please select a database first using /select-database.",
        )

    try:
        # List schemas in the database
        schemas = client.list_schemas(database=database)

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

        # Format schema information for display
        formatted_schemas = []
        for schema_name in schemas:
            formatted_schema = {
                "name": schema_name,
                "database": database,
            }
            formatted_schemas.append(formatted_schema)

        return CommandResult(
            True,
            data={
                "schemas": formatted_schemas,
                "total_count": len(formatted_schemas),
                "display": display,
                "current_schema": current_schema,
                "database": database,
            },
            message=f"Found {len(formatted_schemas)} schema(s) in database '{database}'.",
        )
    except Exception as e:
        logging.error(f"Error listing schemas: {str(e)}")
        return CommandResult(
            False, message=f"Failed to list schemas: {str(e)}", error=e
        )


DEFINITION = CommandDefinition(
    name="list_redshift_schemas",
    description="Lists all schemas in the current Redshift database. By default returns data without showing table. Use display=true when user asks to see schemas.",
    handler=handle_command,
    parameters={
        "display": {
            "type": "boolean",
            "description": "Whether to display the schema table to the user (default: false). Set to true when user asks to see schemas.",
        },
        "database": {
            "type": "string",
            "description": "Database name to list schemas from (uses active database if not provided).",
        },
    },
    required_params=[],
    tui_aliases=["/list-schemas", "/schemas"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="conditional",
    display_condition=lambda result: result.get("display", False),
    condensed_action="Listing schemas",
    usage_hint="Usage: /list-schemas [--display true|false] [--database <database_name>]",
    provider="aws_redshift",  # Redshift-specific command
)
