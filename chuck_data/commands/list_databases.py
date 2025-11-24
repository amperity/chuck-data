"""
Command for listing Redshift databases.
"""

from typing import Optional, Any
from chuck_data.clients.redshift import RedshiftAPIClient
from chuck_data.commands.base import CommandResult
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import get_active_database
import logging


def handle_command(client: Optional[RedshiftAPIClient], **kwargs: Any) -> CommandResult:
    """
    List databases in Redshift.

    Args:
        client: RedshiftAPIClient instance for API calls
        **kwargs: Command parameters
            - display: bool, whether to display the table (default: False)

    Returns:
        CommandResult with list of databases if successful
    """
    if not client:
        return CommandResult(
            False,
            message="No Redshift client available. Please set up your Redshift connection first.",
        )

    # Check if display should be shown (default to False for agent calls)
    display = kwargs.get("display", False)

    # Get current database for highlighting
    current_database = get_active_database()

    try:
        # List databases in Redshift
        databases = client.list_databases()

        if not databases:
            return CommandResult(
                True,
                message="No databases found in this Redshift cluster/workgroup.",
                data={
                    "databases": [],
                    "total_count": 0,
                    "display": display,
                    "current_database": current_database,
                },
            )

        # Format database information for display
        formatted_databases = []
        for db_name in databases:
            formatted_database = {
                "name": db_name,
            }
            formatted_databases.append(formatted_database)

        return CommandResult(
            True,
            data={
                "databases": formatted_databases,
                "total_count": len(formatted_databases),
                "display": display,  # Pass through to display logic
                "current_database": current_database,
            },
            message=f"Found {len(formatted_databases)} database(s).",
        )
    except Exception as e:
        logging.error(f"Error listing databases: {str(e)}")
        return CommandResult(
            False, message=f"Failed to list databases: {str(e)}", error=e
        )


DEFINITION = CommandDefinition(
    name="list_databases",
    description="Lists all databases in the current Redshift cluster/workgroup. By default returns data without showing table. Use display=true when user asks to see databases.",
    handler=handle_command,
    parameters={
        "display": {
            "type": "boolean",
            "description": "Whether to display the database table to the user (default: false). Set to true when user asks to see databases.",
        },
    },
    required_params=[],
    tui_aliases=["/list-databases", "/databases"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="conditional",  # Use conditional display based on display parameter
    display_condition=lambda result: result.get(
        "display", False
    ),  # Show full table only when display=True
    condensed_action="Listing databases",  # Friendly name for condensed display
    usage_hint="Usage: /list-databases [--display true|false]",
    provider="aws_redshift",  # Redshift-specific command
)
