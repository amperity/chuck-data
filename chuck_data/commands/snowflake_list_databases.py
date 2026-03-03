"""
Command for listing Snowflake databases.
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
    List databases visible to the current Snowflake user.

    Args:
        client: SnowflakeAPIClient instance
        **kwargs:
            display (bool): Whether to display the table (default: False)
    """
    if not client:
        return CommandResult(
            False,
            message="No Snowflake client available. Please run /setup first.",
        )

    display = kwargs.get("display", False)
    current_database = get_active_database()

    try:
        result = client.list_databases()
        databases = result.get("databases", [])

        if not databases:
            return CommandResult(
                True,
                message="No databases found for this Snowflake user.",
                data={
                    "databases": [],
                    "total_count": 0,
                    "display": display,
                    "current_database": current_database,
                },
            )

        return CommandResult(
            True,
            data={
                "databases": databases,
                "total_count": len(databases),
                "display": display,
                "current_database": current_database,
            },
            message=f"Found {len(databases)} database(s).",
        )
    except Exception as e:
        logging.error(f"Error listing Snowflake databases: {e}")
        return CommandResult(
            False, message=f"Failed to list databases: {str(e)}", error=e
        )


DEFINITION = CommandDefinition(
    name="snowflake_list_databases",
    description="Lists all Snowflake databases visible to the current user. Use display=true to show the table.",
    handler=handle_command,
    parameters={
        "display": {
            "type": "boolean",
            "description": "Whether to display the database table (default: false).",
        },
    },
    required_params=[],
    tui_aliases=["/list-databases", "/databases", "/sf-databases"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="conditional",
    display_condition=lambda result: result.get("display", False),
    condensed_action="Listing Snowflake databases",
    usage_hint="Usage: /list-databases [--display true|false]",
    provider="snowflake",
)
