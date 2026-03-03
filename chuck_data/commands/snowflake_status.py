"""
Command handler for displaying Snowflake connection status.
"""

import logging
from typing import Optional

from chuck_data.clients.snowflake import SnowflakeAPIClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import (
    get_active_database,
    get_active_schema,
    get_active_model,
    get_snowflake_account,
    get_snowflake_user,
    get_snowflake_warehouse,
    get_snowflake_role,
)
from .base import CommandResult


def handle_command(client: Optional[SnowflakeAPIClient], **kwargs) -> CommandResult:
    """
    Show current status of Snowflake connection, database, schema, and configuration.

    Args:
        client: SnowflakeAPIClient instance
        **kwargs: display (optional) - whether to display full status details
    """
    display = kwargs.get("display", False)

    try:
        account = get_snowflake_account()
        user = get_snowflake_user()
        warehouse = get_snowflake_warehouse()
        role = get_snowflake_role()

        data = {
            "account": account or "Not configured",
            "user": user or "Not configured",
            "warehouse": warehouse or "Not configured",
            "role": role or "default",
            "active_database": get_active_database() or "Not set",
            "active_schema": get_active_schema() or "Not set",
            "active_model": get_active_model() or "Not set",
            "connection_status": "Client not available or not initialized.",
            "display": display,
        }

        if client:
            try:
                is_connected = client.validate_connection()
                data["connection_status"] = (
                    "Connected" if is_connected else "Connection failed"
                )
            except Exception as e:
                data["connection_status"] = f"Connection error: {str(e)}"
                logging.warning(f"Snowflake status check error: {e}")

        return CommandResult(True, data=data)

    except Exception as e:
        logging.error(f"Failed to get Snowflake status: {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="snowflake_status",
    description="Show current status of the Snowflake connection, active database, schema, and configuration.",
    handler=handle_command,
    parameters={
        "display": {
            "type": "boolean",
            "description": "Whether to display the full status details (default: false).",
        },
    },
    required_params=[],
    tui_aliases=["/snowflake-status", "/sf-status"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="condensed",
    condensed_action="Checking Snowflake status",
    usage_hint="Usage: /snowflake-status [--display true|false]",
    provider="snowflake",
)
