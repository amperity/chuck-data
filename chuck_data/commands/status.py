"""
Command handler for displaying system status.

This module contains the handler for showing the current status
of workspace, catalog, schema, model, and API permissions.
"""

import logging
from typing import Optional

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import (
    get_workspace_url,
    get_active_catalog,
    get_active_schema,
    get_active_model,
    get_warehouse_id,
)
from chuck_data.databricks.permission_validator import validate_all_permissions
from .base import CommandResult


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """
    Show current status of workspace, catalog, schema, model, and API permissions.

    Args:
        client: API client instance
        **kwargs: display (optional) - whether to display full status details (default: False for agent)
    """
    # Check if display should be shown (default to False for agent calls)
    display = kwargs.get("display", False)

    try:
        data = {
            "workspace_url": get_workspace_url(),
            "active_catalog": get_active_catalog(),
            "active_schema": get_active_schema(),
            "active_model": get_active_model(),
            "warehouse_id": get_warehouse_id(),
            "connection_status": "Client not available or not initialized.",
            "permissions": {},
            "display": display,
        }
        if client:
            try:
                # Add client.validate_token() if such method exists and is desired here
                # For now, just assume if client exists, basic connection might be possible.
                data["connection_status"] = "Connected (client present)."
                # Note: validate_all_permissions requires an active, valid client.
                # It might fail if token is bad, which is fine for status.
                data["permissions"] = validate_all_permissions(client)
                # To be more precise on token validity, client.validate_token() would be good.
            except Exception as e_client:
                data["connection_status"] = (
                    f"Client connection/permission error: {str(e_client)}"
                )
                logging.warning(f"Status check client error: {e_client}")
        return CommandResult(True, data=data)
    except Exception as e:
        logging.error(f"Failed to get status: {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="status",
    description="Show current status of the configured workspace, catalog, schema, model, and API permissions. Used to verify connection and permissions.",
    handler=handle_command,
    parameters={
        "display": {
            "type": "boolean",
            "description": "Whether to display the full status details to the user (default: false). Set to true when user asks to see status details.",
        },
    },
    required_params=[],
    tui_aliases=[
        "/status",
    ],
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="conditional",  # Use conditional display based on display parameter
    display_condition=lambda result: result.get(
        "display", False
    ),  # Show full status only when display=True
    condensed_action="Getting status",  # Friendly name for condensed display
)
