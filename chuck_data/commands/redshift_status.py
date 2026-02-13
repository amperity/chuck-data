"""
Command handler for displaying Redshift system status.

This module contains the handler for showing the current status
of Redshift connection, active database, schema, and configuration.
"""

import logging
from typing import Optional

from chuck_data.clients.redshift import RedshiftAPIClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import (
    get_active_database,
    get_active_schema,
    get_active_model,
    get_aws_region,
    get_redshift_cluster_identifier,
    get_redshift_workgroup_name,
)
from .base import CommandResult


def handle_command(client: Optional[RedshiftAPIClient], **kwargs) -> CommandResult:
    """
    Show current status of Redshift connection, database, schema, and configuration.

    Args:
        client: RedshiftAPIClient instance
        **kwargs: display (optional) - whether to display full status details (default: False for agent)
    """
    # Check if display should be shown (default to False for agent calls)
    display = kwargs.get("display", False)

    try:
        # Get Redshift configuration
        region = get_aws_region()
        cluster_id = get_redshift_cluster_identifier()
        workgroup_name = get_redshift_workgroup_name()

        # Determine the Redshift resource identifier
        if workgroup_name:
            redshift_resource = f"Workgroup: {workgroup_name}"
        elif cluster_id:
            redshift_resource = f"Cluster: {cluster_id}"
        else:
            redshift_resource = "Not configured"

        data = {
            "region": region or "Not configured",
            "redshift_resource": redshift_resource,
            "active_database": get_active_database() or "Not set",
            "active_schema": get_active_schema() or "Not set",
            "active_model": get_active_model() or "Not set",
            "connection_status": "Client not available or not initialized.",
            "display": display,
        }

        if client:
            try:
                # Test connection by attempting to list databases
                databases_result = client.list_databases()
                databases = databases_result.get("databases", [])
                data["connection_status"] = (
                    f"Connected (found {len(databases)} database(s))."
                )
            except Exception as e_client:
                data["connection_status"] = f"Client connection error: {str(e_client)}"
                logging.warning(f"Status check client error: {e_client}")

        return CommandResult(True, data=data)
    except Exception as e:
        logging.error(f"Failed to get status: {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="redshift_status",
    description="Show current status of Redshift connection, active database, schema, and configuration.",
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
    provider="aws_redshift",  # Redshift-specific status command
)
