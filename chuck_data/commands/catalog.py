"""
Command for showing details of a specific Unity Catalog catalog.
"""

from typing import Optional, Any
from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.commands.base import CommandResult
from chuck_data.catalogs import get_catalog as get_catalog_details
from chuck_data.command_registry import CommandDefinition
import logging


def handle_command(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """
    Get details of a specific catalog from Unity Catalog.

    Args:
        client: DatabricksAPIClient instance for API calls
        **kwargs: Command parameters
            - name: Name of the catalog to get details for

    Returns:
        CommandResult with catalog details if successful
    """
    if not client:
        return CommandResult(
            False,
            message="No Databricks client available. Please set up your workspace first.",
        )

    # Extract parameters
    catalog_name = kwargs.get("name")

    try:
        # Get catalog details
        catalog = get_catalog_details(client, catalog_name)

        if not catalog:
            return CommandResult(False, message=f"Catalog '{catalog_name}' not found.")

        return CommandResult(
            True,
            data=catalog,
            message=f"Catalog details for '{catalog_name}' (Type: {catalog.get('type', 'Unknown')}).",
        )
    except Exception as e:
        logging.error(f"Error getting catalog details: {str(e)}")
        return CommandResult(
            False, message=f"Failed to get catalog details: {str(e)}", error=e
        )


DEFINITION = CommandDefinition(
    name="catalog",
    description="Show details of a specific Unity Catalog catalog.",
    handler=handle_command,
    parameters={
        "name": {
            "type": "string",
            "description": "Name of the catalog to get details for.",
        }
    },
    required_params=["name"],
    tui_aliases=["/catalog"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="full",  # Show full catalog details to agents
    usage_hint="Usage: /catalog --name <catalog_name>",
)
