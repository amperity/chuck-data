"""
Command for showing details of a specific SQL warehouse in Databricks.
"""

from typing import Optional, Any
from src.clients.databricks import DatabricksAPIClient
from src.commands.base import CommandResult
from src.command_registry import CommandDefinition
import logging


def handle_command(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """
    Fetch and return details of a specific SQL warehouse in the Databricks workspace.

    Args:
        client: DatabricksAPIClient instance for API calls
        **kwargs: Command parameters
            - warehouse_id: ID of the warehouse to get details for

    Returns:
        CommandResult with warehouse details if successful
    """
    if not client:
        return CommandResult(
            False,
            message="No Databricks client available. Please set up your workspace first.",
        )

    warehouse_id = kwargs.get("warehouse_id")

    try:
        # Fetch the warehouse details
        warehouse = client.get_warehouse(warehouse_id)

        if not warehouse:
            return CommandResult(
                False, message=f"Warehouse with ID '{warehouse_id}' not found."
            )

        # Return the full warehouse details
        return CommandResult(
            True,
            data=warehouse,
            message=f"Found details for warehouse '{warehouse.get('name')}' (ID: {warehouse_id}).",
        )
    except Exception as e:
        logging.error(f"Error fetching warehouse details: {str(e)}")
        return CommandResult(
            False, message=f"Failed to fetch warehouse details: {str(e)}", error=e
        )


DEFINITION = CommandDefinition(
    name="warehouse",
    description="Show details of a specific SQL warehouse in the Databricks workspace.",
    handler=handle_command,
    parameters={
        "warehouse_id": {
            "type": "string",
            "description": "ID of the warehouse to get details for.",
        }
    },
    required_params=["warehouse_id"],
    tui_aliases=["/warehouse", "/warehouse-details"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    usage_hint="Usage: /warehouse --warehouse_id <warehouse_id>",
)
