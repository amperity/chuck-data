"""
Command for listing all SQL warehouses in the Databricks workspace.
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
    Fetch and return a list of all SQL warehouses in the Databricks workspace.

    Args:
        client: DatabricksAPIClient instance for API calls
        **kwargs: No additional parameters required

    Returns:
        CommandResult with list of warehouses if successful
    """
    if not client:
        return CommandResult(
            False,
            message="No Databricks client available. Please set up your workspace first.",
        )

    try:
        # Fetch the list of warehouses
        warehouses = client.list_warehouses()

        if not warehouses:
            return CommandResult(
                True, message="No SQL warehouses found in this workspace."
            )

        # Format the warehouse information for display
        formatted_warehouses = []
        for warehouse in warehouses:
            formatted_warehouse = {
                "id": warehouse.get("id"),
                "name": warehouse.get("name"),
                "size": warehouse.get("size"),
                "state": warehouse.get("state"),
                "creator_name": warehouse.get("creator_name"),
                "auto_stop_mins": warehouse.get("auto_stop_mins", "N/A"),
            }
            formatted_warehouses.append(formatted_warehouse)

        return CommandResult(
            True,
            data={
                "warehouses": formatted_warehouses,
                "total_count": len(formatted_warehouses),
            },
            message=f"Found {len(formatted_warehouses)} SQL warehouse(s).",
        )
    except Exception as e:
        logging.error(f"Error fetching warehouses: {str(e)}")
        return CommandResult(
            False, message=f"Failed to fetch warehouses: {str(e)}", error=e
        )


DEFINITION = CommandDefinition(
    name="list-warehouses",
    description="Lists all SQL warehouses in the current Databricks workspace.",
    handler=handle_command,
    parameters={},  # No parameters needed
    required_params=[],
    tui_aliases=["/list-warehouses", "/warehouses"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="full",  # Show full warehouse list in tables
    usage_hint="Usage: /list-warehouses",
)
