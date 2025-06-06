"""
Command for listing all SQL warehouses in the Databricks workspace.
"""

from typing import Optional, Any
from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.commands.base import CommandResult
from chuck_data.command_registry import CommandDefinition
import logging


def handle_command(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """
    Fetch and return a list of all SQL warehouses in the Databricks workspace.

    Args:
        client: DatabricksAPIClient instance for API calls
        **kwargs:
            - display: bool, whether to display the table (default: True)

    Returns:
        CommandResult with list of warehouses if successful
    """
    if not client:
        return CommandResult(
            False,
            message="No Databricks client available. Please set up your workspace first.",
        )

    # Check if display should be suppressed (default to False for agent calls)
    display = kwargs.get("display", False)

    # Get current warehouse ID for highlighting
    from chuck_data.config import get_warehouse_id

    current_warehouse_id = get_warehouse_id()

    try:
        # Fetch the list of warehouses
        warehouses = client.list_warehouses()

        if not warehouses:
            return CommandResult(
                True,
                message="No SQL warehouses found in this workspace.",
                data={
                    "warehouses": [],
                    "total_count": 0,
                    "display": display,
                    "current_warehouse_id": current_warehouse_id,
                },
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
                "enable_serverless_compute": warehouse.get(
                    "enable_serverless_compute", False
                ),
                "warehouse_type": warehouse.get("warehouse_type"),
            }
            formatted_warehouses.append(formatted_warehouse)

        return CommandResult(
            True,
            data={
                "warehouses": formatted_warehouses,
                "total_count": len(formatted_warehouses),
                "display": display,  # Pass through to display
                "current_warehouse_id": current_warehouse_id,
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
    description="Lists all SQL warehouses in the current Databricks workspace. By default returns data without showing table. Use display=true when user asks to see warehouses.",
    handler=handle_command,
    parameters={
        "display": {
            "type": "boolean",
            "description": "Whether to display the warehouse table to the user (default: false). Set to true when user asks to see warehouses.",
        }
    },
    required_params=[],
    tui_aliases=["/list-warehouses", "/warehouses"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="conditional",  # Use conditional display based on display parameter
    display_condition=lambda result: result.get(
        "display", False
    ),  # Show full table only when display=True explicitly set
    condensed_action="Listing warehouses",  # Friendly name for condensed display
    usage_hint="Usage: /list-warehouses [--display true|false]",
)
