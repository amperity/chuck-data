"""
Command for creating a new SQL warehouse in Databricks.
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
    Create a new SQL warehouse in the Databricks workspace.

    Args:
        client: DatabricksAPIClient instance for API calls
        **kwargs: Command parameters
            - name: Name for the new warehouse
            - size: Size of the warehouse (e.g., "2X-Small", "Small", "Medium", "Large")
            - auto_stop_mins: Minutes of inactivity before the warehouse stops automatically
            - min_num_clusters: Min number of clusters for serverless warehouses (optional)
            - max_num_clusters: Max number of clusters for serverless warehouses (optional)

    Returns:
        CommandResult with created warehouse details if successful
    """
    if not client:
        return CommandResult(
            False,
            message="No Databricks client available. Please set up your workspace first.",
        )

    # Extract parameters
    name = kwargs.get("name")
    size = kwargs.get("size")
    auto_stop_mins = kwargs.get("auto_stop_mins")
    min_num_clusters = kwargs.get("min_num_clusters")
    max_num_clusters = kwargs.get("max_num_clusters")

    # Prepare warehouse configuration
    warehouse_config = {
        "name": name,
        "size": size,
        "auto_stop_mins": auto_stop_mins,
    }

    # Add optional cluster configuration if provided
    if min_num_clusters is not None:
        warehouse_config["min_num_clusters"] = min_num_clusters

    if max_num_clusters is not None:
        warehouse_config["max_num_clusters"] = max_num_clusters

    try:
        # Create the warehouse
        result = client.create_warehouse(warehouse_config)

        return CommandResult(
            True,
            data=result,
            message=f"Successfully created SQL warehouse '{name}' (ID: {result.get('id')}).",
        )
    except Exception as e:
        logging.error(f"Error creating warehouse: {str(e)}")
        return CommandResult(
            False, message=f"Failed to create warehouse: {str(e)}", error=e
        )


DEFINITION = CommandDefinition(
    name="create-warehouse",
    description="Create a new SQL warehouse in the Databricks workspace.",
    handler=handle_command,
    parameters={
        "name": {"type": "string", "description": "Name for the new warehouse."},
        "size": {
            "type": "string",
            "description": "Size of the warehouse (e.g., '2X-Small', 'Small', 'Medium', 'Large').",
            "default": "Small",
        },
        "auto_stop_mins": {
            "type": "integer",
            "description": "Minutes of inactivity before the warehouse stops automatically.",
            "default": 120,
        },
        "min_num_clusters": {
            "type": "integer",
            "description": "Min number of clusters for serverless warehouses. (Optional)",
        },
        "max_num_clusters": {
            "type": "integer",
            "description": "Max number of clusters for serverless warehouses. (Optional)",
        },
    },
    required_params=["name"],
    tui_aliases=["/create-warehouse"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    usage_hint="Usage: /create-warehouse --name <name> --size <size> --auto_stop_mins <minutes>",
)
