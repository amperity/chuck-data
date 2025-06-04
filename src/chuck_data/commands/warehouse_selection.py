"""
Command handler for SQL warehouse selection.

This module contains the handler for setting the active SQL warehouse
for database operations.
"""

import logging
from typing import Optional

from ..clients.databricks import DatabricksAPIClient
from ..command_registry import CommandDefinition
from ..config import set_warehouse_id
from .base import CommandResult


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """
    Set the active SQL warehouse.

    Args:
        client: API client instance
        **kwargs: warehouse_id (str)
    """
    warehouse_id: str = kwargs.get("warehouse_id")
    if not warehouse_id:
        return CommandResult(False, message="warehouse_id parameter is required.")

    try:
        # Verify that the warehouse exists
        if client:
            try:
                warehouse = client.get_warehouse(warehouse_id)
                if not warehouse:
                    return CommandResult(
                        False,
                        message=f"Warehouse '{warehouse_id}' not found.",
                    )
                warehouse_name = warehouse.get("name", "Unknown")
                warehouse_state = warehouse.get("state", "Unknown")
            except Exception:
                # Set anyway if verification fails
                set_warehouse_id(warehouse_id)
                return CommandResult(
                    True,
                    message=f"Warning: Could not verify warehouse '{warehouse_id}'. Setting anyway.",
                    data={"warehouse_id": warehouse_id},
                )
        else:
            # No client available, set without verification
            set_warehouse_id(warehouse_id)
            return CommandResult(
                True,
                message=f"Warning: No API client available to verify warehouse '{warehouse_id}'. Setting anyway.",
                data={"warehouse_id": warehouse_id},
            )

        # Set the active warehouse
        set_warehouse_id(warehouse_id)
        return CommandResult(
            True,
            message=f"Active SQL warehouse is now set to '{warehouse_name}' (ID: {warehouse_id}, State: {warehouse_state}).",
            data={
                "warehouse_id": warehouse_id,
                "warehouse_name": warehouse_name,
                "state": warehouse_state,
            },
        )
    except Exception as e:
        logging.error(f"Failed to set warehouse '{warehouse_id}': {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="select-warehouse",
    description="Set the active SQL warehouse for database operations",
    handler=handle_command,
    parameters={
        "warehouse_id": {
            "type": "string",
            "description": "ID of the SQL warehouse to set as active",
        }
    },
    required_params=["warehouse_id"],
    tui_aliases=["/select-warehouse"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    usage_hint="Usage: /select-warehouse --warehouse_id <warehouse_id>",
)
