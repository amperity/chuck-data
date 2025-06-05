"""
Command handler for SQL warehouse selection.

This module contains the handler for setting the active SQL warehouse
for database operations.
"""

import logging
from typing import Optional
from difflib import SequenceMatcher

from src.clients.databricks import DatabricksAPIClient
from src.command_registry import CommandDefinition
from src.config import set_warehouse_id
from .base import CommandResult


def _similarity_score(name1: str, name2: str) -> float:
    """Calculate similarity score between two strings (0.0 to 1.0)."""
    return SequenceMatcher(None, name1.lower().strip(), name2.lower().strip()).ratio()


def _find_best_warehouse_match(target_name: str, warehouses: list) -> Optional[dict]:
    """Find the best matching warehouse by name using fuzzy matching."""
    best_match = None
    best_score = 0.0
    target_lower = target_name.lower().strip()

    for warehouse in warehouses:
        warehouse_name = warehouse.get("name", "")
        if not warehouse_name:
            continue

        warehouse_lower = warehouse_name.lower().strip()

        # Check for exact match first (case insensitive)
        if warehouse_lower == target_lower:
            return warehouse

        # Check if target is a substring of warehouse name (e.g., "Test" in "Test Warehouse")
        if target_lower in warehouse_lower or warehouse_lower.startswith(target_lower):
            return warehouse

        # Calculate similarity score for fuzzy matching
        score = _similarity_score(target_name, warehouse_name)
        if score > best_score and score >= 0.4:  # Lower threshold for fuzzy matching
            best_score = score
            best_match = warehouse

    return best_match


def _report_step(message: str, tool_output_callback=None):
    """Report a step in the warehouse selection process."""
    if tool_output_callback:
        # This is being called by the agent - report the step
        tool_output_callback("select-warehouse", {"step": message})


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """
    Set the active SQL warehouse by ID or name.

    Args:
        client: API client instance
        **kwargs: warehouse (str) - warehouse ID or name, tool_output_callback (optional)
    """
    warehouse: str = kwargs.get("warehouse")
    tool_output_callback = kwargs.get("tool_output_callback")

    # Must provide warehouse parameter
    if not warehouse:
        return CommandResult(
            False,
            message="warehouse parameter is required.",
        )

    # Use the provided warehouse value as the identifier
    identifier = warehouse

    if not client:
        return CommandResult(
            False,
            message="No API client available to verify warehouse.",
        )

    try:
        target_warehouse = None

        # Always try as ID first - if it fails for ANY reason, fall back to name matching
        try:
            warehouse = client.get_warehouse(identifier)
            if warehouse:
                target_warehouse = warehouse
        except Exception:
            # ID lookup failed (404, 400, network error, etc.) - fall back to name matching
            pass

        # If not found by ID, search by name
        if not target_warehouse:
            _report_step(
                f"Looking for warehouse matching '{identifier}'", tool_output_callback
            )

            # Get all warehouses
            warehouses = client.list_warehouses()
            if not warehouses:
                return CommandResult(False, message="No warehouses found in workspace.")

            # Find best match by name
            target_warehouse = _find_best_warehouse_match(identifier, warehouses)

            if not target_warehouse:
                return CommandResult(
                    False,
                    message=f"No warehouse found matching '{identifier}'. Available warehouses: {', '.join([w.get('name', 'Unknown') for w in warehouses])}",
                )

            # Report the selection for agent context
            selected_name = target_warehouse.get("name", "Unknown")
            if selected_name.lower().strip() != identifier.lower().strip():
                _report_step(f"Selecting '{selected_name}'", tool_output_callback)
            else:
                # Even for exact matches, report that we found it
                _report_step(f"Found warehouse '{selected_name}'", tool_output_callback)

        # Set the active warehouse
        warehouse_id_to_set = target_warehouse.get("id")
        warehouse_display_name = target_warehouse.get("name", "Unknown")
        warehouse_state = target_warehouse.get("state", "Unknown")

        set_warehouse_id(warehouse_id_to_set)

        return CommandResult(
            True,
            message=f"Active SQL warehouse is now set to '{warehouse_display_name}' (ID: {warehouse_id_to_set}, State: {warehouse_state}).",
            data={
                "warehouse_id": warehouse_id_to_set,
                "warehouse_name": warehouse_display_name,
                "state": warehouse_state,
                "step": f"Warehouse set (ID: {warehouse_id_to_set})",  # Include step in final result
            },
        )

    except Exception as e:
        logging.error(f"Failed to set warehouse: {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="select-warehouse",
    description="Set the active SQL warehouse for database operations by ID or name. Supports fuzzy name matching.",
    handler=handle_command,
    parameters={
        "warehouse": {
            "type": "string",
            "description": "SQL warehouse ID or name to set as active. Supports fuzzy matching for names.",
        },
    },
    required_params=["warehouse"],
    tui_aliases=["/select-warehouse"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="condensed",  # Use condensed display to avoid pagination issues
    condensed_action="Setting warehouse",
    usage_hint="Usage: /select-warehouse --warehouse <id_or_name>",
)
