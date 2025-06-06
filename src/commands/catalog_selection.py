"""
Command handler for catalog selection.

This module contains the handler for setting the active catalog
for database operations.
"""

import logging
from typing import Optional
from difflib import SequenceMatcher

from src.clients.databricks import DatabricksAPIClient
from src.command_registry import CommandDefinition
from src.config import set_active_catalog
from .base import CommandResult


def _similarity_score(name1: str, name2: str) -> float:
    """Calculate similarity score between two strings (0.0 to 1.0)."""
    return SequenceMatcher(None, name1.lower().strip(), name2.lower().strip()).ratio()


def _find_best_catalog_match(target_name: str, catalogs: list) -> Optional[dict]:
    """Find the best matching catalog by name using fuzzy matching."""
    best_match = None
    best_score = 0.0
    target_lower = target_name.lower().strip()

    for catalog in catalogs:
        catalog_name = catalog.get("name", "")
        if not catalog_name:
            continue

        catalog_lower = catalog_name.lower().strip()

        # Check for exact match first (case insensitive)
        if catalog_lower == target_lower:
            return catalog

        # Check if target is a substring of catalog name
        if target_lower in catalog_lower or catalog_lower.startswith(target_lower):
            return catalog

        # Calculate similarity score for fuzzy matching
        score = _similarity_score(target_name, catalog_name)
        if score > best_score and score >= 0.4:  # Threshold for fuzzy matching
            best_score = score
            best_match = catalog

    return best_match


def _report_step(message: str, tool_output_callback=None):
    """Report a step in the catalog selection process."""
    if tool_output_callback:
        tool_output_callback("select-catalog", {"step": message})


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """
    Set the active catalog by ID or name.

    Args:
        client: API client instance
        **kwargs: catalog (str) - catalog name, tool_output_callback (optional)
    """
    catalog: str = kwargs.get("catalog")
    tool_output_callback = kwargs.get("tool_output_callback")

    if not catalog:
        return CommandResult(
            False,
            message="catalog parameter is required.",
        )

    identifier = catalog

    if not client:
        return CommandResult(
            False,
            message="No API client available to verify catalog.",
        )

    try:
        target_catalog = None

        # Try to get catalog directly first
        try:
            from src.catalogs import get_catalog

            catalog_obj = get_catalog(client, identifier)
            if catalog_obj:
                target_catalog = catalog_obj
        except Exception:
            # Direct lookup failed - fall back to name matching
            pass

        # If not found directly, search by name
        if not target_catalog:
            _report_step(
                f"Looking for catalog matching '{identifier}'", tool_output_callback
            )

            # Get all catalogs
            from src.catalogs import list_catalogs

            catalogs_result = list_catalogs(client)
            catalogs = catalogs_result.get("catalogs", [])
            if not catalogs:
                return CommandResult(False, message="No catalogs found in workspace.")

            # Find best match by name
            target_catalog = _find_best_catalog_match(identifier, catalogs)

            if not target_catalog:
                return CommandResult(
                    False,
                    message=f"No catalog found matching '{identifier}'. Available catalogs: {', '.join([c.get('name', 'Unknown') for c in catalogs])}",
                )

            # Report the selection
            selected_name = target_catalog.get("name", "Unknown")
            if selected_name.lower().strip() != identifier.lower().strip():
                _report_step(f"Selecting '{selected_name}'", tool_output_callback)
            else:
                _report_step(f"Found catalog '{selected_name}'", tool_output_callback)

        # Set the active catalog
        catalog_name_to_set = target_catalog.get("name")
        catalog_type = target_catalog.get("type", "Unknown")
        catalog_owner = target_catalog.get("owner", "Unknown")

        set_active_catalog(catalog_name_to_set)

        return CommandResult(
            True,
            message=f"Active catalog is now set to '{catalog_name_to_set}' (Type: {catalog_type}, Owner: {catalog_owner}).",
            data={
                "catalog_name": catalog_name_to_set,
                "catalog_type": catalog_type,
                "owner": catalog_owner,
                "step": f"Catalog set - Name: {catalog_name_to_set}",
            },
        )

    except Exception as e:
        logging.error(f"Failed to set catalog: {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="select-catalog",
    description="Set the active catalog for operations by name with fuzzy matching",
    handler=handle_command,
    parameters={
        "catalog": {
            "type": "string",
            "description": "Catalog name to select",
        }
    },
    required_params=["catalog"],
    tui_aliases=["/select-catalog", "/use-catalog"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="condensed",
    condensed_action="Setting catalog:",
    usage_hint="Usage: /select-catalog <catalog_name>",
)
