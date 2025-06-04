"""
Command handler for catalog selection.

This module contains the handler for setting the active catalog
for database operations.
"""

import logging
from typing import Optional

from ..clients.databricks import DatabricksAPIClient
from ..command_registry import CommandDefinition
from ..config import set_active_catalog
from .base import CommandResult


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """
    Set the active catalog.

    Args:
        client: API client instance
        **kwargs: catalog_name (str)
    """
    catalog_name: str = kwargs.get("catalog_name")
    if not catalog_name:
        return CommandResult(False, message="catalog_name parameter is required.")

    try:
        catalog_type = "Unknown"
        try:
            from ..catalogs import get_catalog

            catalog_info = get_catalog(client, catalog_name)
            catalog_type = catalog_info.get("type", "Unknown").lower()
        except Exception:
            set_active_catalog(catalog_name)  # Set anyway if verification fails
            return CommandResult(
                True,
                message=f"Warning: Could not verify catalog '{catalog_name}'. Setting anyway.",
                data={"catalog_name": catalog_name, "catalog_type": catalog_type},
            )

        set_active_catalog(catalog_name)
        return CommandResult(
            True,
            message=f"Active catalog is now set to '{catalog_name}' (Type: {catalog_type}).",
            data={"catalog_name": catalog_name, "catalog_type": catalog_type},
        )
    except Exception as e:
        logging.error(f"Failed to set catalog '{catalog_name}': {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="set-catalog",
    description="Set the active catalog for database operations",
    handler=handle_command,
    parameters={
        "catalog_name": {
            "type": "string",
            "description": "Name of the catalog to set as active",
        }
    },
    required_params=["catalog_name"],
    tui_aliases=["/select-catalog"],
    visible_to_user=True,
    visible_to_agent=True,
    condensed_action="Setting catalog",
)
