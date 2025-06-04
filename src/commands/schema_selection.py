"""
Command handler for schema selection.

This module contains the handler for setting the active schema
for database operations.
"""

import logging
from typing import Optional

from src.clients.databricks import DatabricksAPIClient
from src.command_registry import CommandDefinition
from src.config import get_active_catalog, set_active_schema
from .base import CommandResult


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """
    Set the active schema.

    Args:
        client: API client instance
        **kwargs: schema_name (str)
    """
    schema_name: str = kwargs.get("schema_name")
    if not schema_name:
        return CommandResult(False, message="schema_name parameter is required.")

    try:
        catalog_name = get_active_catalog()
        if not catalog_name:
            return CommandResult(False, message="No active catalog selected.")

        try:
            from src.catalogs import get_schema

            get_schema(client, f"{catalog_name}.{schema_name}")
        except Exception:
            set_active_schema(schema_name)  # Set anyway
            return CommandResult(
                True,
                message=f"Warning: Could not verify schema '{schema_name}' in catalog '{catalog_name}'. Setting anyway.",
                data={"schema_name": schema_name, "catalog_name": catalog_name},
            )

        set_active_schema(schema_name)
        return CommandResult(
            True,
            message=f"Active schema is now set to '{schema_name}' in catalog '{catalog_name}'.",
            data={"schema_name": schema_name, "catalog_name": catalog_name},
        )
    except Exception as e:
        logging.error(f"Failed to set schema '{schema_name}': {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="set-schema",
    description="Set the active schema for database operations",
    handler=handle_command,
    parameters={
        "schema_name": {
            "type": "string",
            "description": "Name of the schema to set as active",
        }
    },
    required_params=["schema_name"],
    tui_aliases=["/select-schema"],
    visible_to_user=True,
    visible_to_agent=True,
    condensed_action="Setting schema",
)
