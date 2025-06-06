"""
Command handler for schema selection.

This module contains the handler for setting the active schema
for database operations.
"""

import logging
from typing import Optional
from difflib import SequenceMatcher

from src.clients.databricks import DatabricksAPIClient
from src.command_registry import CommandDefinition
from src.config import get_active_catalog, set_active_schema
from src.catalogs import list_schemas as get_schemas_list
from .base import CommandResult


def _similarity_score(name1: str, name2: str) -> float:
    """Calculate similarity score between two strings (0.0 to 1.0)."""
    return SequenceMatcher(None, name1.lower().strip(), name2.lower().strip()).ratio()


def _find_best_schema_match(target_name: str, schemas: list) -> Optional[dict]:
    """Find the best matching schema by name using fuzzy matching."""
    best_match = None
    best_score = 0.0
    target_lower = target_name.lower().strip()

    for schema in schemas:
        schema_name = schema.get("name", "")
        if not schema_name:
            continue

        schema_lower = schema_name.lower().strip()

        # Check for exact match first (case insensitive)
        if schema_lower == target_lower:
            return schema

        # Check if target is a substring of schema name
        if target_lower in schema_lower or schema_lower.startswith(target_lower):
            return schema

        # Calculate similarity score for fuzzy matching
        score = _similarity_score(target_name, schema_name)
        if score > best_score and score >= 0.4:  # Threshold for fuzzy matching
            best_score = score
            best_match = schema

    return best_match


def _report_step(message: str, tool_output_callback=None):
    """Report a step in the schema selection process."""
    if tool_output_callback:
        tool_output_callback("select-schema", {"step": message})


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """
    Set the active schema by ID or name.

    Args:
        client: API client instance
        **kwargs: schema (str) - schema name, tool_output_callback (optional)
    """
    schema: str = kwargs.get("schema")
    tool_output_callback = kwargs.get("tool_output_callback")

    if not schema:
        return CommandResult(
            False,
            message="schema parameter is required.",
        )

    identifier = schema

    if not client:
        return CommandResult(
            False,
            message="No API client available to verify schema.",
        )

    try:
        catalog_name = get_active_catalog()
        if not catalog_name:
            return CommandResult(False, message="No active catalog selected.")

        target_schema = None

        # Try direct verification first
        try:
            from src.catalogs import get_schema

            schema_obj = get_schema(client, f"{catalog_name}.{identifier}")
            if schema_obj:
                target_schema = {"name": identifier}
        except Exception:
            # Direct lookup failed - fall back to name matching
            pass

        # If not found by direct lookup, search by name
        if not target_schema:
            _report_step(
                f"Looking for schema matching '{identifier}'", tool_output_callback
            )

            # Get all schemas
            result = get_schemas_list(client=client, catalog_name=catalog_name)
            schemas = result.get("schemas", [])

            if not schemas:
                return CommandResult(
                    False, message=f"No schemas found in catalog '{catalog_name}'."
                )

            # Find best match by name
            target_schema = _find_best_schema_match(identifier, schemas)

            if not target_schema:
                return CommandResult(
                    False,
                    message=f"No schema found matching '{identifier}'. Available schemas: {', '.join([s.get('name', 'Unknown') for s in schemas])}",
                )

            # Report the selection
            selected_name = target_schema.get("name", "Unknown")
            if selected_name.lower().strip() != identifier.lower().strip():
                _report_step(f"Selecting '{selected_name}'", tool_output_callback)
            else:
                _report_step(f"Found schema '{selected_name}'", tool_output_callback)

        # Set the active schema
        schema_name_to_set = target_schema.get("name")
        owner = target_schema.get("owner", "Unknown")

        set_active_schema(schema_name_to_set)

        return CommandResult(
            True,
            message=f"Active schema is now set to '{schema_name_to_set}' in catalog '{catalog_name}' (Owner: {owner}).",
            data={
                "schema_name": schema_name_to_set,
                "catalog_name": catalog_name,
                "owner": owner,
                "step": f"Schema set - Name: {schema_name_to_set}",
            },
        )

    except Exception as e:
        logging.error(f"Failed to set schema: {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="select-schema",
    description="Set the active schema for operations by name with fuzzy matching",
    handler=handle_command,
    parameters={
        "schema": {
            "type": "string",
            "description": "Schema name to select",
        }
    },
    required_params=["schema"],
    tui_aliases=["/select-schema", "/use-schema"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="condensed",
    condensed_action="Setting schema:",
    usage_hint="Usage: /select-schema <schema_name>",
)
