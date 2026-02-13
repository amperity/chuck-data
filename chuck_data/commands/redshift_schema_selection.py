"""
Command handler for Redshift schema selection.

This module contains the handler for setting the active schema
for Redshift operations.
"""

import logging
from typing import Optional
from difflib import SequenceMatcher

from chuck_data.clients.redshift import RedshiftAPIClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import get_config_manager, get_active_database
from .base import CommandResult


def _similarity_score(name1: str, name2: str) -> float:
    """Calculate similarity score between two strings (0.0 to 1.0)."""
    return SequenceMatcher(None, name1.lower().strip(), name2.lower().strip()).ratio()


def _find_best_schema_match(target_name: str, schemas: list) -> Optional[str]:
    """Find the best matching schema by name using fuzzy matching."""
    best_match = None
    best_score = 0.0
    target_lower = target_name.lower().strip()

    for schema_name in schemas:
        if not schema_name:
            continue

        schema_lower = schema_name.lower().strip()

        # Check for exact match first (case insensitive)
        if schema_lower == target_lower:
            return schema_name

        # Check if target is a substring of schema name
        if target_lower in schema_lower or schema_lower.startswith(target_lower):
            return schema_name

        # Calculate similarity score for fuzzy matching
        score = _similarity_score(target_name, schema_name)
        if score > best_score and score >= 0.4:  # Threshold for fuzzy matching
            best_score = score
            best_match = schema_name

    return best_match


def _report_step(message: str, callback):
    """Report a step to the callback if provided."""
    if callback:
        callback("select_schema", {"step": message})


def handle_command(
    client: Optional[RedshiftAPIClient],
    schema: str = "",
    tool_output_callback=None,
    **kwargs,
) -> CommandResult:
    """
    Set the active schema for Redshift operations.

    Args:
        client: RedshiftAPIClient instance
        schema: Schema name to select
        tool_output_callback: Optional callback for reporting progress
        **kwargs: Additional arguments

    Returns:
        CommandResult indicating success or failure
    """
    if not schema:
        return CommandResult(
            False,
            message="Schema name is required. Usage: /select-schema <schema_name>",
        )

    if not client:
        return CommandResult(
            False,
            message="No Redshift client available to verify schema.",
        )

    try:
        # Get active database
        database = get_active_database()
        if not database:
            return CommandResult(
                False,
                message="No active database set. Please select a database first using /select-database.",
            )

        # List all schemas and find the best match
        _report_step(
            f"Looking for schema matching '{schema}' in database '{database}'",
            tool_output_callback,
        )

        schemas_result = client.list_schemas(database=database)
        schema_dicts = schemas_result.get("schemas", [])
        schemas = [s.get("name") for s in schema_dicts]

        if not schemas:
            return CommandResult(
                False,
                message=f"No schemas found in database '{database}'.",
            )

        # Find best match by name
        target_schema = _find_best_schema_match(schema, schemas)

        if not target_schema:
            # Format available schemas with truncation
            if len(schemas) <= 5:
                available_text = ", ".join(schemas)
            else:
                first_five = ", ".join(schemas[:5])
                remaining_count = len(schemas) - 5
                available_text = f"{first_five} ... and {remaining_count} more"

            return CommandResult(
                False,
                message=f"No schema found matching '{schema}' in database '{database}'. Available schemas: {available_text}",
            )

        # Report the selection
        if target_schema.lower().strip() != schema.lower().strip():
            _report_step(
                f"Selecting schema '{target_schema}' in database '{database}'",
                tool_output_callback,
            )
        else:
            _report_step(
                f"Found schema '{target_schema}' in database '{database}'",
                tool_output_callback,
            )

        # Set the active schema in config
        config_manager = get_config_manager()
        success = config_manager.update(active_schema=target_schema)

        if not success:
            return CommandResult(
                False,
                message=f"Failed to set active schema to '{target_schema}'.",
            )

        return CommandResult(
            True,
            message=f"Active schema is now set to '{target_schema}' in database '{database}'.",
            data={
                "schema_name": target_schema,
                "database": database,
                "step": f"Schema set - Name: {target_schema}, Database: {database}",
            },
        )

    except Exception as e:
        logging.error(f"Failed to set schema: {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="select_redshift_schema",
    description="Set the active schema for Redshift operations by name with fuzzy matching",
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
    provider="aws_redshift",  # Redshift-specific command
)
