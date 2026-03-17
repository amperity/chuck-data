"""
Command handler for Snowflake schema selection.
"""

import logging
from typing import Optional
from difflib import SequenceMatcher

from chuck_data.clients.snowflake import SnowflakeAPIClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import get_config_manager, get_active_database
from .base import CommandResult


def _find_best_match(target: str, names: list) -> Optional[str]:
    """Fuzzy-match target against a list of names."""
    target_lower = target.lower().strip()
    best_match = None
    best_score = 0.0
    for name in names:
        if not name:
            continue
        name_lower = name.lower().strip()
        if name_lower == target_lower or name_lower.startswith(target_lower):
            return name
        score = SequenceMatcher(None, target_lower, name_lower).ratio()
        if score > best_score and score >= 0.4:
            best_score = score
            best_match = name
    return best_match


def handle_command(client: Optional[SnowflakeAPIClient], **kwargs) -> CommandResult:
    """
    Set the active Snowflake schema.

    Args:
        client: SnowflakeAPIClient instance
        **kwargs: schema (str), tool_output_callback (optional)
    """
    schema = kwargs.get("schema")
    if not schema:
        return CommandResult(
            False,
            message="Schema name is required. Usage: /select-schema <schema_name>",
        )

    if not client:
        return CommandResult(False, message="No Snowflake client available.")

    database = get_active_database()
    if not database:
        return CommandResult(
            False,
            message="No active database set. Use /select-database first.",
        )

    try:
        result = client.list_schemas(database=database)
        schema_names = [s["name"] for s in result.get("schemas", [])]

        if not schema_names:
            return CommandResult(
                False, message=f"No schemas found in database '{database}'."
            )

        target = _find_best_match(schema, schema_names)
        if not target:
            available = ", ".join(schema_names[:5])
            if len(schema_names) > 5:
                available += f" ... and {len(schema_names) - 5} more"
            return CommandResult(
                False,
                message=f"No schema matching '{schema}' in database '{database}'. Available: {available}",
            )

        get_config_manager().update(active_schema=target)
        return CommandResult(
            True,
            message=f"Active schema is now set to '{target}' in database '{database}'.",
            data={"schema_name": target, "database": database},
        )
    except Exception as e:
        logging.error(f"Failed to set Snowflake schema: {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="snowflake_select_schema",
    description="Set the active Snowflake schema by name (fuzzy matching supported).",
    handler=handle_command,
    parameters={
        "schema": {"type": "string", "description": "Schema name to select"},
    },
    required_params=["schema"],
    tui_aliases=["/select-schema", "/use-schema"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="condensed",
    condensed_action="Setting Snowflake schema:",
    usage_hint="Usage: /select-schema <schema_name>",
    provider="snowflake",
)
