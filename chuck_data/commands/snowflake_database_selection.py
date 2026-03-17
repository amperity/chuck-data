"""
Command handler for Snowflake database selection.
"""

import logging
from typing import Optional
from difflib import SequenceMatcher

from chuck_data.clients.snowflake import SnowflakeAPIClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import get_config_manager
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
    Set the active Snowflake database.

    Args:
        client: SnowflakeAPIClient instance
        **kwargs: database (str), tool_output_callback (optional)
    """
    database = kwargs.get("database")
    if not database:
        return CommandResult(False, message="database parameter is required.")

    if not client:
        return CommandResult(False, message="No Snowflake client available.")

    try:
        result = client.list_databases()
        db_names = [db["name"] for db in result.get("databases", [])]

        if not db_names:
            return CommandResult(False, message="No databases found in Snowflake.")

        target = _find_best_match(database, db_names)
        if not target:
            available = ", ".join(db_names[:5])
            if len(db_names) > 5:
                available += f" ... and {len(db_names) - 5} more"
            return CommandResult(
                False,
                message=f"No database found matching '{database}'. Available: {available}",
            )

        get_config_manager().update(active_database=target)
        return CommandResult(
            True,
            message=f"Active database is now set to '{target}'.",
            data={"database_name": target},
        )
    except Exception as e:
        logging.error(f"Failed to set Snowflake database: {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="snowflake_select_database",
    description="Set the active Snowflake database by name (fuzzy matching supported).",
    handler=handle_command,
    parameters={
        "database": {"type": "string", "description": "Database name to select"},
    },
    required_params=["database"],
    tui_aliases=["/select-database", "/use-database"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="condensed",
    condensed_action="Setting Snowflake database:",
    usage_hint="Usage: /select-database <database_name>",
    provider="snowflake",
)
