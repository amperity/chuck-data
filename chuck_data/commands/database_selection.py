"""
Command handler for database selection (Redshift).

This module contains the handler for setting the active database
for Redshift operations.
"""

import logging
from typing import Optional
from difflib import SequenceMatcher

from chuck_data.clients.redshift import RedshiftAPIClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import get_config_manager
from .base import CommandResult


def _similarity_score(name1: str, name2: str) -> float:
    """Calculate similarity score between two strings (0.0 to 1.0)."""
    return SequenceMatcher(None, name1.lower().strip(), name2.lower().strip()).ratio()


def _find_best_database_match(target_name: str, databases: list) -> Optional[str]:
    """Find the best matching database by name using fuzzy matching."""
    best_match = None
    best_score = 0.0
    target_lower = target_name.lower().strip()

    for db_name in databases:
        if not db_name:
            continue

        db_lower = db_name.lower().strip()

        # Check for exact match first (case insensitive)
        if db_lower == target_lower:
            return db_name

        # Check if target is a substring of database name
        if target_lower in db_lower or db_lower.startswith(target_lower):
            return db_name

        # Calculate similarity score for fuzzy matching
        score = _similarity_score(target_name, db_name)
        if score > best_score and score >= 0.4:  # Threshold for fuzzy matching
            best_score = score
            best_match = db_name

    return best_match


def _report_step(message: str, tool_output_callback=None):
    """Report a step in the database selection process."""
    if tool_output_callback:
        tool_output_callback("select-database", {"step": message})


def handle_command(client: Optional[RedshiftAPIClient], **kwargs) -> CommandResult:
    """
    Set the active database for Redshift operations.

    Args:
        client: RedshiftAPIClient instance
        **kwargs: database (str) - database name, tool_output_callback (optional)
    """
    database = kwargs.get("database")
    tool_output_callback = kwargs.get("tool_output_callback")

    if not database:
        return CommandResult(
            False,
            message="database parameter is required.",
        )

    if not client:
        return CommandResult(
            False,
            message="No Redshift client available to verify database.",
        )

    try:
        # List all databases and find the best match
        _report_step(
            f"Looking for database matching '{database}'", tool_output_callback
        )

        databases = client.list_databases()
        if not databases:
            return CommandResult(
                False, message="No databases found in Redshift cluster/workgroup."
            )

        # Find best match by name
        target_database = _find_best_database_match(database, databases)

        if not target_database:
            # Format available databases with truncation
            if len(databases) <= 5:
                available_text = ", ".join(databases)
            else:
                first_five = ", ".join(databases[:5])
                remaining_count = len(databases) - 5
                available_text = f"{first_five} ... and {remaining_count} more"

            return CommandResult(
                False,
                message=f"No database found matching '{database}'. Available databases: {available_text}",
            )

        # Report the selection
        if target_database.lower().strip() != database.lower().strip():
            _report_step(f"Selecting '{target_database}'", tool_output_callback)
        else:
            _report_step(f"Found database '{target_database}'", tool_output_callback)

        # Set the active database in config
        config_manager = get_config_manager()
        success = config_manager.update(redshift_database=target_database)

        if not success:
            return CommandResult(
                False,
                message=f"Failed to set active database to '{target_database}'.",
            )

        return CommandResult(
            True,
            message=f"Active database is now set to '{target_database}'.",
            data={
                "database_name": target_database,
                "step": f"Database set - Name: {target_database}",
            },
        )

    except Exception as e:
        logging.error(f"Failed to set database: {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="select_database",
    description="Set the active database for Redshift operations by name with fuzzy matching",
    handler=handle_command,
    parameters={
        "database": {
            "type": "string",
            "description": "Database name to select",
        }
    },
    required_params=["database"],
    tui_aliases=["/select-database", "/use-database"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="condensed",
    condensed_action="Setting database:",
    usage_hint="Usage: /select-database <database_name>",
    provider="aws_redshift",  # Redshift-specific command
)
