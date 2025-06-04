"""
Command handler for displaying help information.

This module contains the handler for displaying help about
available commands.
"""

import logging
from typing import Optional

from src.clients.databricks import DatabricksAPIClient
from src.command_registry import CommandDefinition, get_user_commands, TUI_COMMAND_MAP
from .base import CommandResult


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """
    Display help information about available commands.

    Args:
        client: API client instance (not used by this handler)
        **kwargs: No parameters required
    """
    try:
        from src.ui.help_formatter import format_help_text

        user_commands = get_user_commands()
        help_text = format_help_text(user_commands, TUI_COMMAND_MAP)
        return CommandResult(True, data={"help_text": help_text})
    except Exception as e:
        logging.error(f"Error generating help: {e}", exc_info=True)
        return CommandResult(False, error=e, message="Error generating help text.")


DEFINITION = CommandDefinition(
    name="help",
    description="Display help information about available commands",
    handler=handle_command,
    parameters={},
    required_params=[],
    tui_aliases=["/help"],
    visible_to_user=True,
    visible_to_agent=True,
)
