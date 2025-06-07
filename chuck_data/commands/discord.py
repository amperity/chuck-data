"""
Command handler for opening Discord community link.

This module contains the handler for displaying an invitation to join
the Discord community and optionally opening the link in a browser.
"""

import logging
import webbrowser
from typing import Optional

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.command_registry import CommandDefinition
from .base import CommandResult


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """
    Display an invitation to join the Discord community.

    Args:
        client: API client instance (not used by this handler)
        **kwargs: No parameters required
    """
    try:
        # Discord invite URL
        discord_url = "https://discord.gg/f3UZwyuQqe"

        # Create interactive content for the Discord invitation
        discord_message = f"""
[bold]Join the Chuck Community on Discord![/bold]

Connect with other Chuck users and our developers to:
- Get help with your implementation
- Share your use cases
- Learn about upcoming features
- Contribute to the development of Chuck

[bold]Discord Invite:[/bold] {discord_url}

Would you like to open Discord in your browser?
(Respond with Y/N)
"""
        # Return data with both the message and the URL
        return CommandResult(
            True,
            data={
                "discord_message": discord_message,
                "discord_url": discord_url,
                "prompt_open_browser": True,
            },
        )
    except Exception as e:
        logging.error(f"Error generating Discord invitation: {e}", exc_info=True)
        return CommandResult(
            False, error=e, message="Error generating Discord invitation."
        )


DEFINITION = CommandDefinition(
    name="discord",
    description="Join our Chuck community on Discord",
    handler=handle_command,
    parameters={},
    required_params=[],
    tui_aliases=["/discord"],
    visible_to_user=True,
    visible_to_agent=True,
)
