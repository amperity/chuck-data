"""
Command handler for displaying support options.

This module contains the handler for displaying information
about how to get support for Chuck.
"""

import logging
from typing import Optional

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.command_registry import CommandDefinition
from .base import CommandResult


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """
    Display support options for Chuck users.

    Args:
        client: API client instance (not used by this handler)
        **kwargs: No parameters required
    """
    try:
        # Create content for the support message
        support_text = """
Chuck is a research preview application that is actively being improved all of the time based on your usage and feedback. 
Always be sure to update to the latest version of Chuck to get the best experience!

Having trouble with Chuck? We're here to help! Here are several ways to get support:

[bold]1. GitHub Issues[/bold]
Report bugs or request features on our GitHub repository:
https://github.com/amperity/chuck-data/issues

[bold]2. Discord Community[/bold]
Join our community on Discord to chat with other users and developers:
Run [bold]/discord[/bold] to get an invitation link

[bold]3. Email Support[/bold]
Contact our dedicated support team directly:
chuck-support@amperity.com

[bold]4. Bug Reports[/bold]
Let Chuck submit a bug report automatically with:
[bold]/bug[/bold]

We typically respond to all support requests within 1-2 business days.
"""
        return CommandResult(True, data={"support_text": support_text})
    except Exception as e:
        logging.error(f"Error generating support information: {e}", exc_info=True)
        return CommandResult(
            False, error=e, message="Error displaying support information."
        )


DEFINITION = CommandDefinition(
    name="support",
    description="Get information about Chuck support options",
    handler=handle_command,
    parameters={},
    required_params=[],
    tui_aliases=["/support", "/help-me"],
    visible_to_user=True,
    visible_to_agent=True,
)
