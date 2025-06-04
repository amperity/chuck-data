"""
Command handler for workspace selection.

This module contains the handler for setting the workspace URL
to connect to a Databricks workspace.
"""

import logging
from typing import Optional

from ..clients.databricks import DatabricksAPIClient
from ..command_registry import CommandDefinition
from ..config import set_workspace_url
from .base import CommandResult


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """
    Set the workspace URL.

    Args:
        client: API client instance (not used by this handler)
        **kwargs: workspace_url (str)
    """
    workspace_url: str = kwargs.get("workspace_url")
    if not workspace_url:
        return CommandResult(False, message="workspace_url parameter is required.")

    try:
        from ..databricks.url_utils import (
            validate_workspace_url,
            normalize_workspace_url,
            format_workspace_url_for_display,
            detect_cloud_provider,
        )

        is_valid, error_message = validate_workspace_url(workspace_url)
        if not is_valid:
            return CommandResult(False, message=f"Error: {error_message}")

        normalized_url = normalize_workspace_url(workspace_url)
        cloud_provider = detect_cloud_provider(workspace_url)
        display_url = format_workspace_url_for_display(normalized_url, cloud_provider)
        set_workspace_url(workspace_url)

        return CommandResult(
            True,
            message=f"Workspace URL is now set to '{display_url}'. Restart may be needed.",
            data={
                "workspace_url": workspace_url,
                "display_url": display_url,
                "cloud_provider": cloud_provider,
                "requires_restart": True,
            },
        )
    except Exception as e:
        logging.error(f"Failed to set workspace URL: {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="select-workspace",
    description="Set the Databricks workspace URL",
    handler=handle_command,
    parameters={
        "workspace_url": {
            "type": "string",
            "description": "URL of the Databricks workspace (e.g., my-workspace.cloud.databricks.com)",
        }
    },
    required_params=["workspace_url"],
    tui_aliases=["/select-workspace"],
    visible_to_user=True,
    visible_to_agent=False,  # Agent doesn't need to select workspace
)
