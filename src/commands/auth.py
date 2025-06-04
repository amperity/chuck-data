"""
Commands for user authentication with Amperity and Databricks.
"""

import logging
from typing import Any, Optional

from src.clients.amperity import AmperityAPIClient
from src.clients.databricks import DatabricksAPIClient
from src.commands.base import CommandResult
from src.command_registry import CommandDefinition
from src.config import set_databricks_token


def handle_amperity_login(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """Handle the login command for Amperity."""
    auth_client = AmperityAPIClient()
    success, message = auth_client.start_auth()
    if not success:
        return CommandResult(False, message=f"Login failed: {message}")

    # Wait for auth completion
    success, message = auth_client.wait_for_auth_completion()
    if not success:
        return CommandResult(False, message=f"Login failed: {message}")

    return CommandResult(True, message=message)


def handle_databricks_login(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """Handle the login command for Databricks."""
    token = kwargs.get("token")
    if not token:
        return CommandResult(False, message="Token parameter is required")

    # Save token to config
    try:
        set_databricks_token(token)
        logging.info("Databricks token set successfully")
        return CommandResult(True, message="Databricks token set successfully")
    except Exception as e:
        logging.error("Failed to set Databricks token: %s", e)
        return CommandResult(False, message=f"Failed to set token: {e}")


def handle_logout(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """Handle the logout command for Amperity by default."""
    service = kwargs.get("service", "amperity")

    if service in ["all", "databricks"]:
        try:
            set_databricks_token("")
            logging.info("Databricks token cleared")
        except Exception as e:
            logging.error("Error clearing Databricks token: %s", e)
            return CommandResult(False, message=f"Error clearing Databricks token: {e}")

    if service in ["all", "amperity"]:
        from src.config import set_amperity_token

        try:
            set_amperity_token("")
            logging.info("Amperity token cleared")
        except Exception as e:
            logging.error("Error clearing Amperity token: %s", e)
            return CommandResult(False, message=f"Error clearing Amperity token: {e}")

    return CommandResult(True, message=f"Successfully logged out from {service}")


DEFINITION = [
    CommandDefinition(
        name="amperity-login",
        description="Log in to Amperity",
        handler=handle_amperity_login,
        parameters={},
        required_params=[],
        tui_aliases=["/login", "/amperity-login"],
        needs_api_client=False,
        visible_to_user=True,
        visible_to_agent=False,
    ),
    CommandDefinition(
        name="databricks-login",
        description="Set Databricks API token",
        handler=handle_databricks_login,
        parameters={
            "token": {"type": "string", "description": "Your Databricks API token"}
        },
        required_params=["token"],
        tui_aliases=["/databricks-login", "/set-token"],
        needs_api_client=False,
        visible_to_user=True,
        visible_to_agent=False,
    ),
    CommandDefinition(
        name="logout",
        description="Log out from Amperity (default) or other authentication services",
        handler=handle_logout,
        parameters={
            "service": {
                "type": "string",
                "description": "Service to log out from (amperity, databricks, or all)",
                "default": "amperity",
            }
        },
        required_params=[],
        tui_aliases=["/logout"],
        needs_api_client=False,
        visible_to_user=True,
        visible_to_agent=False,
    ),
]
