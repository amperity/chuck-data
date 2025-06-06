"""
Command handler for submitting bug reports.

This module contains the handler for submitting bug reports to Amperity,
including current configuration (without tokens) and session logs.
"""

import logging
import os
import platform
from datetime import datetime
from typing import Optional, Any, Dict

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.clients.amperity import AmperityAPIClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.commands.base import CommandResult
from chuck_data.config import get_config_manager, get_amperity_token
from chuck_data.logger import get_current_log_file


def handle_command(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """
    Submit a bug report to Amperity's API.

    Args:
        client: Not used for this command
        **kwargs: Command parameters including:
            - description: Bug description from user
            - rest: Alternative way to provide description
            - raw_args: Fallback for unparsed args

    Returns:
        CommandResult indicating success or failure
    """
    # Try to get description from multiple sources
    description = kwargs.get("description", "").strip()

    # If no explicit description, check for rest/raw_args (free-form text)
    if not description:
        description = kwargs.get("rest", "").strip()

    if not description and "raw_args" in kwargs:
        raw_args = kwargs["raw_args"]
        if isinstance(raw_args, list):
            description = " ".join(raw_args).strip()
        elif isinstance(raw_args, str):
            description = raw_args.strip()

    if not description:
        return CommandResult(
            False,
            message="Bug description is required. Usage: /bug Your bug description here",
        )

    # Check for Amperity token
    amperity_token = get_amperity_token()
    if not amperity_token:
        return CommandResult(
            False,
            message="Amperity authentication required. Please run /auth to authenticate first.",
        )

    try:
        # Prepare bug report payload
        payload = _prepare_bug_report(description)

        # Submit to Amperity API using the client
        amperity_client = AmperityAPIClient()
        success, message = amperity_client.submit_bug_report(payload, amperity_token)

        if success:
            logging.debug("Bug report submitted successfully")
            return CommandResult(
                True,
                message="Bug report submitted successfully. Thank you for your feedback!",
            )
        else:
            return CommandResult(False, message=message)

    except Exception as e:
        logging.error(f"Error submitting bug report: {e}", exc_info=True)
        return CommandResult(
            False, error=e, message=f"Error submitting bug report: {str(e)}"
        )


def _prepare_bug_report(description: str) -> Dict[str, Any]:
    """
    Prepare the bug report payload.

    Args:
        description: User's bug description

    Returns:
        Dictionary containing bug report data
    """
    # Get config without tokens
    config_data = _get_sanitized_config()

    # Get session log content
    log_content = _get_session_log()

    # Get system information
    system_info = {
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "system": platform.system(),
        "machine": platform.machine(),
    }

    return {
        "type": "bug_report",
        "timestamp": datetime.utcnow().isoformat(),
        "description": description,
        "config": config_data,
        "session_log": log_content,
        "system_info": system_info,
    }


def _get_sanitized_config() -> Dict[str, Any]:
    """
    Get current configuration without sensitive data (tokens).

    Returns:
        Dictionary of sanitized config data
    """
    config_manager = get_config_manager()
    config = config_manager.get_config()

    # Create sanitized version - NEVER include tokens
    sanitized = {
        "workspace_url": config.workspace_url,
        "active_model": config.active_model,
        "warehouse_id": config.warehouse_id,
        "active_catalog": config.active_catalog,
        "active_schema": config.active_schema,
        "usage_tracking_consent": config.usage_tracking_consent,
    }

    # Remove None values
    return {k: v for k, v in sanitized.items() if v is not None}


def _get_session_log() -> str:
    """
    Get the current session's log content.

    Returns:
        String containing log content or error message
    """
    log_file = get_current_log_file()
    if not log_file or not os.path.exists(log_file):
        return "Session log not available"

    try:
        with open(log_file, "r") as f:
            # Read last 10KB of log to avoid huge payloads
            f.seek(0, os.SEEK_END)
            file_size = f.tell()
            read_size = min(file_size, 10240)  # 10KB max
            f.seek(max(0, file_size - read_size))
            return f.read()
    except Exception as e:
        logging.error(f"Failed to read session log: {e}")
        return f"Error reading session log: {str(e)}"


DEFINITION = CommandDefinition(
    name="bug",
    description="Submit a bug report with current configuration and session logs",
    handler=handle_command,
    parameters={
        "description": {
            "type": "string",
            "description": "Description of the bug or issue you're experiencing",
        },
        "rest": {
            "type": "string",
            "description": "Bug description provided as free-form text after the command",
        },
        "raw_args": {
            "type": ["array", "string"],
            "description": "Raw unparsed arguments for the bug description",
        },
    },
    required_params=[],  # No required params since we accept multiple input methods
    tui_aliases=["/bug"],
    needs_api_client=False,  # We use Amperity token directly
    visible_to_user=True,
    visible_to_agent=False,  # Bug reports should come from users, not agents
    usage_hint="Example: /bug The table list is not refreshing properly",
)
