"""
Command handler for basic model listing.

This module contains the handler for listing available models
in a Databricks workspace.
"""

import logging
from typing import Optional

from ..clients.databricks import DatabricksAPIClient
from ..models import list_models as list_models_api
from ..command_registry import CommandDefinition
from .base import CommandResult


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """List available models."""
    try:
        models_list = list_models_api(client)
        if models_list:
            return CommandResult(True, data=models_list)
        else:
            no_models_help = """No models found. To set up a model in Databricks:
1. Go to the Databricks Model Serving page in your workspace.
2. Click 'Create Model'.
3. Choose a model (e.g., Claude, OpenAI, or another supported LLM).
4. Configure the model settings and deploy the model.
After deployment, run the models command again to verify availability."""
            return CommandResult(True, data=[], message=no_models_help)
    except Exception as e:
        logging.error(f"Failed to list models: {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="list-models",
    description="List available language models in the Databricks workspace",
    handler=handle_command,
    parameters={},
    required_params=[],
    tui_aliases=["/models"],
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="full",  # Show full model list in tables
)
