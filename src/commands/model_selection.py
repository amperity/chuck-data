"""
Command handler for model selection.

This module contains the handler for selecting an active model
for use in a Databricks workspace.
"""

import logging
from typing import Optional

from src.clients.databricks import DatabricksAPIClient
from src.models import list_models as list_models_api
from src.config import set_active_model
from src.command_registry import CommandDefinition
from .base import CommandResult


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """Set the active model.
    Args:
        client: API client instance
        **kwargs: model_name (str)
    """
    model_name: str = kwargs.get("model_name")
    if not model_name:
        return CommandResult(False, message="model_name parameter is required.")
    try:
        models_list = list_models_api(client)
        model_names = [m["name"] for m in models_list]
        if model_name not in model_names:
            return CommandResult(False, message=f"Model '{model_name}' not found.")
        set_active_model(model_name)
        return CommandResult(
            True, message=f"Active model is now set to '{model_name}'."
        )
    except Exception as e:
        logging.error(f"Failed to set model '{model_name}': {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="select-model",
    description="Set the active model for agent operations",
    handler=handle_command,
    parameters={
        "model_name": {
            "type": "string",
            "description": "Name of the model to set as active",
        }
    },
    required_params=["model_name"],
    tui_aliases=["/select-model"],
    visible_to_user=True,
    visible_to_agent=True,
)
