"""
Command handler for listing models.

This module contains the handler for listing available models
in a Databricks workspace.
"""

import logging
from typing import Optional

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.models import list_models as list_models_api
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import get_active_model
from .base import CommandResult


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """
    List available models with optional filtering and detailed information.
    Args:
        client: API client instance
        **kwargs:
            detailed (bool): Whether to show detailed information. Defaults to False.
            filter (str, optional): Filter string for model names.
    """
    detailed: bool = kwargs.get("detailed", False)
    filter_str: Optional[str] = kwargs.get("filter")

    try:
        models_list = list_models_api(client)
        if filter_str:
            normalized_filter = filter_str.lower()
            models_list = [
                m for m in models_list if normalized_filter in m.get("name", "").lower()
            ]

        if detailed and models_list:
            for model_item in models_list:
                from chuck_data.models import get_model

                model_details = get_model(client, model_item["name"])
                if model_details:
                    model_item["details"] = model_details

        active_model_name = get_active_model()
        result_data = {
            "models": models_list,
            "active_model": active_model_name,
            "detailed": detailed,
            "filter": filter_str,
        }

        message = None
        if not models_list:
            message = """No models found. To set up a model in Databricks:
1. Go to the Databricks Model Serving page in your workspace.
2. Click 'Create Model'.
3. Choose a model (e.g., Claude, OpenAI, or another supported LLM).
4. Configure the model settings and deploy the model.
After deployment, run the models command again to verify availability."""
        return CommandResult(True, data=result_data, message=message)
    except Exception as e:
        logging.error(f"Failed to list models: {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="detailed-models",
    description="List available models with filtering and detailed information",
    handler=handle_command,
    parameters={
        "detailed": {
            "type": "boolean",
            "description": "Show detailed information about each model",
        },
        "filter": {
            "type": "string",
            "description": "Filter string to match against model names",
        },
    },
    required_params=[],
    tui_aliases=["/list-models"],
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="full",  # Show full model list in tables
)
