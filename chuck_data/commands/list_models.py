"""
Command handler for listing models.

This module contains the handler for listing available models
from the LLM provider.
"""

import logging
from typing import Optional

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import get_active_model
from chuck_data.llm.factory import LLMProviderFactory
from .base import CommandResult


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """
    List available models with optional filtering.

    Args:
        client: API client instance (injected for testing, otherwise creates via factory)
        **kwargs:
            filter (str, optional): Filter string for model names.
    """
    filter_str: Optional[str] = kwargs.get("filter")

    try:
        # Create provider - inject client if provided (for testing)
        if client:
            from chuck_data.llm.providers.databricks import DatabricksProvider

            provider = DatabricksProvider(client=client)
        else:
            provider = LLMProviderFactory.create()

        # Get models from provider
        models_list = provider.list_models()

        # Apply filter if provided
        if filter_str:
            normalized_filter = filter_str.lower()
            models_list = [
                m
                for m in models_list
                if normalized_filter in m.get("model_name", "").lower()
                or normalized_filter in m.get("model_id", "").lower()
            ]

        active_model_name = get_active_model()
        result_data = {
            "models": models_list,
            "active_model": active_model_name,
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
    name="list-models",
    description="List available language models from the LLM provider",
    handler=handle_command,
    parameters={
        "filter": {
            "type": "string",
            "description": "Filter string to match against model names",
        },
    },
    required_params=[],
    tui_aliases=["/models", "/list-models"],
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="full",  # Show full model list in tables
)
