"""
Command handler for model selection.

This module contains the handler for selecting an active model
for LLM operations (works with any configured LLM provider).
"""

import logging
from typing import Optional

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.config import set_active_model, get_llm_provider
from chuck_data.command_registry import CommandDefinition
from chuck_data.llm.factory import LLMProviderFactory
from .base import CommandResult


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """Set the active model.

    Works with the configured LLM provider (Databricks, AWS Bedrock, etc.)

    Args:
        client: API client instance (used for Databricks provider)
        **kwargs: model_name (str)
    """
    model_name = kwargs.get("model_name")
    if not model_name:
        return CommandResult(False, message="model_name parameter is required.")

    try:
        # Get configured LLM provider
        configured_provider = get_llm_provider() or "databricks"

        # Get list of available models from the configured provider
        if configured_provider == "databricks" and client:
            from chuck_data.llm.providers.databricks import DatabricksProvider

            provider = DatabricksProvider(client=client)
        else:
            # Use factory for non-Databricks providers
            provider = LLMProviderFactory.create()

        # List all models (including non-tool-calling) for selection
        models_list = provider.list_models(tool_calling_only=False)

        # Extract model IDs (field name varies by provider)
        model_ids = [m.get("model_id") or m.get("name") or "" for m in models_list]

        # Validate model exists
        if model_name not in model_ids:
            available = ", ".join(model_ids[:5])  # Show first 5 for brevity
            if len(model_ids) > 5:
                available += f"... (and {len(model_ids) - 5} more)"
            return CommandResult(
                False,
                message=f"Model '{model_name}' not found. Available models: {available}",
            )

        # Set the active model
        set_active_model(model_name)
        return CommandResult(
            True, message=f"Active model is now set to '{model_name}'."
        )
    except Exception as e:
        logging.error(f"Failed to set model '{model_name}': {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="select_model",
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
