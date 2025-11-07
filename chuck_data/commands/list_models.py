"""
Command handler for listing models.

This module contains the handler for listing available models
from the LLM provider.
"""

import logging
from typing import Optional

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import get_active_model, get_llm_provider
from chuck_data.llm.factory import LLMProviderFactory
from .base import CommandResult


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """
    List available models with optional filtering.

    Args:
        client: API client instance (used for Databricks provider if needed)
        **kwargs:
            filter (str, optional): Filter string for model names.
            show_all (bool, optional): Show all models including those without tool calling support.
                                      Defaults to False (only show tool-calling models).
    """
    filter_str: Optional[str] = kwargs.get("filter")
    show_all: bool = kwargs.get("show_all", False)
    # By default, only show tool-calling models (tool_calling_only=True)
    # If show_all=True, then tool_calling_only=False
    tool_calling_only = not show_all

    try:
        # Get configured LLM provider
        configured_provider = get_llm_provider() or "databricks"

        # If Databricks is configured and we have an injected client, use it
        # This supports both testing and production use
        if configured_provider == "databricks" and client:
            from chuck_data.llm.providers.databricks import DatabricksProvider

            provider = DatabricksProvider(client=client)
        else:
            # Use factory for non-Databricks providers or when no client injected
            provider = LLMProviderFactory.create()

        # Get models from provider
        models_list = provider.list_models(tool_calling_only=tool_calling_only)

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
            current_provider = get_llm_provider() or "databricks"

            if current_provider == "aws_bedrock":
                message = """No AWS Bedrock models found. Please check:
1. AWS credentials are configured (aws sso login)
2. AWS_PROFILE and AWS_REGION environment variables are set
3. Bedrock model access is enabled in AWS Console
4. Using a region that supports Bedrock (us-east-1, us-west-2, etc.)"""
            else:  # databricks
                message = """No Databricks models found. To set up a model:
1. Go to the Databricks Model Serving page in your workspace
2. Click 'Create Model'
3. Choose a model (e.g., Claude, OpenAI, or another supported LLM)
4. Configure the model settings and deploy the model
After deployment, run the models command again to verify availability."""

        return CommandResult(True, data=result_data, message=message)
    except Exception as e:
        logging.error(f"Failed to list models: {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="list_models",
    description="List available language models from the LLM provider (by default, only shows models with tool calling support)",
    handler=handle_command,
    parameters={
        "filter": {
            "type": "string",
            "description": "Filter string to match against model names",
        },
        "show_all": {
            "type": "boolean",
            "description": "Show all models including those without tool calling support (default: False)",
        },
    },
    required_params=[],
    tui_aliases=["/models", "/list-models"],
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="full",  # Show full model list in tables
)
