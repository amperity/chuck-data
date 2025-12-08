"""Databricks LLM provider implementation."""

import logging
from typing import Optional, List, Dict, Any
from openai import OpenAI
from openai.types.chat import ChatCompletion

from chuck_data.config import get_workspace_url, get_active_model
from chuck_data.databricks_auth import get_databricks_token
from chuck_data.llm.provider import ModelInfo
from chuck_data.clients.databricks import DatabricksAPIClient

# Silence verbose OpenAI logging
logging.getLogger("openai").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)


class DatabricksProvider:
    """LLM provider for Databricks Model Serving endpoints.

    Uses OpenAI SDK to communicate with Databricks-hosted models.
    Supports model serving endpoints with tool calling capabilities.
    """

    def __init__(
        self,
        workspace_url: Optional[str] = None,
        token: Optional[str] = None,
        model: Optional[str] = None,
        client: Optional[DatabricksAPIClient] = None,
    ):
        """Initialize Databricks provider.

        Args:
            workspace_url: Databricks workspace URL (uses config if not provided)
            token: Databricks personal access token (uses config if not provided)
            model: Default model to use (uses active_model from config if not provided)
            client: Optional pre-configured DatabricksAPIClient (for testing)
        """
        try:
            self.token = token or get_databricks_token()
        except Exception as e:
            logger.error(f"Error getting Databricks token: {e}")
            self.token = None

        self.workspace_url = workspace_url or get_workspace_url()
        self.default_model = model
        self._client = client  # Store injected client for testing

    def chat(
        self,
        messages: List[Dict[str, Any]],
        model: Optional[str] = None,
        tools: Optional[List[Dict[str, Any]]] = None,
        stream: bool = False,
        tool_choice: str = "auto",
    ) -> ChatCompletion:
        """Send chat request to Databricks model serving endpoint.

        Args:
            messages: List of message dicts with 'role' and 'content'
            model: Model endpoint name (uses default/active model if not provided)
            tools: Optional tool definitions (OpenAI format)
            stream: Whether to stream response
            tool_choice: "auto", "required", or "none"

        Returns:
            OpenAI ChatCompletion object
        """
        # Resolve model
        resolved_model = model or self.default_model
        if not resolved_model:
            resolved_model = get_active_model()

        # Create OpenAI client configured for Databricks
        client = OpenAI(
            api_key=self.token,
            base_url=f"{self.workspace_url}/serving-endpoints",
        )

        # Ensure we have a model - raise if none available
        if not resolved_model:
            raise ValueError("No model specified and no active model configured")

        # Make request - using type: ignore for OpenAI SDK strict typing
        # The runtime behavior is correct as OpenAI accepts these formats
        if tools:
            response = client.chat.completions.create(
                model=resolved_model,
                messages=messages,  # type: ignore[arg-type]
                tools=tools,  # type: ignore[arg-type]
                stream=stream,
                tool_choice=tool_choice,  # type: ignore[arg-type]
            )
        else:
            response = client.chat.completions.create(
                model=resolved_model,
                messages=messages,  # type: ignore[arg-type]
                stream=stream,
            )

        return response

    def list_models(self, tool_calling_only: bool = True) -> List[ModelInfo]:
        """List available models from Databricks serving endpoints.

        Args:
            tool_calling_only: If True, only return models that support tool calling.
                             Defaults to True since tool calling is required for agent workflows.

        Returns:
            List of ModelInfo dicts containing model metadata

        Raises:
            ValueError: If API call fails
        """
        # Use injected client if available, otherwise create new one
        if self._client:
            client = self._client
        else:
            client = DatabricksAPIClient(
                workspace_url=self.workspace_url, token=self.token
            )

        # Fetch models from Databricks API
        endpoints = client.list_models()

        # Transform to ModelInfo format
        models: List[ModelInfo] = []
        for endpoint in endpoints:
            # Extract endpoint type safely
            config = endpoint.get("config", {})
            served_entities = config.get("served_entities", [])
            endpoint_type = ""
            if served_entities:
                endpoint_type = served_entities[0].get("entity_name", "")

            model_info: ModelInfo = {
                "model_id": endpoint.get("name", ""),
                "model_name": endpoint.get("name", ""),
                "provider_name": "databricks",
                "state": endpoint.get("state", {}).get("ready", "UNKNOWN"),
                "endpoint_type": endpoint_type,
            }

            # Check if endpoint supports tool use (most Databricks endpoints do)
            # This is inferred from task type or model type
            if served_entities:
                # Assume tool use support for chat models
                # You may want to add more sophisticated detection
                model_info["supports_tool_use"] = True
            else:
                model_info["supports_tool_use"] = False

            # Filter out non-tool-calling models if requested
            if tool_calling_only and not model_info["supports_tool_use"]:
                continue

            models.append(model_info)

        return models
