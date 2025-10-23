"""Databricks LLM provider implementation."""

import logging
from typing import Optional, List, Dict, Any
from openai import OpenAI
from openai.types.chat import ChatCompletion

from chuck_data.config import get_workspace_url, get_active_model
from chuck_data.databricks_auth import get_databricks_token

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
    ):
        """Initialize Databricks provider.

        Args:
            workspace_url: Databricks workspace URL (uses config if not provided)
            token: Databricks personal access token (uses config if not provided)
            model: Default model to use (uses active_model from config if not provided)
        """
        try:
            self.token = token or get_databricks_token()
        except Exception as e:
            logger.error(f"Error getting Databricks token: {e}")
            self.token = None

        self.workspace_url = workspace_url or get_workspace_url()
        self.default_model = model

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

        # Make request
        if tools:
            response = client.chat.completions.create(
                model=resolved_model,
                messages=messages,
                tools=tools,
                stream=stream,
                tool_choice=tool_choice,
            )
        else:
            response = client.chat.completions.create(
                model=resolved_model,
                messages=messages,
                stream=stream,
            )

        return response
