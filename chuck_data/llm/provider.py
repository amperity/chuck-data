"""LLM Provider Protocol."""

from typing import Protocol, Optional, List, Dict, Any, TypedDict
from openai.types.chat import ChatCompletion


class _ModelInfoRequired(TypedDict):
    """Required fields for ModelInfo."""

    model_id: str  # Provider-specific model identifier


class ModelInfo(_ModelInfoRequired, total=False):
    """Unified model information across LLM providers.

    All providers must return model information in this format.
    Required field: model_id
    """

    model_name: str  # Human-readable model name
    provider_name: str  # Provider name (e.g., "databricks", "aws_bedrock")
    supports_tool_use: bool  # Whether model supports function calling
    state: Optional[str]  # Model state (e.g., "READY", "NOT_READY")
    endpoint_type: Optional[str]  # Endpoint type (provider-specific)
    description: Optional[str]  # Model description


class LLMProvider(Protocol):
    """Protocol that all LLM providers must implement."""

    def chat(
        self,
        messages: List[Dict[str, Any]],
        model: Optional[str] = None,
        tools: Optional[List[Dict[str, Any]]] = None,
        stream: bool = False,
        tool_choice: str = "auto",
    ) -> ChatCompletion:
        """Send chat request to LLM.

        Args:
            messages: List of message dicts with 'role' and 'content'
            model: Model identifier (provider-specific)
            tools: Optional tool definitions (OpenAI format)
            stream: Whether to stream response
            tool_choice: "auto", "required", or "none"

        Returns:
            OpenAI ChatCompletion object
        """
        ...

    def list_models(self, tool_calling_only: bool = True) -> List[ModelInfo]:
        """List available models from this provider.

        Args:
            tool_calling_only: If True, only return models that support tool calling.
                             Defaults to True since tool calling is required for agent workflows.

        Returns:
            List of ModelInfo dicts containing model metadata
        """
        ...
