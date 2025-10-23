"""
LLM Client - Backward compatibility facade.

DEPRECATED: This client is maintained for backward compatibility only.
New code should use LLMProviderFactory directly:

    from chuck_data.llm.factory import LLMProviderFactory
    provider = LLMProviderFactory.create()
    response = provider.chat(messages, model="...", tools=[...])

This client now delegates to DatabricksProvider via the factory.
"""

import logging
from typing import Optional, List, Dict, Any
from openai.types.chat import ChatCompletion

logger = logging.getLogger(__name__)


class LLMClient:
    """Legacy LLM client that delegates to provider factory.

    DEPRECATED: Use LLMProviderFactory.create() instead.

    This class is maintained for backward compatibility with existing code.
    It delegates all calls to DatabricksProvider via the factory pattern.
    """

    def __init__(self):
        """Initialize LLM client (delegates to factory)."""
        from chuck_data.llm.factory import LLMProviderFactory

        # Always create databricks provider for backward compatibility
        self._provider = LLMProviderFactory.create("databricks")
        logger.debug("LLMClient initialized (delegating to DatabricksProvider)")

    def chat(
        self,
        messages: List[Dict[str, Any]],
        model: Optional[str] = None,
        tools: Optional[List[Dict[str, Any]]] = None,
        stream: bool = False,
        tool_choice: str = "auto",
    ) -> ChatCompletion:
        """Send chat request (delegates to DatabricksProvider).

        Args:
            messages: List of message objects
            model: Model to use (default from config)
            tools: List of tools to provide
            stream: Whether to stream the response
            tool_choice: Tool selection strategy

        Returns:
            Response from the API
        """
        return self._provider.chat(
            messages=messages,
            model=model,
            tools=tools,
            stream=stream,
            tool_choice=tool_choice,
        )
