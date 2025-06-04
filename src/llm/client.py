# src/llm_client.py
"""
Client for communicating with Databricks-hosted LLM APIs.
"""

from openai import OpenAI
import logging
from src.config import get_workspace_url
from src.databricks_auth import get_databricks_token

logging.getLogger("openai").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.ERROR)


class LLMClient:
    def __init__(self):
        """
        Initialize the LLM client.
        Args:
            client: Optional pre-configured OpenAI client
        """
        # Get token from config or environment
        try:
            self.databricks_token = get_databricks_token()
        except Exception as e:
            logging.error(f"Error getting Databricks token for LLM client: {e}")
            self.databricks_token = None

        self.base_url = get_workspace_url()

    def chat(self, messages, model=None, tools=None, stream=False, tool_choice="auto"):
        """
        Send a chat request to the model.
        Args:
            messages: List of message objects
            model: Model to use (default from config)
            tools: List of tools to provide
            stream: Whether to stream the response
        Returns:
            Response from the API
        """
        # Get active model from config if not specified
        if not model:
            try:
                from src.config import get_active_model

                model = get_active_model()
            except ImportError:
                model = None

        client = OpenAI(
            api_key=self.databricks_token,
            base_url=f"{self.base_url}/serving-endpoints",
        )
        if tools:
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                tools=tools,
                stream=stream,
                tool_choice=tool_choice,
            )
        else:
            response = client.chat.completions.create(
                model=model, messages=messages, stream=stream
            )
        return response
