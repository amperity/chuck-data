"""AWS Bedrock LLM provider implementation.

Uses AWS Bedrock Converse API to support multiple foundation models:
- Anthropic Claude (Sonnet 4.5, Sonnet 4, Claude 3.5)
- Meta Llama (Llama 4 Scout/Maverick, Llama 3.x)
- Amazon Nova (Pro, Lite, Micro)
- Other models (Mistral, Cohere, Command R+)

The Converse API is AWS's recommended unified API for tool calling and
multi-turn conversations across all Bedrock models.

Supports both direct model IDs and cross-region inference profiles for
higher throughput and better resilience.
"""

import json
import logging
import os
import time
from typing import Any, Dict, List, Literal, Optional

from openai.types.chat.chat_completion import ChatCompletion, Choice

from chuck_data.llm.provider import ModelInfo
from openai.types.chat.chat_completion_message import ChatCompletionMessage
from openai.types.completion_usage import CompletionUsage
from openai.types.chat.chat_completion_message_tool_call import (
    ChatCompletionMessageToolCall,
    Function,
)

# Import boto3 at module level for testability
try:
    import boto3  # type: ignore[reportMissingImports]
except ImportError:
    boto3 = None  # type: ignore

logger = logging.getLogger(__name__)


class AWSBedrockProvider:
    """LLM provider for AWS Bedrock foundation models.

    Uses AWS Bedrock Converse API for unified interface across models.
    Supports tool calling for agentic workflows.

    Supports two types of model invocation:

    1. Direct Model IDs (on-demand, single region):
        Nova (recommended):
            - amazon.nova-pro-v1:0 (default, most capable)
            - amazon.nova-lite-v1:0
            - amazon.nova-micro-v1:0
        Claude:
            - us.anthropic.claude-sonnet-4-5-20250929-v1:0 (Sonnet 4.5, recommended)
            - anthropic.claude-3-5-sonnet-20240620-v1:0 (Sonnet 3.5)
            - anthropic.claude-3-haiku-20240307-v1:0 (Haiku 3)
        Llama:
            - meta.llama3-1-70b-instruct-v1:0

    2. Inference Profiles (cross-region, higher throughput):
        Claude:
            - us.anthropic.claude-sonnet-4-5-20250929-v1:0 (Sonnet 4.5, recommended, US regions)
            - us.anthropic.claude-sonnet-4-20250514-v1:0 (Sonnet 4, US regions)
            - eu.anthropic.claude-sonnet-4-5-20250929-v1:0 (Sonnet 4.5, EU regions)
            - global.anthropic.claude-sonnet-4-5-20250929-v1:0 (Sonnet 4.5, global routing)
        Llama:
            - us.meta.llama4-scout-17b-instruct-v1:0
            - us.meta.llama4-maverick-17b-instruct-v1:0

    Inference profiles (prefixed with us./eu./global.) automatically route requests
    across multiple AWS regions for better throughput and resilience. Newer models
    like Claude 4 and 4.5 only support inference profiles.

    Note: Check AWS Bedrock console for model access - newer models may require
    explicit approval before use.
    """

    def __init__(
        self,
        region: Optional[str] = None,
        model_id: Optional[str] = None,
    ):
        """Initialize AWS Bedrock provider.

        Args:
            region: AWS region (defaults to us-east-1 or AWS_REGION env var)
            model_id: Default model ID - can be direct model ID or inference profile
                     (defaults to Amazon Nova Pro)

        Note:
            AWS credentials are resolved using boto3's standard credential chain:
            1. AWS_PROFILE environment variable (for AWS SSO)
            2. AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables
            3. ~/.aws/credentials file
            4. IAM roles (when running on EC2/ECS/Lambda)

            AWS SSO users (recommended):
                aws sso login --profile your-profile
                export AWS_PROFILE=your-profile
                export AWS_REGION=us-east-1

            Model ID can be:
            - Direct: amazon.nova-pro-v1:0 (default)
            - Profile: us.anthropic.claude-sonnet-4-5-20250929-v1:0
        """
        if boto3 is None:
            raise ImportError(
                "AWS Bedrock provider requires boto3. Install with: pip install boto3"
            )

        # Resolve region
        self.region = region or os.getenv("AWS_REGION", "us-east-1")

        # Create Bedrock clients using boto3's standard credential resolution
        # boto3 automatically handles AWS_PROFILE, env vars, ~/.aws/credentials, IAM roles, etc.
        try:
            self.bedrock_runtime = boto3.client(
                "bedrock-runtime", region_name=self.region
            )
            self.bedrock = boto3.client("bedrock", region_name=self.region)
        except Exception as e:
            logger.error(f"Failed to create Bedrock clients: {e}")
            raise

        # Default model - Amazon Nova Pro
        # Nova Pro is AWS's most capable foundation model with strong performance
        self.default_model = model_id or "amazon.nova-pro-v1:0"

        logger.info(
            f"Initialized AWS Bedrock provider in {self.region} with model {self.default_model}"
        )

    def chat(
        self,
        messages: List[Dict[str, Any]],
        model: Optional[str] = None,
        tools: Optional[List[Dict[str, Any]]] = None,
        stream: bool = False,
        tool_choice: str = "auto",
    ) -> ChatCompletion:
        """Send chat request to AWS Bedrock using Converse API.

        Args:
            messages: List of message dicts with 'role' and 'content' (OpenAI format)
            model: Model ID or inference profile (uses default if not provided)
                   Examples:
                   - Direct: "amazon.nova-pro-v1:0" (default)
                   - Profile: "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
            tools: Optional tool definitions (OpenAI format)
            stream: Whether to stream response (not implemented yet)
            tool_choice: "auto", "required", or "none"

        Returns:
            OpenAI ChatCompletion object

        Raises:
            ValueError: If model is not supported or request is invalid
            Exception: If Bedrock API call fails
        """
        # Resolve model
        model_id = model or self.default_model

        # Convert messages to Bedrock format
        system_messages, conversation = self._convert_messages_to_bedrock(messages)

        # Build Converse API request
        request = {"modelId": model_id, "messages": conversation}

        # Add system messages if present
        if system_messages:
            request["system"] = system_messages

        # Add tool configuration if tools provided
        if tools:
            request["toolConfig"] = self._convert_tools_to_bedrock(tools, tool_choice)

        # Log request for debugging
        logger.debug(f"Bedrock Converse request: {json.dumps(request, indent=2)}")

        # Call Bedrock Converse API
        try:
            if stream:
                # TODO: Implement streaming support
                logger.warning(
                    "Streaming not yet implemented for Bedrock, using non-streaming"
                )
                stream = False

            response = self.bedrock_runtime.converse(**request)
            logger.debug(
                f"Bedrock Converse response: {json.dumps(response, indent=2, default=str)}"
            )

        except Exception as e:
            logger.error(f"Bedrock Converse API error: {e}")
            raise

        # Convert response to OpenAI format
        return self._convert_response_to_openai(response, model_id)

    def _convert_messages_to_bedrock(
        self, messages: List[Dict[str, Any]]
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Convert OpenAI-format messages to Bedrock Converse format.

        OpenAI format:
            [
                {"role": "system", "content": "You are helpful"},
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi there!"},
                {"role": "assistant", "tool_calls": [...]},  # Tool use
                {"role": "tool", "content": "...", "tool_call_id": "..."}  # Tool result
            ]

        Bedrock Converse format:
            system = [{"text": "You are helpful"}]  # Separate!
            messages = [
                {"role": "user", "content": [{"text": "Hello"}]},
                {"role": "assistant", "content": [{"text": "Hi there!"}]},
                {"role": "assistant", "content": [{"toolUse": {...}}]},
                {"role": "user", "content": [{"toolResult": {...}}]}
            ]

        Args:
            messages: OpenAI-format messages

        Returns:
            Tuple of (system_messages, conversation)
        """
        system_messages = []
        conversation = []

        for msg in messages:
            role = msg["role"]

            # Extract system messages (Bedrock requires separate system parameter)
            if role == "system":
                system_messages.append({"text": msg["content"]})
                continue

            # Handle tool result messages
            if role == "tool":
                # Tool results must be in user role in Bedrock
                conversation.append(
                    {
                        "role": "user",
                        "content": [
                            {
                                "toolResult": {
                                    "toolUseId": msg.get("tool_call_id", ""),
                                    "content": [{"text": msg["content"]}],
                                }
                            }
                        ],
                    }
                )
                continue

            # Handle assistant messages with tool calls
            if role == "assistant" and "tool_calls" in msg:
                # Convert tool calls to Bedrock toolUse format
                tool_uses = []
                for tc in msg["tool_calls"]:
                    tool_uses.append(
                        {
                            "toolUse": {
                                "toolUseId": tc["id"],
                                "name": tc["function"]["name"],
                                "input": json.loads(tc["function"]["arguments"]),
                            }
                        }
                    )

                # Add text content if present
                content = []
                if msg.get("content"):
                    content.append({"text": msg["content"]})
                content.extend(tool_uses)

                conversation.append({"role": "assistant", "content": content})
                continue

            # Handle regular user/assistant messages
            if role in ["user", "assistant"]:
                conversation.append(
                    {
                        "role": role,
                        "content": [
                            {"text": msg["content"] or ""}
                        ],  # Bedrock requires content array
                    }
                )

        return system_messages, conversation

    def _convert_tools_to_bedrock(
        self, tools: List[Dict[str, Any]], tool_choice: str = "auto"
    ) -> Dict[str, Any]:
        """Convert OpenAI tool definitions to Bedrock toolConfig format.

        OpenAI format:
            [{
                "type": "function",
                "function": {
                    "name": "get_weather",
                    "description": "Get weather",
                    "parameters": {
                        "type": "object",
                        "properties": {...},
                        "required": [...]
                    }
                }
            }]

        Bedrock Converse toolConfig format:
            {
                "tools": [{
                    "toolSpec": {
                        "name": "get_weather",
                        "description": "Get weather",
                        "inputSchema": {
                            "json": {
                                "type": "object",
                                "properties": {...},
                                "required": [...]
                            }
                        }
                    }
                }],
                "toolChoice": {"auto": {}} or {"any": {}} or {"tool": {"name": "..."}}
            }

        Args:
            tools: OpenAI-format tool definitions
            tool_choice: "auto", "required", or "none"

        Returns:
            Bedrock toolConfig dict
        """
        tool_config: Dict[str, Any] = {"tools": []}

        # Convert each tool
        for tool in tools:
            if tool.get("type") == "function":
                func = tool["function"]

                # Get parameters and clean up for Bedrock compatibility
                parameters = func.get("parameters", {})

                # AWS Bedrock requires empty "required" arrays to be omitted entirely
                # If "required" is present but empty, remove it
                if "required" in parameters and not parameters["required"]:
                    parameters = {
                        k: v for k, v in parameters.items() if k != "required"
                    }

                # AWS Bedrock inputSchema only supports a subset of JSON Schema keywords:
                # type, properties, required, items, enum, description
                # Remove unsupported keywords like "default", "additionalProperties", etc.
                if "properties" in parameters:
                    cleaned_properties = {}
                    for prop_name, prop_schema in parameters["properties"].items():
                        # Create a clean copy with only supported keywords
                        cleaned_prop = {}
                        supported_keywords = {
                            "type",
                            "description",
                            "items",
                            "enum",
                            "properties",
                        }
                        for key, value in prop_schema.items():
                            if key in supported_keywords:
                                cleaned_prop[key] = value
                        cleaned_properties[prop_name] = cleaned_prop
                    parameters["properties"] = cleaned_properties

                tool_config["tools"].append(
                    {
                        "toolSpec": {
                            "name": func["name"],
                            "description": func.get("description", ""),
                            "inputSchema": {"json": parameters},
                        }
                    }
                )

        # Convert tool_choice
        if tool_choice == "required":
            # "required" means model MUST use a tool
            tool_config["toolChoice"] = {"any": {}}
        elif tool_choice == "none":
            # Don't include toolChoice - model won't use tools
            pass
        else:  # "auto" or default
            # Model decides whether to use tools
            tool_config["toolChoice"] = {"auto": {}}

        return tool_config

    def _convert_response_to_openai(
        self, bedrock_response: Dict[str, Any], model_id: str
    ) -> ChatCompletion:
        """Convert Bedrock Converse response to OpenAI ChatCompletion format.

        Bedrock Converse response:
            {
                "output": {
                    "message": {
                        "role": "assistant",
                        "content": [
                            {"text": "Let me check the weather"},
                            {
                                "toolUse": {
                                    "toolUseId": "id123",
                                    "name": "get_weather",
                                    "input": {"location": "NYC"}
                                }
                            }
                        ]
                    }
                },
                "stopReason": "tool_use",
                "usage": {
                    "inputTokens": 100,
                    "outputTokens": 50,
                    "totalTokens": 150
                }
            }

        OpenAI ChatCompletion format:
            ChatCompletion(
                id="bedrock-id123",
                model="anthropic.claude-...",
                choices=[Choice(
                    message=ChatCompletionMessage(
                        role="assistant",
                        content="Let me check the weather",
                        tool_calls=[{
                            "id": "id123",
                            "type": "function",
                            "function": {
                                "name": "get_weather",
                                "arguments": '{"location": "NYC"}'
                            }
                        }]
                    ),
                    finish_reason="tool_calls"
                )],
                usage={"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150}
            )

        Args:
            bedrock_response: Bedrock Converse API response
            model_id: Model ID used for the request

        Returns:
            OpenAI ChatCompletion object
        """
        # Extract message from response
        message_data = bedrock_response["output"]["message"]
        content_blocks = message_data["content"]

        # Separate text content and tool uses
        text_parts = []
        tool_calls = []

        for block in content_blocks:
            if "text" in block:
                text_parts.append(block["text"])
            elif "toolUse" in block:
                tool_use = block["toolUse"]
                tool_calls.append(
                    ChatCompletionMessageToolCall(
                        id=tool_use["toolUseId"],
                        type="function",
                        function=Function(
                            name=tool_use["name"],
                            arguments=json.dumps(tool_use["input"]),
                        ),
                    )
                )

        # Build message content
        content = " ".join(text_parts) if text_parts else None

        # Map Bedrock stopReason to OpenAI finish_reason
        stop_reason = bedrock_response.get("stopReason", "stop")
        finish_reason_map: Dict[
            str, Literal["stop", "length", "tool_calls", "content_filter"]
        ] = {
            "end_turn": "stop",
            "tool_use": "tool_calls",
            "max_tokens": "length",
            "stop_sequence": "stop",
            "content_filtered": "content_filter",
        }
        finish_reason = finish_reason_map.get(stop_reason, "stop")

        # Build ChatCompletionMessage
        message = ChatCompletionMessage(
            role="assistant",
            content=content,
            tool_calls=tool_calls if tool_calls else None,
        )

        # Extract usage statistics
        usage_data = bedrock_response.get("usage", {})
        usage = CompletionUsage(
            prompt_tokens=usage_data.get("inputTokens", 0),
            completion_tokens=usage_data.get("outputTokens", 0),
            total_tokens=usage_data.get("totalTokens", 0),
        )

        # Build ChatCompletion
        return ChatCompletion(
            id=f"bedrock-{int(time.time())}",
            model=model_id,
            choices=[Choice(index=0, message=message, finish_reason=finish_reason)],
            created=int(time.time()),
            object="chat.completion",
            usage=usage,
        )

    @staticmethod
    def _supports_tool_calling(model_id: str, provider: str) -> bool:
        """Determine if a model supports tool calling based on provider and model ID.

        Based on AWS Bedrock Converse API documentation:
        https://docs.aws.amazon.com/bedrock/latest/userguide/conversation-inference-supported-models-features.html

        Args:
            model_id: The model identifier
            provider: The provider name (e.g., "Anthropic", "Meta", "Amazon")

        Returns:
            True if the model supports tool calling, False otherwise
        """
        model_lower = model_id.lower()

        # Anthropic Claude - all Claude 3+ models support tool calling
        # Includes: Claude 3, 3.5, 3.7, 4, 4.5 (all variants)
        # Excludes: Claude 2.x
        if provider == "Anthropic":
            return (
                "claude-3" in model_lower
                or "claude-4" in model_lower
                or "sonnet-4" in model_lower
                or "haiku-4" in model_lower
                or "opus-4" in model_lower
            )

        # Amazon Nova - all Nova models support tool calling
        if provider == "Amazon":
            return "nova" in model_lower

        # AI21 Labs - only Jamba 1.5 models
        if provider == "AI21 Labs":
            return "jamba-1.5" in model_lower or "jamba1.5" in model_lower

        # Cohere - Command R models
        if provider == "Cohere":
            return "command-r" in model_lower

        # Meta Llama - only 3.1, 3.2, and 4.x
        # Excludes: Earlier Llama 3 models (e.g., llama3-70b)
        if provider == "Meta":
            return any(
                version in model_lower
                for version in [
                    "llama3-1",
                    "llama3.1",
                    "llama-3-1",
                    "llama3-2",
                    "llama3.2",
                    "llama-3-2",
                    "llama4",
                    "llama-4",
                ]
            )

        # Mistral AI - Large, Small, and Pixtral Large models
        # Excludes: Instruct-only models
        if provider == "Mistral AI":
            return any(
                model in model_lower for model in ["large", "small", "pixtral-large"]
            )

        # Writer - Palmyra X4 and X5
        if provider == "Writer":
            return "palmyra-x4" in model_lower or "palmyra-x5" in model_lower

        return False

    def list_models(self, tool_calling_only: bool = True) -> List[ModelInfo]:
        """List available Bedrock foundation models.

        Similar to Databricks list_models() but for Bedrock model catalog.

        Args:
            tool_calling_only: If True, only return models that support tool calling.
                             Defaults to True since tool calling is required for agent workflows.

        Returns:
            List of ModelInfo dicts with metadata:
                [
                    {
                        "model_id": "anthropic.claude-3-5-sonnet-20241022-v2:0",
                        "model_name": "Claude 3.5 Sonnet",
                        "provider_name": "Anthropic",
                        "supports_tool_use": True,
                        ...
                    },
                    ...
                ]
        """
        try:
            response = self.bedrock.list_foundation_models()
        except Exception as e:
            logger.error(f"Failed to list Bedrock models: {e}")
            return []

        models = []
        for model in response.get("modelSummaries", []):
            model_id = model.get("modelId", "")
            provider = model.get("providerName", "")

            # Determine tool calling support based on provider and model ID
            supports_tool_use = self._supports_tool_calling(model_id, provider)

            # Skip if filtering for tool calling only
            if tool_calling_only and not supports_tool_use:
                continue

            models.append(
                ModelInfo(
                    model_id=model_id,
                    model_name=model.get("modelName"),
                    provider_name=provider,
                    supports_tool_use=supports_tool_use,
                    state="READY",  # AWS Bedrock models are always ready if returned
                )
            )

        return models
