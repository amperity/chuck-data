"""Tests for AWS Bedrock LLM provider.

Behavioral tests focused on provider functionality rather than implementation details.
Following the testing guidelines:
- Mock external boundaries only (boto3 clients)
- Use real AWSBedrockProvider logic (internal)
- Test behavioral outcomes, not implementation
"""

import json
import pytest
from unittest.mock import MagicMock, patch

from chuck_data.llm.providers.aws_bedrock import AWSBedrockProvider


class TestAWSBedrockProvider:
    """Test AWS Bedrock provider behavior."""

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_provider_instantiation_with_explicit_credentials(self, mock_boto3):
        """Provider initializes with explicit AWS credentials."""
        mock_boto3.client.return_value = MagicMock()

        provider = AWSBedrockProvider(
            region="us-west-2",
            aws_access_key_id="AKIATEST123",
            aws_secret_access_key="secret123",
            model_id="anthropic.claude-3-5-sonnet-20241022-v2:0",
        )

        assert provider.region == "us-west-2"
        assert provider.default_model == "anthropic.claude-3-5-sonnet-20241022-v2:0"

        # Verify boto3 clients were created with credentials
        assert mock_boto3.client.call_count == 2  # bedrock-runtime and bedrock

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_provider_instantiation_with_defaults(self, mock_boto3):
        """Provider uses sensible defaults when not configured."""
        mock_boto3.client.return_value = MagicMock()

        provider = AWSBedrockProvider()

        assert provider.region == "us-east-1"  # Default region
        assert provider.default_model == "anthropic.claude-3-5-sonnet-20240620-v1:0"

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_provider_initialization_failure_raises_error(self, mock_boto3):
        """Provider raises error when boto3 client creation fails."""
        mock_boto3.client.side_effect = Exception("AWS credentials not found")

        with pytest.raises(Exception, match="AWS credentials not found"):
            AWSBedrockProvider()

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_basic_conversation_without_tools(self, mock_boto3):
        """Basic chat conversation works without tools."""
        # Mock boto3 client
        mock_bedrock_runtime = MagicMock()
        mock_boto3.client.return_value = mock_bedrock_runtime

        # Mock Converse API response
        mock_bedrock_runtime.converse.return_value = {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [{"text": "Hello! I'm here to help."}],
                }
            },
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 8, "totalTokens": 18},
        }

        provider = AWSBedrockProvider()

        # Test chat
        messages = [{"role": "user", "content": "Hello"}]

        response = provider.chat(messages)

        # Verify response format (OpenAI ChatCompletion)
        assert response.choices[0].message.role == "assistant"
        assert response.choices[0].message.content == "Hello! I'm here to help."
        assert response.choices[0].finish_reason == "stop"
        assert response.usage.prompt_tokens == 10
        assert response.usage.completion_tokens == 8
        assert response.usage.total_tokens == 18

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_conversation_with_system_message(self, mock_boto3):
        """System messages are handled correctly."""
        mock_bedrock_runtime = MagicMock()
        mock_boto3.client.return_value = mock_bedrock_runtime

        mock_bedrock_runtime.converse.return_value = {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [{"text": "I am a helpful assistant."}],
                }
            },
            "stopReason": "end_turn",
            "usage": {"inputTokens": 15, "outputTokens": 6, "totalTokens": 21},
        }

        provider = AWSBedrockProvider()

        messages = [
            {"role": "system", "content": "You are a helpful assistant"},
            {"role": "user", "content": "Who are you?"},
        ]

        response = provider.chat(messages)

        # Verify system message was sent to Bedrock
        call_args = mock_bedrock_runtime.converse.call_args
        assert "system" in call_args[1]
        assert call_args[1]["system"] == [{"text": "You are a helpful assistant"}]

        # Verify response
        assert response.choices[0].message.content == "I am a helpful assistant."

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_tool_calling_request_and_response(self, mock_boto3):
        """Tool calling works end-to-end."""
        mock_bedrock_runtime = MagicMock()
        mock_boto3.client.return_value = mock_bedrock_runtime

        # Mock Bedrock response with tool use
        mock_bedrock_runtime.converse.return_value = {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [
                        {"text": "Let me check the weather for you."},
                        {
                            "toolUse": {
                                "toolUseId": "tool_123",
                                "name": "get_weather",
                                "input": {"location": "San Francisco"},
                            }
                        },
                    ],
                }
            },
            "stopReason": "tool_use",
            "usage": {"inputTokens": 50, "outputTokens": 30, "totalTokens": 80},
        }

        provider = AWSBedrockProvider()

        messages = [{"role": "user", "content": "What's the weather in San Francisco?"}]

        tools = [
            {
                "type": "function",
                "function": {
                    "name": "get_weather",
                    "description": "Get current weather for a location",
                    "parameters": {
                        "type": "object",
                        "properties": {"location": {"type": "string"}},
                        "required": ["location"],
                    },
                },
            }
        ]

        response = provider.chat(messages, tools=tools)

        # Verify tool call in response
        assert (
            response.choices[0].message.content == "Let me check the weather for you."
        )
        assert response.choices[0].message.tool_calls is not None
        assert len(response.choices[0].message.tool_calls) == 1

        tool_call = response.choices[0].message.tool_calls[0]
        assert tool_call.id == "tool_123"
        assert tool_call.type == "function"
        assert tool_call.function.name == "get_weather"
        assert json.loads(tool_call.function.arguments) == {"location": "San Francisco"}

        assert response.choices[0].finish_reason == "tool_calls"

        # Verify tool config was sent to Bedrock
        call_args = mock_bedrock_runtime.converse.call_args
        assert "toolConfig" in call_args[1]
        tool_config = call_args[1]["toolConfig"]
        assert "tools" in tool_config
        assert tool_config["tools"][0]["toolSpec"]["name"] == "get_weather"

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_tool_result_handling(self, mock_boto3):
        """Tool results are converted correctly for Bedrock."""
        mock_bedrock_runtime = MagicMock()
        mock_boto3.client.return_value = mock_bedrock_runtime

        mock_bedrock_runtime.converse.return_value = {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [{"text": "It's sunny in San Francisco!"}],
                }
            },
            "stopReason": "end_turn",
            "usage": {"inputTokens": 60, "outputTokens": 10, "totalTokens": 70},
        }

        provider = AWSBedrockProvider()

        # Conversation with tool result
        messages = [
            {"role": "user", "content": "What's the weather?"},
            {
                "role": "assistant",
                "content": "Let me check",
                "tool_calls": [
                    {
                        "id": "tool_123",
                        "type": "function",
                        "function": {
                            "name": "get_weather",
                            "arguments": '{"location": "San Francisco"}',
                        },
                    }
                ],
            },
            {
                "role": "tool",
                "tool_call_id": "tool_123",
                "content": '{"temperature": 72, "condition": "sunny"}',
            },
        ]

        provider.chat(messages)

        # Verify tool result was converted to Bedrock format
        call_args = mock_bedrock_runtime.converse.call_args
        bedrock_messages = call_args[1]["messages"]

        # Last message should be user role with toolResult
        tool_result_msg = bedrock_messages[-1]
        assert tool_result_msg["role"] == "user"
        assert "toolResult" in tool_result_msg["content"][0]
        assert tool_result_msg["content"][0]["toolResult"]["toolUseId"] == "tool_123"

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_tool_choice_required(self, mock_boto3):
        """tool_choice='required' is handled correctly."""
        mock_bedrock_runtime = MagicMock()
        mock_boto3.client.return_value = mock_bedrock_runtime

        mock_bedrock_runtime.converse.return_value = {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [
                        {
                            "toolUse": {
                                "toolUseId": "tool_456",
                                "name": "search",
                                "input": {"query": "test"},
                            }
                        }
                    ],
                }
            },
            "stopReason": "tool_use",
            "usage": {"inputTokens": 20, "outputTokens": 15, "totalTokens": 35},
        }

        provider = AWSBedrockProvider()

        messages = [{"role": "user", "content": "Search for test"}]
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "search",
                    "description": "Search function",
                    "parameters": {"type": "object", "properties": {}},
                },
            }
        ]

        provider.chat(messages, tools=tools, tool_choice="required")

        # Verify toolChoice was set to "any" in Bedrock request
        call_args = mock_bedrock_runtime.converse.call_args
        tool_config = call_args[1]["toolConfig"]
        assert "toolChoice" in tool_config
        assert "any" in tool_config["toolChoice"]

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_tool_choice_auto(self, mock_boto3):
        """tool_choice='auto' is handled correctly."""
        mock_bedrock_runtime = MagicMock()
        mock_boto3.client.return_value = mock_bedrock_runtime

        mock_bedrock_runtime.converse.return_value = {
            "output": {
                "message": {"role": "assistant", "content": [{"text": "Response"}]}
            },
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 5, "totalTokens": 15},
        }

        provider = AWSBedrockProvider()

        messages = [{"role": "user", "content": "Hello"}]
        tools = [
            {"type": "function", "function": {"name": "test_tool", "parameters": {}}}
        ]

        provider.chat(messages, tools=tools, tool_choice="auto")

        # Verify toolChoice was set to "auto"
        call_args = mock_bedrock_runtime.converse.call_args
        tool_config = call_args[1]["toolConfig"]
        assert "toolChoice" in tool_config
        assert "auto" in tool_config["toolChoice"]

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_model_parameter_overrides_default(self, mock_boto3):
        """Explicit model parameter overrides default model."""
        mock_bedrock_runtime = MagicMock()
        mock_boto3.client.return_value = mock_bedrock_runtime

        mock_bedrock_runtime.converse.return_value = {
            "output": {
                "message": {"role": "assistant", "content": [{"text": "Response"}]}
            },
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 5, "totalTokens": 15},
        }

        provider = AWSBedrockProvider(model_id="amazon.titan-text-express-v1")

        messages = [{"role": "user", "content": "Test"}]

        # Use different model
        provider.chat(messages, model="meta.llama3-70b-instruct-v1:0")

        # Verify correct model was used
        call_args = mock_bedrock_runtime.converse.call_args
        assert call_args[1]["modelId"] == "meta.llama3-70b-instruct-v1:0"

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_bedrock_api_error_propagates(self, mock_boto3):
        """Bedrock API errors are propagated correctly."""
        mock_bedrock_runtime = MagicMock()
        mock_boto3.client.return_value = mock_bedrock_runtime

        # Simulate API error
        mock_bedrock_runtime.converse.side_effect = Exception("Model not found")

        provider = AWSBedrockProvider()

        messages = [{"role": "user", "content": "Test"}]

        with pytest.raises(Exception, match="Model not found"):
            provider.chat(messages)

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_list_models_returns_model_catalog(self, mock_boto3):
        """list_models() returns Bedrock model catalog."""
        mock_bedrock = MagicMock()
        mock_boto3.client.return_value = mock_bedrock

        # Mock list_foundation_models response
        mock_bedrock.list_foundation_models.return_value = {
            "modelSummaries": [
                {
                    "modelId": "anthropic.claude-3-5-sonnet-20241022-v2:0",
                    "modelName": "Claude 3.5 Sonnet",
                    "providerName": "Anthropic",
                    "inferenceTypesSupported": ["ON_DEMAND"],
                    "inputModalities": ["TEXT"],
                    "outputModalities": ["TEXT"],
                    "responseStreamingSupported": True,
                },
                {
                    "modelId": "meta.llama3-70b-instruct-v1:0",
                    "modelName": "Llama 3 70B Instruct",
                    "providerName": "Meta",
                    "inferenceTypesSupported": ["ON_DEMAND"],
                    "inputModalities": ["TEXT"],
                    "outputModalities": ["TEXT"],
                    "responseStreamingSupported": False,
                },
            ]
        }

        provider = AWSBedrockProvider()
        models = provider.list_models()

        assert len(models) == 2

        # Verify first model
        assert models[0]["model_id"] == "anthropic.claude-3-5-sonnet-20241022-v2:0"
        assert models[0]["model_name"] == "Claude 3.5 Sonnet"
        assert models[0]["provider"] == "Anthropic"
        assert models[0]["supports_tool_use"] is True  # ON_DEMAND inference

        # Verify second model
        assert models[1]["model_id"] == "meta.llama3-70b-instruct-v1:0"
        assert models[1]["provider"] == "Meta"

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_list_models_handles_api_error(self, mock_boto3):
        """list_models() handles API errors gracefully."""
        mock_bedrock = MagicMock()
        mock_boto3.client.return_value = mock_bedrock

        mock_bedrock.list_foundation_models.side_effect = Exception("Access denied")

        provider = AWSBedrockProvider()
        models = provider.list_models()

        assert models == []  # Returns empty list on error

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_multiple_tool_calls_in_response(self, mock_boto3):
        """Multiple tool calls in one response are handled correctly."""
        mock_bedrock_runtime = MagicMock()
        mock_boto3.client.return_value = mock_bedrock_runtime

        mock_bedrock_runtime.converse.return_value = {
            "output": {
                "message": {
                    "role": "assistant",
                    "content": [
                        {"text": "I'll check both locations."},
                        {
                            "toolUse": {
                                "toolUseId": "tool_1",
                                "name": "get_weather",
                                "input": {"location": "NYC"},
                            }
                        },
                        {
                            "toolUse": {
                                "toolUseId": "tool_2",
                                "name": "get_weather",
                                "input": {"location": "SF"},
                            }
                        },
                    ],
                }
            },
            "stopReason": "tool_use",
            "usage": {"inputTokens": 40, "outputTokens": 35, "totalTokens": 75},
        }

        provider = AWSBedrockProvider()

        messages = [{"role": "user", "content": "Weather in NYC and SF?"}]
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "get_weather",
                    "parameters": {
                        "type": "object",
                        "properties": {"location": {"type": "string"}},
                    },
                },
            }
        ]

        response = provider.chat(messages, tools=tools)

        # Verify multiple tool calls
        assert len(response.choices[0].message.tool_calls) == 2
        assert response.choices[0].message.tool_calls[0].function.name == "get_weather"
        assert response.choices[0].message.tool_calls[1].function.name == "get_weather"

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_empty_content_handled(self, mock_boto3):
        """Empty or None content in messages is handled gracefully."""
        mock_bedrock_runtime = MagicMock()
        mock_boto3.client.return_value = mock_bedrock_runtime

        mock_bedrock_runtime.converse.return_value = {
            "output": {
                "message": {"role": "assistant", "content": [{"text": "Response"}]}
            },
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 5, "totalTokens": 15},
        }

        provider = AWSBedrockProvider()

        # Message with None content (can happen with tool-only responses)
        messages = [
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": None},
        ]

        response = provider.chat(messages)

        # Should handle gracefully
        assert response.choices[0].message.content == "Response"

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_streaming_not_yet_implemented(self, mock_boto3):
        """Streaming parameter is acknowledged but falls back to non-streaming."""
        mock_bedrock_runtime = MagicMock()
        mock_boto3.client.return_value = mock_bedrock_runtime

        mock_bedrock_runtime.converse.return_value = {
            "output": {
                "message": {"role": "assistant", "content": [{"text": "Response"}]}
            },
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 5, "totalTokens": 15},
        }

        provider = AWSBedrockProvider()

        messages = [{"role": "user", "content": "Test"}]

        # Request streaming (not implemented yet, should fall back gracefully)
        response = provider.chat(messages, stream=True)

        # Should still work (non-streaming)
        assert response.choices[0].message.content == "Response"


class TestAWSBedrockMessageConversion:
    """Test message format conversion helpers."""

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_convert_simple_messages(self, mock_boto3):
        """Simple user/assistant messages convert correctly."""
        mock_boto3.client.return_value = MagicMock()
        provider = AWSBedrockProvider()

        messages = [
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi there"},
        ]

        system, conversation = provider._convert_messages_to_bedrock(messages)

        assert system == []
        assert len(conversation) == 2
        assert conversation[0] == {"role": "user", "content": [{"text": "Hello"}]}
        assert conversation[1] == {
            "role": "assistant",
            "content": [{"text": "Hi there"}],
        }

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_convert_system_message(self, mock_boto3):
        """System messages are extracted correctly."""
        mock_boto3.client.return_value = MagicMock()
        provider = AWSBedrockProvider()

        messages = [
            {"role": "system", "content": "You are helpful"},
            {"role": "user", "content": "Hello"},
        ]

        system, conversation = provider._convert_messages_to_bedrock(messages)

        assert system == [{"text": "You are helpful"}]
        assert len(conversation) == 1
        assert conversation[0]["role"] == "user"


class TestAWSBedrockToolConversion:
    """Test tool configuration conversion."""

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_convert_single_tool(self, mock_boto3):
        """Single tool converts to Bedrock toolSpec format."""
        mock_boto3.client.return_value = MagicMock()
        provider = AWSBedrockProvider()

        tools = [
            {
                "type": "function",
                "function": {
                    "name": "get_weather",
                    "description": "Get weather",
                    "parameters": {
                        "type": "object",
                        "properties": {"location": {"type": "string"}},
                    },
                },
            }
        ]

        tool_config = provider._convert_tools_to_bedrock(tools, "auto")

        assert "tools" in tool_config
        assert len(tool_config["tools"]) == 1
        assert tool_config["tools"][0]["toolSpec"]["name"] == "get_weather"
        assert tool_config["tools"][0]["toolSpec"]["description"] == "Get weather"
        assert "inputSchema" in tool_config["tools"][0]["toolSpec"]

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_tool_choice_conversions(self, mock_boto3):
        """tool_choice parameter converts correctly."""
        mock_boto3.client.return_value = MagicMock()
        provider = AWSBedrockProvider()

        tools = [{"type": "function", "function": {"name": "test", "parameters": {}}}]

        # Test "auto"
        config_auto = provider._convert_tools_to_bedrock(tools, "auto")
        assert "auto" in config_auto["toolChoice"]

        # Test "required"
        config_required = provider._convert_tools_to_bedrock(tools, "required")
        assert "any" in config_required["toolChoice"]

        # Test "none"
        config_none = provider._convert_tools_to_bedrock(tools, "none")
        assert "toolChoice" not in config_none
