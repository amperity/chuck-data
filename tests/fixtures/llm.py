"""LLM client fixtures."""


class LLMClientStub:
    """Comprehensive stub for LLMClient with predictable responses."""

    def __init__(self):
        self.databricks_token = "test-token"
        self.base_url = "https://test.databricks.com"

        # Test configuration
        self.should_fail_chat = False
        self.should_raise_exception = False
        self.response_content = "Test LLM response"
        self.tool_calls = []
        self.streaming_responses = []

        # Track method calls for testing
        self.chat_calls = []

        # Pre-configured responses for specific scenarios
        self.configured_responses = {}

    def chat(self, messages, model=None, tools=None, stream=False, tool_choice="auto"):
        """Simulate LLM chat completion."""
        # Track the call
        call_info = {
            "messages": messages,
            "model": model,
            "tools": tools,
            "stream": stream,
            "tool_choice": tool_choice,
        }
        self.chat_calls.append(call_info)

        if self.should_raise_exception:
            raise Exception("Test LLM exception")

        if self.should_fail_chat:
            raise Exception("LLM API error")

        # Check for configured response based on messages
        messages_key = str(messages)
        if messages_key in self.configured_responses:
            return self.configured_responses[messages_key]

        # Create mock response structure
        mock_choice = MockChoice()
        mock_choice.message = MockMessage()

        if self.tool_calls:
            # Return tool calls if configured
            mock_choice.message.tool_calls = self.tool_calls
            mock_choice.message.content = None
        else:
            # Return content response
            mock_choice.message.content = self.response_content
            mock_choice.message.tool_calls = None

        mock_response = MockChatResponse()
        mock_response.choices = [mock_choice]

        return mock_response

    def set_response_content(self, content):
        """Set the content for the next chat response."""
        self.response_content = content

    def set_tool_calls(self, tool_calls):
        """Set tool calls for the next chat response."""
        self.tool_calls = tool_calls

    def configure_response_for_messages(self, messages, response):
        """Configure a specific response for specific messages."""
        self.configured_responses[str(messages)] = response

    def set_chat_failure(self, should_fail=True):
        """Configure chat to fail."""
        self.should_fail_chat = should_fail

    def set_exception(self, should_raise=True):
        """Configure chat to raise exception."""
        self.should_raise_exception = should_raise


class MockMessage:
    """Mock LLM message object."""

    def __init__(self):
        self.content = None
        self.tool_calls = None


class MockChoice:
    """Mock LLM choice object."""

    def __init__(self):
        self.message = None


class MockChatResponse:
    """Mock LLM chat response object."""

    def __init__(self):
        self.choices = []


class MockToolCall:
    """Mock LLM tool call object."""

    def __init__(self, id="test-id", name="test-function", arguments="{}"):
        self.id = id
        self.function = MockFunction(name, arguments)


class MockFunction:
    """Mock LLM function object."""

    def __init__(self, name, arguments):
        self.name = name
        self.arguments = arguments
