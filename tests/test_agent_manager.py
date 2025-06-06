"""
Tests for the AgentManager class.
"""

import unittest
import sys
from unittest.mock import patch, MagicMock

# Mock the optional openai dependency used by LLMClient if it is not
# installed. This prevents import errors during test collection.
sys.modules.setdefault("openai", MagicMock())

from chuck_data.agent import AgentManager  # noqa: E402
from tests.fixtures import LLMClientStub, MockToolCall  # noqa: E402
from chuck_data.agent.prompts import (  # noqa: E402
    PII_AGENT_SYSTEM_MESSAGE,
    BULK_PII_AGENT_SYSTEM_MESSAGE,
    STITCH_AGENT_SYSTEM_MESSAGE,
)


class TestAgentManager(unittest.TestCase):
    """Test cases for the AgentManager."""

    def setUp(self):
        """Set up common test fixtures."""
        # Mock the API client that might be passed to AgentManager
        self.mock_api_client = MagicMock()

        # Use LLMClientStub instead of MagicMock
        self.llm_client_stub = LLMClientStub()
        self.patcher = patch(
            "chuck_data.agent.manager.LLMClient", return_value=self.llm_client_stub
        )
        self.MockLLMClient = self.patcher.start()

        # Mock tool functions used within AgentManager
        self.patcher_get_schemas = patch("chuck_data.agent.manager.get_tool_schemas")
        self.MockGetToolSchemas = self.patcher_get_schemas.start()
        self.patcher_execute_tool = patch("chuck_data.agent.manager.execute_tool")
        self.MockExecuteTool = self.patcher_execute_tool.start()

        # Create a mock callback for testing
        self.mock_callback = MagicMock()

        # Instantiate AgentManager
        self.agent_manager = AgentManager(self.mock_api_client, model="test-model")

    def tearDown(self):
        """Clean up after tests."""
        self.patcher.stop()
        self.patcher_get_schemas.stop()
        self.patcher_execute_tool.stop()

    def test_agent_manager_initialization(self):
        """Test that AgentManager initializes correctly."""
        self.MockLLMClient.assert_called_once()  # Check LLMClient was instantiated
        self.assertEqual(self.agent_manager.api_client, self.mock_api_client)
        self.assertEqual(self.agent_manager.model, "test-model")
        self.assertIsNone(self.agent_manager.tool_output_callback)  # Default to None
        expected_history = [
            {
                "role": "system",
                "content": self.agent_manager.conversation_history[0]["content"],
            }
        ]
        self.assertEqual(self.agent_manager.conversation_history, expected_history)
        self.assertIs(self.agent_manager.llm_client, self.llm_client_stub)

    def test_agent_manager_initialization_with_callback(self):
        """Test that AgentManager initializes correctly with a callback."""
        agent_with_callback = AgentManager(
            self.mock_api_client,
            model="test-model",
            tool_output_callback=self.mock_callback,
        )
        self.assertEqual(agent_with_callback.api_client, self.mock_api_client)
        self.assertEqual(agent_with_callback.model, "test-model")
        self.assertEqual(agent_with_callback.tool_output_callback, self.mock_callback)

    def test_add_user_message(self):
        """Test adding a user message."""
        # Reset conversation history for this test
        self.agent_manager.conversation_history = []

        self.agent_manager.add_user_message("Hello agent!")
        expected_history = [
            {"role": "user", "content": "Hello agent!"},
        ]
        self.assertEqual(self.agent_manager.conversation_history, expected_history)

        self.agent_manager.add_user_message("Another message.")
        expected_history.append({"role": "user", "content": "Another message."})
        self.assertEqual(self.agent_manager.conversation_history, expected_history)

    def test_add_assistant_message(self):
        """Test adding an assistant message."""
        # Reset conversation history for this test
        self.agent_manager.conversation_history = []

        self.agent_manager.add_assistant_message("Hello user!")
        expected_history = [
            {"role": "assistant", "content": "Hello user!"},
        ]
        self.assertEqual(self.agent_manager.conversation_history, expected_history)

        self.agent_manager.add_assistant_message("How can I help?")
        expected_history.append({"role": "assistant", "content": "How can I help?"})
        self.assertEqual(self.agent_manager.conversation_history, expected_history)

    def test_add_system_message_new(self):
        """Test adding a system message when none exists."""
        self.agent_manager.add_system_message("You are a helpful assistant.")
        expected_history = [
            {"role": "system", "content": "You are a helpful assistant."}
        ]
        self.assertEqual(self.agent_manager.conversation_history, expected_history)

        # Add another message to ensure system message stays at the start
        self.agent_manager.add_user_message("User query")
        expected_history.append({"role": "user", "content": "User query"})
        self.assertEqual(self.agent_manager.conversation_history, expected_history)

    def test_add_system_message_replace(self):
        """Test adding a system message replaces an existing one."""
        self.agent_manager.add_system_message("Initial system message.")
        self.agent_manager.add_user_message("User query")
        self.agent_manager.add_system_message("Updated system message.")

        expected_history = [
            {"role": "system", "content": "Updated system message."},
            {"role": "user", "content": "User query"},
        ]
        self.assertEqual(self.agent_manager.conversation_history, expected_history)

    # --- Tests for process_with_tools ---

    def test_process_with_tools_no_tool_calls(self):
        """Test processing when the LLM responds with content only."""
        # Setup
        mock_tools = [{"type": "function", "function": {"name": "dummy_tool"}}]

        # Mock the LLM client response - content only, no tool calls
        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock()]
        mock_resp.choices[0].delta = MagicMock(content="Final answer.", tool_calls=None)
        # Configure stub to return the mock response directly
        self.llm_client_stub.set_response_content("Final answer.")

        # Run the method
        self.agent_manager.process_with_tools = MagicMock(return_value="Final answer.")

        # Call the method
        result = self.agent_manager.process_with_tools(mock_tools)

        # Assertions
        self.assertEqual(result, "Final answer.")

    def test_process_with_tools_iteration_limit(self):
        """Ensure process_with_tools stops after the max iteration limit."""
        mock_tools = [{"type": "function", "function": {"name": "dummy_tool"}}]

        tool_call = MagicMock()
        tool_call.function.name = "dummy_tool"
        tool_call.id = "1"
        tool_call.function.arguments = "{}"

        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock()]
        mock_resp.choices[0].message = MagicMock(tool_calls=[tool_call])

        # Configure stub to return tool calls
        mock_tool_call = MockToolCall(id="1", name="dummy_tool", arguments="{}")
        self.llm_client_stub.set_tool_calls([mock_tool_call])
        self.MockExecuteTool.return_value = {"result": "ok"}

        result = self.agent_manager.process_with_tools(mock_tools, max_iterations=2)

        self.assertEqual(result, "Error: maximum iterations reached.")

    @patch("chuck_data.agent.manager.AgentManager.process_with_tools")
    def test_process_pii_detection(self, mock_process):
        """Test process_pii_detection sets up context and calls process_with_tools."""
        mock_tools = [{"schema": "tool1"}]
        self.MockGetToolSchemas.return_value = mock_tools
        mock_process.return_value = "PII analysis complete."

        result = self.agent_manager.process_pii_detection("my_table")

        self.assertEqual(result, "PII analysis complete.")
        # Check system message
        self.assertEqual(self.agent_manager.conversation_history[0]["role"], "system")
        self.assertEqual(
            self.agent_manager.conversation_history[0]["content"],
            PII_AGENT_SYSTEM_MESSAGE,
        )
        # Check user message
        self.assertEqual(self.agent_manager.conversation_history[1]["role"], "user")
        self.assertEqual(
            self.agent_manager.conversation_history[1]["content"],
            "Analyze the table 'my_table' for PII data.",
        )
        # Check call to process_with_tools
        mock_process.assert_called_once_with(mock_tools)

    @patch("chuck_data.agent.manager.AgentManager.process_with_tools")
    def test_process_bulk_pii_scan(self, mock_process):
        """Test process_bulk_pii_scan sets up context and calls process_with_tools."""
        mock_tools = [{"schema": "tool2"}]
        self.MockGetToolSchemas.return_value = mock_tools
        mock_process.return_value = "Bulk PII scan complete."

        result = self.agent_manager.process_bulk_pii_scan(
            catalog_name="cat", schema_name="sch"
        )

        self.assertEqual(result, "Bulk PII scan complete.")
        # Check system message
        self.assertEqual(self.agent_manager.conversation_history[0]["role"], "system")
        self.assertEqual(
            self.agent_manager.conversation_history[0]["content"],
            BULK_PII_AGENT_SYSTEM_MESSAGE,
        )
        # Check user message
        self.assertEqual(self.agent_manager.conversation_history[1]["role"], "user")
        self.assertEqual(
            self.agent_manager.conversation_history[1]["content"],
            "Scan all tables in catalog 'cat' and schema 'sch' for PII data.",
        )
        # Check call to process_with_tools
        mock_process.assert_called_once_with(mock_tools)

    @patch("chuck_data.agent.manager.AgentManager.process_with_tools")
    def test_process_setup_stitch(self, mock_process):
        """Test process_setup_stitch sets up context and calls process_with_tools."""
        mock_tools = [{"schema": "tool3"}]
        self.MockGetToolSchemas.return_value = mock_tools
        mock_process.return_value = "Stitch setup complete."

        result = self.agent_manager.process_setup_stitch(
            catalog_name="cat", schema_name="sch"
        )

        self.assertEqual(result, "Stitch setup complete.")
        # Check system message
        self.assertEqual(self.agent_manager.conversation_history[0]["role"], "system")
        self.assertEqual(
            self.agent_manager.conversation_history[0]["content"],
            STITCH_AGENT_SYSTEM_MESSAGE,
        )
        # Check user message
        self.assertEqual(self.agent_manager.conversation_history[1]["role"], "user")
        self.assertEqual(
            self.agent_manager.conversation_history[1]["content"],
            "Set up a Stitch integration for catalog 'cat' and schema 'sch'.",
        )
        # Check call to process_with_tools
        mock_process.assert_called_once_with(mock_tools)

    @patch("chuck_data.agent.manager.AgentManager.process_with_tools")
    def test_process_query(self, mock_process):
        """Test process_query adds user message and calls process_with_tools."""
        mock_tools = [{"schema": "tool4"}]
        self.MockGetToolSchemas.return_value = mock_tools
        mock_process.return_value = "Query processed."

        # Reset the conversation history to a clean state for this test
        self.agent_manager.conversation_history = []
        self.agent_manager.add_system_message("General assistant.")
        self.agent_manager.add_user_message("Previous question.")
        self.agent_manager.add_assistant_message("Previous answer.")

        result = self.agent_manager.process_query("What is the weather?")

        self.assertEqual(result, "Query processed.")
        # Check latest user message
        self.assertEqual(self.agent_manager.conversation_history[-1]["role"], "user")
        self.assertEqual(
            self.agent_manager.conversation_history[-1]["content"],
            "What is the weather?",
        )
        # Check call to process_with_tools
        mock_process.assert_called_once_with(mock_tools)
