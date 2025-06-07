"""
Tests for the AgentManager class.
"""

import pytest
import sys
from unittest.mock import patch, MagicMock

# Mock the optional openai dependency used by LLMClient if it is not
# installed. This prevents import errors during test collection.
sys.modules.setdefault("openai", MagicMock())

from chuck_data.agent import AgentManager  # noqa: E402
from tests.fixtures.llm import LLMClientStub, MockToolCall  # noqa: E402
from chuck_data.agent.prompts import (  # noqa: E402
    PII_AGENT_SYSTEM_MESSAGE,
    BULK_PII_AGENT_SYSTEM_MESSAGE,
    STITCH_AGENT_SYSTEM_MESSAGE,
)


@pytest.fixture
def mock_api_client():
    """Mock API client fixture."""
    return MagicMock()


@pytest.fixture
def llm_client_stub():
    """LLM client stub fixture."""
    return LLMClientStub()


@pytest.fixture
def mock_callback():
    """Mock callback fixture."""
    return MagicMock()


@pytest.fixture
def agent_manager_setup(mock_api_client, llm_client_stub):
    """Set up AgentManager with mocked dependencies."""
    with (
        patch(
            "chuck_data.agent.manager.LLMClient", return_value=llm_client_stub
        ) as mock_llm_client,
        patch("chuck_data.agent.manager.get_tool_schemas") as mock_get_schemas,
        patch("chuck_data.agent.manager.execute_tool") as mock_execute_tool,
    ):

        agent_manager = AgentManager(mock_api_client, model="test-model")

        return {
            "agent_manager": agent_manager,
            "mock_api_client": mock_api_client,
            "llm_client_stub": llm_client_stub,
            "mock_llm_client": mock_llm_client,
            "mock_get_schemas": mock_get_schemas,
            "mock_execute_tool": mock_execute_tool,
        }


def test_agent_manager_initialization(agent_manager_setup):
    """Test that AgentManager initializes correctly."""
    setup = agent_manager_setup
    agent_manager = setup["agent_manager"]
    mock_api_client = setup["mock_api_client"]
    llm_client_stub = setup["llm_client_stub"]
    mock_llm_client = setup["mock_llm_client"]

    mock_llm_client.assert_called_once()  # Check LLMClient was instantiated
    assert agent_manager.api_client == mock_api_client
    assert agent_manager.model == "test-model"
    assert agent_manager.tool_output_callback is None  # Default to None
    expected_history = [
        {
            "role": "system",
            "content": agent_manager.conversation_history[0]["content"],
        }
    ]
    assert agent_manager.conversation_history == expected_history
    assert agent_manager.llm_client is llm_client_stub


def test_agent_manager_initialization_with_callback(
    mock_api_client, mock_callback, llm_client_stub
):
    """Test that AgentManager initializes correctly with a callback."""
    with patch("chuck_data.agent.manager.LLMClient", return_value=llm_client_stub):
        agent_with_callback = AgentManager(
            mock_api_client,
            model="test-model",
            tool_output_callback=mock_callback,
        )
        assert agent_with_callback.api_client == mock_api_client
        assert agent_with_callback.model == "test-model"
        assert agent_with_callback.tool_output_callback == mock_callback


def test_add_user_message(agent_manager_setup):
    """Test adding a user message."""
    agent_manager = agent_manager_setup["agent_manager"]
    # Reset conversation history for this test
    agent_manager.conversation_history = []

    agent_manager.add_user_message("Hello agent!")
    expected_history = [
        {"role": "user", "content": "Hello agent!"},
    ]
    assert agent_manager.conversation_history == expected_history

    agent_manager.add_user_message("Another message.")
    expected_history.append({"role": "user", "content": "Another message."})
    assert agent_manager.conversation_history == expected_history


def test_add_assistant_message(agent_manager_setup):
    """Test adding an assistant message."""
    agent_manager = agent_manager_setup["agent_manager"]
    # Reset conversation history for this test
    agent_manager.conversation_history = []

    agent_manager.add_assistant_message("Hello user!")
    expected_history = [
        {"role": "assistant", "content": "Hello user!"},
    ]
    assert agent_manager.conversation_history == expected_history

    agent_manager.add_assistant_message("How can I help?")
    expected_history.append({"role": "assistant", "content": "How can I help?"})
    assert agent_manager.conversation_history == expected_history


def test_add_system_message_new(agent_manager_setup):
    """Test adding a system message when none exists."""
    agent_manager = agent_manager_setup["agent_manager"]
    agent_manager.add_system_message("You are a helpful assistant.")
    expected_history = [{"role": "system", "content": "You are a helpful assistant."}]
    assert agent_manager.conversation_history == expected_history

    # Add another message to ensure system message stays at the start
    agent_manager.add_user_message("User query")
    expected_history.append({"role": "user", "content": "User query"})
    assert agent_manager.conversation_history == expected_history


def test_add_system_message_replace(agent_manager_setup):
    """Test adding a system message replaces an existing one."""
    agent_manager = agent_manager_setup["agent_manager"]
    agent_manager.add_system_message("Initial system message.")
    agent_manager.add_user_message("User query")
    agent_manager.add_system_message("Updated system message.")

    expected_history = [
        {"role": "system", "content": "Updated system message."},
        {"role": "user", "content": "User query"},
    ]
    assert agent_manager.conversation_history == expected_history

    # --- Tests for process_with_tools ---


def test_process_with_tools_no_tool_calls(agent_manager_setup):
    """Test processing when the LLM responds with content only."""
    agent_manager = agent_manager_setup["agent_manager"]
    llm_client_stub = agent_manager_setup["llm_client_stub"]

    # Setup
    mock_tools = [{"type": "function", "function": {"name": "dummy_tool"}}]

    # Mock the LLM client response - content only, no tool calls
    mock_resp = MagicMock()
    mock_resp.choices = [MagicMock()]
    mock_resp.choices[0].delta = MagicMock(content="Final answer.", tool_calls=None)
    # Configure stub to return the mock response directly
    llm_client_stub.set_response_content("Final answer.")

    # Run the method
    agent_manager.process_with_tools = MagicMock(return_value="Final answer.")

    # Call the method
    result = agent_manager.process_with_tools(mock_tools)

    # Assertions
    assert result == "Final answer."


def test_process_with_tools_iteration_limit(agent_manager_setup):
    """Ensure process_with_tools stops after the max iteration limit."""
    agent_manager = agent_manager_setup["agent_manager"]
    llm_client_stub = agent_manager_setup["llm_client_stub"]
    mock_execute_tool = agent_manager_setup["mock_execute_tool"]

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
    llm_client_stub.set_tool_calls([mock_tool_call])
    mock_execute_tool.return_value = {"result": "ok"}

    result = agent_manager.process_with_tools(mock_tools, max_iterations=2)

    assert result == "Error: maximum iterations reached."


def test_process_pii_detection(agent_manager_setup):
    """Test process_pii_detection sets up context and calls process_with_tools."""
    agent_manager = agent_manager_setup["agent_manager"]

    with patch.object(
        agent_manager, "process_with_tools", return_value="PII analysis complete."
    ) as mock_process:
        result = agent_manager.process_pii_detection("my_table")

        assert result == "PII analysis complete."
        # Check system message
        assert agent_manager.conversation_history[0]["role"] == "system"
        assert (
            agent_manager.conversation_history[0]["content"] == PII_AGENT_SYSTEM_MESSAGE
        )
        # Check user message
        assert agent_manager.conversation_history[1]["role"] == "user"
        assert (
            agent_manager.conversation_history[1]["content"]
            == "Analyze the table 'my_table' for PII data."
        )
        # Check call to process_with_tools - it should be called with real tool schemas
        mock_process.assert_called_once()
        # Verify the call was made with some tools (the exact tools will be from get_tool_schemas)
        call_args = mock_process.call_args[0][0]  # First argument of the call
        assert isinstance(call_args, list)
        assert len(call_args) > 0  # Should have at least some tools


def test_process_bulk_pii_scan(agent_manager_setup):
    """Test process_bulk_pii_scan sets up context and calls process_with_tools."""
    agent_manager = agent_manager_setup["agent_manager"]

    with patch.object(
        agent_manager, "process_with_tools", return_value="Bulk PII scan complete."
    ) as mock_process:
        result = agent_manager.process_bulk_pii_scan(
            catalog_name="cat", schema_name="sch"
        )

        assert result == "Bulk PII scan complete."
        # Check system message
        assert agent_manager.conversation_history[0]["role"] == "system"
        assert (
            agent_manager.conversation_history[0]["content"]
            == BULK_PII_AGENT_SYSTEM_MESSAGE
        )
        # Check user message
        assert agent_manager.conversation_history[1]["role"] == "user"
        assert (
            agent_manager.conversation_history[1]["content"]
            == "Scan all tables in catalog 'cat' and schema 'sch' for PII data."
        )
        # Check call to process_with_tools
        mock_process.assert_called_once()
        # Verify the call was made with some tools (the exact tools will be from get_tool_schemas)
        call_args = mock_process.call_args[0][0]  # First argument of the call
        assert isinstance(call_args, list)
        assert len(call_args) > 0  # Should have at least some tools


def test_process_setup_stitch(agent_manager_setup):
    """Test process_setup_stitch sets up context and calls process_with_tools."""
    agent_manager = agent_manager_setup["agent_manager"]

    with patch.object(
        agent_manager, "process_with_tools", return_value="Stitch setup complete."
    ) as mock_process:
        result = agent_manager.process_setup_stitch(
            catalog_name="cat", schema_name="sch"
        )

        assert result == "Stitch setup complete."
        # Check system message
        assert agent_manager.conversation_history[0]["role"] == "system"
        assert (
            agent_manager.conversation_history[0]["content"]
            == STITCH_AGENT_SYSTEM_MESSAGE
        )
        # Check user message
        assert agent_manager.conversation_history[1]["role"] == "user"
        assert (
            agent_manager.conversation_history[1]["content"]
            == "Set up a Stitch integration for catalog 'cat' and schema 'sch'."
        )
        # Check call to process_with_tools
        mock_process.assert_called_once()
        # Verify the call was made with some tools (the exact tools will be from get_tool_schemas)
        call_args = mock_process.call_args[0][0]  # First argument of the call
        assert isinstance(call_args, list)
        assert len(call_args) > 0  # Should have at least some tools


def test_process_query(agent_manager_setup):
    """Test process_query adds user message and calls process_with_tools."""
    agent_manager = agent_manager_setup["agent_manager"]

    # Reset the conversation history to a clean state for this test
    agent_manager.conversation_history = []
    agent_manager.add_system_message("General assistant.")
    agent_manager.add_user_message("Previous question.")
    agent_manager.add_assistant_message("Previous answer.")

    with patch.object(
        agent_manager, "process_with_tools", return_value="Query processed."
    ) as mock_process:
        result = agent_manager.process_query("What is the weather?")

        assert result == "Query processed."
        # Check latest user message
        assert agent_manager.conversation_history[-1]["role"] == "user"
        assert (
            agent_manager.conversation_history[-1]["content"] == "What is the weather?"
        )
        # Check call to process_with_tools
        mock_process.assert_called_once()
        # Verify the call was made with some tools (the exact tools will be from get_tool_schemas)
        call_args = mock_process.call_args[0][0]  # First argument of the call
        assert isinstance(call_args, list)
        assert len(call_args) > 0  # Should have at least some tools
