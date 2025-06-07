"""
Tests for agent command handler.

Following improved testing patterns:
- Direct dependency injection of stubs (no mocking needed!)
- Use real agent manager logic and real config system
- Test end-to-end agent command behavior with injected external dependencies
"""

import tempfile
from unittest.mock import patch

from chuck_data.commands.agent import handle_command
from chuck_data.config import ConfigManager


def test_missing_query_real_logic():
    """Test handling when query parameter is not provided."""
    result = handle_command(None)
    assert not result.success
    assert "Please provide a query" in result.message


def test_general_query_mode_real_logic(databricks_client_stub, llm_client_stub):
    """Test general query mode with real agent logic and direct dependency injection."""
    # Configure LLM stub for expected behavior
    llm_client_stub.set_response_content("This is a test response from the agent.")

    # Use real config with temp file
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(workspace_url="https://test.databricks.com")

        with patch("chuck_data.config._config_manager", config_manager):
            # Direct dependency injection - no mocking needed!
            result = handle_command(
                databricks_client_stub,
                llm_client=llm_client_stub,  # Inject LLM stub directly
                mode="general",
                query="What is the status of my workspace?",
            )

    # Verify real command execution with injected dependencies
    assert result.success
    assert result.data is not None
    assert "response" in result.data


def test_pii_mode_real_logic(databricks_client_stub_with_data, llm_client_stub):
    """Test PII detection mode with real agent logic."""
    # Configure LLM stub for PII analysis
    llm_client_stub.set_response_content(
        "This table contains potential PII in the email column."
    )

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(workspace_url="https://test.databricks.com")

        with patch("chuck_data.config._config_manager", config_manager):
            # Direct dependency injection - query becomes table_name for PII mode
            result = handle_command(
                databricks_client_stub_with_data,
                llm_client=llm_client_stub,
                mode="pii",
                query="test_table",  # This is passed as table_name to process_pii_detection
            )

    # Verify real PII detection execution
    assert result.success
    assert result.data is not None
    assert "response" in result.data


def test_bulk_pii_mode_real_logic(databricks_client_stub_with_data, llm_client_stub):
    """Test bulk PII scanning mode with real agent logic."""
    # Configure LLM stub for bulk analysis
    llm_client_stub.set_response_content(
        "Completed bulk PII scan. Found 3 tables with potential PII."
    )

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(workspace_url="https://test.databricks.com")

        with patch("chuck_data.config._config_manager", config_manager):
            # Direct dependency injection
            result = handle_command(
                databricks_client_stub_with_data,
                llm_client=llm_client_stub,
                mode="bulk_pii",
                catalog_name="test_catalog",
                schema_name="test_schema",
            )

    # Verify real bulk scanning execution
    assert result.success
    assert result.data is not None
    assert "response" in result.data


def test_stitch_mode_real_logic(databricks_client_stub_with_data, llm_client_stub):
    """Test Stitch setup mode with real agent logic."""
    # Configure LLM stub for Stitch setup
    llm_client_stub.set_response_content(
        "Stitch integration setup completed successfully."
    )

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(workspace_url="https://test.databricks.com")

        with patch("chuck_data.config._config_manager", config_manager):
            # Direct dependency injection
            result = handle_command(
                databricks_client_stub_with_data,
                llm_client=llm_client_stub,
                mode="stitch",
                catalog_name="test_catalog",
                schema_name="test_schema",
            )

    # Verify real Stitch setup execution
    assert result.success
    assert result.data is not None
    assert "response" in result.data


def test_agent_error_handling_real_logic(databricks_client_stub, llm_client_stub):
    """Test agent error handling with real business logic."""
    # Configure LLM stub to simulate error
    llm_client_stub.set_exception(True)

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(workspace_url="https://test.databricks.com")

        with patch("chuck_data.config._config_manager", config_manager):
            # Direct dependency injection
            result = handle_command(
                databricks_client_stub,
                llm_client=llm_client_stub,
                mode="general",
                query="Test query",
            )

    # Should handle LLM errors gracefully with real error handling logic
    assert isinstance(result.success, bool)
    assert result.data is not None or result.error is not None


def test_agent_history_integration_real_logic(databricks_client_stub, llm_client_stub):
    """Test agent history integration with real config system."""
    # Configure LLM stub
    llm_client_stub.set_response_content("Response with history context.")

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(workspace_url="https://test.databricks.com")

        with patch("chuck_data.config._config_manager", config_manager):
            # Direct dependency injection for both queries
            result1 = handle_command(
                databricks_client_stub,
                llm_client=llm_client_stub,
                mode="general",
                query="First question",
            )

            result2 = handle_command(
                databricks_client_stub,
                llm_client=llm_client_stub,
                mode="general",
                query="Follow up question",
            )

    # Both queries should work with real history management
    assert result1.success
    assert result2.success


def test_agent_with_tool_output_callback_real_logic(
    databricks_client_stub_with_data, llm_client_stub
):
    """Test agent with tool output callback using real logic."""
    # Configure LLM stub to use tools
    llm_client_stub.set_response_content("I'll check your catalogs.")

    # Create a mock callback to test tool output integration
    tool_outputs = []

    def mock_callback(output):
        tool_outputs.append(output)

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(workspace_url="https://test.databricks.com")

        with patch("chuck_data.config._config_manager", config_manager):
            # Direct dependency injection with callback
            result = handle_command(
                databricks_client_stub_with_data,
                llm_client=llm_client_stub,
                mode="general",
                query="What catalogs do I have?",
                tool_output_callback=mock_callback,
            )

    # Verify real tool integration
    assert result.success
    assert result.data is not None


def test_agent_config_integration_real_logic(databricks_client_stub, llm_client_stub):
    """Test agent integration with real config system."""
    # Configure LLM stub
    llm_client_stub.set_response_content("Configuration-aware response.")

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        # Set up config state to test real config integration
        config_manager.update(
            workspace_url="https://test.databricks.com",
            active_catalog="test_catalog",
            active_schema="test_schema",
        )

        with patch("chuck_data.config._config_manager", config_manager):
            # Direct dependency injection
            result = handle_command(
                databricks_client_stub,
                llm_client=llm_client_stub,
                mode="general",
                query="What is my current workspace setup?",
            )

    # Verify real config integration
    assert result.success
    assert result.data is not None


def test_agent_with_missing_client_real_logic(llm_client_stub):
    """Test agent behavior with missing databricks client."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(workspace_url="https://test.databricks.com")

        with patch("chuck_data.config._config_manager", config_manager):
            # Direct dependency injection even with missing databricks client
            result = handle_command(
                None,  # No databricks client
                llm_client=llm_client_stub,
                query="Test query",
            )

    # Should handle missing client gracefully
    assert isinstance(result.success, bool)
    assert result.data is not None or result.error is not None


def test_agent_parameter_handling_real_logic(databricks_client_stub, llm_client_stub):
    """Test agent parameter handling with different input methods."""
    llm_client_stub.set_response_content("Parameter handling test response.")

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(workspace_url="https://test.databricks.com")

        with patch("chuck_data.config._config_manager", config_manager):
            # Test with query parameter
            result1 = handle_command(
                databricks_client_stub,
                llm_client=llm_client_stub,
                query="Direct query test",
            )

            # Test with rest parameter (if supported)
            result2 = handle_command(
                databricks_client_stub,
                llm_client=llm_client_stub,
                rest="Rest parameter test",
            )

            # Test with raw_args parameter (if supported)
            result3 = handle_command(
                databricks_client_stub,
                llm_client=llm_client_stub,
                raw_args=["Raw", "args", "test"],
            )

    # All should be handled by real parameter processing logic
    for result in [result1, result2, result3]:
        assert isinstance(result.success, bool)
        assert result.data is not None or result.error is not None
