"""
Tests for models command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
This file focuses on the /models command specifically, with other model commands tested in dedicated files.
"""

from unittest.mock import patch

from chuck_data.commands.models import handle_command


# Direct command execution tests
def test_direct_command_lists_available_models(databricks_client_stub, temp_config):
    """Direct command successfully lists available models."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Setup test data
        databricks_client_stub.add_model("claude-v1", created_timestamp=123456789)
        databricks_client_stub.add_model("gpt-4", created_timestamp=987654321)

        # Execute command
        result = handle_command(databricks_client_stub)

        # Verify behavioral outcome
        assert result.success
        assert len(result.data) == 2
        assert any(model["name"] == "claude-v1" for model in result.data)
        assert any(model["name"] == "gpt-4" for model in result.data)


def test_direct_command_handles_empty_model_list(databricks_client_stub, temp_config):
    """Direct command handles empty model list gracefully."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Don't add any models to stub

        # Execute command
        result = handle_command(databricks_client_stub)

        # Verify behavioral outcome
        assert result.success
        assert result.data == []
        assert "No models found" in result.message


def test_databricks_api_errors_handled_gracefully(databricks_client_stub, temp_config):
    """Databricks API errors are handled gracefully."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Configure stub to raise API exception
        databricks_client_stub.set_list_models_error(Exception("API error"))

        # Execute command
        result = handle_command(databricks_client_stub)

        # Verify graceful error handling
        assert not result.success
        assert str(result.error) == "API error"


# Agent-specific behavioral tests
def test_agent_tool_executor_end_to_end_integration(
    databricks_client_stub, temp_config
):
    """Agent tool_executor integration works end-to-end."""
    from chuck_data.agent.tool_executor import execute_tool

    with patch("chuck_data.config._config_manager", temp_config):
        # Setup test data
        databricks_client_stub.add_model("claude-v1", created_timestamp=123456789)
        databricks_client_stub.add_model("gpt-4", created_timestamp=987654321)

        # Execute via agent tool_executor
        result = execute_tool(
            api_client=databricks_client_stub, tool_name="models", tool_args={}
        )

        # Verify agent gets proper result format
        assert isinstance(result, list)
        assert len(result) == 2
        assert any(model["name"] == "claude-v1" for model in result)
        assert any(model["name"] == "gpt-4" for model in result)
