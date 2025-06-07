"""
Tests for model_selection command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
"""

from unittest.mock import patch

from chuck_data.commands.model_selection import handle_command
from chuck_data.config import get_active_model


# Parameter validation tests
def test_missing_model_parameter_returns_error(databricks_client_stub, temp_config):
    """Missing model_name parameter returns error."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_command(databricks_client_stub)
        assert not result.success
        assert "model_name parameter is required" in result.message


# Direct command execution tests
def test_direct_command_selects_existing_model(databricks_client_stub, temp_config):
    """Direct command successfully selects existing model."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Setup test data
        databricks_client_stub.add_model("claude-v1", created_timestamp=123456789)
        databricks_client_stub.add_model("gpt-4", created_timestamp=987654321)

        # Execute command
        result = handle_command(databricks_client_stub, model_name="claude-v1")

        # Verify behavioral outcome
        assert result.success
        assert "Active model is now set to 'claude-v1'" in result.message
        assert get_active_model() == "claude-v1"


def test_direct_command_failure_shows_helpful_error(
    databricks_client_stub, temp_config
):
    """Direct command failure shows helpful error for nonexistent model."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Setup available models
        databricks_client_stub.add_model("claude-v1", created_timestamp=123456789)
        databricks_client_stub.add_model("gpt-4", created_timestamp=987654321)

        # Execute command with nonexistent model
        result = handle_command(databricks_client_stub, model_name="nonexistent-model")

        # Verify helpful error behavior
        assert not result.success
        assert "Model 'nonexistent-model' not found" in result.message
        assert get_active_model() is None


def test_databricks_api_errors_handled_gracefully(databricks_client_stub, temp_config):
    """Databricks API errors are handled gracefully."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Configure stub to raise API exception
        databricks_client_stub.set_list_models_error(Exception("API error"))

        # Execute command
        result = handle_command(databricks_client_stub, model_name="claude-v1")

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

        # Execute via agent tool_executor
        result = execute_tool(
            api_client=databricks_client_stub,
            tool_name="select-model",
            tool_args={"model_name": "claude-v1"},
        )

        # Verify agent gets proper result format (model_selection returns no data, falls back to success message)
        assert "success" in result
        assert result["success"] is True
        assert "Active model is now set to 'claude-v1'" in result["message"]

        # Verify state actually changed
        assert get_active_model() == "claude-v1"
