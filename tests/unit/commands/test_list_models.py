"""
Tests for list_models command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
"""

from unittest.mock import patch

from chuck_data.commands.list_models import handle_command
from chuck_data.config import set_active_model


# Direct command execution tests
def test_direct_command_lists_models_basic_format(databricks_client_stub, temp_config):
    """Direct command lists models in basic format by default."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Setup test data
        databricks_client_stub.add_model("claude-v1", created_timestamp=123456789)
        databricks_client_stub.add_model("gpt-4", created_timestamp=987654321)
        set_active_model("claude-v1")

        # Execute command
        result = handle_command(databricks_client_stub)

        # Verify behavioral outcome
        assert result.success
        assert len(result.data["models"]) == 2
        assert result.data["active_model"] == "claude-v1"
        assert result.data["filter"] is None
        assert result.message is None

        # Verify ModelInfo format
        assert result.data["models"][0]["model_id"] == "claude-v1"
        assert result.data["models"][0]["provider_name"] == "databricks"


def test_direct_command_lists_multiple_models(databricks_client_stub, temp_config):
    """Direct command lists multiple models correctly."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Setup test data with multiple models
        databricks_client_stub.add_model("claude-v1", created_timestamp=123456789)
        databricks_client_stub.add_model("gpt-4", created_timestamp=987654321)
        set_active_model("claude-v1")

        # Execute command
        result = handle_command(databricks_client_stub)

        # Verify multiple models returned
        assert result.success
        assert len(result.data["models"]) == 2
        model_ids = [m["model_id"] for m in result.data["models"]]
        assert "claude-v1" in model_ids
        assert "gpt-4" in model_ids


def test_direct_command_filters_models_by_name_pattern(
    databricks_client_stub, temp_config
):
    """Direct command filters models by name pattern."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Setup test data with mixed model names
        databricks_client_stub.add_model("claude-v1", created_timestamp=123456789)
        databricks_client_stub.add_model("gpt-4", created_timestamp=987654321)
        databricks_client_stub.add_model("claude-instant", created_timestamp=456789123)
        set_active_model("claude-v1")

        # Execute command with filter
        result = handle_command(databricks_client_stub, filter="claude")

        # Verify filtering behavior
        assert result.success
        assert len(result.data["models"]) == 2
        assert all(
            "claude" in model["model_name"].lower()
            or "claude" in model["model_id"].lower()
            for model in result.data["models"]
        )
        assert result.data["filter"] == "claude"


def test_direct_command_handles_empty_model_list(databricks_client_stub, temp_config):
    """Direct command handles empty model list gracefully."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Don't add any models to stub

        # Execute command
        result = handle_command(databricks_client_stub)

        # Verify graceful handling of empty list
        assert result.success
        assert len(result.data["models"]) == 0
        # Updated message includes provider-specific guidance
        assert "No Databricks models found" in result.message
        assert "Model Serving" in result.message


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
        set_active_model("claude-v1")

        # Execute via agent tool_executor
        result = execute_tool(
            api_client=databricks_client_stub, tool_name="list-models", tool_args={}
        )

        # Verify agent gets proper result format (list_models returns data dict)
        assert "models" in result
        assert "active_model" in result
        assert len(result["models"]) == 2
        assert result["active_model"] == "claude-v1"
