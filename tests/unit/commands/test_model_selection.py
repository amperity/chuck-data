"""
Tests for model_selection command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
These tests verify what users see when they interact with the model_selection command,
both directly and when an agent uses the select-model tool.
"""

import pytest
from unittest.mock import patch, MagicMock

from chuck_data.commands.model_selection import handle_command, DEFINITION
from chuck_data.config import get_active_model, set_active_model
from chuck_data.agent.tool_executor import execute_tool


class TestModelSelectionParameterValidation:
    """Test parameter validation for model_selection command."""

    def test_none_client_error(self, temp_config):
        """None client should be handled properly."""
        with patch("chuck_data.config._config_manager", temp_config):
            result = handle_command(None, model_name="test-model")

            assert not result.success
            assert isinstance(result.error, Exception)
            # The actual error is about NoneType not having list_models attribute
            assert "NoneType" in str(result.error) or "None" in str(result.error)

    def test_missing_model_parameter_returns_error(
        self, databricks_client_stub, temp_config
    ):
        """Missing model_name parameter returns error."""
        with patch("chuck_data.config._config_manager", temp_config):
            result = handle_command(databricks_client_stub)

            assert not result.success
            assert "model_name parameter is required" in result.message


class TestDirectModelSelectionCommand:
    """Test direct model_selection command execution."""

    def test_direct_command_selects_existing_model(
        self, databricks_client_stub, temp_config
    ):
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

    def test_direct_command_changes_existing_model(
        self, databricks_client_stub, temp_config
    ):
        """Direct command changes from one valid model to another."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup test data with multiple models
            databricks_client_stub.add_model("model-1", created_timestamp=111111)
            databricks_client_stub.add_model("model-2", created_timestamp=222222)

            # Set initial model
            set_active_model("model-1")
            assert get_active_model() == "model-1"

            # Change to different model
            result = handle_command(databricks_client_stub, model_name="model-2")

            # Verify model was changed
            assert result.success
            assert "Active model is now set to 'model-2'" in result.message
            assert get_active_model() == "model-2"

    def test_direct_command_nonexistent_model_returns_error(
        self, databricks_client_stub, temp_config
    ):
        """Direct command shows helpful error for nonexistent model."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup available models
            databricks_client_stub.add_model("claude-v1", created_timestamp=123456789)
            databricks_client_stub.add_model("gpt-4", created_timestamp=987654321)

            # Set initial model
            set_active_model("claude-v1")

            # Execute command with nonexistent model
            result = handle_command(
                databricks_client_stub, model_name="nonexistent-model"
            )

            # Verify helpful error behavior
            assert not result.success
            assert "Model 'nonexistent-model' not found" in result.message
            # Verify original model is still set
            assert get_active_model() == "claude-v1"

    def test_direct_command_case_sensitive_model_names(
        self, databricks_client_stub, temp_config
    ):
        """Direct command respects case-sensitivity in model names."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup models with case differences
            databricks_client_stub.add_model("Claude", created_timestamp=111111)
            databricks_client_stub.add_model("claude", created_timestamp=222222)

            # First select using exact name
            result1 = handle_command(databricks_client_stub, model_name="Claude")

            # Verify case-sensitive selection works
            assert result1.success
            assert get_active_model() == "Claude"

            # Then select the other case variant
            result2 = handle_command(databricks_client_stub, model_name="claude")

            # Verify different case is treated as different model
            assert result2.success
            assert get_active_model() == "claude"

    def test_direct_command_with_special_character_model_names(
        self, databricks_client_stub, temp_config
    ):
        """Direct command handles model names with special characters."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup models with special characters
            special_model_name = "claude-v2-100k@2023-05"
            databricks_client_stub.add_model(
                special_model_name, created_timestamp=123456789
            )

            # Select model with special characters
            result = handle_command(
                databricks_client_stub, model_name=special_model_name
            )

            # Verify model selection works with special characters
            assert result.success
            assert get_active_model() == special_model_name

    def test_databricks_api_errors_handled_gracefully(
        self, databricks_client_stub, temp_config
    ):
        """Databricks API errors are handled gracefully."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Configure stub to raise API exception
            databricks_client_stub.set_list_models_error(Exception("API error"))

            # Execute command
            result = handle_command(databricks_client_stub, model_name="claude-v1")

            # Verify graceful error handling
            assert not result.success
            assert str(result.error) == "API error"
            assert "API error" in result.message


class TestModelSelectionCommandConfiguration:
    """Test model_selection command configuration and registry integration."""

    def test_command_definition_properties(self):
        """Model_selection command definition has correct configuration."""
        assert DEFINITION.name == "select-model"
        assert "model" in DEFINITION.description.lower()
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True

    def test_command_parameter_definitions(self):
        """Model_selection command has correct parameter definitions."""
        parameters = DEFINITION.parameters
        assert "model_name" in parameters
        assert parameters["model_name"]["type"] == "string"

    def test_command_aliases(self):
        """Model_selection command has expected aliases."""
        assert "/select-model" in DEFINITION.tui_aliases

    def test_command_required_parameters(self):
        """Model_selection command properly specifies required parameters."""
        assert "model_name" in DEFINITION.required_params


class TestModelSelectionAgentBehavior:
    """Test model_selection command agent-specific behavior."""

    def test_agent_tool_executor_end_to_end_integration(
        self, databricks_client_stub, temp_config
    ):
        """Agent tool_executor integration works end-to-end."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup test data
            databricks_client_stub.add_model("claude-v1", created_timestamp=123456789)

            # Execute via agent tool_executor
            result = execute_tool(
                api_client=databricks_client_stub,
                tool_name="select-model",
                tool_args={"model_name": "claude-v1"},
            )

            # Verify agent gets proper result format
            assert "success" in result
            assert result["success"] is True
            assert "Active model is now set to 'claude-v1'" in result["message"]

            # Verify state actually changed
            assert get_active_model() == "claude-v1"

    def test_agent_nonexistent_model_error_format(
        self, databricks_client_stub, temp_config
    ):
        """Agent receives properly formatted error for nonexistent model."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup available models
            databricks_client_stub.add_model(
                "available-model", created_timestamp=123456789
            )

            # Execute via agent tool_executor with nonexistent model
            result = execute_tool(
                api_client=databricks_client_stub,
                tool_name="select-model",
                tool_args={"model_name": "nonexistent-model"},
            )

            # Verify agent gets proper error format
            # Note: The actual format includes 'error' key instead of 'success'/'message' keys
            assert isinstance(result, dict)
            assert "error" in result
            assert "nonexistent-model" in result["error"]

    def test_agent_api_error_format(self, databricks_client_stub, temp_config):
        """Agent receives properly formatted error for API failures."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Configure stub to raise API exception
            databricks_client_stub.set_list_models_error(
                Exception("API connection failure")
            )

            # Execute via agent tool_executor
            result = execute_tool(
                api_client=databricks_client_stub,
                tool_name="select-model",
                tool_args={"model_name": "claude-v1"},
            )

            # Verify agent gets proper error format
            # Note: The actual error format includes 'error' and 'details' keys
            assert isinstance(result, dict)
            assert "error" in result
            assert "API connection failure" in result["error"]
            assert "details" in result
            assert "API connection failure" in result["details"]


class TestModelSelectionEdgeCases:
    """Test edge cases and boundary conditions for model_selection command."""

    def test_model_selection_with_unicode_model_names(
        self, databricks_client_stub, temp_config
    ):
        """Model_selection command handles Unicode characters in model names."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add models with Unicode in name
            databricks_client_stub.add_model("模型-v1")
            databricks_client_stub.add_model("üñîçødé-model")

            # Select Unicode model
            result = handle_command(databricks_client_stub, model_name="模型-v1")

            # Verify Unicode handling
            assert result.success
            assert get_active_model() == "模型-v1"

            # Select another Unicode model
            result2 = handle_command(databricks_client_stub, model_name="üñîçødé-model")

            assert result2.success
            assert get_active_model() == "üñîçødé-model"

    def test_model_selection_with_extremely_long_model_name(
        self, databricks_client_stub, temp_config
    ):
        """Model_selection command handles extremely long model names."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Create very long model name (approximately 256 characters)
            # Adjust length to exactly 256
            long_name = "very-long-model-name-" + "x" * 235
            assert len(long_name) == 256

            # Add model with long name
            databricks_client_stub.add_model(long_name)

            # Select long name model
            result = handle_command(databricks_client_stub, model_name=long_name)

            # Verify long name handling
            assert result.success
            assert get_active_model() == long_name
            assert long_name in result.message
