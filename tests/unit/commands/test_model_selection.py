"""
Tests for model_selection command handler.

This module contains tests for the model_selection command handler.
"""

from unittest.mock import patch

from chuck_data.commands.model_selection import handle_command
from chuck_data.config import get_active_model


def test_missing_model_name(databricks_client_stub, temp_config):
    """Test handling when model_name is not provided."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_command(databricks_client_stub)
        assert not result.success
        assert "model_name parameter is required" in result.message


def test_successful_model_selection(databricks_client_stub, temp_config):
    """Test successful model selection."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Set up test data using stub
        databricks_client_stub.add_model("claude-v1", created_timestamp=123456789)
        databricks_client_stub.add_model("gpt-4", created_timestamp=987654321)

        # Call function
        result = handle_command(databricks_client_stub, model_name="claude-v1")

        # Verify results
        assert result.success
        assert "Active model is now set to 'claude-v1'" in result.message

        # Verify config was updated
        assert get_active_model() == "claude-v1"


def test_model_not_found(databricks_client_stub, temp_config):
    """Test model selection when model is not found."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Set up test data using stub - but don't include the requested model
        databricks_client_stub.add_model("claude-v1", created_timestamp=123456789)
        databricks_client_stub.add_model("gpt-4", created_timestamp=987654321)

        # Call function with nonexistent model
        result = handle_command(databricks_client_stub, model_name="nonexistent-model")

        # Verify results
        assert not result.success
        assert "Model 'nonexistent-model' not found" in result.message

        # Verify config was not updated
        assert get_active_model() is None


def test_model_selection_api_exception(databricks_client_stub, temp_config):
    """Test model selection when API call throws an exception."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Configure stub to raise an exception for list_models
        databricks_client_stub.set_list_models_error(Exception("API error"))

        # Call function
        result = handle_command(databricks_client_stub, model_name="claude-v1")

        # Verify results
        assert not result.success
        assert str(result.error) == "API error"
