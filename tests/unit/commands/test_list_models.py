"""
Tests for list_models command handler.

This module contains tests for the list_models command handler.
"""

from unittest.mock import patch

from chuck_data.commands.list_models import handle_command
from chuck_data.config import set_active_model


def test_basic_list_models(databricks_client_stub, temp_config):
    """Test listing models without detailed information."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Set up test data using stub
        databricks_client_stub.add_model("model1", created_timestamp=123456789)
        databricks_client_stub.add_model("model2", created_timestamp=987654321)
        set_active_model("model1")

        # Call function
        result = handle_command(databricks_client_stub)

        # Verify results
        assert result.success
        assert len(result.data["models"]) == 2
        assert result.data["active_model"] == "model1"
        assert not result.data["detailed"]
        assert result.data["filter"] is None
        assert result.message is None


def test_detailed_list_models(databricks_client_stub, temp_config):
    """Test listing models with detailed information."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Set up test data using stub
        databricks_client_stub.add_model(
            "model1", created_timestamp=123456789, details="model1 details"
        )
        databricks_client_stub.add_model(
            "model2", created_timestamp=987654321, details="model2 details"
        )
        set_active_model("model1")

        # Call function
        result = handle_command(databricks_client_stub, detailed=True)

        # Verify results
        assert result.success
        assert len(result.data["models"]) == 2
        assert result.data["detailed"]
        assert result.data["models"][0]["details"]["name"] == "model1"
        assert result.data["models"][1]["details"]["name"] == "model2"


def test_filtered_list_models(databricks_client_stub, temp_config):
    """Test listing models with filtering."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Set up test data using stub
        databricks_client_stub.add_model("claude-v1", created_timestamp=123456789)
        databricks_client_stub.add_model("gpt-4", created_timestamp=987654321)
        databricks_client_stub.add_model("claude-instant", created_timestamp=456789123)
        set_active_model("claude-v1")

        # Call function
        result = handle_command(databricks_client_stub, filter="claude")

        # Verify results
        assert result.success
        assert len(result.data["models"]) == 2
        assert result.data["models"][0]["name"] == "claude-v1"
        assert result.data["models"][1]["name"] == "claude-instant"
        assert result.data["filter"] == "claude"


def test_empty_list_models(databricks_client_stub, temp_config):
    """Test listing models when no models are found."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Don't add any models to stub
        # Don't set active model

        # Call function
        result = handle_command(databricks_client_stub)

        # Verify results
        assert result.success
        assert len(result.data["models"]) == 0
        assert result.message is not None
        assert "No models found" in result.message


def test_list_models_exception(databricks_client_stub, temp_config):
    """Test listing models with exception."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Configure the stub to raise an exception for list_models
        databricks_client_stub.set_list_models_error(Exception("API error"))

        # Call function
        result = handle_command(databricks_client_stub)

        # Verify results
        assert not result.success
        assert str(result.error) == "API error"
