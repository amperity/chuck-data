"""
Tests for the model-related command modules.
"""

import pytest
from unittest.mock import patch

from chuck_data.config import set_active_model, get_active_model
from chuck_data.commands.models import handle_command as handle_models
from chuck_data.commands.list_models import handle_command as handle_list_models
from chuck_data.commands.model_selection import handle_command as handle_model_selection


class StubClient:
    """Simple client stub for model commands."""

    def __init__(self, models=None, active_model=None):
        self.models = models or []
        self.active_model = active_model

    def list_models(self):
        return self.models

    def get_active_model(self):
        return self.active_model


@pytest.fixture
def stub_client():
    """Create a basic stub client."""
    return StubClient()


def test_handle_models_with_models(temp_config):
    """Test handling models command with available models."""
    with patch("chuck_data.config._config_manager", temp_config):
        client = StubClient(
            models=[
                {"name": "model1", "status": "READY"},
                {"name": "model2", "status": "READY"},
            ]
        )

        result = handle_models(client)

        assert result.success
        assert result.data == client.list_models()


def test_handle_models_empty(temp_config):
    """Test handling models command with no available models."""
    with patch("chuck_data.config._config_manager", temp_config):
        client = StubClient(models=[])

        result = handle_models(client)

        assert result.success
        assert result.data == []
        assert "No models found" in result.message


def test_handle_list_models_basic(temp_config):
    """Test list models command (basic)."""
    with patch("chuck_data.config._config_manager", temp_config):
        client = StubClient(
            models=[
                {"name": "model1", "status": "READY"},
                {"name": "model2", "status": "READY"},
            ],
            active_model="model1",
        )
        set_active_model(client.active_model)

        result = handle_list_models(client)

        assert result.success
        assert result.data["models"] == client.list_models()
        assert result.data["active_model"] == client.active_model
        assert not result.data["detailed"]
        assert result.data["filter"] is None


def test_handle_list_models_filter(temp_config):
    """Test list models command with filter."""
    with patch("chuck_data.config._config_manager", temp_config):
        client = StubClient(
            models=[
                {"name": "model1", "status": "READY"},
                {"name": "model2", "status": "READY"},
            ],
            active_model="model1",
        )
        set_active_model(client.active_model)

        result = handle_list_models(client, filter="model1")

        assert result.success
        assert len(result.data["models"]) == 1
        assert result.data["models"][0]["name"] == "model1"
        assert result.data["filter"] == "model1"


def test_handle_model_selection_success(temp_config):
    """Test successful model selection."""
    with patch("chuck_data.config._config_manager", temp_config):
        client = StubClient(models=[{"name": "model1"}, {"name": "valid-model"}])

        result = handle_model_selection(client, model_name="valid-model")

        assert result.success
        assert get_active_model() == "valid-model"
        assert "Active model is now set to 'valid-model'" in result.message


def test_handle_model_selection_invalid(temp_config):
    """Test selecting an invalid model."""
    with patch("chuck_data.config._config_manager", temp_config):
        client = StubClient(models=[{"name": "model1"}, {"name": "model2"}])

        result = handle_model_selection(client, model_name="nonexistent-model")

        assert not result.success
        assert "not found" in result.message


def test_handle_model_selection_no_name(temp_config):
    """Test model selection with no model name provided."""
    with patch("chuck_data.config._config_manager", temp_config):
        client = StubClient(models=[])  # models unused

        result = handle_model_selection(client)

        # Verify the result
        assert not result.success
        assert "model_name parameter is required" in result.message
