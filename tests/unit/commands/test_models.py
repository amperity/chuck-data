"""
Tests for the model-related command modules.
"""

import unittest
import os
import tempfile
from unittest.mock import patch

from chuck_data.config import ConfigManager, set_active_model, get_active_model
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


class TestModelsCommands(unittest.TestCase):
    """Test cases for the model-related command handlers."""

    def setUp(self):
        """Set up common test fixtures."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.temp_dir.name, "test_config.json")
        self.config_manager = ConfigManager(self.config_path)
        self.patcher = patch("chuck_data.config._config_manager", self.config_manager)
        self.patcher.start()
        self.client = None

    def tearDown(self):
        self.patcher.stop()
        self.temp_dir.cleanup()

    def test_handle_models_with_models(self):
        """Test handling models command with available models."""
        self.client = StubClient(
            models=[
                {"name": "model1", "status": "READY"},
                {"name": "model2", "status": "READY"},
            ]
        )

        result = handle_models(self.client)

        self.assertTrue(result.success)
        self.assertEqual(result.data, self.client.list_models())

    def test_handle_models_empty(self):
        """Test handling models command with no available models."""
        self.client = StubClient(models=[])

        result = handle_models(self.client)

        self.assertTrue(result.success)
        self.assertEqual(result.data, [])
        self.assertIn("No models found", result.message)

    def test_handle_list_models_basic(self):
        """Test list models command (basic)."""
        self.client = StubClient(
            models=[
                {"name": "model1", "status": "READY"},
                {"name": "model2", "status": "READY"},
            ],
            active_model="model1",
        )
        set_active_model(self.client.active_model)

        result = handle_list_models(self.client)

        self.assertTrue(result.success)
        self.assertEqual(result.data["models"], self.client.list_models())
        self.assertEqual(result.data["active_model"], self.client.active_model)
        self.assertFalse(result.data["detailed"])
        self.assertIsNone(result.data["filter"])

    def test_handle_list_models_filter(self):
        """Test list models command with filter."""
        self.client = StubClient(
            models=[
                {"name": "model1", "status": "READY"},
                {"name": "model2", "status": "READY"},
            ],
            active_model="model1",
        )
        set_active_model(self.client.active_model)

        result = handle_list_models(self.client, filter="model1")

        self.assertTrue(result.success)
        self.assertEqual(len(result.data["models"]), 1)
        self.assertEqual(result.data["models"][0]["name"], "model1")
        self.assertEqual(result.data["filter"], "model1")

    def test_handle_model_selection_success(self):
        """Test successful model selection."""
        self.client = StubClient(models=[{"name": "model1"}, {"name": "valid-model"}])

        result = handle_model_selection(self.client, model_name="valid-model")

        self.assertTrue(result.success)
        self.assertEqual(get_active_model(), "valid-model")
        self.assertIn("Active model is now set to 'valid-model'", result.message)

    def test_handle_model_selection_invalid(self):
        """Test selecting an invalid model."""
        self.client = StubClient(models=[{"name": "model1"}, {"name": "model2"}])

        result = handle_model_selection(self.client, model_name="nonexistent-model")

        self.assertFalse(result.success)
        self.assertIn("not found", result.message)

    def test_handle_model_selection_no_name(self):
        """Test model selection with no model name provided."""
        self.client = StubClient(models=[])  # models unused

        result = handle_model_selection(self.client)

        # Verify the result
        self.assertFalse(result.success)
        self.assertIn("model_name parameter is required", result.message)
