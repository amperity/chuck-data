"""
Tests for model_selection command handler.

This module contains tests for the model_selection command handler.
"""

import unittest
import os
import tempfile
from unittest.mock import patch

from chuck_data.commands.model_selection import handle_command
from chuck_data.config import ConfigManager, get_active_model
from tests.fixtures import DatabricksClientStub


class TestModelSelection(unittest.TestCase):
    """Tests for model selection command handler."""

    def setUp(self):
        """Set up test fixtures."""
        self.client_stub = DatabricksClientStub()

        # Set up config management
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.temp_dir.name, "test_config.json")
        self.config_manager = ConfigManager(self.config_path)
        self.patcher = patch("chuck_data.config._config_manager", self.config_manager)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        self.temp_dir.cleanup()

    def test_missing_model_name(self):
        """Test handling when model_name is not provided."""
        result = handle_command(self.client_stub)
        self.assertFalse(result.success)
        self.assertIn("model_name parameter is required", result.message)

    def test_successful_model_selection(self):
        """Test successful model selection."""
        # Set up test data using stub
        self.client_stub.add_model("claude-v1", created_timestamp=123456789)
        self.client_stub.add_model("gpt-4", created_timestamp=987654321)

        # Call function
        result = handle_command(self.client_stub, model_name="claude-v1")

        # Verify results
        self.assertTrue(result.success)
        self.assertIn("Active model is now set to 'claude-v1'", result.message)

        # Verify config was updated
        self.assertEqual(get_active_model(), "claude-v1")

    def test_model_not_found(self):
        """Test model selection when model is not found."""
        # Set up test data using stub - but don't include the requested model
        self.client_stub.add_model("claude-v1", created_timestamp=123456789)
        self.client_stub.add_model("gpt-4", created_timestamp=987654321)

        # Call function with nonexistent model
        result = handle_command(self.client_stub, model_name="nonexistent-model")

        # Verify results
        self.assertFalse(result.success)
        self.assertIn("Model 'nonexistent-model' not found", result.message)

        # Verify config was not updated
        self.assertIsNone(get_active_model())

    def test_model_selection_api_exception(self):
        """Test model selection when API call throws an exception."""

        # Create a stub that raises an exception for list_models
        class FailingClientStub(DatabricksClientStub):
            def list_models(self, **kwargs):
                raise Exception("API error")

        failing_client = FailingClientStub()

        # Call function
        result = handle_command(failing_client, model_name="claude-v1")

        # Verify results
        self.assertFalse(result.success)
        self.assertEqual(str(result.error), "API error")
