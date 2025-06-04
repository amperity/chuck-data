"""
Tests for list_models command handler.

This module contains tests for the list_models command handler.
"""

import unittest
import os
import tempfile
from unittest.mock import patch

from src.commands.list_models import handle_command
from src.config import ConfigManager, set_active_model
from tests.fixtures import DatabricksClientStub


class TestListModels(unittest.TestCase):
    """Tests for list_models command handler."""

    def setUp(self):
        """Set up test fixtures."""
        self.client_stub = DatabricksClientStub()

        # Set up config management
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.temp_dir.name, "test_config.json")
        self.config_manager = ConfigManager(self.config_path)
        self.patcher = patch("src.config._config_manager", self.config_manager)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        self.temp_dir.cleanup()

    def test_basic_list_models(self):
        """Test listing models without detailed information."""
        # Set up test data using stub
        self.client_stub.add_model("model1", created_timestamp=123456789)
        self.client_stub.add_model("model2", created_timestamp=987654321)
        set_active_model("model1")

        # Call function
        result = handle_command(self.client_stub)

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(len(result.data["models"]), 2)
        self.assertEqual(result.data["active_model"], "model1")
        self.assertFalse(result.data["detailed"])
        self.assertIsNone(result.data["filter"])
        self.assertIsNone(result.message)

    def test_detailed_list_models(self):
        """Test listing models with detailed information."""
        # Set up test data using stub
        self.client_stub.add_model(
            "model1", created_timestamp=123456789, details="model1 details"
        )
        self.client_stub.add_model(
            "model2", created_timestamp=987654321, details="model2 details"
        )
        set_active_model("model1")

        # Call function
        result = handle_command(self.client_stub, detailed=True)

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(len(result.data["models"]), 2)
        self.assertTrue(result.data["detailed"])
        self.assertEqual(result.data["models"][0]["details"]["name"], "model1")
        self.assertEqual(result.data["models"][1]["details"]["name"], "model2")

    def test_filtered_list_models(self):
        """Test listing models with filtering."""
        # Set up test data using stub
        self.client_stub.add_model("claude-v1", created_timestamp=123456789)
        self.client_stub.add_model("gpt-4", created_timestamp=987654321)
        self.client_stub.add_model("claude-instant", created_timestamp=456789123)
        set_active_model("claude-v1")

        # Call function
        result = handle_command(self.client_stub, filter="claude")

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(len(result.data["models"]), 2)
        self.assertEqual(result.data["models"][0]["name"], "claude-v1")
        self.assertEqual(result.data["models"][1]["name"], "claude-instant")
        self.assertEqual(result.data["filter"], "claude")

    def test_empty_list_models(self):
        """Test listing models when no models are found."""
        # Don't add any models to stub
        # Don't set active model

        # Call function
        result = handle_command(self.client_stub)

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(len(result.data["models"]), 0)
        self.assertIsNotNone(result.message)
        self.assertIn("No models found", result.message)

    def test_list_models_exception(self):
        """Test listing models with exception."""

        # Create a stub that raises an exception for list_models
        class FailingClientStub(DatabricksClientStub):
            def list_models(self, **kwargs):
                raise Exception("API error")

        failing_client = FailingClientStub()

        # Call function
        result = handle_command(failing_client)

        # Verify results
        self.assertFalse(result.success)
        self.assertEqual(str(result.error), "API error")
