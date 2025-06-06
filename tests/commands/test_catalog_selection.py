"""
Tests for catalog_selection command handler.

This module contains tests for the catalog selection command handler.
"""

import unittest
import os
import tempfile
from unittest.mock import patch

from chuck_data.commands.catalog_selection import handle_command
from chuck_data.config import ConfigManager, get_active_catalog
from tests.fixtures import DatabricksClientStub


class TestCatalogSelection(unittest.TestCase):
    """Tests for catalog selection command handler."""

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

    def test_missing_catalog_name(self):
        """Test handling when catalog parameter is not provided."""
        result = handle_command(self.client_stub)
        self.assertFalse(result.success)
        self.assertIn("catalog parameter is required", result.message)

    def test_successful_catalog_selection(self):
        """Test successful catalog selection."""
        # Set up catalog in stub
        self.client_stub.add_catalog("test_catalog", catalog_type="MANAGED")

        # Call function
        result = handle_command(self.client_stub, catalog="test_catalog")

        # Verify results
        self.assertTrue(result.success)
        self.assertIn("Active catalog is now set to 'test_catalog'", result.message)
        self.assertIn("Type: MANAGED", result.message)
        self.assertEqual(result.data["catalog_name"], "test_catalog")
        self.assertEqual(result.data["catalog_type"], "MANAGED")

        # Verify config was updated
        self.assertEqual(get_active_catalog(), "test_catalog")

    def test_catalog_selection_with_verification_failure(self):
        """Test catalog selection when verification fails."""
        # Add some catalogs but not the one we're looking for (make sure names are very different)
        self.client_stub.add_catalog("xyz", catalog_type="MANAGED")

        # Call function with nonexistent catalog that won't fuzzy match
        result = handle_command(self.client_stub, catalog="completely_different_name")

        # Verify results - should fail since catalog doesn't exist and no fuzzy match
        self.assertFalse(result.success)
        self.assertIn(
            "No catalog found matching 'completely_different_name'", result.message
        )
        self.assertIn("Available catalogs: xyz", result.message)

    def test_catalog_selection_exception(self):
        """Test catalog selection with unexpected exception."""
        # Create a stub that raises an exception during config setting
        # We'll simulate this by using an invalid config path
        self.patcher.stop()  # Stop the existing patcher
        self.temp_dir.cleanup()  # Clean up temp directory

        # Try to use an invalid config path that will cause an exception
        invalid_config_manager = ConfigManager("/invalid/path/config.json")
        with patch("chuck_data.config._config_manager", invalid_config_manager):
            result = handle_command(self.client_stub, catalog_name="test_catalog")

        # This might succeed despite the invalid path, so let's test a different exception scenario
        # Instead, let's create a custom stub that fails on get_catalog
        class FailingStub(DatabricksClientStub):
            def get_catalog(self, catalog_name):
                raise Exception("Failed to set catalog")

        failing_stub = FailingStub()
        # Set up a new temp directory and config for this test
        temp_dir = tempfile.TemporaryDirectory()
        config_path = os.path.join(temp_dir.name, "test_config.json")
        config_manager = ConfigManager(config_path)

        with patch("chuck_data.config._config_manager", config_manager):
            # This should trigger the exception in the catalog verification
            result = handle_command(failing_stub, catalog="test_catalog")

            # Should fail since get_catalog fails and no catalogs in list
            self.assertFalse(result.success)
            self.assertIn("No catalogs found in workspace", result.message)

        temp_dir.cleanup()

    def test_select_catalog_by_name(self):
        """Test catalog selection by name."""
        self.client_stub.add_catalog("Test Catalog", catalog_type="MANAGED")

        result = handle_command(self.client_stub, catalog="Test Catalog")

        self.assertTrue(result.success)
        self.assertIn("Active catalog is now set to 'Test Catalog'", result.message)

    def test_select_catalog_fuzzy_matching(self):
        """Test catalog selection with fuzzy matching."""
        self.client_stub.add_catalog("Test Catalog Long Name", catalog_type="MANAGED")

        result = handle_command(self.client_stub, catalog="Test")

        self.assertTrue(result.success)
        self.assertIn("Test Catalog Long Name", result.message)
