"""
Tests for schema_selection command handler.

This module contains tests for the schema selection command handler.
"""

import unittest
import os
import tempfile
from unittest.mock import patch

from src.commands.schema_selection import handle_command
from src.config import ConfigManager, get_active_schema, set_active_catalog
from tests.fixtures import DatabricksClientStub


class TestSchemaSelection(unittest.TestCase):
    """Tests for schema selection command handler."""

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

    def test_missing_schema_name(self):
        """Test handling when schema_name is not provided."""
        result = handle_command(self.client_stub)
        self.assertFalse(result.success)
        self.assertIn("schema_name parameter is required", result.message)

    def test_no_active_catalog(self):
        """Test handling when no active catalog is selected."""
        # Don't set any active catalog in config

        # Call function
        result = handle_command(self.client_stub, schema_name="test_schema")

        # Verify results
        self.assertFalse(result.success)
        self.assertIn("No active catalog selected", result.message)

    def test_successful_schema_selection(self):
        """Test successful schema selection."""
        # Set up active catalog and test data
        set_active_catalog("test_catalog")
        self.client_stub.add_catalog("test_catalog")
        self.client_stub.add_schema("test_catalog", "test_schema")

        # Call function
        result = handle_command(self.client_stub, schema_name="test_schema")

        # Verify results
        self.assertTrue(result.success)
        self.assertIn("Active schema is now set to 'test_schema'", result.message)
        self.assertIn("in catalog 'test_catalog'", result.message)
        self.assertEqual(result.data["schema_name"], "test_schema")
        self.assertEqual(result.data["catalog_name"], "test_catalog")

        # Verify config was updated
        self.assertEqual(get_active_schema(), "test_schema")

    def test_schema_selection_with_verification_failure(self):
        """Test schema selection when verification fails."""
        # Set up active catalog but don't add the schema to stub
        set_active_catalog("test_catalog")
        self.client_stub.add_catalog("test_catalog")
        # Don't add the schema - this will cause verification to fail

        # Call function
        result = handle_command(self.client_stub, schema_name="nonexistent_schema")

        # Verify results - should still succeed but with warning
        self.assertTrue(result.success)
        self.assertIn(
            "Warning: Could not verify schema 'nonexistent_schema'", result.message
        )
        self.assertEqual(result.data["schema_name"], "nonexistent_schema")
        self.assertEqual(result.data["catalog_name"], "test_catalog")

        # Verify config was still updated
        self.assertEqual(get_active_schema(), "nonexistent_schema")

    def test_schema_selection_exception(self):
        """Test schema selection with unexpected exception."""
        # Set up active catalog
        set_active_catalog("test_catalog")

        # Create a stub that raises an exception during schema verification
        class FailingStub(DatabricksClientStub):
            def get_schema(self, catalog_name, schema_name):
                raise Exception("Failed to set schema")

        failing_stub = FailingStub()
        failing_stub.add_catalog("test_catalog")

        # Call function
        result = handle_command(failing_stub, schema_name="test_schema")

        # Should still succeed with warning since it catches the exception
        self.assertTrue(result.success)
        self.assertIn("Warning: Could not verify schema", result.message)
