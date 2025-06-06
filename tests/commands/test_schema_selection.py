"""
Tests for schema_selection command handler.

This module contains tests for the schema selection command handler.
"""

import unittest
import os
import tempfile
from unittest.mock import patch

from chuck_data.commands.schema_selection import handle_command
from chuck_data.config import ConfigManager, get_active_schema, set_active_catalog
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
        self.patcher = patch("chuck_data.config._config_manager", self.config_manager)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        self.temp_dir.cleanup()

    def test_missing_schema_name(self):
        """Test handling when schema parameter is not provided."""
        result = handle_command(self.client_stub)
        self.assertFalse(result.success)
        self.assertIn("schema parameter is required", result.message)

    def test_no_active_catalog(self):
        """Test handling when no active catalog is selected."""
        # Don't set any active catalog in config

        # Call function
        result = handle_command(self.client_stub, schema="test_schema")

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
        result = handle_command(self.client_stub, schema="test_schema")

        # Verify results
        self.assertTrue(result.success)
        self.assertIn("Active schema is now set to 'test_schema'", result.message)
        self.assertIn("in catalog 'test_catalog'", result.message)
        self.assertEqual(result.data["schema_name"], "test_schema")
        self.assertEqual(result.data["catalog_name"], "test_catalog")

        # Verify config was updated
        self.assertEqual(get_active_schema(), "test_schema")

    def test_schema_selection_with_verification_failure(self):
        """Test schema selection when no matching schema exists."""
        # Set up active catalog but don't add the schema to stub
        set_active_catalog("test_catalog")
        self.client_stub.add_catalog("test_catalog")
        self.client_stub.add_schema("test_catalog", "completely_different_schema_name")

        # Call function with non-existent schema that won't match via fuzzy matching
        result = handle_command(self.client_stub, schema="xyz_nonexistent_abc")

        # Verify results - should fail cleanly
        self.assertFalse(result.success)
        self.assertIn("No schema found matching 'xyz_nonexistent_abc'", result.message)
        self.assertIn("Available schemas:", result.message)

    def test_schema_selection_exception(self):
        """Test schema selection with list_schemas exception."""
        # Set up active catalog
        set_active_catalog("test_catalog")

        # Create a stub that raises an exception during list_schemas
        class FailingStub(DatabricksClientStub):
            def list_schemas(
                self,
                catalog_name,
                include_browse=False,
                max_results=None,
                page_token=None,
                **kwargs,
            ):
                raise Exception("Failed to list schemas")

        failing_stub = FailingStub()
        failing_stub.add_catalog("test_catalog")

        # Call function
        result = handle_command(failing_stub, schema="test_schema")

        # Should fail due to the exception
        self.assertFalse(result.success)
        self.assertIn("Failed to list schemas", result.message)
