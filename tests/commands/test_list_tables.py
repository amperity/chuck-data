"""
Tests for list_tables command handler.

This module contains tests for the list_tables command handler.
"""

import unittest
import os
import tempfile
from unittest.mock import patch

from src.commands.list_tables import handle_command
from src.config import ConfigManager
from tests.fixtures import DatabricksClientStub


class TestListTables(unittest.TestCase):
    """Tests for list_tables command handler."""

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

    def test_no_client(self):
        """Test handling when no client is provided."""
        result = handle_command(None)
        self.assertFalse(result.success)
        self.assertIn("No Databricks client available", result.message)

    def test_no_active_catalog(self):
        """Test handling when no catalog is provided and no active catalog is set."""
        # Don't set any active catalog in config

        result = handle_command(self.client_stub)
        self.assertFalse(result.success)
        self.assertIn(
            "No catalog specified and no active catalog selected", result.message
        )

    def test_no_active_schema(self):
        """Test handling when no schema is provided and no active schema is set."""
        # Set active catalog but not schema
        from src.config import set_active_catalog

        set_active_catalog("test_catalog")

        result = handle_command(self.client_stub)
        self.assertFalse(result.success)
        self.assertIn(
            "No schema specified and no active schema selected", result.message
        )

    def test_successful_list_tables_with_parameters(self):
        """Test successful list tables with all parameters specified."""
        # Set up test data using stub
        self.client_stub.add_catalog("test_catalog")
        self.client_stub.add_schema("test_catalog", "test_schema")
        self.client_stub.add_table(
            "test_catalog",
            "test_schema",
            "table1",
            table_type="MANAGED",
            comment="Test table 1",
            created_at="2023-01-01",
        )
        self.client_stub.add_table(
            "test_catalog",
            "test_schema",
            "table2",
            table_type="VIEW",
            comment="Test table 2",
            created_at="2023-01-02",
        )

        # Call function
        result = handle_command(
            self.client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
            include_delta_metadata=True,
            omit_columns=False,
        )

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(len(result.data["tables"]), 2)
        self.assertEqual(result.data["total_count"], 2)
        self.assertEqual(result.data["catalog_name"], "test_catalog")
        self.assertEqual(result.data["schema_name"], "test_schema")
        self.assertIn("Found 2 table(s) in 'test_catalog.test_schema'", result.message)

        # Verify table data
        table_names = [t["name"] for t in result.data["tables"]]
        self.assertIn("table1", table_names)
        self.assertIn("table2", table_names)

    def test_successful_list_tables_with_defaults(self):
        """Test successful list tables using default active catalog and schema."""
        # Set up active catalog and schema
        from src.config import set_active_catalog, set_active_schema

        set_active_catalog("active_catalog")
        set_active_schema("active_schema")

        # Set up test data
        self.client_stub.add_catalog("active_catalog")
        self.client_stub.add_schema("active_catalog", "active_schema")
        self.client_stub.add_table("active_catalog", "active_schema", "table1")

        # Call function with no catalog or schema parameters
        result = handle_command(self.client_stub)

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(len(result.data["tables"]), 1)
        self.assertEqual(result.data["catalog_name"], "active_catalog")
        self.assertEqual(result.data["schema_name"], "active_schema")
        self.assertEqual(result.data["tables"][0]["name"], "table1")

    def test_empty_table_list(self):
        """Test handling when no tables are found."""
        # Set up catalog and schema but no tables
        self.client_stub.add_catalog("test_catalog")
        self.client_stub.add_schema("test_catalog", "test_schema")
        # Don't add any tables

        # Call function
        result = handle_command(
            self.client_stub, catalog_name="test_catalog", schema_name="test_schema"
        )

        # Verify results
        self.assertTrue(result.success)
        self.assertIn(
            "No tables found in schema 'test_catalog.test_schema'", result.message
        )

    def test_list_tables_exception(self):
        """Test list_tables with unexpected exception."""

        # Create a stub that raises an exception for list_tables
        class FailingClientStub(DatabricksClientStub):
            def list_tables(self, *args, **kwargs):
                raise Exception("API error")

        failing_client = FailingClientStub()

        # Call function
        result = handle_command(
            failing_client, catalog_name="test_catalog", schema_name="test_schema"
        )

        # Verify results
        self.assertFalse(result.success)
        self.assertIn("Failed to list tables", result.message)
        self.assertEqual(str(result.error), "API error")
