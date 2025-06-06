"""
Tests for list_catalogs command handler.

This module contains tests for the list_catalogs command handler.
"""

import unittest
import os
import tempfile
from unittest.mock import patch

from src.commands.list_catalogs import handle_command
from src.config import ConfigManager
from tests.fixtures import DatabricksClientStub


class TestListCatalogs(unittest.TestCase):
    """Tests for list_catalogs command handler."""

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

    def test_successful_list_catalogs(self):
        """Test successful list catalogs."""
        # Set up test data using stub
        self.client_stub.add_catalog(
            "catalog1",
            catalog_type="MANAGED",
            comment="Test catalog 1",
            provider={"name": "provider1"},
            created_at="2023-01-01",
        )
        self.client_stub.add_catalog(
            "catalog2",
            catalog_type="EXTERNAL",
            comment="Test catalog 2",
            provider={"name": "provider2"},
            created_at="2023-01-02",
        )

        # Call function with parameters
        result = handle_command(self.client_stub, include_browse=True, max_results=50)

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(len(result.data["catalogs"]), 2)
        self.assertEqual(result.data["total_count"], 2)
        self.assertIn("Found 2 catalog(s).", result.message)
        self.assertFalse(result.data.get("display", True))  # Should default to False
        self.assertIn("current_catalog", result.data)

        # Verify catalog data
        catalog_names = [c["name"] for c in result.data["catalogs"]]
        self.assertIn("catalog1", catalog_names)
        self.assertIn("catalog2", catalog_names)

    def test_successful_list_catalogs_with_pagination(self):
        """Test successful list catalogs with pagination."""
        # Set up test data
        self.client_stub.add_catalog("catalog1", catalog_type="MANAGED")
        self.client_stub.add_catalog("catalog2", catalog_type="EXTERNAL")

        # For pagination testing, we need to modify the stub to return pagination token
        class PaginatingClientStub(DatabricksClientStub):
            def list_catalogs(
                self, include_browse=False, max_results=None, page_token=None
            ):
                result = super().list_catalogs(include_browse, max_results, page_token)
                # Add pagination token if page_token was provided
                if page_token:
                    result["next_page_token"] = "abc123"
                return result

        paginating_stub = PaginatingClientStub()
        paginating_stub.add_catalog("catalog1", catalog_type="MANAGED")
        paginating_stub.add_catalog("catalog2", catalog_type="EXTERNAL")

        # Call function with page token
        result = handle_command(paginating_stub, page_token="xyz789")

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(result.data["next_page_token"], "abc123")
        self.assertIn("More catalogs available with page token: abc123", result.message)

    def test_empty_catalog_list(self):
        """Test handling when no catalogs are found."""
        # Don't add any catalogs to stub

        # Call function
        result = handle_command(self.client_stub)

        # Verify results
        self.assertTrue(result.success)
        self.assertIn("No catalogs found in this workspace.", result.message)
        self.assertEqual(result.data["total_count"], 0)
        self.assertFalse(result.data.get("display", True))
        self.assertIn("current_catalog", result.data)

    def test_list_catalogs_exception(self):
        """Test list_catalogs with unexpected exception."""

        # Create a stub that raises an exception for list_catalogs
        class FailingClientStub(DatabricksClientStub):
            def list_catalogs(
                self, include_browse=False, max_results=None, page_token=None
            ):
                raise Exception("API error")

        failing_client = FailingClientStub()

        # Call function
        result = handle_command(failing_client)

        # Verify results
        self.assertFalse(result.success)
        self.assertIn("Failed to list catalogs", result.message)
        self.assertEqual(str(result.error), "API error")

    def test_list_catalogs_with_display_true(self):
        """Test list catalogs with display=true shows table."""
        # Set up test data
        self.client_stub.add_catalog("Test Catalog", catalog_type="MANAGED")

        result = handle_command(self.client_stub, display=True)

        self.assertTrue(result.success)
        self.assertTrue(result.data.get("display"))
        self.assertEqual(len(result.data.get("catalogs", [])), 1)

    def test_list_catalogs_with_display_false(self):
        """Test list catalogs with display=false returns data without display."""
        # Set up test data
        self.client_stub.add_catalog("Test Catalog", catalog_type="MANAGED")

        result = handle_command(self.client_stub, display=False)

        self.assertTrue(result.success)
        self.assertFalse(result.data.get("display"))
        self.assertEqual(len(result.data.get("catalogs", [])), 1)
