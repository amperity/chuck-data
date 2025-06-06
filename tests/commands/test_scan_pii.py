"""
Tests for scan_pii command handler.

This module contains tests for the scan_pii command handler.
"""

import unittest
from unittest.mock import patch, MagicMock

from chuck_data.commands.scan_pii import handle_command
from tests.fixtures import LLMClientStub


class TestScanPII(unittest.TestCase):
    """Tests for scan_pii command handler."""

    def setUp(self):
        """Set up common test fixtures."""
        self.client = MagicMock()

    def test_missing_client(self):
        """Test handling when client is not provided."""
        result = handle_command(None)
        self.assertFalse(result.success)
        self.assertIn("Client is required", result.message)

    @patch("chuck_data.commands.scan_pii.get_active_catalog")
    @patch("chuck_data.commands.scan_pii.get_active_schema")
    def test_missing_context(self, mock_get_active_schema, mock_get_active_catalog):
        """Test handling when catalog or schema is missing."""
        # Setup mocks
        mock_get_active_catalog.return_value = None
        mock_get_active_schema.return_value = None

        # Call function
        result = handle_command(self.client)

        # Verify results
        self.assertFalse(result.success)
        self.assertIn("Catalog and schema must be specified", result.message)

    @patch("chuck_data.commands.scan_pii.LLMClient")
    @patch("chuck_data.commands.scan_pii._helper_scan_schema_for_pii_logic")
    def test_successful_scan(self, mock_helper_scan, mock_llm_client):
        """Test successful schema scan for PII."""
        # Setup mocks
        llm_client_stub = LLMClientStub()
        mock_llm_client.return_value = llm_client_stub

        mock_helper_scan.return_value = {
            "tables_successfully_processed": 5,
            "tables_scanned_attempted": 6,
            "tables_with_pii": 3,
            "total_pii_columns": 8,
            "catalog": "test_catalog",
            "schema": "test_schema",
            "results_detail": [
                {"full_name": "test_catalog.test_schema.table1", "has_pii": True},
                {"full_name": "test_catalog.test_schema.table2", "has_pii": True},
                {"full_name": "test_catalog.test_schema.table3", "has_pii": True},
                {"full_name": "test_catalog.test_schema.table4", "has_pii": False},
                {"full_name": "test_catalog.test_schema.table5", "has_pii": False},
            ],
        }

        # Call function
        result = handle_command(
            self.client, catalog_name="test_catalog", schema_name="test_schema"
        )

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(result.data["tables_successfully_processed"], 5)
        self.assertEqual(result.data["tables_with_pii"], 3)
        self.assertEqual(result.data["total_pii_columns"], 8)
        self.assertIn("Scanned 5/6 tables", result.message)
        self.assertIn("Found 3 tables with 8 PII columns", result.message)
        mock_helper_scan.assert_called_once_with(
            self.client, llm_client_stub, "test_catalog", "test_schema"
        )

    @patch("chuck_data.commands.scan_pii.get_active_catalog")
    @patch("chuck_data.commands.scan_pii.get_active_schema")
    @patch("chuck_data.commands.scan_pii.LLMClient")
    @patch("chuck_data.commands.scan_pii._helper_scan_schema_for_pii_logic")
    def test_scan_with_active_context(
        self,
        mock_helper_scan,
        mock_llm_client,
        mock_get_active_schema,
        mock_get_active_catalog,
    ):
        """Test schema scan using active catalog and schema."""
        # Setup mocks
        mock_get_active_catalog.return_value = "active_catalog"
        mock_get_active_schema.return_value = "active_schema"

        llm_client_stub = LLMClientStub()
        mock_llm_client.return_value = llm_client_stub

        mock_helper_scan.return_value = {
            "tables_successfully_processed": 3,
            "tables_scanned_attempted": 3,
            "tables_with_pii": 1,
            "total_pii_columns": 2,
        }

        # Call function without catalog/schema args
        result = handle_command(self.client)

        # Verify results
        self.assertTrue(result.success)
        mock_helper_scan.assert_called_once_with(
            self.client, llm_client_stub, "active_catalog", "active_schema"
        )

    @patch("chuck_data.commands.scan_pii.LLMClient")
    @patch("chuck_data.commands.scan_pii._helper_scan_schema_for_pii_logic")
    def test_scan_with_helper_error(self, mock_helper_scan, mock_llm_client):
        """Test handling when helper returns an error."""
        # Setup mocks
        llm_client_stub = LLMClientStub()
        mock_llm_client.return_value = llm_client_stub

        mock_helper_scan.return_value = {"error": "Failed to list tables"}

        # Call function
        result = handle_command(
            self.client, catalog_name="test_catalog", schema_name="test_schema"
        )

        # Verify results
        self.assertFalse(result.success)
        self.assertEqual(result.message, "Failed to list tables")

    @patch("chuck_data.commands.scan_pii.LLMClient")
    def test_scan_with_exception(self, mock_llm_client):
        """Test handling when an exception occurs."""
        # Setup mocks
        mock_llm_client.side_effect = Exception("LLM client error")

        # Call function
        result = handle_command(
            self.client, catalog_name="test_catalog", schema_name="test_schema"
        )

        # Verify results
        self.assertFalse(result.success)
        self.assertIn("Error during bulk PII scan", result.message)
        self.assertEqual(str(result.error), "LLM client error")
