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

    @patch("chuck_data.commands.scan_pii.LLMClient")
    @patch("chuck_data.commands.scan_pii._helper_scan_schema_for_pii_logic")
    @patch("chuck_data.ui.tui.get_console")
    def test_interactive_display_shows_table_progress(
        self, mock_get_console, mock_helper_scan, mock_llm_client
    ):
        """Test that interactive display shows catalog.schema.table progress during scanning."""
        # Setup mocks
        mock_console = MagicMock()
        mock_get_console.return_value = mock_console
        
        llm_client_stub = LLMClientStub()
        mock_llm_client.return_value = llm_client_stub

        # Mock the helper to simulate table scanning
        mock_helper_scan.return_value = {
            "tables_successfully_processed": 2,
            "tables_scanned_attempted": 2,
            "tables_with_pii": 1,
            "total_pii_columns": 3,
            "catalog": "test_catalog",
            "schema": "test_schema",
            "results_detail": [
                {"full_name": "test_catalog.test_schema.users", "has_pii": True},
                {"full_name": "test_catalog.test_schema.products", "has_pii": False},
            ],
        }

        # Call function with show_progress enabled (will be default)
        result = handle_command(
            self.client, catalog_name="test_catalog", schema_name="test_schema"
        )

        # Verify results
        self.assertTrue(result.success)
        
        # Verify that the helper was called with show_progress=True (default)
        mock_helper_scan.assert_called_once()
        call_args = mock_helper_scan.call_args
        # The helper should be called with show_progress=True by default
        
    @patch("chuck_data.commands.scan_pii.LLMClient")
    @patch("chuck_data.commands.scan_pii._helper_scan_schema_for_pii_logic")
    @patch("chuck_data.ui.tui.get_console")
    def test_interactive_display_console_integration(
        self, mock_get_console, mock_helper_scan, mock_llm_client
    ):
        """Test that console.print is called correctly during interactive scanning."""
        # Setup mocks
        mock_console = MagicMock()
        mock_get_console.return_value = mock_console
        
        llm_client_stub = LLMClientStub()
        mock_llm_client.return_value = llm_client_stub

        # Create a custom mock that captures console calls during processing
        def mock_helper_side_effect(*args, **kwargs):
            # This simulates the helper calling console.print for each table
            console = mock_get_console()
            console.print("[dim]Scanning test_catalog.test_schema.users...[/dim]")
            console.print("[dim]Scanning test_catalog.test_schema.products...[/dim]")
            
            return {
                "tables_successfully_processed": 2,
                "tables_scanned_attempted": 2,
                "tables_with_pii": 1,
                "total_pii_columns": 3,
                "results_detail": [
                    {"full_name": "test_catalog.test_schema.users", "has_pii": True},
                    {"full_name": "test_catalog.test_schema.products", "has_pii": False},
                ],
            }
        
        mock_helper_scan.side_effect = mock_helper_side_effect

        # Call function
        result = handle_command(
            self.client, catalog_name="test_catalog", schema_name="test_schema"
        )

        # Verify results
        self.assertTrue(result.success)
        
        # Verify console.print was called with the expected format
        expected_calls = [
            "[dim]Scanning test_catalog.test_schema.users...[/dim]",
            "[dim]Scanning test_catalog.test_schema.products...[/dim]"
        ]
        
        # Check that console.print was called with our expected messages
        print_calls = [call[0][0] for call in mock_console.print.call_args_list]
        for expected_msg in expected_calls:
            self.assertIn(expected_msg, print_calls)

    @patch("chuck_data.commands.scan_pii.LLMClient")
    @patch("chuck_data.commands.scan_pii._helper_scan_schema_for_pii_logic")
    @patch("chuck_data.ui.tui.get_console")
    def test_interactive_display_can_be_disabled(
        self, mock_get_console, mock_helper_scan, mock_llm_client
    ):
        """Test that interactive display can be disabled via show_progress parameter."""
        # Setup mocks
        mock_console = MagicMock()
        mock_get_console.return_value = mock_console
        
        llm_client_stub = LLMClientStub()
        mock_llm_client.return_value = llm_client_stub

        mock_helper_scan.return_value = {
            "tables_successfully_processed": 1,
            "tables_scanned_attempted": 1,
            "tables_with_pii": 0,
            "total_pii_columns": 0,
        }

        # Call function with show_progress disabled
        result = handle_command(
            self.client, 
            catalog_name="test_catalog", 
            schema_name="test_schema",
            show_progress=False
        )

        # Verify results
        self.assertTrue(result.success)
        
        # Verify that the helper was called with show_progress=False
        mock_helper_scan.assert_called_once()
        call_args = mock_helper_scan.call_args
        # The helper should be called with show_progress=False
