"""
Tests for add_stitch_report command handler.

This module contains tests for the add_stitch_report command handler.
"""

import unittest
from unittest.mock import patch

from src.commands.add_stitch_report import handle_command
from tests.fixtures import DatabricksClientStub, MetricsCollectorStub


class TestAddStitchReport(unittest.TestCase):
    """Tests for add_stitch_report command handler."""

    def setUp(self):
        """Set up common test fixtures."""
        self.client = DatabricksClientStub()
        # Client stub has create_stitch_notebook method by default

    def test_missing_client(self):
        """Test handling when client is not provided."""
        result = handle_command(None, table_path="catalog.schema.table")
        self.assertFalse(result.success)
        self.assertIn("Client is required", result.message)

    def test_missing_table_path(self):
        """Test handling when table_path is missing."""
        result = handle_command(self.client)
        self.assertFalse(result.success)
        self.assertIn("Table path must be provided", result.message)

    def test_invalid_table_path_format(self):
        """Test handling when table_path format is invalid."""
        result = handle_command(self.client, table_path="invalid_format")
        self.assertFalse(result.success)
        self.assertIn("must be in the format", result.message)

    @patch("src.commands.add_stitch_report.get_metrics_collector")
    def test_successful_report_creation(self, mock_get_metrics_collector):
        """Test successful stitch report notebook creation."""
        # Setup mocks
        metrics_collector_stub = MetricsCollectorStub()
        mock_get_metrics_collector.return_value = metrics_collector_stub

        self.client.set_create_stitch_notebook_result(
            {
                "path": "/Workspace/Users/user@example.com/Stitch Results",
                "status": "success",
            }
        )

        # Call function
        result = handle_command(self.client, table_path="catalog.schema.table")

        # Verify results
        self.assertTrue(result.success)
        self.assertIn("Successfully created", result.message)
        # Verify the call was made with correct arguments
        self.assertEqual(len(self.client.create_stitch_notebook_calls), 1)
        args, kwargs = self.client.create_stitch_notebook_calls[0]
        self.assertEqual(args, ("catalog.schema.table", None))

        # Verify metrics collection
        self.assertEqual(len(metrics_collector_stub.track_event_calls), 1)
        call = metrics_collector_stub.track_event_calls[0]
        self.assertEqual(call["prompt"], "add-stitch-report command")
        self.assertEqual(call["additional_data"]["status"], "success")

    @patch("src.commands.add_stitch_report.get_metrics_collector")
    def test_report_creation_with_custom_name(self, mock_get_metrics_collector):
        """Test stitch report creation with custom notebook name."""
        # Setup mocks
        metrics_collector_stub = MetricsCollectorStub()
        mock_get_metrics_collector.return_value = metrics_collector_stub

        self.client.set_create_stitch_notebook_result(
            {
                "path": "/Workspace/Users/user@example.com/My Custom Report",
                "status": "success",
            }
        )

        # Call function
        result = handle_command(
            self.client, table_path="catalog.schema.table", name="My Custom Report"
        )

        # Verify results
        self.assertTrue(result.success)
        self.assertIn("Successfully created", result.message)
        # Verify the call was made with correct arguments
        self.assertEqual(len(self.client.create_stitch_notebook_calls), 1)
        args, kwargs = self.client.create_stitch_notebook_calls[0]
        self.assertEqual(args, ("catalog.schema.table", "My Custom Report"))

    @patch("src.commands.add_stitch_report.get_metrics_collector")
    def test_report_creation_with_rest_args(self, mock_get_metrics_collector):
        """Test stitch report creation with rest arguments as notebook name."""
        # Setup mocks
        metrics_collector_stub = MetricsCollectorStub()
        mock_get_metrics_collector.return_value = metrics_collector_stub

        self.client.set_create_stitch_notebook_result(
            {
                "path": "/Workspace/Users/user@example.com/Multi Word Name",
                "status": "success",
            }
        )

        # Call function with rest parameter
        result = handle_command(
            self.client, table_path="catalog.schema.table", rest="Multi Word Name"
        )

        # Verify results
        self.assertTrue(result.success)
        self.assertIn("Successfully created", result.message)
        # Verify the call was made with correct arguments
        self.assertEqual(len(self.client.create_stitch_notebook_calls), 1)
        args, kwargs = self.client.create_stitch_notebook_calls[0]
        self.assertEqual(args, ("catalog.schema.table", "Multi Word Name"))

    @patch("src.commands.add_stitch_report.get_metrics_collector")
    def test_report_creation_api_error(self, mock_get_metrics_collector):
        """Test handling when API call to create notebook fails."""
        # Setup mocks
        metrics_collector_stub = MetricsCollectorStub()
        mock_get_metrics_collector.return_value = metrics_collector_stub

        self.client.set_create_stitch_notebook_error(ValueError("API Error"))

        # Call function
        result = handle_command(self.client, table_path="catalog.schema.table")

        # Verify results
        self.assertFalse(result.success)
        self.assertIn("Error creating Stitch report", result.message)

        # Verify metrics collection for error
        self.assertEqual(len(metrics_collector_stub.track_event_calls), 1)
        call = metrics_collector_stub.track_event_calls[0]
        self.assertEqual(call["prompt"], "add-stitch-report command")
        self.assertEqual(call["error"], "API Error")
