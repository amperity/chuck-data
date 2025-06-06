"""
Tests for setup_stitch command handler.

This module contains tests for the setup_stitch command handler.
"""

import unittest
from unittest.mock import patch, MagicMock

from chuck_data.commands.setup_stitch import handle_command
from tests.fixtures import LLMClientStub


class TestSetupStitch(unittest.TestCase):
    """Tests for setup_stitch command handler."""

    def setUp(self):
        """Set up common test fixtures."""
        self.client = MagicMock()

    def test_missing_client(self):
        """Test handling when client is not provided."""
        result = handle_command(None)
        self.assertFalse(result.success)
        self.assertIn("Client is required", result.message)

    @patch("chuck_data.commands.setup_stitch.get_active_catalog")
    @patch("chuck_data.commands.setup_stitch.get_active_schema")
    def test_missing_context(self, mock_get_active_schema, mock_get_active_catalog):
        """Test handling when catalog or schema is missing."""
        # Setup mocks
        mock_get_active_catalog.return_value = None
        mock_get_active_schema.return_value = None

        # Call function
        result = handle_command(self.client)

        # Verify results
        self.assertFalse(result.success)
        self.assertIn("Target catalog and schema must be specified", result.message)

    @patch("chuck_data.commands.setup_stitch._helper_launch_stitch_job")
    @patch("chuck_data.commands.setup_stitch.LLMClient")
    @patch("chuck_data.commands.setup_stitch._helper_setup_stitch_logic")
    @patch("chuck_data.commands.setup_stitch.get_metrics_collector")
    def test_successful_setup(
        self,
        mock_get_metrics_collector,
        mock_helper_setup,
        mock_llm_client,
        mock_launch_job,
    ):
        """Test successful Stitch setup."""
        # Setup mocks
        llm_client_stub = LLMClientStub()
        mock_llm_client.return_value = llm_client_stub
        mock_metrics_collector = MagicMock()
        mock_get_metrics_collector.return_value = mock_metrics_collector

        mock_helper_setup.return_value = {
            "stitch_config": {},
            "metadata": {
                "target_catalog": "test_catalog",
                "target_schema": "test_schema",
            },
        }
        mock_launch_job.return_value = {
            "message": "Stitch setup completed successfully.",
            "tables_processed": 5,
            "pii_columns_tagged": 8,
            "config_created": True,
            "config_path": "/Volumes/test_catalog/test_schema/_stitch/config.json",
        }

        # Call function with auto_confirm to use legacy behavior
        result = handle_command(
            self.client,
            **{
                "catalog_name": "test_catalog",
                "schema_name": "test_schema",
                "auto_confirm": True,
            },
        )

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(result.message, "Stitch setup completed successfully.")
        self.assertEqual(result.data["tables_processed"], 5)
        self.assertEqual(result.data["pii_columns_tagged"], 8)
        self.assertTrue(result.data["config_created"])
        mock_helper_setup.assert_called_once_with(
            self.client, llm_client_stub, "test_catalog", "test_schema"
        )
        mock_launch_job.assert_called_once_with(
            self.client,
            {},
            {"target_catalog": "test_catalog", "target_schema": "test_schema"},
        )

        # Verify metrics collection
        mock_metrics_collector.track_event.assert_called_once_with(
            prompt="setup-stitch command",
            tools=[
                {
                    "name": "setup_stitch",
                    "arguments": {"catalog": "test_catalog", "schema": "test_schema"},
                }
            ],
            additional_data={
                "event_context": "direct_stitch_command",
                "status": "success",
                "tables_processed": 5,
                "pii_columns_tagged": 8,
                "config_created": True,
                "config_path": "/Volumes/test_catalog/test_schema/_stitch/config.json",
            },
        )

    @patch("chuck_data.commands.setup_stitch._helper_launch_stitch_job")
    @patch("chuck_data.commands.setup_stitch.get_active_catalog")
    @patch("chuck_data.commands.setup_stitch.get_active_schema")
    @patch("chuck_data.commands.setup_stitch.LLMClient")
    @patch("chuck_data.commands.setup_stitch._helper_setup_stitch_logic")
    def test_setup_with_active_context(
        self,
        mock_helper_setup,
        mock_llm_client,
        mock_get_active_schema,
        mock_get_active_catalog,
        mock_launch_job,
    ):
        """Test Stitch setup using active catalog and schema."""
        # Setup mocks
        mock_get_active_catalog.return_value = "active_catalog"
        mock_get_active_schema.return_value = "active_schema"

        llm_client_stub = LLMClientStub()
        mock_llm_client.return_value = llm_client_stub

        mock_helper_setup.return_value = {
            "stitch_config": {},
            "metadata": {
                "target_catalog": "active_catalog",
                "target_schema": "active_schema",
            },
        }
        mock_launch_job.return_value = {
            "message": "Stitch setup completed.",
            "tables_processed": 3,
            "config_created": True,
        }

        # Call function without catalog/schema args, with auto_confirm
        result = handle_command(self.client, **{"auto_confirm": True})

        # Verify results
        self.assertTrue(result.success)
        mock_helper_setup.assert_called_once_with(
            self.client, llm_client_stub, "active_catalog", "active_schema"
        )
        mock_launch_job.assert_called_once_with(
            self.client,
            {},
            {"target_catalog": "active_catalog", "target_schema": "active_schema"},
        )

    @patch("chuck_data.commands.setup_stitch.LLMClient")
    @patch("chuck_data.commands.setup_stitch._helper_setup_stitch_logic")
    @patch("chuck_data.commands.setup_stitch.get_metrics_collector")
    def test_setup_with_helper_error(
        self, mock_get_metrics_collector, mock_helper_setup, mock_llm_client
    ):
        """Test handling when helper returns an error."""
        # Setup mocks
        llm_client_stub = LLMClientStub()
        mock_llm_client.return_value = llm_client_stub
        mock_metrics_collector = MagicMock()
        mock_get_metrics_collector.return_value = mock_metrics_collector

        mock_helper_setup.return_value = {"error": "Failed to scan tables for PII"}

        # Call function with auto_confirm
        result = handle_command(
            self.client,
            **{
                "catalog_name": "test_catalog",
                "schema_name": "test_schema",
                "auto_confirm": True,
            },
        )

        # Verify results
        self.assertFalse(result.success)
        self.assertEqual(result.message, "Failed to scan tables for PII")

        # Verify metrics collection for error
        mock_metrics_collector.track_event.assert_called_once_with(
            prompt="setup-stitch command",
            tools=[
                {
                    "name": "setup_stitch",
                    "arguments": {"catalog": "test_catalog", "schema": "test_schema"},
                }
            ],
            error="Failed to scan tables for PII",
            additional_data={
                "event_context": "direct_stitch_command",
                "status": "error",
            },
        )

    @patch("chuck_data.commands.setup_stitch.LLMClient")
    def test_setup_with_exception(self, mock_llm_client):
        """Test handling when an exception occurs."""
        # Setup mocks
        mock_llm_client.side_effect = Exception("LLM client error")

        # Call function
        result = handle_command(
            self.client, catalog_name="test_catalog", schema_name="test_schema"
        )

        # Verify results
        self.assertFalse(result.success)
        self.assertIn("Error setting up Stitch", result.message)
        self.assertEqual(str(result.error), "LLM client error")
