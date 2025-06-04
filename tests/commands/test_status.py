"""
Tests for the status command module.
"""

import unittest
from unittest.mock import patch, MagicMock

from src.commands.status import handle_command


class TestStatusCommand(unittest.TestCase):
    """Test cases for the status command handler."""

    def setUp(self):
        """Set up common test fixtures."""
        self.client = MagicMock()

    @patch("src.commands.status.get_workspace_url")
    @patch("src.commands.status.get_active_catalog")
    @patch("src.commands.status.get_active_schema")
    @patch("src.commands.status.get_active_model")
    @patch("src.commands.status.validate_all_permissions")
    def test_handle_status_with_valid_connection(
        self,
        mock_permissions,
        mock_get_model,
        mock_get_schema,
        mock_get_catalog,
        mock_get_url,
    ):
        """Test status command with valid connection."""
        # Setup mocks
        mock_get_url.return_value = "test-workspace"
        mock_get_catalog.return_value = "test-catalog"
        mock_get_schema.return_value = "test-schema"
        mock_get_model.return_value = "test-model"
        mock_permissions.return_value = {"test_resource": {"authorized": True}}

        # Call function
        result = handle_command(self.client)

        # Verify result
        self.assertTrue(result.success)
        self.assertEqual(result.data["workspace_url"], "test-workspace")
        self.assertEqual(result.data["active_catalog"], "test-catalog")
        self.assertEqual(result.data["active_schema"], "test-schema")
        self.assertEqual(result.data["active_model"], "test-model")
        self.assertEqual(
            result.data["connection_status"], "Connected (client present)."
        )
        self.assertEqual(result.data["permissions"], mock_permissions.return_value)

    @patch("src.commands.status.get_workspace_url")
    @patch("src.commands.status.get_active_catalog")
    @patch("src.commands.status.get_active_schema")
    @patch("src.commands.status.get_active_model")
    def test_handle_status_with_no_client(
        self, mock_get_model, mock_get_schema, mock_get_catalog, mock_get_url
    ):
        """Test status command with no client provided."""
        # Setup mocks
        mock_get_url.return_value = "test-workspace"
        mock_get_catalog.return_value = "test-catalog"
        mock_get_schema.return_value = "test-schema"
        mock_get_model.return_value = "test-model"

        # Call function with no client
        result = handle_command(None)

        # Verify result
        self.assertTrue(result.success)
        self.assertEqual(result.data["workspace_url"], "test-workspace")
        self.assertEqual(result.data["active_catalog"], "test-catalog")
        self.assertEqual(result.data["active_schema"], "test-schema")
        self.assertEqual(result.data["active_model"], "test-model")
        self.assertEqual(
            result.data["connection_status"],
            "Client not available or not initialized.",
        )

    @patch("src.commands.status.get_workspace_url")
    @patch("src.commands.status.get_active_catalog")
    @patch("src.commands.status.get_active_schema")
    @patch("src.commands.status.get_active_model")
    @patch("src.commands.status.validate_all_permissions")
    @patch("logging.error")
    def test_handle_status_with_exception(
        self,
        mock_log,
        mock_permissions,
        mock_get_model,
        mock_get_schema,
        mock_get_catalog,
        mock_get_url,
    ):
        """Test status command when an exception occurs."""
        # Setup mock to raise exception
        mock_get_url.side_effect = ValueError("Config error")

        # Call function
        result = handle_command(self.client)

        # Verify result
        self.assertFalse(result.success)
        self.assertIsNotNone(result.error)
        mock_log.assert_called_once()
