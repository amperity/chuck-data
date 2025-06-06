"""Unit tests for the auth commands module."""

import unittest
from unittest.mock import patch

from chuck_data.commands.auth import (
    handle_amperity_login,
    handle_databricks_login,
    handle_logout,
)
from tests.fixtures import AmperityClientStub


class TestAuthCommands(unittest.TestCase):
    """Test cases for authentication commands."""

    @patch("chuck_data.commands.auth.AmperityAPIClient")
    def test_amperity_login_success(self, mock_auth_client_class):
        """Test successful Amperity login flow."""
        # Use AmperityClientStub instead of MagicMock
        client_stub = AmperityClientStub()
        mock_auth_client_class.return_value = client_stub

        # Execute
        result = handle_amperity_login(None)

        # Verify
        self.assertTrue(result.success)
        self.assertEqual(result.message, "Authentication completed successfully.")

    @patch("chuck_data.commands.auth.AmperityAPIClient")
    def test_amperity_login_start_failure(self, mock_auth_client_class):
        """Test failure during start of Amperity login flow."""
        # Use AmperityClientStub configured to fail at start
        client_stub = AmperityClientStub()
        client_stub.set_auth_start_failure(True)
        mock_auth_client_class.return_value = client_stub

        # Execute
        result = handle_amperity_login(None)

        # Verify
        self.assertFalse(result.success)
        self.assertEqual(
            result.message, "Login failed: Failed to start auth: 500 - Server Error"
        )

    @patch("chuck_data.commands.auth.AmperityAPIClient")
    def test_amperity_login_completion_failure(self, mock_auth_client_class):
        """Test failure during completion of Amperity login flow."""
        # Use AmperityClientStub configured to fail at completion
        client_stub = AmperityClientStub()
        client_stub.set_auth_completion_failure(True)
        mock_auth_client_class.return_value = client_stub

        # Execute
        result = handle_amperity_login(None)

        # Verify
        self.assertFalse(result.success)
        self.assertEqual(result.message, "Login failed: Authentication failed: error")

    @patch("chuck_data.commands.auth.set_databricks_token")
    def test_databricks_login_success(self, mock_set_token):
        """Test setting the Databricks token."""
        # Setup
        mock_set_token.return_value = True
        test_token = "test-token-123"

        # Execute
        result = handle_databricks_login(None, token=test_token)

        # Verify
        self.assertTrue(result.success)
        self.assertEqual(result.message, "Databricks token set successfully")
        mock_set_token.assert_called_with(test_token)

    def test_databricks_login_missing_token(self):
        """Test error when token is missing."""
        # Execute
        result = handle_databricks_login(None)

        # Verify
        self.assertFalse(result.success)
        self.assertEqual(result.message, "Token parameter is required")

    @patch("chuck_data.commands.auth.set_databricks_token")
    def test_logout_databricks(self, mock_set_db_token):
        """Test logout from Databricks."""
        # Setup
        mock_set_db_token.return_value = True

        # Execute
        result = handle_logout(None, service="databricks")

        # Verify
        self.assertTrue(result.success)
        self.assertEqual(result.message, "Successfully logged out from databricks")
        mock_set_db_token.assert_called_with("")

    @patch("chuck_data.config.set_amperity_token")
    def test_logout_amperity(self, mock_set_amp_token):
        """Test logout from Amperity."""
        # Setup
        mock_set_amp_token.return_value = True

        # Execute
        result = handle_logout(None, service="amperity")

        # Verify
        self.assertTrue(result.success)
        self.assertEqual(result.message, "Successfully logged out from amperity")
        mock_set_amp_token.assert_called_with("")

    @patch("chuck_data.config.set_amperity_token")
    @patch("chuck_data.commands.auth.set_databricks_token")
    def test_logout_default(self, mock_set_db_token, mock_set_amp_token):
        """Test default logout behavior (only Amperity)."""
        # Setup
        mock_set_amp_token.return_value = True

        # Execute
        result = handle_logout(None)  # No service specified

        # Verify
        self.assertTrue(result.success)
        self.assertEqual(result.message, "Successfully logged out from amperity")
        mock_set_amp_token.assert_called_with("")
        mock_set_db_token.assert_not_called()

    @patch("chuck_data.commands.auth.set_databricks_token")
    @patch("chuck_data.config.set_amperity_token")
    def test_logout_all(self, mock_set_amp_token, mock_set_db_token):
        """Test logout from all services."""
        # Setup
        mock_set_db_token.return_value = True
        mock_set_amp_token.return_value = True

        # Execute
        result = handle_logout(None, service="all")

        # Verify
        self.assertTrue(result.success)
        self.assertEqual(result.message, "Successfully logged out from all")
        mock_set_db_token.assert_called_with("")
        mock_set_amp_token.assert_called_with("")
