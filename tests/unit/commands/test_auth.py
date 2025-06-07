"""Unit tests for the auth commands module."""

from unittest.mock import patch

from chuck_data.commands.auth import (
    handle_amperity_login,
    handle_databricks_login,
    handle_logout,
)


@patch("chuck_data.commands.auth.AmperityAPIClient")
def test_amperity_login_success(mock_auth_client_class, amperity_client_stub):
    """Test successful Amperity login flow."""
    # Use AmperityClientStub instead of MagicMock
    mock_auth_client_class.return_value = amperity_client_stub

    # Execute
    result = handle_amperity_login(None)

    # Verify
    assert result.success
    assert result.message == "Authentication completed successfully."


@patch("chuck_data.commands.auth.AmperityAPIClient")
def test_amperity_login_start_failure(mock_auth_client_class, amperity_client_stub):
    """Test failure during start of Amperity login flow."""
    # Use AmperityClientStub configured to fail at start
    amperity_client_stub.set_auth_start_failure(True)
    mock_auth_client_class.return_value = amperity_client_stub

    # Execute
    result = handle_amperity_login(None)

    # Verify
    assert not result.success
    assert result.message == "Login failed: Failed to start auth: 500 - Server Error"


@patch("chuck_data.commands.auth.AmperityAPIClient")
def test_amperity_login_completion_failure(
    mock_auth_client_class, amperity_client_stub
):
    """Test failure during completion of Amperity login flow."""
    # Use AmperityClientStub configured to fail at completion
    amperity_client_stub.set_auth_completion_failure(True)
    mock_auth_client_class.return_value = amperity_client_stub

    # Execute
    result = handle_amperity_login(None)

    # Verify
    assert not result.success
    assert result.message == "Login failed: Authentication failed: error"


@patch("chuck_data.commands.auth.set_databricks_token")
def test_databricks_login_success(mock_set_token):
    """Test setting the Databricks token."""
    # Setup
    mock_set_token.return_value = True
    test_token = "test-token-123"

    # Execute
    result = handle_databricks_login(None, token=test_token)

    # Verify
    assert result.success
    assert result.message == "Databricks token set successfully"
    mock_set_token.assert_called_with(test_token)


def test_databricks_login_missing_token():
    """Test error when token is missing."""
    # Execute
    result = handle_databricks_login(None)

    # Verify
    assert not result.success
    assert result.message == "Token parameter is required"


@patch("chuck_data.commands.auth.set_databricks_token")
def test_logout_databricks(mock_set_db_token):
    """Test logout from Databricks."""
    # Setup
    mock_set_db_token.return_value = True

    # Execute
    result = handle_logout(None, service="databricks")

    # Verify
    assert result.success
    assert result.message == "Successfully logged out from databricks"
    mock_set_db_token.assert_called_with("")


@patch("chuck_data.config.set_amperity_token")
def test_logout_amperity(mock_set_amp_token):
    """Test logout from Amperity."""
    # Setup
    mock_set_amp_token.return_value = True

    # Execute
    result = handle_logout(None, service="amperity")

    # Verify
    assert result.success
    assert result.message == "Successfully logged out from amperity"
    mock_set_amp_token.assert_called_with("")


@patch("chuck_data.config.set_amperity_token")
@patch("chuck_data.commands.auth.set_databricks_token")
def test_logout_default(mock_set_db_token, mock_set_amp_token):
    """Test default logout behavior (only Amperity)."""
    # Setup
    mock_set_amp_token.return_value = True

    # Execute
    result = handle_logout(None)  # No service specified

    # Verify
    assert result.success
    assert result.message == "Successfully logged out from amperity"
    mock_set_amp_token.assert_called_with("")
    mock_set_db_token.assert_not_called()


@patch("chuck_data.commands.auth.set_databricks_token")
@patch("chuck_data.config.set_amperity_token")
def test_logout_all(mock_set_amp_token, mock_set_db_token):
    """Test logout from all services."""
    # Setup
    mock_set_db_token.return_value = True
    mock_set_amp_token.return_value = True

    # Execute
    result = handle_logout(None, service="all")

    # Verify
    assert result.success
    assert result.message == "Successfully logged out from all"
    mock_set_db_token.assert_called_with("")
    mock_set_amp_token.assert_called_with("")
