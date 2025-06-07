"""Unit tests for the Databricks auth utilities."""

import pytest
import os
from unittest.mock import patch, MagicMock
from chuck_data.databricks_auth import get_databricks_token, validate_databricks_token


@patch("os.getenv", return_value="mock_env_token")
@patch("chuck_data.databricks_auth.get_token_from_config", return_value=None)
@patch("logging.info")
def test_get_databricks_token_from_env(mock_log, mock_config_token, mock_getenv):
    """
    Test that the token is retrieved from environment when not in config.

    This validates the fallback to environment variable when config doesn't have a token.
    """
    token = get_databricks_token()
    assert token == "mock_env_token"
    mock_config_token.assert_called_once()
    mock_getenv.assert_called_once_with("DATABRICKS_TOKEN")
    mock_log.assert_called_once()


@patch("os.getenv", return_value="mock_env_token")
@patch(
    "chuck_data.databricks_auth.get_token_from_config",
    return_value="mock_config_token",
)
def test_get_databricks_token_from_config(mock_config_token, mock_getenv):
    """
    Test that the token is retrieved from config first when available.

    This validates that config is prioritized over environment variable.
    """
    token = get_databricks_token()
    assert token == "mock_config_token"
    mock_config_token.assert_called_once()
    # Environment variable should not be checked when config has token
    mock_getenv.assert_not_called()


@patch("os.getenv", return_value=None)
@patch("chuck_data.databricks_auth.get_token_from_config", return_value=None)
def test_get_databricks_token_missing(mock_config_token, mock_getenv):
    """
    Test behavior when token is not available in config or environment.

    This validates error handling when the required token is missing from both sources.
    """
    with pytest.raises(EnvironmentError) as excinfo:
        get_databricks_token()
    assert "Databricks token not found" in str(excinfo.value)
    mock_config_token.assert_called_once()
    mock_getenv.assert_called_once_with("DATABRICKS_TOKEN")


@patch("chuck_data.clients.databricks.DatabricksAPIClient.validate_token")
@patch(
    "chuck_data.databricks_auth.get_workspace_url", return_value="test-workspace"
)
def test_validate_databricks_token_success(mock_workspace_url, mock_validate):
    """
    Test successful validation of a Databricks token.

    This validates the API call structure and successful response handling.
    """
    mock_validate.return_value = True

    result = validate_databricks_token("mock_token")

    assert result
    mock_validate.assert_called_once()


def test_workspace_url_defined():
    """
    Test that the workspace URL can be retrieved from the configuration.

    This is more of a smoke test to ensure the function exists and returns a value.
    """
    from chuck_data.config import get_workspace_url, _config_manager

    # Patch the config manager to provide a workspace URL
    mock_config = MagicMock()
    mock_config.workspace_url = "test-workspace"
    with patch.object(_config_manager, "get_config", return_value=mock_config):
        workspace_url = get_workspace_url()
        assert workspace_url == "test-workspace"


@patch("chuck_data.clients.databricks.DatabricksAPIClient.validate_token")
@patch(
    "chuck_data.databricks_auth.get_workspace_url", return_value="test-workspace"
)
@patch("logging.error")
def test_validate_databricks_token_failure(mock_log, mock_workspace_url, mock_validate):
    """
    Test failed validation of a Databricks token.

    This validates error handling for invalid or expired tokens.
    """
    mock_validate.return_value = False

    result = validate_databricks_token("mock_token")

    assert not result
    mock_validate.assert_called_once()


@patch("chuck_data.clients.databricks.DatabricksAPIClient.validate_token")
@patch(
    "chuck_data.databricks_auth.get_workspace_url", return_value="test-workspace"
)
@patch("logging.error")
def test_validate_databricks_token_connection_error(
    mock_log, mock_workspace_url, mock_validate
):
    """
    Test failed validation due to connection error.

    This validates network error handling during token validation.
    """
    mock_validate.side_effect = ConnectionError("Connection Error")

    # The function should still raise ConnectionError for connection errors
    with pytest.raises(ConnectionError) as excinfo:
        validate_databricks_token("mock_token")

    assert "Connection Error" in str(excinfo.value)
    # Verify errors were logged - may be multiple logs for connection errors
    assert mock_log.call_count >= 1, "Error logging was expected"


@patch("chuck_data.databricks_auth.get_token_from_config", return_value=None)
@patch("logging.info")
def test_get_databricks_token_from_real_env(mock_log, mock_config_token, mock_databricks_env):
    """
    Test retrieving token from actual environment variable when not in config.

    This test checks actual environment integration rather than mocked calls.
    """
    token = get_databricks_token()
    # mock_databricks_env fixture sets DATABRICKS_TOKEN to "test_token"
    assert token == "test_token"
    mock_config_token.assert_called_once()