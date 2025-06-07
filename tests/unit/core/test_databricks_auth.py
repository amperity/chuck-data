"""
Unit tests for the Databricks auth utilities.

Following approved testing patterns:
- Mock external boundaries only (os.getenv, API calls)
- Use real config system with temporary files
- Test end-to-end auth behavior with real business logic
"""

import pytest
import tempfile
from unittest.mock import patch

from chuck_data.databricks_auth import get_databricks_token, validate_databricks_token
from chuck_data.config import ConfigManager


def test_get_databricks_token_from_config_real_logic():
    """Test that the token is retrieved from real config first when available."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        # Set up real config with token
        config_manager.update(databricks_token="config_token")

        with patch("chuck_data.config._config_manager", config_manager):
            # Mock os.getenv to return None for environment checks (config should have priority)
            with patch("os.getenv", return_value=None):
                # Test real config token retrieval
                token = get_databricks_token()

                # Should get token from real config, not environment
                assert token == "config_token"


def test_get_databricks_token_from_env_real_logic():
    """Test that the token falls back to environment when not in real config."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        # Don't set databricks_token in config - should be None

        with patch("chuck_data.config._config_manager", config_manager):
            with patch("os.getenv", return_value="env_token"):
                # Test real config fallback to environment
                token = get_databricks_token()

                assert token == "env_token"


def test_get_databricks_token_missing_real_logic():
    """Test behavior when token is not available in real config or environment."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        # No token in config

        with patch("chuck_data.config._config_manager", config_manager):
            with patch("os.getenv", return_value=None):
                # Test real error handling when no token available
                with pytest.raises(EnvironmentError) as excinfo:
                    get_databricks_token()

                assert "Databricks token not found" in str(excinfo.value)


def test_validate_databricks_token_success_real_logic(databricks_client_stub):
    """Test successful validation of a Databricks token with real config."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(workspace_url="https://test.databricks.com")

        with patch("chuck_data.config._config_manager", config_manager):
            # Mock only the external API boundary (client creation and validation)
            with patch(
                "chuck_data.databricks_auth.DatabricksAPIClient"
            ) as mock_client_class:
                mock_client = mock_client_class.return_value
                mock_client.validate_token.return_value = True

                # Test real validation logic with external API mock
                result = validate_databricks_token("test_token")

                assert result is True
                mock_client_class.assert_called_once_with(
                    "https://test.databricks.com", "test_token"
                )
                mock_client.validate_token.assert_called_once()


def test_validate_databricks_token_failure_real_logic():
    """Test failed validation of a Databricks token with real config."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(workspace_url="https://test.databricks.com")

        with patch("chuck_data.config._config_manager", config_manager):
            # Mock external API to return validation failure
            with patch(
                "chuck_data.databricks_auth.DatabricksAPIClient"
            ) as mock_client_class:
                mock_client = mock_client_class.return_value
                mock_client.validate_token.return_value = False

                # Test real error handling with API failure
                result = validate_databricks_token("invalid_token")

                assert result is False


def test_validate_databricks_token_connection_error_real_logic():
    """Test validation with connection error using real config."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(workspace_url="https://test.databricks.com")

        with patch("chuck_data.config._config_manager", config_manager):
            # Mock external API to raise connection error
            with patch(
                "chuck_data.databricks_auth.DatabricksAPIClient"
            ) as mock_client_class:
                mock_client = mock_client_class.return_value
                mock_client.validate_token.side_effect = ConnectionError(
                    "Network error"
                )

                # Test real error handling with connection failure
                with pytest.raises(ConnectionError) as excinfo:
                    validate_databricks_token("test_token")

                assert "Network error" in str(excinfo.value)


def test_get_databricks_token_with_real_env(monkeypatch):
    """Test retrieving token from actual environment variable with real config."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        # No token in config, should fall back to real environment

        with patch("chuck_data.config._config_manager", config_manager):
            # Set environment variable with monkeypatch
            monkeypatch.setenv("DATABRICKS_TOKEN", "test_token")

            # Test real config + real environment integration
            token = get_databricks_token()

            # Environment variable should be used when no token in config
            assert token == "test_token"


def test_token_priority_real_logic():
    """Test that config token takes priority over environment token."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(databricks_token="config_priority_token")

        with patch("chuck_data.config._config_manager", config_manager):
            # Even with environment variable set, config should take priority
            with patch("os.getenv") as mock_getenv:

                def side_effect(key):
                    if key == "DATABRICKS_TOKEN":
                        return "env_fallback_token"
                    return None  # Return None for other env vars during config loading

                mock_getenv.side_effect = side_effect

                # Test real priority logic: config should override environment
                token = get_databricks_token()

                assert token == "config_priority_token"


def test_workspace_url_integration_real_logic():
    """Test workspace URL integration with real config system."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(workspace_url="https://custom.databricks.com")

        with patch("chuck_data.config._config_manager", config_manager):
            with patch(
                "chuck_data.databricks_auth.DatabricksAPIClient"
            ) as mock_client_class:
                mock_client = mock_client_class.return_value
                mock_client.validate_token.return_value = True

                # Test real workspace URL retrieval
                result = validate_databricks_token("test_token")

                # Should use real config workspace URL
                mock_client_class.assert_called_once_with(
                    "https://custom.databricks.com", "test_token"
                )
                assert result is True
