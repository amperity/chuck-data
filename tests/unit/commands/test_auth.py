"""Unit tests for the auth commands module."""

from unittest.mock import patch

from chuck_data.commands.auth import (
    handle_amperity_login,
    handle_databricks_login,
    handle_logout,
)


def test_amperity_login_success(amperity_client_stub):
    """Test successful Amperity login flow."""
    # NOTE: We need to patch the class instantiation because handle_amperity_login
    # doesn't support dependency injection. This is a pragmatic compromise.
    # The AmperityClientStub provides the actual behavior with mocked HTTP calls.
    with patch("chuck_data.commands.auth.AmperityAPIClient") as mock_client_class:
        mock_client_class.return_value = amperity_client_stub

        # Execute
        result = handle_amperity_login(None)

        # Verify
        assert result.success
        assert result.message == "Authentication completed successfully."


def test_amperity_login_start_failure(amperity_client_stub):
    """Test failure during start of Amperity login flow."""
    # Configure stub to fail at start
    amperity_client_stub.set_auth_start_failure(True)

    with patch("chuck_data.commands.auth.AmperityAPIClient") as mock_client_class:
        mock_client_class.return_value = amperity_client_stub

        # Execute
        result = handle_amperity_login(None)

        # Verify
        assert not result.success
        assert (
            result.message == "Login failed: Failed to start auth: 500 - Server Error"
        )


def test_amperity_login_completion_failure(amperity_client_stub):
    """Test failure during completion of Amperity login flow."""
    # Configure stub to fail at completion
    amperity_client_stub.set_auth_completion_failure(True)

    with patch("chuck_data.commands.auth.AmperityAPIClient") as mock_client_class:
        mock_client_class.return_value = amperity_client_stub

        # Execute
        result = handle_amperity_login(None)

        # Verify
        assert not result.success
        assert result.message == "Login failed: Authentication failed: error"


def test_databricks_login_success():
    """Test setting the Databricks token."""
    import tempfile
    from chuck_data.config import ConfigManager, get_databricks_token

    # Use real config system
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        test_token = "test-token-123"

        with patch("chuck_data.config._config_manager", config_manager):
            # Execute
            result = handle_databricks_login(None, token=test_token)

            # Verify result
            assert result.success
            assert result.message == "Databricks token set successfully"

            # Verify token was actually set in config
            saved_token = get_databricks_token()
            assert saved_token == test_token


def test_databricks_login_missing_token():
    """Test error when token is missing."""
    # Execute
    result = handle_databricks_login(None)

    # Verify
    assert not result.success
    assert result.message == "Token parameter is required"


def test_logout_databricks():
    """Test logout from Databricks."""
    import tempfile
    from chuck_data.config import (
        ConfigManager,
        get_databricks_token,
        set_databricks_token,
    )

    # Use real config system
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            # Set initial token
            set_databricks_token("initial-token")
            assert get_databricks_token() == "initial-token"

            # Execute logout
            result = handle_logout(None, service="databricks")

            # Verify result
            assert result.success
            assert result.message == "Successfully logged out from databricks"

            # Verify token was actually cleared
            cleared_token = get_databricks_token()
            assert cleared_token == ""


def test_logout_amperity():
    """Test logout from Amperity."""
    import tempfile
    from chuck_data.config import ConfigManager, get_amperity_token, set_amperity_token

    # Use real config system
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            # Set initial token
            set_amperity_token("initial-amperity-token")
            assert get_amperity_token() == "initial-amperity-token"

            # Execute logout
            result = handle_logout(None, service="amperity")

            # Verify result
            assert result.success
            assert result.message == "Successfully logged out from amperity"

            # Verify token was actually cleared
            cleared_token = get_amperity_token()
            assert cleared_token == "" or cleared_token is None


def test_logout_default():
    """Test default logout behavior (only Amperity)."""
    import tempfile
    from chuck_data.config import (
        ConfigManager,
        get_amperity_token,
        set_amperity_token,
        get_databricks_token,
        set_databricks_token,
    )

    # Use real config system
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            # Set initial tokens
            set_amperity_token("initial-amperity-token")
            set_databricks_token("initial-databricks-token")
            assert get_amperity_token() == "initial-amperity-token"
            assert get_databricks_token() == "initial-databricks-token"

            # Execute default logout (no service specified)
            result = handle_logout(None)

            # Verify result
            assert result.success
            assert result.message == "Successfully logged out from amperity"

            # Verify only amperity token was cleared
            amperity_token = get_amperity_token()
            assert amperity_token == "" or amperity_token is None
            assert (
                get_databricks_token() == "initial-databricks-token"
            )  # Should remain unchanged


def test_logout_all():
    """Test logout from all services."""
    import tempfile
    from chuck_data.config import (
        ConfigManager,
        get_amperity_token,
        set_amperity_token,
        get_databricks_token,
        set_databricks_token,
    )

    # Use real config system
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            # Set initial tokens
            set_amperity_token("initial-amperity-token")
            set_databricks_token("initial-databricks-token")
            assert get_amperity_token() == "initial-amperity-token"
            assert get_databricks_token() == "initial-databricks-token"

            # Execute logout from all services
            result = handle_logout(None, service="all")

            # Verify result
            assert result.success
            assert result.message == "Successfully logged out from all"

            # Verify both tokens were cleared
            amperity_token = get_amperity_token()
            databricks_token = get_databricks_token()
            assert amperity_token == "" or amperity_token is None
            assert databricks_token == "" or databricks_token is None
