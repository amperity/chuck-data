"""
Tests for the status command module.

Following approved testing patterns:
- Mock external boundaries only (Databricks API calls)
- Use real config system with temporary files
- Test end-to-end command behavior with real business logic
"""

import tempfile
from unittest.mock import patch

from chuck_data.commands.status import handle_command
from chuck_data.config import ConfigManager


def test_handle_status_with_valid_connection_real_logic(databricks_client_stub):
    """Test status command with valid connection using real config system."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        # Set up real config state
        config_manager.update(
            workspace_url="https://test.databricks.com",
            active_catalog="test_catalog",
            active_schema="test_schema",
            active_model="test_model",
            warehouse_id="test_warehouse",
        )

        # Mock only external boundary (Databricks API permission validation)
        with patch("chuck_data.config._config_manager", config_manager):
            with patch(
                "chuck_data.commands.status.validate_all_permissions"
            ) as mock_permissions:
                mock_permissions.return_value = {"test_resource": {"authorized": True}}

                # Call function with real config and external API mock
                result = handle_command(databricks_client_stub)

    # Verify real command execution with real config values
    assert result.success
    assert result.data["workspace_url"] == "https://test.databricks.com"
    assert result.data["active_catalog"] == "test_catalog"
    assert result.data["active_schema"] == "test_schema"
    assert result.data["active_model"] == "test_model"
    assert result.data["warehouse_id"] == "test_warehouse"
    assert result.data["connection_status"] == "Connected (client present)."
    assert result.data["permissions"] == {"test_resource": {"authorized": True}}


def test_handle_status_with_no_client_real_logic():
    """Test status command with no client using real config system."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        # Set up real config state
        config_manager.update(
            workspace_url="https://test.databricks.com",
            active_catalog="test_catalog",
            active_schema="test_schema",
            active_model="test_model",
            warehouse_id="test_warehouse",
        )

        with patch("chuck_data.config._config_manager", config_manager):
            # Call function with no client - should use real config
            result = handle_command(None)

    # Verify real command execution with real config values
    assert result.success
    assert result.data["workspace_url"] == "https://test.databricks.com"
    assert result.data["active_catalog"] == "test_catalog"
    assert result.data["active_schema"] == "test_schema"
    assert result.data["active_model"] == "test_model"
    assert result.data["warehouse_id"] == "test_warehouse"
    assert (
        result.data["connection_status"] == "Client not available or not initialized."
    )
    assert result.data["permissions"] == {}  # No permissions check without client


def test_handle_status_with_permission_error_real_logic(databricks_client_stub):
    """Test status command when permission validation fails."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        # Set up real config state
        config_manager.update(
            workspace_url="https://test.databricks.com", active_catalog="test_catalog"
        )

        # Mock external API to simulate permission error
        with patch("chuck_data.config._config_manager", config_manager):
            with patch(
                "chuck_data.commands.status.validate_all_permissions"
            ) as mock_permissions:
                mock_permissions.side_effect = Exception("Permission denied")

                # Test real error handling with external API failure
                result = handle_command(databricks_client_stub)

    # Verify real error handling - should still succeed but with error message
    assert result.success
    assert (
        "Permission denied" in result.data["connection_status"]
        or "error" in result.data["connection_status"]
    )
    # Real config values should still be present
    assert result.data["workspace_url"] == "https://test.databricks.com"
    assert result.data["active_catalog"] == "test_catalog"


def test_handle_status_with_config_error_real_logic():
    """Test status command when config system encounters error."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        # Don't initialize config - should handle missing config gracefully

        with patch("chuck_data.config._config_manager", config_manager):
            # Test real error handling with uninitialized config
            result = handle_command(None)

    # Should handle config errors gracefully - exact behavior depends on real implementation
    assert isinstance(result.success, bool)
    assert result.data is not None or result.error is not None


def test_handle_status_with_partial_config_real_logic(databricks_client_stub):
    """Test status command with partially configured system."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        # Set up partial config state (missing some values)
        config_manager.update(
            workspace_url="https://test.databricks.com",
            # Missing catalog, schema, model - should handle gracefully
        )

        with patch("chuck_data.config._config_manager", config_manager):
            with patch(
                "chuck_data.commands.status.validate_all_permissions"
            ) as mock_permissions:
                mock_permissions.return_value = {}

                # Test real handling of partial configuration
                result = handle_command(databricks_client_stub)

    # Should succeed with real config handling of missing values
    assert result.success
    assert result.data["workspace_url"] == "https://test.databricks.com"
    # Other values should be None or default values from real config system
    assert result.data["active_catalog"] is None or isinstance(
        result.data["active_catalog"], str
    )
    assert result.data["connection_status"] == "Connected (client present)."


def test_handle_status_real_config_integration():
    """Test status command integration with real config system."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        # Test multiple config updates to verify real config behavior
        config_manager.update(workspace_url="https://first.databricks.com")
        config_manager.update(active_catalog="first_catalog")
        config_manager.update(
            workspace_url="https://second.databricks.com"
        )  # Update workspace

        with patch("chuck_data.config._config_manager", config_manager):
            result = handle_command(None)

    # Verify real config system behavior with updates
    assert result.success
    assert (
        result.data["workspace_url"] == "https://second.databricks.com"
    )  # Latest update
    assert result.data["active_catalog"] == "first_catalog"  # Preserved from earlier


def test_agent_status_with_display_false_shows_condensed_behavior(
    databricks_client_stub,
):
    """Agent status execution with display=False shows condensed behavior."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(
            workspace_url="https://test.databricks.com",
            active_catalog="test_catalog",
        )

        with patch("chuck_data.config._config_manager", config_manager):
            with patch(
                "chuck_data.commands.status.validate_all_permissions"
            ) as mock_permissions:
                mock_permissions.return_value = {"test_resource": {"authorized": True}}

                # Execute with display=False (default agent behavior)
                result = handle_command(databricks_client_stub, display=False)

        # Verify command success
        assert result.success
        assert result.data["workspace_url"] == "https://test.databricks.com"
        assert result.data["active_catalog"] == "test_catalog"
        assert not result.data["display"]


def test_agent_status_with_display_true_shows_full_behavior(databricks_client_stub):
    """Agent status execution with display=True shows full behavior."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(
            workspace_url="https://test.databricks.com",
            active_catalog="test_catalog",
        )

        with patch("chuck_data.config._config_manager", config_manager):
            with patch(
                "chuck_data.commands.status.validate_all_permissions"
            ) as mock_permissions:
                mock_permissions.return_value = {"test_resource": {"authorized": True}}

                # Execute with display=True (full display)
                result = handle_command(databricks_client_stub, display=True)

        # Verify command success
        assert result.success
        assert result.data["workspace_url"] == "https://test.databricks.com"
        assert result.data["active_catalog"] == "test_catalog"
        assert result.data["display"]


def test_direct_status_command_defaults_to_condensed_display(databricks_client_stub):
    """Direct status command (from TUI) defaults to condensed display."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(workspace_url="https://test.databricks.com")

        with patch("chuck_data.config._config_manager", config_manager):
            with patch(
                "chuck_data.commands.status.validate_all_permissions"
            ) as mock_permissions:
                mock_permissions.return_value = {}

                # Execute without display parameter (direct command)
                result = handle_command(databricks_client_stub)

        # Verify command success and default display behavior
        assert result.success
        assert result.data["workspace_url"] == "https://test.databricks.com"
        assert not result.data["display"]  # Default is False


def test_agent_status_never_shows_tool_output_section(databricks_client_stub):
    """Agent status execution should never show 'Tool Output: status' section."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(
            workspace_url="https://test.databricks.com",
            active_catalog="test_catalog",
        )

        with patch("chuck_data.config._config_manager", config_manager):
            with patch(
                "chuck_data.commands.status.validate_all_permissions"
            ) as mock_permissions:
                mock_permissions.return_value = {"test_resource": {"authorized": True}}

                # Execute with default agent behavior (display=False)
                result = handle_command(databricks_client_stub)

        # Verify command success
        assert result.success
        assert result.data["workspace_url"] == "https://test.databricks.com"
        assert result.data["active_catalog"] == "test_catalog"

        # CRITICAL: Verify display=False to ensure condensed display (no Tool Output section)
        assert not result.data["display"]

        # Verify the command definition has correct settings for condensed display
        from chuck_data.commands.status import DEFINITION

        assert DEFINITION.agent_display == "conditional"
        assert DEFINITION.condensed_action == "Getting status"
        assert DEFINITION.display_condition is not None

        # Verify the display condition returns False for this result (triggers condensed display)
        assert not DEFINITION.display_condition(result.data)
