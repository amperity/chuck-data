"""
Tests for TUI status command agent display.

Tests the specific status command migration to the new agent display system.
"""

import pytest
import tempfile
from unittest.mock import MagicMock, patch
from rich.console import Console
from rich.panel import Panel

from chuck_data.ui.tui import ChuckTUI
from chuck_data.config import ConfigManager


@pytest.fixture
def mock_chuck_service_init():
    """Mock ChuckService to avoid complex dependencies in TUI tests."""
    with patch("chuck_data.ui.tui.ChuckService") as mock:
        mock.return_value = MagicMock()
        yield mock


@pytest.fixture
def tui_with_mocked_console(mock_chuck_service_init):
    """Create ChuckTUI instance with mocked console for testing."""
    tui = ChuckTUI()
    tui.console = MagicMock(spec=Console)
    return tui


class TestStatusCommandAgentDisplay:
    """Test status command integration with agent display system."""

    @patch("chuck_data.ui.tui.get_command")
    def test_status_command_registered_for_agent_full_display(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Status command should be registered in agent_full_display_handlers."""
        tui = tui_with_mocked_console

        # Status should be registered in the handlers registry
        assert "status" in tui.agent_full_display_handlers
        assert callable(tui.agent_full_display_handlers["status"])

    @patch("chuck_data.ui.tui.get_command")
    def test_status_tool_uses_custom_agent_display_handler(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Status tool should use its custom agent display handler, not the generic one."""
        tui = tui_with_mocked_console

        # Mock the command definition to return full display
        from chuck_data.commands.status import DEFINITION

        mock_get_cmd.return_value = DEFINITION

        # Mock the status data that would be returned
        status_data = {
            "workspace_url": "https://test.databricks.com",
            "active_catalog": "test_catalog",
            "active_schema": "test_schema",
            "active_model": "test_model",
            "warehouse_id": "test_warehouse",
            "connection_status": "Connected (client present).",
            "permissions": {"test_resource": {"authorized": True}},
        }

        tui.display_tool_output("status", status_data)

        # Should call console.print (the custom handler will print status table)
        tui.console.print.assert_called()
        # Should NOT show the generic "Agent Tool Output: status" panel
        # (This test will help ensure our custom handler is being used)

    def test_status_agent_display_handler_shows_condensed_summary(
        self, tui_with_mocked_console
    ):
        """Status agent display handler should show a condensed summary for agents."""
        tui = tui_with_mocked_console

        status_data = {
            "workspace_url": "https://test.databricks.com",
            "active_catalog": "production",
            "active_schema": "bronze",
            "connection_status": "Connected (client present).",
            "permissions": {},
        }

        # Call the specific status agent display handler
        tui._display_status_for_agent("status", status_data)

        # Should print a condensed status summary, not the full table
        tui.console.print.assert_called_once()
        call_args = tui.console.print.call_args[0][0]

        # Should include key status info in condensed format
        assert isinstance(call_args, Panel)
        panel_content = str(call_args.renderable)
        assert "test.databricks.com" in panel_content
        assert "production" in panel_content
        assert "bronze" in panel_content
        assert "Connected" in panel_content

    def test_status_agent_display_handler_shows_connection_issues(
        self, tui_with_mocked_console
    ):
        """Status agent display handler should highlight connection issues."""
        tui = tui_with_mocked_console

        status_data = {
            "workspace_url": "https://test.databricks.com",
            "active_catalog": "production",
            "connection_status": "Client connection/permission error: Invalid token",
            "permissions": {},
        }

        tui._display_status_for_agent("status", status_data)

        tui.console.print.assert_called_once()
        call_args = tui.console.print.call_args[0][0]

        # Should highlight the connection issue
        panel_content = str(call_args.renderable)
        assert "Invalid token" in panel_content or "error" in panel_content.lower()

    def test_status_agent_display_handler_formats_workspace_url(
        self, tui_with_mocked_console
    ):
        """Status agent display handler should format workspace URL nicely."""
        tui = tui_with_mocked_console

        status_data = {
            "workspace_url": "https://very-long-workspace-name.cloud.databricks.com",
            "active_catalog": "test",
            "connection_status": "Connected",
            "permissions": {},
        }

        tui._display_status_for_agent("status", status_data)

        tui.console.print.assert_called_once()
        call_args = tui.console.print.call_args[0][0]

        # Should show a nice formatted display
        panel_content = str(call_args.renderable)
        assert "very-long-workspace-name.cloud.databricks.com" in panel_content


class TestStatusCommandBackwardsCompatibility:
    """Test that status command changes don't break existing functionality."""

    @patch("chuck_data.ui.tui.get_command")
    def test_direct_status_command_still_works(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Direct user /status command should still work with full display."""
        tui = tui_with_mocked_console

        # When called directly (not through agent), should show full status
        # This simulates the existing _process_command_result path
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            config_manager.update(
                workspace_url="https://test.databricks.com",
                active_catalog="test_catalog",
            )

            with patch("chuck_data.config._config_manager", config_manager):
                # This simulates the direct command result display
                status_data = {
                    "workspace_url": "https://test.databricks.com",
                    "active_catalog": "test_catalog",
                    "connection_status": "Connected",
                    "permissions": {},
                }

                # The existing _display_status method should still work
                from chuck_data.exceptions import PaginationCancelled

                with pytest.raises(PaginationCancelled):
                    tui._display_status(status_data)

                # Should print the full status table (existing behavior)
                assert tui.console.print.call_count > 0

    def test_status_command_migration_preserves_pagination_cancelled(
        self, tui_with_mocked_console
    ):
        """Status command should still raise PaginationCancelled for full displays."""
        tui = tui_with_mocked_console

        status_data = {
            "workspace_url": "https://test.databricks.com",
            "connection_status": "Connected",
            "permissions": {},
        }

        # The full status display should still raise PaginationCancelled
        from chuck_data.exceptions import PaginationCancelled

        with pytest.raises(PaginationCancelled):
            tui._display_status(status_data)


class TestStatusAgentDisplayIntegration:
    """Integration tests for status command with agent display system."""

    def test_status_command_end_to_end_agent_flow(self, mock_chuck_service_init):
        """Test complete agent flow for status command."""
        with patch("chuck_data.ui.tui.get_command") as mock_get_cmd:
            tui = ChuckTUI()
            tui.console = MagicMock(spec=Console)

            # Mock command definition (matches real status command)
            from chuck_data.commands.status import DEFINITION

            mock_get_cmd.return_value = DEFINITION

            status_data = {
                "workspace_url": "https://test.databricks.com",
                "active_catalog": "production",
                "connection_status": "Connected (client present).",
                "permissions": {"basic": {"authorized": True}},
            }

            # This should route through the new agent display system
            tui.display_tool_output("status", status_data)

            # Should have displayed something (either condensed or custom agent format)
            tui.console.print.assert_called()

            # Should NOT show the generic "Agent Tool Output: status" panel
            # (because status has a custom agent display handler)
            for call in tui.console.print.call_args_list:
                panel_arg = call[0][0] if call[0] else None
                if isinstance(panel_arg, Panel):
                    # If it's a panel, it shouldn't be the generic one
                    assert "Agent Tool Output: status" not in str(panel_arg.title)
