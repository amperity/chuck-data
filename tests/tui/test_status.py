"""
Tests for status command TUI integration.

This module tests how the status command integrates with the TUI,
including both direct user commands and agent display functionality.
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
    """Test status command agent display functionality."""

    def test_status_command_registered_for_agent_display(self, tui_with_mocked_console):
        """Status command should be registered in agent display handlers."""
        tui = tui_with_mocked_console

        # Status should be registered in the handlers registry
        assert "status" in tui.agent_full_display_handlers
        assert callable(tui.agent_full_display_handlers["status"])

    def test_status_agent_display_shows_condensed_summary(
        self, tui_with_mocked_console
    ):
        """Status agent display should show a condensed summary with key info."""
        tui = tui_with_mocked_console

        status_data = {
            "workspace_url": "https://test.databricks.com",
            "active_catalog": "production",
            "active_schema": "bronze",
            "connection_status": "Connected (client present).",
            "permissions": {},
        }

        # Call the status agent display handler
        tui._display_status_for_agent("status", status_data)

        # Should print a condensed status summary as a Panel
        tui.console.print.assert_called_once()
        call_args = tui.console.print.call_args[0][0]

        assert isinstance(call_args, Panel)
        panel_content = str(call_args.renderable)
        assert "test.databricks.com" in panel_content
        assert "production" in panel_content
        assert "bronze" in panel_content
        assert "Connected" in panel_content

    def test_status_agent_display_highlights_connection_issues(
        self, tui_with_mocked_console
    ):
        """Status agent display should highlight connection problems."""
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

        panel_content = str(call_args.renderable)
        assert "Invalid token" in panel_content or "error" in panel_content.lower()

    def test_status_agent_display_formats_long_workspace_urls(
        self, tui_with_mocked_console
    ):
        """Status agent display should handle long workspace URLs gracefully."""
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

        panel_content = str(call_args.renderable)
        assert "very-long-workspace-name.cloud.databricks.com" in panel_content

    @patch("chuck_data.ui.tui.get_command")
    def test_status_tool_uses_custom_agent_handler(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Status tool should use its custom agent display handler."""
        tui = tui_with_mocked_console

        # Use real status command definition
        from chuck_data.commands.status import DEFINITION

        mock_get_cmd.return_value = DEFINITION

        status_data = {
            "workspace_url": "https://test.databricks.com",
            "active_catalog": "test_catalog",
            "connection_status": "Connected (client present).",
            "permissions": {"test_resource": {"authorized": True}},
        }

        tui.display_tool_output("status", status_data)

        # Should call console.print (the custom handler will display)
        tui.console.print.assert_called()

    def test_status_command_end_to_end_agent_flow(self, mock_chuck_service_init):
        """Test complete agent flow for status command."""
        with patch("chuck_data.ui.tui.get_command") as mock_get_cmd:
            tui = ChuckTUI()
            tui.console = MagicMock(spec=Console)

            # Use real status command definition
            from chuck_data.commands.status import DEFINITION

            mock_get_cmd.return_value = DEFINITION

            status_data = {
                "workspace_url": "https://test.databricks.com",
                "active_catalog": "production",
                "connection_status": "Connected (client present).",
                "permissions": {"basic": {"authorized": True}},
            }

            # This should route through the agent display system
            tui.display_tool_output("status", status_data)

            # Should have displayed something
            tui.console.print.assert_called()

            # Should NOT show generic "Agent Tool Output: status" panel
            # (because status has a custom agent display handler)
            for call in tui.console.print.call_args_list:
                panel_arg = call[0][0] if call[0] else None
                if isinstance(panel_arg, Panel):
                    assert "Agent Tool Output: status" not in str(panel_arg.title)


class TestStatusCommandDirectDisplay:
    """Test status command direct user display functionality."""

    @patch("chuck_data.ui.tui.get_command")
    def test_direct_status_command_shows_full_display(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Direct user /status command should show full status table."""
        tui = tui_with_mocked_console

        # Simulate direct command execution path
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            config_manager.update(
                workspace_url="https://test.databricks.com",
                active_catalog="test_catalog",
            )

            with patch("chuck_data.config._config_manager", config_manager):
                status_data = {
                    "workspace_url": "https://test.databricks.com",
                    "active_catalog": "test_catalog",
                    "connection_status": "Connected",
                    "permissions": {},
                }

                # The full status display should raise PaginationCancelled
                from chuck_data.exceptions import PaginationCancelled

                with pytest.raises(PaginationCancelled):
                    tui._display_status(status_data)

                # Should have printed the full status table
                assert tui.console.print.call_count > 0

    def test_status_full_display_preserves_pagination_cancelled(
        self, tui_with_mocked_console
    ):
        """Status full display should raise PaginationCancelled for TUI flow."""
        tui = tui_with_mocked_console

        status_data = {
            "workspace_url": "https://test.databricks.com",
            "connection_status": "Connected",
            "permissions": {},
        }

        from chuck_data.exceptions import PaginationCancelled

        with pytest.raises(PaginationCancelled):
            tui._display_status(status_data)


class TestStatusCommandIntegration:
    """Integration tests for status command TUI behavior."""

    def test_status_command_conditional_display_logic(self, mock_chuck_service_init):
        """Status command should use conditional display logic correctly."""
        from chuck_data.commands.status import DEFINITION

        # Status command should be configured for conditional display
        assert DEFINITION.agent_display == "conditional"
        assert callable(DEFINITION.display_condition)

        # Display condition should always return False (always condensed for agents)
        test_data = {"workspace_url": "test", "connection_status": "Connected"}
        assert DEFINITION.display_condition(test_data) is False

    def test_status_backwards_compatibility_preserved(self, tui_with_mocked_console):
        """Status command changes should preserve backwards compatibility."""
        tui = tui_with_mocked_console

        # Should still have the full display method for direct commands
        assert hasattr(tui, "_display_status")
        assert callable(tui._display_status)

        # Should still have the agent display handler
        assert "status" in tui.agent_full_display_handlers
        assert callable(tui.agent_full_display_handlers["status"])
