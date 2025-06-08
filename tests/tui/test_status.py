"""
Tests for status command user experience.

This module tests what users see when they interact with the status command,
both directly through the TUI and when an agent uses the status tool.
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


class TestWhatUsersSeeDuringAgentStatusCalls:
    """Test what users see when the agent calls the status tool."""

    def test_user_sees_compact_status_summary_when_agent_checks_status(
        self, tui_with_mocked_console
    ):
        """When agent checks status, user sees a compact panel with key workspace info."""
        tui = tui_with_mocked_console

        # Agent collects status information
        workspace_status = {
            "workspace_url": "https://acme-corp.databricks.com",
            "active_catalog": "production_data",
            "active_schema": "analytics",
            "connection_status": "Connected (client present).",
            "permissions": {"catalog_access": {"authorized": True}},
        }

        # User sees the status display when agent calls status tool
        tui._display_status_for_agent("status", workspace_status)

        # User should see a single compact panel printed to console
        tui.console.print.assert_called_once()
        displayed_panel = tui.console.print.call_args[0][0]

        # User sees a Rich panel with workspace information
        assert isinstance(displayed_panel, Panel)
        panel_text = str(displayed_panel.renderable)

        # User can see their current workspace and active settings
        assert "acme-corp.databricks.com" in panel_text
        assert "production_data" in panel_text
        assert "analytics" in panel_text
        assert "Connected" in panel_text

    def test_user_sees_connection_problems_highlighted_when_agent_checks_status(
        self, tui_with_mocked_console
    ):
        """When agent detects connection issues, user sees error info prominently displayed."""
        tui = tui_with_mocked_console

        # Agent detects authentication problem
        problem_status = {
            "workspace_url": "https://test.databricks.com",
            "active_catalog": "production",
            "connection_status": "Client connection/permission error: Token expired",
            "permissions": {},
        }

        # User sees the error status when agent reports it
        tui._display_status_for_agent("status", problem_status)

        tui.console.print.assert_called_once()
        displayed_panel = tui.console.print.call_args[0][0]
        panel_text = str(displayed_panel.renderable)

        # User can clearly see what went wrong
        assert "Token expired" in panel_text or "error" in panel_text.lower()

    def test_user_sees_clean_format_for_long_workspace_names(
        self, tui_with_mocked_console
    ):
        """User sees readable format even with very long workspace URLs."""
        tui = tui_with_mocked_console

        # Agent works with enterprise workspace with long name
        enterprise_status = {
            "workspace_url": "https://very-long-enterprise-workspace-name.cloud.databricks.com",
            "active_catalog": "enterprise_catalog",
            "connection_status": "Connected",
            "permissions": {},
        }

        # User sees formatted output regardless of URL length
        tui._display_status_for_agent("status", enterprise_status)

        tui.console.print.assert_called_once()
        displayed_panel = tui.console.print.call_args[0][0]
        panel_text = str(displayed_panel.renderable)

        # User can see the full workspace name without layout issues
        assert "very-long-enterprise-workspace-name.cloud.databricks.com" in panel_text

    def test_user_sees_status_panel_not_generic_tool_output_when_agent_calls_status(
        self, tui_with_mocked_console
    ):
        """When agent uses status tool, user sees custom status display, not generic tool output."""
        tui = tui_with_mocked_console

        # Mock the command definition lookup
        with patch("chuck_data.ui.tui.get_command") as mock_get_cmd:
            from chuck_data.commands.status import DEFINITION

            mock_get_cmd.return_value = DEFINITION

            workspace_info = {
                "workspace_url": "https://test.databricks.com",
                "active_catalog": "test_catalog",
                "connection_status": "Connected (client present).",
                "permissions": {"resource": {"authorized": True}},
            }

            # Agent calls status, user sees the display
            tui.display_tool_output("status", workspace_info)

            # User should see some output from the custom handler
            tui.console.print.assert_called()

            # User should NOT see generic "Agent Tool Output: status" panel
            for call in tui.console.print.call_args_list:
                if call[0]:
                    displayed_content = call[0][0]
                    if isinstance(displayed_content, Panel):
                        panel_title = (
                            str(displayed_content.title)
                            if displayed_content.title
                            else ""
                        )
                        assert "Agent Tool Output: status" not in panel_title

    def test_user_experiences_complete_agent_status_flow(self, mock_chuck_service_init):
        """User experiences the complete flow when agent checks workspace status."""
        # User interacts with agent, agent needs to check current status
        with patch("chuck_data.ui.tui.get_command") as mock_get_cmd:
            tui = ChuckTUI()
            tui.console = MagicMock(spec=Console)

            from chuck_data.commands.status import DEFINITION

            mock_get_cmd.return_value = DEFINITION

            # Agent gathers comprehensive status info
            comprehensive_status = {
                "workspace_url": "https://company.databricks.com",
                "active_catalog": "production",
                "connection_status": "Connected (client present).",
                "permissions": {"basic_access": {"authorized": True}},
            }

            # User sees agent's status check result
            tui.display_tool_output("status", comprehensive_status)

            # User should see meaningful output on their screen
            tui.console.print.assert_called()


class TestWhatUsersSeeDuringDirectStatusCommands:
    """Test what users see when they directly run status commands."""

    def test_user_sees_comprehensive_status_table_for_direct_status_command(
        self, tui_with_mocked_console
    ):
        """When user runs /status directly, they see a comprehensive status table."""
        tui = tui_with_mocked_console

        # User has configured their workspace
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            config_manager.update(
                workspace_url="https://mycompany.databricks.com",
                active_catalog="analytics_catalog",
            )

            with patch("chuck_data.config._config_manager", config_manager):
                user_status = {
                    "workspace_url": "https://mycompany.databricks.com",
                    "active_catalog": "analytics_catalog",
                    "connection_status": "Connected",
                    "permissions": {"catalog_read": {"authorized": True}},
                }

                # User runs /status command and expects full detailed view
                from chuck_data.exceptions import PaginationCancelled

                with pytest.raises(PaginationCancelled):
                    tui._display_status(user_status)

                # User should have seen detailed status information printed
                assert tui.console.print.call_count > 0

    def test_user_direct_status_maintains_expected_tui_pagination_behavior(
        self, tui_with_mocked_console
    ):
        """User's direct /status command maintains expected TUI pagination flow."""
        tui = tui_with_mocked_console

        user_workspace_status = {
            "workspace_url": "https://test.databricks.com",
            "connection_status": "Connected",
            "permissions": {"workspace": {"authorized": True}},
        }

        # User's direct command should follow TUI pagination pattern
        from chuck_data.exceptions import PaginationCancelled

        with pytest.raises(PaginationCancelled):
            tui._display_status(user_workspace_status)


class TestStatusCommandBehaviorConsistency:
    """Test that status command behavior is consistent and reliable."""

    def test_status_command_always_shows_condensed_view_for_agents(
        self, mock_chuck_service_init
    ):
        """Status command consistently shows condensed view when agents use it."""
        from chuck_data.commands.status import DEFINITION

        # Command is configured to always use condensed display for agents
        assert DEFINITION.agent_display == "conditional"
        assert callable(DEFINITION.display_condition)

        # Any status data should result in condensed display for agents
        various_status_scenarios = [
            {"workspace_url": "test", "connection_status": "Connected"},
            {"workspace_url": "test", "connection_status": "Error", "permissions": {}},
            {
                "workspace_url": "different",
                "active_catalog": "prod",
                "connection_status": "Connected",
            },
        ]

        for status_data in various_status_scenarios:
            # Should always return False = condensed display for agents
            assert DEFINITION.display_condition(status_data) is False

    def test_status_command_infrastructure_remains_available_for_both_flows(
        self, tui_with_mocked_console
    ):
        """Status command preserves both direct user and agent display capabilities."""
        tui = tui_with_mocked_console

        # Users can still access direct status display method
        assert hasattr(tui, "_display_status")
        assert callable(tui._display_status)

        # Agents can access their specialized status display handler
        assert "status" in tui.agent_full_display_handlers
        assert callable(tui.agent_full_display_handlers["status"])
