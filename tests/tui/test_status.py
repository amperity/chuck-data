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

    def test_user_sees_full_status_table_when_agent_checks_status(
        self, tui_with_mocked_console
    ):
        """When agent checks status, user sees the full status table and pagination is triggered."""
        tui = tui_with_mocked_console

        # Agent collects status information
        workspace_status = {
            "workspace_url": "https://acme-corp.databricks.com",
            "active_catalog": "production_data",
            "active_schema": "analytics",
            "connection_status": "Connected (client present).",
            "permissions": {"catalog_access": {"authorized": True}},
        }

        # User sees the full status display when agent calls status tool
        # This should trigger pagination to prevent further agent interaction
        from chuck_data.exceptions import PaginationCancelled

        with pytest.raises(PaginationCancelled):
            tui.display_tool_output("status", workspace_status)

        # User should see the full status table displayed
        assert tui.console.print.called

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

        # User sees the full error status when agent reports it
        # This should trigger pagination to prevent further agent interaction
        from chuck_data.exceptions import PaginationCancelled

        with pytest.raises(PaginationCancelled):
            tui.display_tool_output("status", problem_status)

        # User should see the full status table displayed
        assert tui.console.print.called

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

        # User sees full status table regardless of URL length
        # This should trigger pagination to prevent further agent interaction
        from chuck_data.exceptions import PaginationCancelled

        with pytest.raises(PaginationCancelled):
            tui.display_tool_output("status", enterprise_status)

        # User should see the full status table displayed
        assert tui.console.print.called

    def test_user_sees_full_status_table_not_generic_tool_output_when_agent_calls_status(
        self, tui_with_mocked_console
    ):
        """When agent uses status tool, user sees full status table, not generic tool output."""
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

            # Agent calls status, user sees the full display with pagination
            from chuck_data.exceptions import PaginationCancelled

            with pytest.raises(PaginationCancelled):
                tui.display_tool_output("status", workspace_info)

            # User should see the full status table displayed
            assert tui.console.print.called

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

            # User sees agent's status check result with full display and pagination
            from chuck_data.exceptions import PaginationCancelled

            with pytest.raises(PaginationCancelled):
                tui.display_tool_output("status", comprehensive_status)

            # User should see the full status table displayed
            assert tui.console.print.called


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

    def test_status_command_always_shows_full_view_for_agents(
        self, mock_chuck_service_init
    ):
        """Status command consistently shows full view when agents use it."""
        from chuck_data.commands.status import DEFINITION

        # Command is configured to always use full display for agents
        assert DEFINITION.agent_display == "full"

        # Status command should no longer use conditional display
        assert (
            not hasattr(DEFINITION, "display_condition")
            or DEFINITION.display_condition is None
        )

    def test_status_command_infrastructure_remains_available_for_both_flows(
        self, tui_with_mocked_console
    ):
        """Status command preserves both direct user and agent display capabilities."""
        tui = tui_with_mocked_console

        # Users can still access direct status display method
        assert hasattr(tui, "_display_status")
        assert callable(tui._display_status)

        # Agents now use the same full display method as direct users
        # No specialized agent display handler needed since we want full display
