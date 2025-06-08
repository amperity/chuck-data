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

    def test_user_sees_condensed_status_when_agent_checks_status_internally(
        self, tui_with_mocked_console
    ):
        """When agent checks status internally (display=False), user sees condensed progress."""
        tui = tui_with_mocked_console

        # Agent collects status information internally (not showing to user)
        workspace_status = {
            "workspace_url": "https://acme-corp.databricks.com",
            "active_catalog": "production_data",
            "active_schema": "analytics",
            "connection_status": "Connected (client present).",
            "permissions": {"catalog_access": {"authorized": True}},
            "display": False,  # Agent checking status, not showing to user
        }

        # User sees condensed status display (no pagination)
        tui.display_tool_output("status", workspace_status)

        # User should see condensed output, not full table
        assert tui.console.print.called
        call_args = tui.console.print.call_args[0][0]
        assert "→" in call_args  # Condensed format indicator
        assert "Status check" in call_args or "checking status" in call_args.lower()

    def test_user_sees_full_status_table_when_agent_shows_status_to_user(
        self, tui_with_mocked_console
    ):
        """When agent shows status to user (display=True), user sees full table with pagination."""
        tui = tui_with_mocked_console

        # Agent showing status information to user
        workspace_status = {
            "workspace_url": "https://acme-corp.databricks.com",
            "active_catalog": "production_data",
            "active_schema": "analytics",
            "connection_status": "Connected (client present).",
            "permissions": {"catalog_access": {"authorized": True}},
            "display": True,  # Agent showing status to user
        }

        # User sees the full status display when agent shows status
        # This should trigger pagination to prevent further agent interaction
        from chuck_data.exceptions import PaginationCancelled

        # Test that _display_status directly raises PaginationCancelled
        with pytest.raises(PaginationCancelled):
            tui._display_status(workspace_status)

        # Test the conditional display logic
        from chuck_data.command_registry import get_command

        status_def = get_command("status")
        assert status_def is not None
        assert status_def.agent_display == "conditional"
        assert status_def.display_condition(workspace_status) is True

        # Test the full flow through display_tool_output
        with pytest.raises(PaginationCancelled):
            tui.display_tool_output("status", workspace_status)

    def test_user_sees_condensed_connection_problems_when_agent_checks_internally(
        self, tui_with_mocked_console
    ):
        """When agent detects connection issues internally, user sees condensed error info."""
        tui = tui_with_mocked_console

        # Agent detects authentication problem during internal check
        problem_status = {
            "workspace_url": "https://test.databricks.com",
            "active_catalog": "production",
            "connection_status": "Client connection/permission error: Token expired",
            "permissions": {},
            "display": False,  # Internal check, not showing to user
        }

        # User sees condensed error status
        tui.display_tool_output("status", problem_status)

        # User should see condensed output (error details not shown in condensed view)
        assert tui.console.print.called
        call_args = tui.console.print.call_args[0][0]
        assert "→" in call_args  # Condensed format indicator
        assert (
            "checking status" in call_args.lower()
            or "status check" in call_args.lower()
        )

    def test_user_sees_condensed_status_by_default_when_no_display_parameter(
        self, tui_with_mocked_console
    ):
        """When no display parameter is provided, user sees condensed status by default."""
        tui = tui_with_mocked_console

        # Agent works with status data but no display parameter
        enterprise_status = {
            "workspace_url": "https://very-long-enterprise-workspace-name.cloud.databricks.com",
            "active_catalog": "enterprise_catalog",
            "connection_status": "Connected",
            "permissions": {},
            # No display parameter - should default to condensed
        }

        # User sees condensed status display by default
        tui.display_tool_output("status", enterprise_status)

        # User should see condensed output by default
        assert tui.console.print.called
        call_args = tui.console.print.call_args[0][0]
        assert "→" in call_args  # Condensed format indicator

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
                "display": True,  # Agent showing status to user
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
                "display": True,  # Agent showing status to user
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


class TestStatusCommandParameterHandling:
    """Test that status command properly handles the display parameter."""

    def test_status_command_handler_accepts_display_parameter(self):
        """Status command handler should accept and process display parameter."""
        from chuck_data.commands.status import handle_command

        # Mock client
        mock_client = MagicMock()

        # Test with display=True
        result_with_display = handle_command(mock_client, display=True)
        assert result_with_display.success
        assert result_with_display.data["display"] is True

        # Test with display=False
        result_without_display = handle_command(mock_client, display=False)
        assert result_without_display.success
        assert result_without_display.data["display"] is False

        # Test with no display parameter (should default to False)
        result_no_display = handle_command(mock_client)
        assert result_no_display.success
        assert result_no_display.data["display"] is False

    def test_status_command_definition_has_display_parameter(self):
        """Status command definition should include display parameter."""
        from chuck_data.commands.status import DEFINITION

        # Should have display parameter defined
        assert "display" in DEFINITION.parameters
        display_param = DEFINITION.parameters["display"]
        assert display_param["type"] == "boolean"
        assert "user asks to see" in display_param["description"].lower()

    def test_status_command_description_includes_display_guidance(self):
        """Status command description should guide agent on when to use display=true."""
        from chuck_data.commands.status import DEFINITION

        description = DEFINITION.description.lower()
        assert (
            "display=true" in description
            or "display=true when user asks" in description
        )


class TestStatusCommandBehaviorConsistency:
    """Test that status command behavior is consistent and reliable."""

    def test_status_command_uses_conditional_display_based_on_user_intent(
        self, mock_chuck_service_init
    ):
        """Status command uses conditional display based on user intent."""
        from chuck_data.commands.status import DEFINITION

        # Command should be configured for conditional display
        assert DEFINITION.agent_display == "conditional"
        assert callable(DEFINITION.display_condition)

        # Should show full display when display=True (user wants to see status)
        status_data_with_display = {"workspace_url": "test", "display": True}
        assert DEFINITION.display_condition(status_data_with_display) is True

        # Should show condensed display when display=False (agent checking status)
        status_data_without_display = {"workspace_url": "test", "display": False}
        assert DEFINITION.display_condition(status_data_without_display) is False

        # Should default to condensed when no display parameter
        status_data_no_display = {"workspace_url": "test"}
        assert DEFINITION.display_condition(status_data_no_display) is False

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
