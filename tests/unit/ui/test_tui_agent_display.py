"""
Tests for TUI agent display infrastructure.

Following TDD approach to implement registry-based agent display handlers.
"""

import pytest
from unittest.mock import MagicMock, patch
from rich.console import Console

from chuck_data.ui.tui import ChuckTUI


class SimpleMockCommandDef:
    """Helper for creating mock CommandDefinition objects in tests."""

    def __init__(
        self, agent_display="condensed", condensed_action=None, display_condition=None
    ):
        self.agent_display = agent_display
        self.condensed_action = condensed_action
        self.display_condition = display_condition


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


class TestAgentDisplayInfrastructure:
    """Test the basic agent display infrastructure."""

    def test_tui_has_agent_display_handlers_registry(self, mock_chuck_service_init):
        """TUI should have agent_full_display_handlers registry."""
        tui = ChuckTUI()

        # Should have the registry attribute
        assert hasattr(tui, "agent_full_display_handlers")
        assert isinstance(tui.agent_full_display_handlers, dict)
        # Should have status handler pre-registered
        assert "status" in tui.agent_full_display_handlers

    def test_tui_defaults_to_condensed_for_unknown_tools(self, mock_chuck_service_init):
        """TUI should default to condensed display for unknown tools (no generic full handler)."""
        tui = ChuckTUI()

        # Should still have the registry for specific handlers
        assert hasattr(tui, "agent_full_display_handlers")
        # Should NOT have a generic full display handler
        assert not hasattr(tui, "default_agent_full_display_handler")

    @patch("chuck_data.ui.tui.get_command")
    def test_agent_tool_shows_condensed_output(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Agent tool with condensed display should show condensed output."""
        tui = tui_with_mocked_console
        mock_def = SimpleMockCommandDef(
            agent_display="condensed", condensed_action="Testing Action"
        )
        mock_get_cmd.return_value = mock_def

        tui.display_tool_output("test-tool", {"success": True, "count": 5})

        # Should print condensed format
        tui.console.print.assert_called_once()
        call_args = tui.console.print.call_args[0][0]
        assert "Testing Action" in call_args
        assert "✓" in call_args  # Success indicator
        assert "5 items" in call_args  # Count metric

    @patch("chuck_data.ui.tui.get_command")
    def test_agent_tool_shows_nothing_for_none_display(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Agent tool with 'none' display should show nothing."""
        tui = tui_with_mocked_console
        mock_def = SimpleMockCommandDef(agent_display="none")
        mock_get_cmd.return_value = mock_def

        tui.display_tool_output("test-tool", {"success": True})

        # Should not print anything
        tui.console.print.assert_not_called()

    @patch("chuck_data.ui.tui.get_command")
    def test_agent_tool_falls_back_to_condensed_when_no_custom_handler(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Agent tool with 'full' display should fall back to condensed when no custom handler."""
        tui = tui_with_mocked_console
        mock_def = SimpleMockCommandDef(agent_display="full")
        mock_get_cmd.return_value = mock_def

        # Ensure no custom handler exists for this tool
        assert "test-tool-full" not in tui.agent_full_display_handlers

        tui.display_tool_output("test-tool-full", {"success": True})

        # Should fall back to condensed format instead of generic panel
        tui.console.print.assert_called_once()
        call_args = tui.console.print.call_args[0][0]
        assert "test-tool-full" in call_args
        assert "✓" in call_args

    @patch("chuck_data.ui.tui.get_command")
    def test_agent_tool_uses_specific_full_handler(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Agent tool with 'full' display should use specific registered handler."""
        tui = tui_with_mocked_console
        mock_specific_handler = MagicMock()
        tui.agent_full_display_handlers["test-tool-specific"] = mock_specific_handler

        mock_def = SimpleMockCommandDef(agent_display="full")
        mock_get_cmd.return_value = mock_def

        tool_data = {"info": "specific data"}
        tui.display_tool_output("test-tool-specific", tool_data)

        # Should call the specific handler, not the default
        mock_specific_handler.assert_called_once_with("test-tool-specific", tool_data)
        # Default handler (console.print) should not be called by the routing logic
        # (though the specific handler might call it)

    @patch("chuck_data.ui.tui.get_command")
    def test_agent_tool_conditional_display_routes_to_custom_handler_when_true(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Agent tool with conditional display should route to custom handler when condition returns True."""
        tui = tui_with_mocked_console
        condition_to_full = MagicMock(return_value=True)
        mock_def = SimpleMockCommandDef(
            agent_display="conditional", display_condition=condition_to_full
        )
        mock_get_cmd.return_value = mock_def

        # Register a custom handler for this tool
        mock_custom_handler = MagicMock()
        tui.agent_full_display_handlers["cond-tool"] = mock_custom_handler

        tool_data = {"trigger": "full"}
        tui.display_tool_output("cond-tool", tool_data)

        # Should call condition function
        condition_to_full.assert_called_once_with(tool_data)

        # Should call custom handler
        mock_custom_handler.assert_called_once_with("cond-tool", tool_data)

    @patch("chuck_data.ui.tui.get_command")
    def test_agent_tool_conditional_display_fallback_to_condensed_when_no_custom_handler(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Agent tool with conditional display should fallback to condensed when condition returns True but no custom handler."""
        tui = tui_with_mocked_console
        condition_to_full = MagicMock(return_value=True)
        mock_def = SimpleMockCommandDef(
            agent_display="conditional", display_condition=condition_to_full
        )
        mock_get_cmd.return_value = mock_def

        # No custom handler registered for this tool
        assert "cond-tool-no-handler" not in tui.agent_full_display_handlers

        tool_data = {"trigger": "full", "success": True}
        tui.display_tool_output("cond-tool-no-handler", tool_data)

        # Should call condition function
        condition_to_full.assert_called_once_with(tool_data)

        # Should fallback to condensed display instead of generic panel
        tui.console.print.assert_called_once()
        call_args = tui.console.print.call_args[0][0]
        assert "cond-tool-no-handler" in call_args
        assert "✓" in call_args

    @patch("chuck_data.ui.tui.get_command")
    def test_agent_tool_conditional_display_routes_to_condensed(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Agent tool with conditional display should route to condensed when condition returns False."""
        tui = tui_with_mocked_console
        condition_to_condensed = MagicMock(return_value=False)
        mock_def = SimpleMockCommandDef(
            agent_display="conditional",
            display_condition=condition_to_condensed,
            condensed_action="Conditional Action",
        )
        mock_get_cmd.return_value = mock_def

        tool_data = {"trigger": "condensed", "success": False}
        tui.display_tool_output("cond-tool", tool_data)

        # Should call condition function
        condition_to_condensed.assert_called_once_with(tool_data)

        # Should display condensed format
        tui.console.print.assert_called_once()
        call_args = tui.console.print.call_args[0][0]
        assert "Conditional Action" in call_args
        assert "✗" in call_args  # Failure indicator

    @patch("chuck_data.ui.tui.get_command")
    def test_agent_display_handles_missing_command_definition(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Agent display should handle gracefully when command definition is not found."""
        tui = tui_with_mocked_console
        mock_get_cmd.return_value = None  # Command not found

        # Should not raise exception
        tui.display_tool_output("unknown-tool", {"data": "test"})

        # Should fallback to some reasonable behavior (condensed with tool name)
        tui.console.print.assert_called_once()
        call_args = tui.console.print.call_args[0][0]
        assert "unknown-tool" in call_args


class TestAgentDisplayCondensedFormat:
    """Test the condensed display formatting logic."""

    def test_condensed_display_formats_success_with_metrics(
        self, tui_with_mocked_console
    ):
        """Condensed display should format success indicators and metrics correctly."""
        tui = tui_with_mocked_console

        tui._display_condensed_tool_output(
            "test-tool",
            {"success": True, "count": 10, "message": "Done"},
        )

        call_args = tui.console.print.call_args[0][0]
        assert "test-tool" in call_args  # Should show tool name
        assert "✓" in call_args
        assert "10 items" in call_args

    def test_condensed_display_uses_friendly_name_from_real_command(
        self, tui_with_mocked_console
    ):
        """Condensed display should use condensed_action from real command definitions."""
        tui = tui_with_mocked_console

        # Use a real command that has condensed_action defined
        tui._display_condensed_tool_output(
            "list-catalogs",  # This command has condensed_action="Listing catalogs"
            {"success": True, "count": 5},
        )

        call_args = tui.console.print.call_args[0][0]
        # Should show friendly name from condensed_action, not just "list-catalogs"
        assert "Listing catalogs" in call_args
        assert "✓" in call_args
        assert "5 items" in call_args

    def test_condensed_display_formats_failure(self, tui_with_mocked_console):
        """Condensed display should format failure indicators correctly."""
        tui = tui_with_mocked_console

        tui._display_condensed_tool_output(
            "test-tool",
            {"success": False, "message": "Error occurred"},
        )

        call_args = tui.console.print.call_args[0][0]
        assert "test-tool" in call_args  # Should show tool name
        assert "✗" in call_args
        assert "Error occurred" in call_args  # Should fallback to message

    @patch("chuck_data.ui.tui.get_command")
    def test_condensed_display_uses_tool_name_when_no_action(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Condensed display should use tool name when no condensed_action provided."""
        tui = tui_with_mocked_console
        mock_get_cmd.return_value = None  # No command definition

        tui._display_condensed_tool_output("fallback-tool", {"success": True})

        call_args = tui.console.print.call_args[0][0]
        assert "fallback-tool" in call_args
        assert "✓" in call_args
