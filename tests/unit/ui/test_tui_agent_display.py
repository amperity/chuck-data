"""
Tests for TUI agent display infrastructure.

Following TDD approach to implement registry-based agent display handlers.
"""

import pytest
from unittest.mock import MagicMock, patch
from rich.console import Console
from rich.panel import Panel

from chuck_data.ui.tui import ChuckTUI
from chuck_data.command_registry import CommandDefinition


class SimpleMockCommandDef:
    """Helper for creating mock CommandDefinition objects in tests."""
    
    def __init__(self, agent_display="condensed", condensed_action=None, display_condition=None):
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
        assert hasattr(tui, 'agent_full_display_handlers')
        assert isinstance(tui.agent_full_display_handlers, dict)
        assert len(tui.agent_full_display_handlers) == 0  # Initially empty

    def test_tui_has_default_agent_full_display_handler(self, mock_chuck_service_init):
        """TUI should have a default agent full display handler."""
        tui = ChuckTUI()
        
        # Should have the default handler attribute
        assert hasattr(tui, 'default_agent_full_display_handler')
        assert callable(tui.default_agent_full_display_handler)

    @patch("chuck_data.ui.tui.get_command")
    def test_agent_tool_shows_condensed_output(self, mock_get_cmd, tui_with_mocked_console):
        """Agent tool with condensed display should show condensed output."""
        tui = tui_with_mocked_console
        mock_def = SimpleMockCommandDef(
            agent_display="condensed", 
            condensed_action="Testing Action"
        )
        mock_get_cmd.return_value = mock_def

        tui.display_tool_output("test-tool", {"success": True, "count": 5})

        # Should print condensed format
        tui.console.print.assert_called_once()
        call_args = tui.console.print.call_args[0][0]
        assert "→ Testing Action" in call_args
        assert "✓" in call_args  # Success indicator
        assert "5 items" in call_args  # Count metric

    @patch("chuck_data.ui.tui.get_command")
    def test_agent_tool_shows_nothing_for_none_display(self, mock_get_cmd, tui_with_mocked_console):
        """Agent tool with 'none' display should show nothing."""
        tui = tui_with_mocked_console
        mock_def = SimpleMockCommandDef(agent_display="none")
        mock_get_cmd.return_value = mock_def

        tui.display_tool_output("test-tool", {"success": True})

        # Should not print anything
        tui.console.print.assert_not_called()

    @patch("chuck_data.ui.tui.get_command")
    def test_agent_tool_shows_default_full_panel(self, mock_get_cmd, tui_with_mocked_console):
        """Agent tool with 'full' display should show default panel when no custom handler."""
        tui = tui_with_mocked_console
        mock_def = SimpleMockCommandDef(agent_display="full")
        mock_get_cmd.return_value = mock_def

        # Ensure no custom handler exists for this tool
        assert "test-tool-full" not in tui.agent_full_display_handlers

        tui.display_tool_output("test-tool-full", {"data_point": "value"})

        # Should print a Panel
        tui.console.print.assert_called_once()
        panel_arg = tui.console.print.call_args[0][0]
        assert isinstance(panel_arg, Panel)
        assert "Agent Tool Output: test-tool-full" in str(panel_arg.title)

    @patch("chuck_data.ui.tui.get_command")
    def test_agent_tool_uses_specific_full_handler(self, mock_get_cmd, tui_with_mocked_console):
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
    def test_agent_tool_conditional_display_routes_to_full(self, mock_get_cmd, tui_with_mocked_console):
        """Agent tool with conditional display should route to full when condition returns True."""
        tui = tui_with_mocked_console
        condition_to_full = MagicMock(return_value=True)
        mock_def = SimpleMockCommandDef(
            agent_display="conditional", 
            display_condition=condition_to_full
        )
        mock_get_cmd.return_value = mock_def

        tool_data = {"trigger": "full"}
        tui.display_tool_output("cond-tool", tool_data)

        # Should call condition function
        condition_to_full.assert_called_once_with(tool_data)
        
        # Should display full panel (default handler)
        tui.console.print.assert_called_once()
        panel_arg = tui.console.print.call_args[0][0]
        assert isinstance(panel_arg, Panel)

    @patch("chuck_data.ui.tui.get_command")
    def test_agent_tool_conditional_display_routes_to_condensed(self, mock_get_cmd, tui_with_mocked_console):
        """Agent tool with conditional display should route to condensed when condition returns False."""
        tui = tui_with_mocked_console
        condition_to_condensed = MagicMock(return_value=False)
        mock_def = SimpleMockCommandDef(
            agent_display="conditional",
            display_condition=condition_to_condensed,
            condensed_action="Conditional Action"
        )
        mock_get_cmd.return_value = mock_def

        tool_data = {"trigger": "condensed", "success": False}
        tui.display_tool_output("cond-tool", tool_data)

        # Should call condition function
        condition_to_condensed.assert_called_once_with(tool_data)
        
        # Should display condensed format
        tui.console.print.assert_called_once()
        call_args = tui.console.print.call_args[0][0]
        assert "→ Conditional Action" in call_args
        assert "✗" in call_args  # Failure indicator

    @patch("chuck_data.ui.tui.get_command")
    def test_agent_display_handles_missing_command_definition(self, mock_get_cmd, tui_with_mocked_console):
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

    def test_condensed_display_formats_success_with_metrics(self, tui_with_mocked_console):
        """Condensed display should format success indicators and metrics correctly."""
        tui = tui_with_mocked_console
        mock_command_def = SimpleMockCommandDef(condensed_action="Test Action")

        tui._display_condensed_tool_output(
            "test-tool", 
            {"success": True, "count": 10, "message": "Done"}, 
            mock_command_def
        )

        call_args = tui.console.print.call_args[0][0]
        assert "→ Test Action" in call_args
        assert "✓" in call_args
        assert "10 items" in call_args

    def test_condensed_display_formats_failure(self, tui_with_mocked_console):
        """Condensed display should format failure indicators correctly."""
        tui = tui_with_mocked_console
        mock_command_def = SimpleMockCommandDef(condensed_action="Failed Action")

        tui._display_condensed_tool_output(
            "test-tool", 
            {"success": False, "message": "Error occurred"}, 
            mock_command_def
        )

        call_args = tui.console.print.call_args[0][0]
        assert "→ Failed Action" in call_args
        assert "✗" in call_args
        assert "Error occurred" in call_args  # Should fallback to message

    def test_condensed_display_uses_tool_name_when_no_action(self, tui_with_mocked_console):
        """Condensed display should use tool name when no condensed_action provided."""
        tui = tui_with_mocked_console

        tui._display_condensed_tool_output(
            "fallback-tool", 
            {"success": True}, 
            None  # No command definition
        )

        call_args = tui.console.print.call_args[0][0]
        assert "→ fallback-tool" in call_args
        assert "✓" in call_args


class TestGenericAgentFullDisplay:
    """Test the generic agent full display handler."""

    def test_generic_handler_displays_json_panel(self, tui_with_mocked_console):
        """Generic agent full display should show formatted JSON in a panel."""
        tui = tui_with_mocked_console

        tool_data = {"key": "value", "nested": {"data": 123}}
        tui._display_generic_tool_panel_for_agent("generic-tool", tool_data)

        # Should print a Panel with JSON content
        tui.console.print.assert_called_once()
        panel_arg = tui.console.print.call_args[0][0]
        assert isinstance(panel_arg, Panel)
        assert "Agent Tool Output: generic-tool" in str(panel_arg.title)
        # Panel content should be JSON formatted
        panel_content = str(panel_arg.renderable)
        assert '"key": "value"' in panel_content
        assert '"nested"' in panel_content

    def test_generic_handler_handles_non_json_data(self, tui_with_mocked_console):
        """Generic handler should handle non-JSON-serializable data gracefully."""
        tui = tui_with_mocked_console

        # Non-serializable data
        tool_data = {"func": lambda x: x}  # Functions can't be JSON serialized
        tui._display_generic_tool_panel_for_agent("generic-tool", tool_data)

        # Should still print a Panel, but with string representation
        tui.console.print.assert_called_once()
        panel_arg = tui.console.print.call_args[0][0]
        assert isinstance(panel_arg, Panel)