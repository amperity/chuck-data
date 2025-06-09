"""
Tests for display error handling in TUI.

These tests ensure that display errors are handled gracefully and don't break
agent execution or user experience.
"""

import pytest
import logging
from unittest.mock import MagicMock, patch
from rich.console import Console

from chuck_data.ui.tui import ChuckTUI
from chuck_data.exceptions import PaginationCancelled


class MockCommandDef:
    """Helper for creating mock CommandDefinition objects."""
    
    def __init__(self, agent_display="condensed", display_condition=None):
        self.agent_display = agent_display
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


class TestDisplayExceptionHandling:
    """Test exception handling in display methods."""

    @patch("chuck_data.ui.tui.get_command")
    def test_pagination_cancelled_exception_bubbles_up(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """PaginationCancelled exceptions should bubble up and not be caught."""
        tui = tui_with_mocked_console
        mock_get_cmd.return_value = MockCommandDef(agent_display="condensed")
        
        # Mock the condensed display method to raise PaginationCancelled
        with patch.object(tui, '_display_condensed_tool_output') as mock_condensed:
            mock_condensed.side_effect = PaginationCancelled()
            
            # Should re-raise PaginationCancelled, not catch it
            with pytest.raises(PaginationCancelled):
                tui.display_tool_output("test-tool", {"data": "test"})
            
            mock_condensed.assert_called_once()

    @patch("chuck_data.ui.tui.get_command")
    def test_full_display_pagination_cancelled_bubbles_up(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """PaginationCancelled from full display methods should bubble up."""
        tui = tui_with_mocked_console
        mock_get_cmd.return_value = MockCommandDef(agent_display="full")
        
        # Mock the full display method to raise PaginationCancelled
        with patch.object(tui, '_display_full_tool_output') as mock_full:
            mock_full.side_effect = PaginationCancelled()
            
            # Should re-raise PaginationCancelled
            with pytest.raises(PaginationCancelled):
                tui.display_tool_output("test-tool", {"display": True})
            
            mock_full.assert_called_once()

    @patch("chuck_data.ui.tui.get_command")
    def test_custom_handler_pagination_cancelled_bubbles_up(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """PaginationCancelled from custom handlers should bubble up."""
        tui = tui_with_mocked_console
        mock_get_cmd.return_value = MockCommandDef(agent_display="full")
        
        # Register custom handler that raises PaginationCancelled
        mock_handler = MagicMock(side_effect=PaginationCancelled())
        tui.agent_full_display_handlers["test-tool"] = mock_handler
        
        # Should re-raise PaginationCancelled
        with pytest.raises(PaginationCancelled):
            tui.display_tool_output("test-tool", {"data": "test"})
        
        mock_handler.assert_called_once()

    @patch("chuck_data.ui.tui.get_command")
    @patch("chuck_data.ui.tui.logging.warning")
    def test_generic_exception_logged_and_contained(
        self, mock_log_warning, mock_get_cmd, tui_with_mocked_console
    ):
        """Generic exceptions should be logged and contained, not break execution."""
        tui = tui_with_mocked_console
        mock_get_cmd.return_value = MockCommandDef(agent_display="condensed")
        
        # Mock the condensed display method to raise a generic exception
        test_exception = Exception("Display method failed")
        with patch.object(tui, '_display_condensed_tool_output') as mock_condensed:
            mock_condensed.side_effect = test_exception
            
            # Should not raise exception - should handle gracefully
            tui.display_tool_output("test-tool", {"data": "test"})
            
            # Should log the warning
            mock_log_warning.assert_called_once()
            log_call_args = mock_log_warning.call_args[0][0]
            assert "Failed to display tool output for test-tool" in log_call_args
            assert "Display method failed" in log_call_args
            
            # Should print fallback notification
            tui.console.print.assert_called_with("[dim][Tool: test-tool executed][/dim]")

    @patch("chuck_data.ui.tui.get_command")
    @patch("chuck_data.ui.tui.logging.warning")
    def test_full_display_exception_logged_and_contained(
        self, mock_log_warning, mock_get_cmd, tui_with_mocked_console
    ):
        """Full display exceptions should be logged and contained."""
        tui = tui_with_mocked_console
        mock_get_cmd.return_value = MockCommandDef(agent_display="full")
        
        # Mock the full display method to raise a generic exception
        test_exception = RuntimeError("Full display failed")
        with patch.object(tui, '_display_full_tool_output') as mock_full:
            mock_full.side_effect = test_exception
            
            # Should not raise exception
            tui.display_tool_output("test-tool", {"display": True})
            
            # Should log the warning
            mock_log_warning.assert_called_once()
            log_call_args = mock_log_warning.call_args[0][0]
            assert "Failed to display tool output for test-tool" in log_call_args
            assert "Full display failed" in log_call_args
            
            # Should print fallback notification
            tui.console.print.assert_called_with("[dim][Tool: test-tool executed][/dim]")

    @patch("chuck_data.ui.tui.get_command")
    @patch("chuck_data.ui.tui.logging.warning")
    def test_custom_handler_exception_logged_and_contained(
        self, mock_log_warning, mock_get_cmd, tui_with_mocked_console
    ):
        """Custom handler exceptions should be logged and contained."""
        tui = tui_with_mocked_console
        mock_get_cmd.return_value = MockCommandDef(agent_display="full")
        
        # Register custom handler that raises exception
        test_exception = ValueError("Custom handler failed")
        mock_handler = MagicMock(side_effect=test_exception)
        tui.agent_full_display_handlers["test-tool"] = mock_handler
        
        # Should not raise exception
        tui.display_tool_output("test-tool", {"data": "test"})
        
        # Should log the warning
        mock_log_warning.assert_called_once()
        log_call_args = mock_log_warning.call_args[0][0]
        assert "Failed to display tool output for test-tool" in log_call_args
        assert "Custom handler failed" in log_call_args
        
        # Should print fallback notification
        tui.console.print.assert_called_with("[dim][Tool: test-tool executed][/dim]")

    @patch("chuck_data.ui.tui.get_command")
    @patch("chuck_data.ui.tui.logging.warning")
    def test_display_condition_exception_handled(
        self, mock_log_warning, mock_get_cmd, tui_with_mocked_console
    ):
        """Exceptions in display_condition functions should be handled gracefully."""
        tui = tui_with_mocked_console
        
        # Create condition function that raises exception
        def failing_condition(result):
            raise ValueError("Condition evaluation failed")
        
        mock_get_cmd.return_value = MockCommandDef(
            agent_display="conditional",
            display_condition=failing_condition
        )
        
        with patch.object(tui, '_display_condensed_tool_output') as mock_condensed:
            # Should not raise exception - should handle gracefully
            tui.display_tool_output("test-tool", {"data": "test"})
            
            # Should log the warning about the failure
            mock_log_warning.assert_called_once()
            
            # Should print fallback notification instead of trying to display
            tui.console.print.assert_called_with("[dim][Tool: test-tool executed][/dim]")
            
            # Condensed display should not be called due to the condition failure
            mock_condensed.assert_not_called()


class TestDisplayMethodErrorScenarios:
    """Test error scenarios in specific display methods."""

    @pytest.fixture
    def tui_with_failing_display_methods(self, mock_chuck_service_init):
        """Create TUI with display methods that can be configured to fail."""
        tui = ChuckTUI()
        tui.console = MagicMock(spec=Console)
        return tui

    def test_display_catalogs_exception_handling(self, tui_with_failing_display_methods):
        """Test that _display_catalogs exceptions are handled by parent display_tool_output."""
        tui = tui_with_failing_display_methods
        
        # Test through normal display_tool_output flow
        with patch("chuck_data.ui.tui.get_command") as mock_get_cmd:
            mock_get_cmd.return_value = MockCommandDef(agent_display="full")
            
            with patch.object(tui, '_display_catalogs') as mock_display:
                mock_display.side_effect = Exception("Catalog display failed")
                
                with patch("chuck_data.ui.tui.logging.warning") as mock_log:
                    # Should not raise - should be caught by display_tool_output
                    tui.display_tool_output("list-catalogs", {"display": True, "catalogs": []})
                    
                    # Should log the error
                    mock_log.assert_called()
                    
                    # Should print fallback notification
                    tui.console.print.assert_called_with("[dim][Tool: list-catalogs executed][/dim]")

    def test_display_status_for_agent_exception_handling(self, tui_with_failing_display_methods):
        """Test that custom agent handler exceptions are handled."""
        tui = tui_with_failing_display_methods
        
        # Register failing status handler
        def failing_status_handler(tool_name, tool_data):
            raise Exception("Status display failed")
        
        tui.agent_full_display_handlers["status"] = failing_status_handler
        
        with patch("chuck_data.ui.tui.get_command") as mock_get_cmd:
            mock_get_cmd.return_value = MockCommandDef(agent_display="full")
            
            with patch("chuck_data.ui.tui.logging.warning") as mock_log:
                # Should not raise - should be caught
                tui.display_tool_output("status", {"workspace_url": "test"})
                
                # Should log the error
                mock_log.assert_called()
                
                # Should print fallback notification
                tui.console.print.assert_called_with("[dim][Tool: status executed][/dim]")


class TestInputValidationAndEdgeCases:
    """Test edge cases and input validation in display methods."""

    @patch("chuck_data.ui.tui.get_command")
    def test_none_tool_name_handled_gracefully(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Test that None tool_name is handled gracefully."""
        tui = tui_with_mocked_console
        mock_get_cmd.return_value = MockCommandDef(agent_display="condensed")
        
        with patch.object(tui, '_display_condensed_tool_output') as mock_condensed:
            # Should not crash with None tool_name
            tui.display_tool_output(None, {"data": "test"})
            
            mock_condensed.assert_called_once_with(None, {"data": "test"})

    @patch("chuck_data.ui.tui.get_command")
    def test_none_tool_result_handled_gracefully(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Test that None tool_result is handled gracefully."""
        tui = tui_with_mocked_console
        mock_get_cmd.return_value = MockCommandDef(agent_display="condensed")
        
        with patch.object(tui, '_display_condensed_tool_output') as mock_condensed:
            # Should not crash with None tool_result
            tui.display_tool_output("test-tool", None)
            
            mock_condensed.assert_called_once_with("test-tool", None)

    @patch("chuck_data.ui.tui.get_command")
    def test_get_command_exception_handled(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Test that get_command exceptions are handled gracefully."""
        tui = tui_with_mocked_console
        mock_get_cmd.side_effect = Exception("Command registry failed")
        
        with patch.object(tui, '_display_condensed_tool_output') as mock_condensed:
            with patch("chuck_data.ui.tui.logging.warning") as mock_log:
                # Should not crash when get_command fails
                tui.display_tool_output("test-tool", {"data": "test"})
                
                # Should log the error
                mock_log.assert_called()
                
                # Should print fallback notification
                tui.console.print.assert_called_with("[dim][Tool: test-tool executed][/dim]")

    def test_console_none_causes_attribute_error(self, mock_chuck_service_init):
        """Test that missing console causes AttributeError (current behavior)."""
        tui = ChuckTUI()
        tui.console = None  # Simulate missing console
        
        with patch("chuck_data.ui.tui.get_command") as mock_get_cmd:
            mock_get_cmd.return_value = MockCommandDef(agent_display="condensed")
            
            # Current implementation doesn't handle None console gracefully
            # This documents the current behavior - could be improved in future
            with pytest.raises(AttributeError, match="'NoneType' object has no attribute 'print'"):
                tui.display_tool_output("test-tool", {"data": "test"})

    @patch("chuck_data.ui.tui.get_command")
    def test_complex_nested_exception_scenarios(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Test complex nested exception scenarios."""
        tui = tui_with_mocked_console
        
        # Create a condition function that itself has errors
        def problematic_condition(result):
            # This will fail when result is not a dict or doesn't have the key
            return result["missing_key"]  # KeyError
        
        mock_get_cmd.return_value = MockCommandDef(
            agent_display="conditional",
            display_condition=problematic_condition
        )
        
        with patch("chuck_data.ui.tui.logging.warning") as mock_log:
            # Should handle the KeyError in condition gracefully
            tui.display_tool_output("test-tool", {"data": "test"})  # No "missing_key"
            
            # Should log the error
            mock_log.assert_called()
            
            # Should print fallback notification
            tui.console.print.assert_called_with("[dim][Tool: test-tool executed][/dim]")


class TestErrorRecoveryAndFallbacks:
    """Test error recovery and fallback behavior."""

    @patch("chuck_data.ui.tui.get_command")
    def test_display_errors_dont_affect_subsequent_calls(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Test that display errors don't affect subsequent display calls."""
        tui = tui_with_mocked_console
        mock_get_cmd.return_value = MockCommandDef(agent_display="condensed")
        
        # First call fails
        with patch.object(tui, '_display_condensed_tool_output') as mock_condensed:
            mock_condensed.side_effect = [Exception("First call failed"), None]
            
            with patch("chuck_data.ui.tui.logging.warning"):
                # First call should fail gracefully
                tui.display_tool_output("test-tool1", {"data": "test1"})
                
                # Second call should work normally
                tui.display_tool_output("test-tool2", {"data": "test2"})
                
                # Both calls should have been attempted
                assert mock_condensed.call_count == 2

    @patch("chuck_data.ui.tui.get_command")
    def test_partial_display_data_handled(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Test that partial or malformed display data is handled gracefully."""
        tui = tui_with_mocked_console
        mock_get_cmd.return_value = MockCommandDef(agent_display="full")
        
        # Test with various malformed data structures
        malformed_data_cases = [
            {"display": True},  # Missing expected data fields
            {"display": True, "catalogs": "not-a-list"},  # Wrong data type
            {"display": True, "schemas": None},  # Null data
            {"display": True, "tables": []},  # Empty list (should be OK)
        ]
        
        with patch.object(tui, '_display_full_tool_output') as mock_full:
            # Configure to handle all calls without error
            mock_full.side_effect = None
            
            for malformed_data in malformed_data_cases:
                # Should handle each case without crashing
                tui.display_tool_output("list-catalogs", malformed_data)
            
            # All calls should have been made
            assert mock_full.call_count == len(malformed_data_cases)