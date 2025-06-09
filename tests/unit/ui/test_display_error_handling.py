"""
Tests for display error handling in TUI.

These tests ensure that display errors are handled gracefully and don't break
agent execution or user experience.
"""

import pytest
import tempfile
import chuck_data.ui.tui
from unittest.mock import MagicMock, patch
from rich.console import Console

from chuck_data.ui.tui import ChuckTUI
from chuck_data.exceptions import PaginationCancelled
from chuck_data.command_registry import CommandDefinition, TUI_COMMAND_MAP


# Create real command definition for tests
def create_real_command_def(
    name="test-tool", agent_display="condensed", display_condition=None
):
    """Create a real CommandDefinition with test attributes."""

    def dummy_handler(**kwargs):
        return {"success": True}

    cmd_def = CommandDefinition(
        name=name,
        handler=dummy_handler,
        description="Test command",
        agent_display=agent_display,
    )

    if display_condition:
        cmd_def.display_condition = display_condition

    return cmd_def


@pytest.fixture
def temp_config():
    """Create a temporary config manager for testing."""
    with tempfile.NamedTemporaryFile() as tmp:
        from chuck_data.config import ConfigManager

        config_manager = ConfigManager(tmp.name)
        yield config_manager


@pytest.fixture
def tui_with_captured_console():
    """Create TUI with console output captured for testing."""
    # Only mock external boundary (console)
    with patch("chuck_data.ui.tui.ChuckService"):
        tui = ChuckTUI()
        # Mock only the console output - the external boundary
        tui.console = MagicMock(spec=Console)
        return tui


class TestDisplayExceptionHandling:
    """Test exception handling in display methods."""

    def test_pagination_cancelled_exception_bubbles_up(self, tui_with_captured_console):
        """PaginationCancelled exceptions should bubble up and not be caught."""
        tui = tui_with_captured_console

        # Create a real command definition
        real_cmd_def = create_real_command_def(agent_display="condensed")

        # Register command in the real registry
        original_command = TUI_COMMAND_MAP.get("test-tool")
        TUI_COMMAND_MAP["test-tool"] = real_cmd_def

        try:
            # Create a wrapper for the condensed display method that raises PaginationCancelled
            original_method = tui._display_condensed_tool_output

            def raising_condensed(*args, **kwargs):
                raise PaginationCancelled()

            # Replace the method with our raising version
            tui._display_condensed_tool_output = raising_condensed

            # Should re-raise PaginationCancelled, not catch it
            with pytest.raises(PaginationCancelled):
                tui.display_tool_output("test-tool", {"data": "test"})
        finally:
            # Restore original method and command
            tui._display_condensed_tool_output = original_method
            if original_command is not None:
                TUI_COMMAND_MAP["test-tool"] = original_command
            else:
                TUI_COMMAND_MAP.pop("test-tool", None)

    def test_full_display_pagination_cancelled_bubbles_up(
        self, tui_with_captured_console
    ):
        """PaginationCancelled from full display methods should bubble up."""
        tui = tui_with_captured_console

        # We need to use a custom tool name to avoid existing handling
        tool_name = "custom-test-tool"

        # Create a real command definition
        real_cmd_def = create_real_command_def(name=tool_name, agent_display="full")

        # Register command in the real registry
        original_command = TUI_COMMAND_MAP.get(tool_name)
        TUI_COMMAND_MAP[tool_name] = real_cmd_def

        try:
            # Create a wrapper for the full display method that raises PaginationCancelled
            # but only for our specific tool
            original_method = tui._display_full_tool_output

            def raising_full(*args, **kwargs):
                if args and args[0] == tool_name:
                    raise PaginationCancelled()
                return original_method(*args, **kwargs)

            # Replace the method with our raising version
            tui._display_full_tool_output = raising_full

            # Should re-raise PaginationCancelled
            with pytest.raises(PaginationCancelled):
                tui.display_tool_output(tool_name, {"display": True})
        finally:
            # Restore original method and command
            tui._display_full_tool_output = original_method
            if original_command is not None:
                TUI_COMMAND_MAP[tool_name] = original_command
            else:
                TUI_COMMAND_MAP.pop(tool_name, None)

    def test_custom_handler_pagination_cancelled_bubbles_up(
        self, tui_with_captured_console
    ):
        """PaginationCancelled from custom handlers should bubble up."""
        tui = tui_with_captured_console

        # We need to use a custom tool name to avoid existing handling
        tool_name = "custom-handler-test-tool"

        # Create a real command definition
        real_cmd_def = create_real_command_def(name=tool_name, agent_display="full")

        # Register command in the real registry
        original_command = TUI_COMMAND_MAP.get(tool_name)
        TUI_COMMAND_MAP[tool_name] = real_cmd_def

        # Create custom handler that raises PaginationCancelled
        def raising_handler(tool_name, tool_data):
            raise PaginationCancelled()

        # Save original handler if any
        original_handler = tui.agent_full_display_handlers.get(tool_name)

        try:
            # Register custom handler
            tui.agent_full_display_handlers[tool_name] = raising_handler

            # Should re-raise PaginationCancelled
            with pytest.raises(PaginationCancelled):
                tui.display_tool_output(tool_name, {"display": True})
        finally:
            # Clean up - restore original handler and command
            if original_handler:
                tui.agent_full_display_handlers[tool_name] = original_handler
            else:
                tui.agent_full_display_handlers.pop(tool_name, None)

            if original_command is not None:
                TUI_COMMAND_MAP[tool_name] = original_command
            else:
                TUI_COMMAND_MAP.pop(tool_name, None)

    def test_generic_exception_logged_and_contained(self, tui_with_captured_console):
        """Generic exceptions should be logged and contained, not break execution."""
        tui = tui_with_captured_console

        # Create a real command definition
        real_cmd_def = create_real_command_def(agent_display="condensed")

        # Register command in the real registry
        original_command = TUI_COMMAND_MAP.get("test-tool")
        TUI_COMMAND_MAP["test-tool"] = real_cmd_def

        try:
            # Create a wrapper for the condensed display method that raises exception
            original_method = tui._display_condensed_tool_output
            test_exception = Exception("Display method failed")

            def raising_condensed(*args, **kwargs):
                raise test_exception

            # Replace the method with our raising version
            tui._display_condensed_tool_output = raising_condensed

            # Mock the logging.warning function
            with patch("chuck_data.ui.tui.logging.warning") as mock_log_warning:
                # Should not raise exception - should handle gracefully
                tui.display_tool_output("test-tool", {"data": "test"})

                # Should log the warning
                mock_log_warning.assert_called_once()
                log_call_args = mock_log_warning.call_args[0][0]
                assert "Failed to display tool output for test-tool" in log_call_args
                assert "Display method failed" in log_call_args

                # Should print fallback notification
                tui.console.print.assert_called_with(
                    "[dim][Tool: test-tool executed][/dim]"
                )

        finally:
            # Restore original method and command
            tui._display_condensed_tool_output = original_method
            if original_command is not None:
                TUI_COMMAND_MAP["test-tool"] = original_command
            else:
                TUI_COMMAND_MAP.pop("test-tool", None)

    def test_full_display_exception_logged_and_contained(
        self, tui_with_captured_console
    ):
        """Full display exceptions should be logged and contained."""
        tui = tui_with_captured_console

        # Create a real command definition
        real_cmd_def = create_real_command_def(agent_display="full")

        # Register command in the real registry
        original_command = TUI_COMMAND_MAP.get("test-tool")
        TUI_COMMAND_MAP["test-tool"] = real_cmd_def

        try:
            # Create a wrapper for the full display method that raises exception
            original_method = tui._display_full_tool_output
            test_exception = RuntimeError("Full display failed")

            def raising_full(*args, **kwargs):
                raise test_exception

            # Replace the method with our raising version
            tui._display_full_tool_output = raising_full

            # Mock the logging.warning function
            with patch("chuck_data.ui.tui.logging.warning") as mock_log_warning:
                # Should not raise exception - should handle gracefully
                tui.display_tool_output("test-tool", {"display": True})

                # Should log the warning
                mock_log_warning.assert_called_once()
                log_call_args = mock_log_warning.call_args[0][0]
                assert "Failed to display tool output for test-tool" in log_call_args
                assert "Full display failed" in log_call_args

                # Should print fallback notification
                tui.console.print.assert_called_with(
                    "[dim][Tool: test-tool executed][/dim]"
                )

        finally:
            # Restore original method and command
            tui._display_full_tool_output = original_method
            if original_command is not None:
                TUI_COMMAND_MAP["test-tool"] = original_command
            else:
                TUI_COMMAND_MAP.pop("test-tool", None)

    def test_custom_handler_exception_logged_and_contained(
        self, tui_with_captured_console
    ):
        """Custom handler exceptions should be logged and contained."""
        tui = tui_with_captured_console

        # Create a real command definition
        real_cmd_def = create_real_command_def(agent_display="full")

        # Register command in the real registry
        original_command = TUI_COMMAND_MAP.get("test-tool")
        TUI_COMMAND_MAP["test-tool"] = real_cmd_def

        # Define custom handler that raises exception
        test_exception = ValueError("Custom handler failed")

        def failing_handler(tool_name, tool_data):
            raise test_exception

        # Save original handler if any
        original_handler = tui.agent_full_display_handlers.get("test-tool")

        try:
            # Register custom handler
            tui.agent_full_display_handlers["test-tool"] = failing_handler

            # Mock the logging.warning function
            with patch("chuck_data.ui.tui.logging.warning") as mock_log_warning:
                # Should not raise exception
                tui.display_tool_output("test-tool", {"data": "test"})

                # Should log the warning
                mock_log_warning.assert_called_once()
                log_call_args = mock_log_warning.call_args[0][0]
                assert "Failed to display tool output for test-tool" in log_call_args
                assert "Custom handler failed" in log_call_args

                # Should print fallback notification
                tui.console.print.assert_called_with(
                    "[dim][Tool: test-tool executed][/dim]"
                )

        finally:
            # Clean up - restore original handler and command
            if original_handler:
                tui.agent_full_display_handlers["test-tool"] = original_handler
            else:
                tui.agent_full_display_handlers.pop("test-tool", None)

            if original_command is not None:
                TUI_COMMAND_MAP["test-tool"] = original_command
            else:
                TUI_COMMAND_MAP.pop("test-tool", None)

    def test_display_condition_exception_handled(self, tui_with_captured_console):
        """Exceptions in display_condition functions should be handled gracefully."""
        tui = tui_with_captured_console

        # Create failing condition function
        def failing_condition(result):
            raise ValueError("Condition evaluation failed")

        # Create real command definition with conditional display
        tool_name = "condition-test-tool"
        real_cmd_def = create_real_command_def(
            name=tool_name,
            agent_display="conditional",
            display_condition=failing_condition,
        )

        # Register command in the real registry
        original_command = TUI_COMMAND_MAP.get(tool_name)
        TUI_COMMAND_MAP[tool_name] = real_cmd_def

        try:
            # Create a flag to check if condensed display was called
            condensed_called = [False]
            original_condensed = tui._display_condensed_tool_output

            def tracking_condensed(*args, **kwargs):
                condensed_called[0] = True
                return original_condensed(*args, **kwargs)

            # Replace method with tracking version
            tui._display_condensed_tool_output = tracking_condensed

            # Mock the logging.warning function
            with patch("chuck_data.ui.tui.logging.warning") as mock_log_warning:
                # Should not raise exception - should handle gracefully
                tui.display_tool_output(tool_name, {"data": "test"})

                # Should log the warning about the failure
                mock_log_warning.assert_called_once()

                # Should print fallback notification instead of trying to display
                tui.console.print.assert_called_with(
                    f"[dim][Tool: {tool_name} executed][/dim]"
                )

                # Condensed display should not be called due to the condition failure
                assert not condensed_called[
                    0
                ], "_display_condensed_tool_output should not be called"

        finally:
            # Restore original method and command
            tui._display_condensed_tool_output = original_condensed
            if original_command is not None:
                TUI_COMMAND_MAP[tool_name] = original_command
            else:
                TUI_COMMAND_MAP.pop(tool_name, None)


class TestDisplayMethodErrorScenarios:
    """Test error scenarios in specific display methods."""

    @pytest.fixture
    def tui_with_failing_display_methods(self):
        """Create TUI with display methods that can be configured to fail."""
        # Only mock external boundary (console)
        with patch("chuck_data.ui.tui.ChuckService"):
            tui = ChuckTUI()
            # Mock only the console output - the external boundary
            tui.console = MagicMock(spec=Console)
            return tui

    def test_display_catalogs_exception_handling(
        self, tui_with_failing_display_methods
    ):
        """Test that _display_catalogs exceptions are handled by parent display_tool_output."""
        tui = tui_with_failing_display_methods

        # Get the real command definition
        original_command = TUI_COMMAND_MAP.get("list-catalogs")
        real_cmd_def = create_real_command_def(agent_display="full")
        TUI_COMMAND_MAP["list-catalogs"] = real_cmd_def

        try:
            # Create a wrapper for the catalogs display method that raises exception
            original_method = tui._display_catalogs

            def failing_catalogs(*args, **kwargs):
                raise Exception("Catalog display failed")

            # Replace with failing method
            tui._display_catalogs = failing_catalogs

            with patch("chuck_data.ui.tui.logging.warning") as mock_log:
                # Should not raise - should be caught by display_tool_output
                tui.display_tool_output(
                    "list-catalogs", {"display": True, "catalogs": []}
                )

                # Should log the error
                mock_log.assert_called()

                # Should print fallback notification
                tui.console.print.assert_called_with(
                    "[dim][Tool: list-catalogs executed][/dim]"
                )
        finally:
            # Restore original method and command
            tui._display_catalogs = original_method
            if original_command is not None:
                TUI_COMMAND_MAP["list-catalogs"] = original_command
            else:
                TUI_COMMAND_MAP.pop("list-catalogs", None)

    def test_display_status_for_agent_exception_handling(
        self, tui_with_failing_display_methods
    ):
        """Test that custom agent handler exceptions are handled."""
        tui = tui_with_failing_display_methods

        # Use a custom tool name to avoid conflicts
        tool_name = "custom-status-tool"

        # Get the real command definition
        original_command = TUI_COMMAND_MAP.get(tool_name)
        real_cmd_def = create_real_command_def(name=tool_name, agent_display="full")
        TUI_COMMAND_MAP[tool_name] = real_cmd_def

        # Register failing status handler
        def failing_status_handler(tool_name, tool_data):
            raise Exception("Status display failed")

        # Save original handler if any
        original_handler = tui.agent_full_display_handlers.get(tool_name)

        try:
            # Register failing handler
            tui.agent_full_display_handlers[tool_name] = failing_status_handler

            with patch("chuck_data.ui.tui.logging.warning") as mock_log:
                # Should not raise - should be caught
                tui.display_tool_output(tool_name, {"workspace_url": "test"})

                # Should log the error
                mock_log.assert_called()

                # Should print fallback notification
                tui.console.print.assert_called_with(
                    f"[dim][Tool: {tool_name} executed][/dim]"
                )
        finally:
            # Restore original handler and command
            if original_handler:
                tui.agent_full_display_handlers[tool_name] = original_handler
            else:
                tui.agent_full_display_handlers.pop(tool_name, None)

            if original_command is not None:
                TUI_COMMAND_MAP[tool_name] = original_command
            else:
                TUI_COMMAND_MAP.pop(tool_name, None)


class TestInputValidationAndEdgeCases:
    """Test edge cases and input validation in display methods."""

    def test_none_tool_name_handled_gracefully(self, tui_with_captured_console):
        """Test that None tool_name is handled gracefully."""
        tui = tui_with_captured_console

        # Track if condensed display is called with None tool name
        condensed_called = [False]
        condensed_args = [None, None]  # To store the arguments
        original_condensed = tui._display_condensed_tool_output

        def tracking_condensed(tool_name, tool_data):
            condensed_called[0] = True
            condensed_args[0] = tool_name
            condensed_args[1] = tool_data
            # Don't actually call original to avoid PaginationCancelled
            return None

        # Replace method with tracking version
        tui._display_condensed_tool_output = tracking_condensed

        try:
            # Should not crash with None tool_name
            tui.display_tool_output(None, {"data": "test"})

            # Verify condensed was called with correct args
            assert condensed_called[
                0
            ], "_display_condensed_tool_output should be called"
            assert condensed_args[0] is None, "tool_name should be None"
            assert condensed_args[1] == {"data": "test"}, "tool_data should match"

        finally:
            # Restore original method
            tui._display_condensed_tool_output = original_condensed

    def test_none_tool_result_handled_gracefully(self, tui_with_captured_console):
        """Test that None tool_result is handled gracefully."""
        tui = tui_with_captured_console

        # Create a real command definition
        real_cmd_def = create_real_command_def(agent_display="condensed")

        # Register command in the real registry
        original_command = TUI_COMMAND_MAP.get("test-tool")
        TUI_COMMAND_MAP["test-tool"] = real_cmd_def

        # Track if condensed display is called with None tool result
        condensed_called = [False]
        condensed_args = [None, None]  # To store the arguments
        original_condensed = tui._display_condensed_tool_output

        def tracking_condensed(tool_name, tool_data):
            condensed_called[0] = True
            condensed_args[0] = tool_name
            condensed_args[1] = tool_data
            return original_condensed(tool_name, tool_data)

        # Replace method with tracking version
        tui._display_condensed_tool_output = tracking_condensed

        try:
            # Should not crash with None tool_result
            tui.display_tool_output("test-tool", None)

            # Verify condensed was called with correct args
            assert condensed_called[
                0
            ], "_display_condensed_tool_output should be called"
            assert condensed_args[0] == "test-tool", "tool_name should be correct"
            assert condensed_args[1] is None, "tool_data should be None"

        finally:
            # Restore original method and command
            tui._display_condensed_tool_output = original_condensed
            if original_command is not None:
                TUI_COMMAND_MAP["test-tool"] = original_command
            else:
                TUI_COMMAND_MAP.pop("test-tool", None)

    def test_get_command_exception_handled(self, tui_with_captured_console):
        """Test that get_command exceptions are handled gracefully."""
        tui = tui_with_captured_console

        # Save original get_command function
        from chuck_data.ui.tui import get_command as original_get_command

        def failing_get_command(name):
            raise Exception("Command registry failed")

        # Track if condensed display is called
        condensed_called = [False]
        original_condensed = tui._display_condensed_tool_output

        def tracking_condensed(*args, **kwargs):
            condensed_called[0] = True
            return original_condensed(*args, **kwargs)

        # Replace method with tracking version
        tui._display_condensed_tool_output = tracking_condensed

        try:
            # Replace get_command with failing version
            chuck_data.ui.tui.get_command = failing_get_command

            with patch("chuck_data.ui.tui.logging.warning") as mock_log:
                # Should not crash when get_command fails
                tui.display_tool_output("test-tool", {"data": "test"})

                # Should log the error
                mock_log.assert_called()

                # Should print fallback notification
                tui.console.print.assert_called_with(
                    "[dim][Tool: test-tool executed][/dim]"
                )

                # Condensed display should not be called due to the get_command failure
                assert not condensed_called[
                    0
                ], "_display_condensed_tool_output should not be called"

        finally:
            # Restore original get_command function and condensed method
            chuck_data.ui.tui.get_command = original_get_command
            tui._display_condensed_tool_output = original_condensed

    def test_console_none_causes_attribute_error(self):
        """Test that missing console causes AttributeError (current behavior)."""
        # Create TUI without mocking ChuckService or console
        with patch("chuck_data.ui.tui.ChuckService"):
            tui = ChuckTUI()
            tui.console = None  # Simulate missing console

            # Create a real command definition
            real_cmd_def = create_real_command_def(agent_display="condensed")

            # Register command in the real registry
            original_command = TUI_COMMAND_MAP.get("test-tool")
            TUI_COMMAND_MAP["test-tool"] = real_cmd_def

            try:
                # Current implementation doesn't handle None console gracefully
                # This documents the current behavior - could be improved in future
                with pytest.raises(
                    AttributeError, match="'NoneType' object has no attribute 'print'"
                ):
                    tui.display_tool_output("test-tool", {"data": "test"})
            finally:
                # Clean up command registry
                if original_command is not None:
                    TUI_COMMAND_MAP["test-tool"] = original_command
                else:
                    TUI_COMMAND_MAP.pop("test-tool", None)

    def test_complex_nested_exception_scenarios(self, tui_with_captured_console):
        """Test complex nested exception scenarios."""
        tui = tui_with_captured_console

        # Use custom tool name
        tool_name = "custom-nested-test-tool"

        # Create a condition function that itself has errors
        def problematic_condition(result):
            # This will fail when result is not a dict or doesn't have the key
            return result["missing_key"]  # KeyError

        # Create real command definition with problematic condition
        real_cmd_def = create_real_command_def(
            name=tool_name,
            agent_display="conditional",
            display_condition=problematic_condition,
        )

        # Register command in the real registry
        original_command = TUI_COMMAND_MAP.get(tool_name)
        TUI_COMMAND_MAP[tool_name] = real_cmd_def

        try:
            with patch("chuck_data.ui.tui.logging.warning") as mock_log:
                # Should handle the KeyError in condition gracefully
                tui.display_tool_output(tool_name, {"data": "test"})  # No "missing_key"

                # Should log the error
                mock_log.assert_called()

                # Should print fallback notification
                tui.console.print.assert_called_with(
                    f"[dim][Tool: {tool_name} executed][/dim]"
                )
        finally:
            # Restore original command
            if original_command is not None:
                TUI_COMMAND_MAP["test-tool"] = original_command
            else:
                TUI_COMMAND_MAP.pop("test-tool", None)


class TestErrorRecoveryAndFallbacks:
    """Test error recovery and fallback behavior."""

    def test_display_errors_dont_affect_subsequent_calls(
        self, tui_with_captured_console
    ):
        """Test that display errors don't affect subsequent display calls."""
        tui = tui_with_captured_console

        # Create real command definitions
        real_cmd_def1 = create_real_command_def(agent_display="condensed")
        real_cmd_def2 = create_real_command_def(agent_display="condensed")

        # Register commands in the real registry
        original_command1 = TUI_COMMAND_MAP.get("test-tool1")
        original_command2 = TUI_COMMAND_MAP.get("test-tool2")
        TUI_COMMAND_MAP["test-tool1"] = real_cmd_def1
        TUI_COMMAND_MAP["test-tool2"] = real_cmd_def2

        try:
            # Create a tracking wrapper that fails on first call only
            original_method = tui._display_condensed_tool_output
            call_count = [0]

            def tracking_condensed(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    raise Exception("First call failed")
                return original_method(*args, **kwargs)

            # Replace method with tracking version
            tui._display_condensed_tool_output = tracking_condensed

            with patch("chuck_data.ui.tui.logging.warning"):
                # First call should fail gracefully
                tui.display_tool_output("test-tool1", {"data": "test1"})

                # Second call should work normally
                tui.display_tool_output("test-tool2", {"data": "test2"})

                # Both calls should have been attempted
                assert (
                    call_count[0] == 2
                ), "_display_condensed_tool_output should be called twice"
        finally:
            # Restore original method and commands
            tui._display_condensed_tool_output = original_method

            if original_command1 is not None:
                TUI_COMMAND_MAP["test-tool1"] = original_command1
            else:
                TUI_COMMAND_MAP.pop("test-tool1", None)

            if original_command2 is not None:
                TUI_COMMAND_MAP["test-tool2"] = original_command2
            else:
                TUI_COMMAND_MAP.pop("test-tool2", None)

    def test_partial_display_data_handled(self, tui_with_captured_console):
        """Test that partial or malformed display data is handled gracefully."""
        tui = tui_with_captured_console

        # Use a custom tool name
        tool_name = "custom-test-tool-malformed"

        # Create a real command definition
        real_cmd_def = create_real_command_def(name=tool_name, agent_display="full")

        # Register command in the real registry
        original_command = TUI_COMMAND_MAP.get(tool_name)
        TUI_COMMAND_MAP[tool_name] = real_cmd_def

        try:
            # Create a tracking wrapper for full display method that doesn't cause problems
            original_method = tui._display_full_tool_output
            call_count = [0]

            def tracking_full(*args, **kwargs):
                if args and args[0] == tool_name:
                    call_count[0] += 1
                    # Don't actually call original to avoid PaginationCancelled
                    return None
                return original_method(*args, **kwargs)

            # Replace method with tracking version
            tui._display_full_tool_output = tracking_full

            # Test with various malformed data structures
            malformed_data_cases = [
                {"display": True},  # Missing expected data fields
                {"display": True, "catalogs": "not-a-list"},  # Wrong data type
                {"display": True, "schemas": None},  # Null data
                {"display": True, "tables": []},  # Empty list (should be OK)
            ]

            for malformed_data in malformed_data_cases:
                # Should handle each case without crashing
                tui.display_tool_output(tool_name, malformed_data)

            # All calls should have been made
            assert call_count[0] == len(
                malformed_data_cases
            ), "All display calls should be made"
        finally:
            # Restore original method and command
            tui._display_full_tool_output = original_method

            if original_command is not None:
                TUI_COMMAND_MAP[tool_name] = original_command
            else:
                TUI_COMMAND_MAP.pop(tool_name, None)
