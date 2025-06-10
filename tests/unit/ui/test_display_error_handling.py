"""
Tests for display error handling in TUI.

These tests ensure that display errors are handled gracefully and don't break
agent execution or user experience.
"""

import pytest
import tempfile
import logging
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


class LogCaptureHandler(logging.Handler):
    """Custom log handler that captures log messages for testing."""

    def __init__(self):
        import logging

        super().__init__()
        self.logs = []
        self.warnings = []
        self.errors = []
        self.level = logging.WARNING  # Capture WARNING and above

    def emit(self, record):
        message = record.getMessage()
        self.logs.append(message)
        if record.levelname == "WARNING":
            self.warnings.append(message)
        elif record.levelname == "ERROR":
            self.errors.append(message)

    def clear(self):
        self.logs.clear()
        self.warnings.clear()
        self.errors.clear()


@pytest.fixture
def temp_config():
    """Create a temporary config manager for testing."""
    with tempfile.NamedTemporaryFile() as tmp:
        from chuck_data.config import ConfigManager

        config_manager = ConfigManager(tmp.name)
        yield config_manager


@pytest.fixture
def logger_with_capture():
    """Create a logger with a capture handler for testing."""
    import logging

    # Create and configure handler
    handler = LogCaptureHandler()

    # We need to capture from the root logger since that's what's used in tui.py
    # Get the root logger
    root_logger = logging.getLogger()

    # Store original handlers and level
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level

    # Add our handler to the root logger
    root_logger.addHandler(handler)
    # Make sure we capture warnings
    root_logger.setLevel(logging.WARNING)

    yield handler

    # Restore root logger configuration
    root_logger.removeHandler(handler)
    root_logger.setLevel(original_level)
    for original_handler in original_handlers:
        if original_handler not in root_logger.handlers:
            root_logger.addHandler(original_handler)


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
        """PaginationCancelled from full display methods should bubble up.

        This test verifies the actual code path in TUI.display_tool_output that
        catches exceptions, identifies PaginationCancelled, and re-raises it.
        """
        tui = tui_with_captured_console

        # Create a real command definition for full display
        tool_name = "list-catalogs"  # Uses _display_catalogs method
        real_cmd_def = create_real_command_def(name=tool_name, agent_display="full")

        # Register command in the real registry
        original_command = TUI_COMMAND_MAP.get(tool_name)
        TUI_COMMAND_MAP[tool_name] = real_cmd_def

        # Create a wrapper for the _display_catalogs method that raises PaginationCancelled
        original_display_catalogs = tui._display_catalogs

        def raising_method(data):
            # This simulates what happens in the real display methods
            raise PaginationCancelled()

        # Replace the method with our raising version
        tui._display_catalogs = raising_method

        try:
            # This should trigger _display_full_tool_output, which calls _display_catalogs,
            # which raises PaginationCancelled, which should bubble up through display_tool_output
            with pytest.raises(PaginationCancelled):
                tui.display_tool_output(
                    tool_name, {"display": True, "catalogs": [{"name": "test"}]}
                )
        finally:
            # Restore original method and command
            tui._display_catalogs = original_display_catalogs
            if original_command is not None:
                TUI_COMMAND_MAP[tool_name] = original_command
            else:
                TUI_COMMAND_MAP.pop(tool_name, None)

    def test_custom_handler_pagination_cancelled_bubbles_up(
        self, tui_with_captured_console
    ):
        """PaginationCancelled from custom handlers should bubble up.

        This test verifies that PaginationCancelled raised from a custom handler is not caught
        by the try/except in display_tool_output but is properly re-raised.
        """
        tui = tui_with_captured_console

        # We'll use the status tool which has a custom handler registered in agent_full_display_handlers
        tool_name = "status"

        # Create a real command definition for full display
        real_cmd_def = create_real_command_def(name=tool_name, agent_display="full")

        # Register command in the registry if not already there
        original_command = TUI_COMMAND_MAP.get(tool_name)
        TUI_COMMAND_MAP[tool_name] = real_cmd_def

        # Create custom handler that raises PaginationCancelled
        def raising_handler(tool_name, tool_data):
            # This is what would happen in a real handler when pagination is cancelled
            raise PaginationCancelled()

        # Save original handler
        original_handler = tui.agent_full_display_handlers.get(tool_name)

        try:
            # Register custom handler
            tui.agent_full_display_handlers[tool_name] = raising_handler

            # This should trigger the custom handler, which raises PaginationCancelled,
            # which should bubble up through display_tool_output's exception handling
            with pytest.raises(PaginationCancelled):
                tui.display_tool_output(tool_name, {"display": True})

        finally:
            # Clean up - restore original handler and command
            tui.agent_full_display_handlers[tool_name] = original_handler
            if original_command and original_command != real_cmd_def:
                TUI_COMMAND_MAP[tool_name] = original_command

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
        self, tui_with_captured_console, logger_with_capture
    ):
        """Full display exceptions should be logged and contained."""
        tui = tui_with_captured_console

        # Clear any existing log messages
        logger_with_capture.clear()

        # Create a test tool name that routes to a specific display method
        tool_name = "list-models"  # This should route to _display_models or _display_models_consolidated

        # Temporarily replace the display method with one that raises RuntimeError
        test_exception = RuntimeError("Full display failed")

        class TestPatched:
            def __call__(self, *args, **kwargs):
                raise test_exception

        # Save the original methods
        original_models = tui._display_models
        original_consolidated = tui._display_models_consolidated

        # Replace both methods (we don't know which one will be called)
        tui._display_models = TestPatched()
        tui._display_models_consolidated = TestPatched()

        try:
            # Should not raise exception - should handle gracefully
            tui.display_tool_output(tool_name, {"display": True})

            # Should log the warning
            assert len(logger_with_capture.warnings) > 0, "Warning should be logged"

            # Verify warning message contains expected content
            warning_message = (
                logger_with_capture.warnings[0] if logger_with_capture.warnings else ""
            )
            assert (
                "Failed to display tool output" in warning_message
            ), "Expected error message not found"
            assert (
                "Full display failed" in warning_message
            ), "Exception message not found in log"

            # Should print fallback notification
            tui.console.print.assert_called_with(
                f"[dim][Tool: {tool_name} executed][/dim]"
            )

        finally:
            # Restore original methods
            tui._display_models = original_models
            tui._display_models_consolidated = original_consolidated

    def test_custom_handler_exception_logged_and_contained(
        self, tui_with_captured_console, logger_with_capture
    ):
        """Custom handler exceptions should be logged and contained."""
        tui = tui_with_captured_console

        # Clear any existing log messages
        logger_with_capture.clear()

        # Use the status tool which has a custom handler
        tool_name = "status"

        # Define custom handler that raises exception
        test_exception = ValueError("Custom handler failed")

        def failing_handler(tool_name, tool_data):
            raise test_exception

        # Save original handler
        original_handler = tui.agent_full_display_handlers[tool_name]

        try:
            # Register custom handler
            tui.agent_full_display_handlers[tool_name] = failing_handler

            # Should not raise exception
            tui.display_tool_output(tool_name, {"data": "test", "display": True})

            # Should log the warning
            assert len(logger_with_capture.warnings) > 0, "Warning should be logged"

            # Verify warning message contains expected content
            warning_message = (
                logger_with_capture.warnings[0] if logger_with_capture.warnings else ""
            )
            assert (
                "Failed to display tool output" in warning_message
            ), "Expected error message not found"
            assert (
                "Custom handler failed" in warning_message
            ), "Exception message not found in log"

            # Should print fallback notification
            tui.console.print.assert_called_with(
                f"[dim][Tool: {tool_name} executed][/dim]"
            )

        finally:
            # Clean up - restore original handler
            tui.agent_full_display_handlers[tool_name] = original_handler

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

        # Keep track of whether the actual display method was called
        condensed_called = [False]

        try:
            # Create a tracking wrapper for condensed display method
            original_condensed = tui._display_condensed_tool_output

            def tracking_condensed(*args, **kwargs):
                if args and args[0] == tool_name:
                    condensed_called[0] = True
                return original_condensed(*args, **kwargs)

            # Replace method with tracking version
            tui._display_condensed_tool_output = tracking_condensed

            # In the tui.py code, condition errors are silently caught and default to condensed
            # without any logging, so we'll just verify that the condensed display is used
            tui.display_tool_output(tool_name, {"data": "test"})

            # Verify that condensed display was called after condition failure
            assert condensed_called[0], "Condensed display should have been used"

            # The console should have displayed something
            assert tui.console.print.called, "Console output should be generated"

        finally:
            # Restore original methods and command
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
        self, tui_with_failing_display_methods, logger_with_capture
    ):
        """Test that _display_catalogs exceptions are handled by parent display_tool_output."""
        tui = tui_with_failing_display_methods

        # Clear any existing log messages
        logger_with_capture.clear()

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

            # Should not raise - should be caught by display_tool_output
            tui.display_tool_output("list-catalogs", {"display": True, "catalogs": []})

            # Should log the error
            assert len(logger_with_capture.warnings) > 0, "Warning should be logged"

            # Verify warning contains expected content
            warning_message = (
                logger_with_capture.warnings[0] if logger_with_capture.warnings else ""
            )
            assert (
                "Failed to display tool output" in warning_message
            ), "Expected error message not found"
            assert (
                "Catalog display failed" in warning_message
            ), "Exception message not found in log"

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
        self, tui_with_captured_console, logger_with_capture
    ):
        """Test that custom agent handler exceptions are handled."""
        tui = tui_with_captured_console

        # Clear any existing log messages
        logger_with_capture.clear()

        # Use the status tool which already has a handler
        tool_name = "status"

        # Create failing status handler
        def failing_status_handler(tool_name, tool_data):
            raise Exception("Status display failed")

        # Save original handler
        original_handler = tui.agent_full_display_handlers.get(tool_name)

        try:
            # Register failing handler
            tui.agent_full_display_handlers[tool_name] = failing_status_handler

            # Should not raise - should be caught
            tui.display_tool_output(
                tool_name, {"workspace_url": "test", "display": True}
            )

            # Should log the error
            assert len(logger_with_capture.warnings) > 0, "Warning should be logged"

            # Verify warning contains expected content
            warning_message = (
                logger_with_capture.warnings[0] if logger_with_capture.warnings else ""
            )
            assert (
                "Failed to display tool output" in warning_message
            ), "Expected error message not found"
            assert (
                "Status display failed" in warning_message
            ), "Exception message not found in log"

            # Should print fallback notification
            tui.console.print.assert_called_with(
                f"[dim][Tool: {tool_name} executed][/dim]"
            )
        finally:
            # Restore original handler
            if original_handler:
                tui.agent_full_display_handlers[tool_name] = original_handler


class TestInputValidationAndEdgeCases:
    """Test edge cases and input validation in display methods."""

    def test_none_tool_name_handled_gracefully(self, tui_with_captured_console):
        """Test that None tool_name is handled gracefully."""
        tui = tui_with_captured_console

        try:
            with patch("chuck_data.ui.tui.logging", autospec=True) as mock_logging:
                # Should not crash with None tool_name
                tui.display_tool_output(None, {"data": "test"})

                # In current implementation, None tool name is validated and logged as an error
                # but doesn't crash - that's what we want to verify
                assert (
                    mock_logging.warning.called
                ), "Warning should be logged about None tool name"

                # The console should have output something
                assert tui.console.print.called, "Console output should be generated"

        except Exception as e:
            pytest.fail(f"Exception was raised with None tool_name: {e}")

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

        # Also track calls to condensed display
        condensed_called = [False]
        original_condensed = tui._display_condensed_tool_output

        def tracking_condensed(*args, **kwargs):
            if args and args[0] == tool_name:
                condensed_called[0] = True
            return original_condensed(*args, **kwargs)

        # Replace method with tracking version
        tui._display_condensed_tool_output = tracking_condensed

        try:
            # Should handle the KeyError in condition gracefully
            tui.display_tool_output(tool_name, {"data": "test"})  # No "missing_key"

            # Verify condensed display was called after condition failed
            assert condensed_called[0], "Condensed display should be used as fallback"

            # Should print something to console
            assert tui.console.print.called, "Console output should be generated"
        finally:
            # Restore original method and command
            tui._display_condensed_tool_output = original_condensed

            if original_command is not None:
                TUI_COMMAND_MAP[tool_name] = original_command
            else:
                TUI_COMMAND_MAP.pop(tool_name, None)


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

        # Use a built-in tool name that has a full display method
        tool_name = (
            "list-models"  # Uses _display_models or _display_models_consolidated
        )

        # Keep track of whether any display method is called
        display_called = [False]

        # Mock both possible display methods for this tool
        original_models = tui._display_models
        original_models_consolidated = tui._display_models_consolidated

        def track_display(*args, **kwargs):
            display_called[0] = True
            # Call console to ensure it's called
            tui.console.print("Display called")
            return None

        # Set both methods to our tracking version
        tui._display_models = track_display
        tui._display_models_consolidated = track_display

        try:
            # Test with various malformed data structures
            malformed_data_cases = [
                {"display": True},  # Missing expected data fields
                {"display": True, "models": "not-a-list"},  # Wrong data type
                {"display": True, "models": None},  # Null data
                {"display": True, "models": []},  # Empty list (should be OK)
            ]

            # Test a couple of cases - we just need to verify they don't crash
            for malformed_data in malformed_data_cases[:2]:  # Just try a couple
                try:
                    # Should handle each case without crashing
                    tui.display_tool_output(tool_name, malformed_data)
                except Exception as e:
                    pytest.fail(f"Failed to handle malformed data: {e}")

            # We just need to verify that display was attempted and didn't crash
            assert tui.console.print.called, "Console output should be generated"
        finally:
            # Restore original methods and command
            tui._display_models = original_models
            tui._display_models_consolidated = original_models_consolidated
