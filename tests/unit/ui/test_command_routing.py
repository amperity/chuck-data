"""
Tests for TUI command routing behavior.

These tests focus on how the TUI routes and processes commands,
ensuring proper parameter passing between TUI and service layers.
"""

import pytest
from unittest.mock import patch, MagicMock
from chuck_data.ui.tui import ChuckTUI


@pytest.fixture
def tui():
    """Create a ChuckTUI instance for testing."""
    tui_instance = ChuckTUI()
    # Replace the service with a mock
    tui_instance.service = MagicMock()
    return tui_instance


def test_all_slash_commands_set_display_true_for_tables(tui):
    """
    Test that all list-* slash commands set display=True when executing.

    This test verifies that the TUI passes display=True to the service layer
    for all list-* commands, ensuring tables are always displayed in full format.

    This test would catch the issue where only warehouses commands were setting
    display=True, while other list commands were not.
    """
    # Define list commands that should display full tables
    list_commands = [
        "/list-schemas",
        "/list-catalogs",
        "/list-tables",
        "/list-models",
        "/list-warehouses",
        "/list-volumes",
    ]

    # Test each command
    for cmd in list_commands:
        # Reset mock for this command
        tui.service.execute_command.reset_mock()

        # Process the command
        tui._process_command(cmd)

        # Verify service was called with display=True
        tui.service.execute_command.assert_called_once()
        args, kwargs = tui.service.execute_command.call_args
        assert "display" in kwargs, f"Command {cmd} should set display=True"
        assert kwargs["display"] is True, f"Command {cmd} should set display=True"


def test_agent_commands_pass_tool_output_callback(tui):
    """
    Test that agent commands pass the tool_output_callback parameter.
    """
    # Test agent commands
    agent_commands = ["/agent query text", "/ask a question"]

    for cmd in agent_commands:
        # Reset mock
        tui.service.execute_command.reset_mock()

        # Process the command
        tui._process_command(cmd)

        # Verify service was called with tool_output_callback
        tui.service.execute_command.assert_called_once()
        args, kwargs = tui.service.execute_command.call_args
        assert (
            "tool_output_callback" in kwargs
        ), f"Command {cmd} should set tool_output_callback"
        assert kwargs["tool_output_callback"] == tui.display_tool_output


def test_regular_commands_dont_set_special_parameters(tui):
    """
    Test that regular commands don't set special parameters.
    """
    # Test some regular commands that should not set display or callback
    regular_commands = ["/status", "/help"]

    for cmd in regular_commands:
        # Reset mock
        tui.service.execute_command.reset_mock()

        # Process the command
        tui._process_command(cmd)

        # Verify service was called without special parameters
        tui.service.execute_command.assert_called_once()
        args, kwargs = tui.service.execute_command.call_args
        assert (
            "display" not in kwargs
        ), f"Command {cmd} should not set display parameter"
        assert (
            "tool_output_callback" not in kwargs
        ), f"Command {cmd} should not set tool_output_callback"


def test_command_argument_parsing(tui):
    """
    Test that command arguments are correctly parsed and passed to service.
    """
    # Command with arguments
    cmd = "/list-schemas my_catalog"

    # Process the command
    tui._process_command(cmd)

    # Verify service was called with correct arguments
    tui.service.execute_command.assert_called_once()
    args, kwargs = tui.service.execute_command.call_args

    # First argument should be the command
    assert args[0] == "/list-schemas"
    # Second argument should be the catalog name
    assert args[1] == "my_catalog"


def test_command_with_quoted_arguments(tui):
    """
    Test that quoted arguments are properly preserved.
    """
    # Command with quoted argument
    cmd = '/list-schemas "my catalog with spaces"'

    # Process the command
    with patch(
        "chuck_data.ui.tui.shlex.split",
        return_value=["/list-schemas", "my catalog with spaces"],
    ):
        tui._process_command(cmd)

    # Verify service was called with correct arguments
    tui.service.execute_command.assert_called_once()
    args, kwargs = tui.service.execute_command.call_args

    # First argument should be the command
    assert args[0] == "/list-schemas"
    # Second argument should be the quoted catalog name, preserved as one argument
    assert args[1] == "my catalog with spaces"
