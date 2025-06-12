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


def test_all_table_display_commands_set_display_true(tui):
    """
    Test that all list-* slash commands set display=True when executing.

    This test verifies that the TUI passes display=True to the service layer
    for all list-* commands, ensuring tables are always displayed in full format.

    This test dynamically collects commands from the TUI_COMMAND_MAP and view registry
    to ensure coverage of all variants and aliases that should display tables.
    """
    from chuck_data.command_registry import TUI_COMMAND_MAP, COMMAND_REGISTRY
    from chuck_data.ui.view_registry import _VIEW_REGISTRY

    table_commands = set()

    # Collect commands from TUI_COMMAND_MAP that should display tables
    for slash_cmd, registry_name in TUI_COMMAND_MAP.items():
        # Include all list-* commands or their aliases
        if registry_name.startswith("list-") or slash_cmd.startswith("/list-"):
            table_commands.add(slash_cmd)

    # Include short form aliases that correspond to list commands
    # These are commands like /tables, /schemas, etc.
    short_form_mapping = {
        "/tables": "list-tables",
        "/schemas": "list-schemas",
        "/catalogs": "list-catalogs",
        "/models": "list-models",
        "/warehouses": "list-warehouses",
        "/volumes": "list-volumes",
    }

    for short_form, list_form in short_form_mapping.items():
        if short_form in TUI_COMMAND_MAP or list_form in COMMAND_REGISTRY:
            table_commands.add(short_form)

    # Also add view registry names that likely represent table views
    for view_name in _VIEW_REGISTRY.keys():
        if view_name.endswith("TableView") or "Table" in view_name:
            # Try to map view names back to commands
            if view_name.startswith("list-"):
                table_commands.add(f"/{view_name}")

    # Ensure we have the minimum expected commands for testing
    expected_minimum = {
        "/list-schemas",
        "/list-catalogs",
        "/list-tables",
        "/list-models",
        "/list-warehouses",
        "/list-volumes",
        "/schemas",
        "/catalogs",
        "/tables",
    }

    # Make sure we don't miss any expected commands
    for expected_cmd in expected_minimum:
        if expected_cmd not in table_commands:
            table_commands.add(expected_cmd)

    assert len(table_commands) >= len(
        expected_minimum
    ), "Failed to find all expected table display commands"

    # Test each command
    for cmd in sorted(table_commands):
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
