"""
Tests for help command handler.

This module contains tests for the help command handler.
"""

from unittest.mock import patch, MagicMock

from chuck_data.commands.help import handle_command


@patch("chuck_data.commands.help.get_user_commands")
@patch("chuck_data.ui.help_formatter.format_help_text")
def test_help_command_success(mock_format_help_text, mock_get_user_commands):
    """Test successful help command execution."""
    # Setup mocks
    mock_user_commands = {"command1": MagicMock(), "command2": MagicMock()}
    mock_get_user_commands.return_value = mock_user_commands
    mock_format_help_text.return_value = "Formatted help text"

    # Call function
    result = handle_command(None)

    # Verify results
    assert result.success
    assert result.data["help_text"] == "Formatted help text"
    mock_get_user_commands.assert_called_once()
    mock_format_help_text.assert_called_once()


@patch("chuck_data.commands.help.get_user_commands")
def test_help_command_exception(mock_get_user_commands):
    """Test help command with exception."""
    # Setup mock
    mock_get_user_commands.side_effect = Exception("Test error")

    # Call function
    result = handle_command(None)

    # Verify results
    assert not result.success
    assert "Error generating help text" in result.message
    assert str(result.error) == "Test error"
