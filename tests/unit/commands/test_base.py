"""
Tests for the base module in the commands package.
"""

from chuck_data.commands.base import CommandResult


def test_command_result_success():
    """Test creating a successful CommandResult."""
    result = CommandResult(True, data="test data", message="test message")
    assert result.success
    assert result.data == "test data"
    assert result.message == "test message"
    assert result.error is None


def test_command_result_failure():
    """Test creating a failure CommandResult."""
    error = ValueError("test error")
    result = CommandResult(False, error=error, message="test error message")
    assert not result.success
    assert result.data is None
    assert result.message == "test error message"
    assert result.error == error


def test_command_result_defaults():
    """Test CommandResult with default values."""
    result = CommandResult(True)
    assert result.success
    assert result.data is None
    assert result.message is None
    assert result.error is None
