"""
Tests for the base module in the commands package.
"""

import unittest
from chuck_data.commands.base import CommandResult


class TestCommandResult(unittest.TestCase):
    """Test cases for the CommandResult class."""

    def test_command_result_success(self):
        """Test creating a successful CommandResult."""
        result = CommandResult(True, data="test data", message="test message")
        self.assertTrue(result.success)
        self.assertEqual(result.data, "test data")
        self.assertEqual(result.message, "test message")
        self.assertIsNone(result.error)

    def test_command_result_failure(self):
        """Test creating a failure CommandResult."""
        error = ValueError("test error")
        result = CommandResult(False, error=error, message="test error message")
        self.assertFalse(result.success)
        self.assertIsNone(result.data)
        self.assertEqual(result.message, "test error message")
        self.assertEqual(result.error, error)

    def test_command_result_defaults(self):
        """Test CommandResult with default values."""
        result = CommandResult(True)
        self.assertTrue(result.success)
        self.assertIsNone(result.data)
        self.assertIsNone(result.message)
        self.assertIsNone(result.error)
