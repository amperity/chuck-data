"""
Tests for the agent tool implementations.
"""

import unittest
from unittest.mock import patch, MagicMock, Mock
from jsonschema.exceptions import ValidationError
from src.agent import (
    execute_tool,
    get_tool_schemas,
)
from src.commands.base import CommandResult


class TestAgentTools(unittest.TestCase):
    """Test cases for agent tool implementations."""

    def setUp(self):
        """Set up common test fixtures."""
        self.mock_client = MagicMock()
        self.mock_callback = MagicMock()

    @patch("src.agent.tool_executor.get_command")
    def test_execute_tool_unknown(self, mock_get_command):
        """Test execute_tool with unknown tool name."""
        # Configure the mock to return None for the unknown tool
        mock_get_command.return_value = None

        result = execute_tool(self.mock_client, "unknown_tool", {})

        # Verify the command was looked up
        mock_get_command.assert_called_once_with("unknown_tool")
        # Verify the expected error response
        self.assertEqual(result, {"error": "Tool 'unknown_tool' not found."})

    @patch("src.agent.tool_executor.get_command")
    def test_execute_tool_not_visible_to_agent(self, mock_get_command):
        """Test execute_tool with a tool that's not visible to the agent."""
        # Create a mock command definition that's not visible to agents
        mock_command_def = Mock()
        mock_command_def.visible_to_agent = False
        mock_get_command.return_value = mock_command_def

        result = execute_tool(self.mock_client, "hidden_tool", {})

        # Verify proper error is returned
        self.assertEqual(
            result, {"error": "Tool 'hidden_tool' is not available to the agent."}
        )
        mock_get_command.assert_called_once_with("hidden_tool")

    @patch("src.agent.tool_executor.get_command")
    @patch("src.agent.tool_executor.jsonschema.validate")
    def test_execute_tool_validation_error(self, mock_validate, mock_get_command):
        """Test execute_tool with validation error."""
        # Setup mock command definition
        mock_command_def = Mock()
        mock_command_def.visible_to_agent = True
        mock_command_def.parameters = {"param1": {"type": "string"}}
        mock_command_def.required_params = ["param1"]
        mock_get_command.return_value = mock_command_def

        # Setup validation error
        mock_validate.side_effect = ValidationError(
            "Invalid arguments", schema={"type": "object"}
        )

        result = execute_tool(self.mock_client, "test_tool", {})

        # Verify an error response is returned containing the validation message
        self.assertIn("error", result)
        self.assertIn("Invalid arguments", result["error"])

    @patch("src.agent.tool_executor.get_command")
    @patch("src.agent.tool_executor.jsonschema.validate")
    def test_execute_tool_success(self, mock_validate, mock_get_command):
        """Test execute_tool with successful execution."""
        # Setup mock command definition with handler name
        mock_command_def = Mock()
        mock_command_def.visible_to_agent = True
        mock_command_def.parameters = {"param1": {"type": "string"}}
        mock_command_def.required_params = ["param1"]
        mock_command_def.needs_api_client = True
        mock_command_def.output_formatter = None  # No output formatter

        # Create a handler with a __name__ attribute
        mock_handler = Mock()
        mock_handler.__name__ = "mock_success_handler"
        mock_command_def.handler = mock_handler

        mock_get_command.return_value = mock_command_def

        # Setup handler to return success
        mock_handler.return_value = CommandResult(
            True, data={"result": "success"}, message="Success"
        )

        result = execute_tool(self.mock_client, "test_tool", {"param1": "test"})

        # Verify the handler was called with correct arguments
        mock_handler.assert_called_once_with(self.mock_client, param1="test")
        # Verify the successful result is returned
        self.assertEqual(result, {"result": "success"})

    @patch("src.agent.tool_executor.get_command")
    @patch("src.agent.tool_executor.jsonschema.validate")
    def test_execute_tool_success_with_callback(self, mock_validate, mock_get_command):
        """Test execute_tool with successful execution and callback."""
        # Setup mock command definition with handler name
        mock_command_def = Mock()
        mock_command_def.visible_to_agent = True
        mock_command_def.parameters = {"param1": {"type": "string"}}
        mock_command_def.required_params = ["param1"]
        mock_command_def.needs_api_client = True
        mock_command_def.output_formatter = None  # No output formatter

        # Create a handler with a __name__ attribute
        mock_handler = Mock()
        mock_handler.__name__ = "mock_callback_handler"
        mock_command_def.handler = mock_handler

        mock_get_command.return_value = mock_command_def

        # Setup handler to return success with data
        mock_handler.return_value = CommandResult(
            True, data={"result": "callback_test"}, message="Success"
        )

        result = execute_tool(
            self.mock_client,
            "test_tool",
            {"param1": "test"},
            output_callback=self.mock_callback,
        )

        # Verify the handler was called with correct arguments (including tool_output_callback)
        mock_handler.assert_called_once_with(
            self.mock_client, param1="test", tool_output_callback=self.mock_callback
        )
        # Verify the callback was called with tool name and data
        self.mock_callback.assert_called_once_with(
            "test_tool", {"result": "callback_test"}
        )
        # Verify the successful result is returned
        self.assertEqual(result, {"result": "callback_test"})

    @patch("src.agent.tool_executor.get_command")
    @patch("src.agent.tool_executor.jsonschema.validate")
    def test_execute_tool_success_callback_exception(
        self, mock_validate, mock_get_command
    ):
        """Test execute_tool with callback that throws exception."""
        # Setup mock command definition with handler name
        mock_command_def = Mock()
        mock_command_def.visible_to_agent = True
        mock_command_def.parameters = {"param1": {"type": "string"}}
        mock_command_def.required_params = ["param1"]
        mock_command_def.needs_api_client = True
        mock_command_def.output_formatter = None  # No output formatter

        # Create a handler with a __name__ attribute
        mock_handler = Mock()
        mock_handler.__name__ = "mock_callback_exception_handler"
        mock_command_def.handler = mock_handler

        mock_get_command.return_value = mock_command_def

        # Setup handler to return success with data
        mock_handler.return_value = CommandResult(
            True, data={"result": "callback_exception_test"}, message="Success"
        )

        # Setup callback to throw exception
        self.mock_callback.side_effect = Exception("Callback failed")

        result = execute_tool(
            self.mock_client,
            "test_tool",
            {"param1": "test"},
            output_callback=self.mock_callback,
        )

        # Verify the handler was called with correct arguments (including tool_output_callback)
        mock_handler.assert_called_once_with(
            self.mock_client, param1="test", tool_output_callback=self.mock_callback
        )
        # Verify the callback was called (and failed)
        self.mock_callback.assert_called_once_with(
            "test_tool", {"result": "callback_exception_test"}
        )
        # Verify the successful result is still returned despite callback failure
        self.assertEqual(result, {"result": "callback_exception_test"})

    @patch("src.agent.tool_executor.get_command")
    @patch("src.agent.tool_executor.jsonschema.validate")
    def test_execute_tool_success_no_data(self, mock_validate, mock_get_command):
        """Test execute_tool with successful execution but no data."""
        # Setup mock command definition with handler name
        mock_command_def = Mock()
        mock_command_def.visible_to_agent = True
        mock_command_def.parameters = {"param1": {"type": "string"}}
        mock_command_def.required_params = ["param1"]
        mock_command_def.needs_api_client = True
        mock_command_def.output_formatter = None  # No output formatter

        # Create a handler with a __name__ attribute
        mock_handler = Mock()
        mock_handler.__name__ = "mock_no_data_handler"
        mock_command_def.handler = mock_handler

        mock_get_command.return_value = mock_command_def

        # Setup handler to return success but no data
        mock_handler.return_value = CommandResult(True, data=None, message="Success")

        result = execute_tool(self.mock_client, "test_tool", {"param1": "test"})

        # Verify the default success response is returned when no data
        self.assertEqual(result, {"success": True, "message": "Success"})

    @patch("src.agent.tool_executor.get_command")
    @patch("src.agent.tool_executor.jsonschema.validate")
    def test_execute_tool_failure(self, mock_validate, mock_get_command):
        """Test execute_tool with handler failure."""
        # Setup mock command definition with handler name
        mock_command_def = Mock()
        mock_command_def.visible_to_agent = True
        mock_command_def.parameters = {"param1": {"type": "string"}}
        mock_command_def.required_params = ["param1"]
        mock_command_def.needs_api_client = True

        # Create a handler with a __name__ attribute
        mock_handler = Mock()
        mock_handler.__name__ = "mock_failure_handler"
        mock_command_def.handler = mock_handler

        mock_get_command.return_value = mock_command_def

        # Setup handler to return failure
        error = ValueError("Test error")
        mock_handler.return_value = CommandResult(False, error=error, message="Failed")

        result = execute_tool(self.mock_client, "test_tool", {"param1": "test"})

        # Verify error details are included in response
        self.assertEqual(result, {"error": "Failed", "details": "Test error"})

    @patch("src.agent.tool_executor.get_command")
    @patch("src.agent.tool_executor.jsonschema.validate")
    def test_execute_tool_handler_exception(self, mock_validate, mock_get_command):
        """Test execute_tool with handler throwing exception."""
        # Setup mock command definition with handler name
        mock_command_def = Mock()
        mock_command_def.visible_to_agent = True
        mock_command_def.parameters = {"param1": {"type": "string"}}
        mock_command_def.required_params = ["param1"]
        mock_command_def.needs_api_client = True
        mock_command_def.output_formatter = None  # No output formatter

        # Create a handler with a __name__ attribute
        mock_handler = Mock()
        mock_handler.__name__ = "mock_exception_handler"
        mock_command_def.handler = mock_handler

        mock_get_command.return_value = mock_command_def

        # Setup handler to throw exception
        mock_handler.side_effect = Exception("Unexpected error")

        result = execute_tool(self.mock_client, "test_tool", {"param1": "test"})

        # Verify exception is caught and returned as error
        self.assertIn("error", result)
        self.assertIn("Unexpected error", result["error"])

    @patch("src.agent.tool_executor.get_command_registry_tool_schemas")
    def test_get_tool_schemas(self, mock_get_schemas):
        """Test get_tool_schemas returns schemas from command registry."""
        # Setup mock schemas
        mock_schemas = [
            {
                "type": "function",
                "function": {
                    "name": "test_tool",
                    "description": "Test tool",
                    "parameters": {"type": "object", "properties": {}},
                },
            }
        ]
        mock_get_schemas.return_value = mock_schemas

        schemas = get_tool_schemas()

        # Verify schemas are returned correctly
        self.assertEqual(schemas, mock_schemas)
        mock_get_schemas.assert_called_once()
