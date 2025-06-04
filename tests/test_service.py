"""
Tests for the service layer.
"""

import unittest
from unittest.mock import patch, MagicMock

from src.service import ChuckService
from src.command_registry import CommandDefinition
from src.commands.base import CommandResult


class TestChuckService(unittest.TestCase):
    """Test cases for ChuckService."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.service = ChuckService(client=self.mock_client)

    def test_service_initialization(self):
        """Test service initialization with client."""
        self.assertEqual(self.service.client, self.mock_client)

    @patch("src.service.get_command")
    def test_execute_command_status(self, mock_get_command):
        """Test execute_command with status command (which now includes auth functionality)."""
        # Setup mock handler and command definition
        mock_handle_status = MagicMock()
        mock_handle_status.return_value = CommandResult(
            success=True,
            message="Status checked",
            data={
                "connection": {"status": "valid", "message": "Connected"},
                "permissions": {"unity_catalog": True, "models": True},
            },
        )

        # Create mock command definition
        mock_command_def = MagicMock(spec=CommandDefinition)
        mock_command_def.handler = mock_handle_status
        mock_command_def.name = "status"
        mock_command_def.visible_to_user = True
        mock_command_def.needs_api_client = True
        mock_command_def.parameters = {}
        mock_command_def.required_params = []
        mock_command_def.supports_interactive_input = False

        # Setup mock to return our command definition
        mock_get_command.return_value = mock_command_def

        # Execute command
        result = self.service.execute_command("/status")

        # Verify
        mock_get_command.assert_called_once_with("/status")
        mock_handle_status.assert_called_once_with(self.mock_client)
        self.assertTrue(result.success)
        self.assertEqual(result.message, "Status checked")
        self.assertIn("connection", result.data)
        self.assertIn("permissions", result.data)

    @patch("src.service.get_command")
    def test_execute_command_models(self, mock_get_command):
        """Test execute_command with models command."""
        # Setup mock handler
        mock_data = [{"name": "model1"}, {"name": "model2"}]
        mock_handle_models = MagicMock()
        mock_handle_models.return_value = CommandResult(success=True, data=mock_data)

        # Create mock command definition
        mock_command_def = MagicMock(spec=CommandDefinition)
        mock_command_def.handler = mock_handle_models
        mock_command_def.name = "models"
        mock_command_def.visible_to_user = True
        mock_command_def.needs_api_client = True
        mock_command_def.parameters = {}
        mock_command_def.required_params = []
        mock_command_def.supports_interactive_input = False

        # Setup mock to return our command definition
        mock_get_command.return_value = mock_command_def

        # Execute command
        result = self.service.execute_command("/models")

        # Verify
        mock_get_command.assert_called_once_with("/models")
        mock_handle_models.assert_called_once_with(self.mock_client)
        self.assertTrue(result.success)
        self.assertEqual(result.data, mock_data)

    def test_execute_unknown_command(self):
        """Test execute_command with unknown command."""
        result = self.service.execute_command("unknown_command")
        self.assertFalse(result.success)
        self.assertIn("Unknown command", result.message)

    @patch("src.service.get_command")
    def test_execute_command_with_params(self, mock_get_command):
        """Test execute_command with parameters."""
        # Setup mock handler
        mock_handle_model_selection = MagicMock()
        mock_handle_model_selection.return_value = CommandResult(
            success=True, message="Model selected"
        )

        # Create mock command definition
        mock_command_def = MagicMock(spec=CommandDefinition)
        mock_command_def.handler = mock_handle_model_selection
        mock_command_def.name = "select_model"
        mock_command_def.visible_to_user = True
        mock_command_def.needs_api_client = True
        mock_command_def.parameters = {
            "model_name": {
                "type": "string",
                "description": "The name of the model to make active.",
            }
        }
        mock_command_def.required_params = ["model_name"]
        mock_command_def.supports_interactive_input = False

        # Setup mock to return our command definition
        mock_get_command.return_value = mock_command_def

        # Execute command
        result = self.service.execute_command("/select_model", "test-model")

        # Verify - use keyword arguments instead of positional
        mock_get_command.assert_called_once_with("/select_model")
        mock_handle_model_selection.assert_called_once_with(
            self.mock_client, model_name="test-model"
        )
        self.assertTrue(result.success)
        self.assertEqual(result.message, "Model selected")

    @patch("src.service.get_command")
    @patch("src.service.get_metrics_collector")
    def test_execute_command_error_handling(
        self, mock_get_metrics_collector, mock_get_command
    ):
        """Test error handling with metrics collection in execute_command."""
        # Setup mock handler that raises exception
        mock_handler = MagicMock()
        mock_handler.side_effect = Exception("Command failed")

        # Setup metrics collector mock
        mock_metrics_collector = MagicMock()
        mock_get_metrics_collector.return_value = mock_metrics_collector

        # Create mock command definition
        mock_command_def = MagicMock(spec=CommandDefinition)
        mock_command_def.handler = mock_handler
        mock_command_def.name = "test_command"
        mock_command_def.visible_to_user = True
        mock_command_def.needs_api_client = True
        mock_command_def.parameters = {"param1": {"type": "string"}}
        mock_command_def.required_params = []
        mock_command_def.supports_interactive_input = False

        # Setup mock to return our command definition
        mock_get_command.return_value = mock_command_def

        # Execute command that will raise an exception
        result = self.service.execute_command("/test_command", "param_value")

        # Verify error handling
        self.assertFalse(result.success)
        self.assertIn("Error during command execution", result.message)

        # Verify metrics collection for error reporting
        mock_metrics_collector.track_event.assert_called_once()

        # Check parameters in the metrics call
        call_args = mock_metrics_collector.track_event.call_args[1]
        self.assertIn("prompt", call_args)  # Should have command context as prompt
        self.assertIn("error", call_args)  # Should have error traceback
        self.assertEqual(call_args["tools"][0]["name"], "test_command")
        self.assertEqual(call_args["additional_data"]["event_context"], "error_report")
