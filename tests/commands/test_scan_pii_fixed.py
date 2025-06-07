"""
Tests for scan_pii command handler - following CLAUDE.md guidelines.

This module contains tests for the scan_pii command handler that follow
the approved testing patterns: mock external boundaries only, use real
internal business logic.
"""

import unittest
from unittest.mock import patch, MagicMock

from chuck_data.commands.scan_pii import handle_command
from tests.fixtures import LLMClientStub, DatabricksClientStub


class TestScanPIIInteractiveDisplay(unittest.TestCase):
    """Tests for interactive display functionality in scan_pii command."""

    def setUp(self):
        """Set up common test fixtures."""
        # Use real databricks client stub (external boundary)
        self.client_stub = DatabricksClientStub()

    @patch("rich.console.Console")
    @patch("chuck_data.ui.tui._tui_instance", None)  # Ensure fallback path is used  
    def test_interactive_display_with_real_business_logic(self, mock_console_class):
        """Test interactive display using real business logic (following CLAUDE.md guidelines)."""
        # Setup mock console (external boundary - UI/Terminal)
        mock_console = MagicMock()
        mock_console_class.return_value = mock_console

        # Setup real databricks data using stub
        self.client_stub.add_catalog("test_catalog")
        self.client_stub.add_schema("test_catalog", "test_schema")
        self.client_stub.add_table(
            "test_catalog",
            "test_schema",
            "users",
            columns=[
                {"name": "email", "type_name": "string"},
                {"name": "first_name", "type_name": "string"},
                {"name": "signup_date", "type_name": "date"},
            ],
        )
        self.client_stub.add_table(
            "test_catalog",
            "test_schema",
            "products",
            columns=[
                {"name": "product_id", "type_name": "string"},
                {"name": "description", "type_name": "string"},
            ],
        )

        # Configure LLM client stub (external boundary - API calls)
        pii_response = '[{"name":"email","semantic":"email"},{"name":"first_name","semantic":"given-name"},{"name":"signup_date","semantic":null}]'

        # Mock the LLM class but use real LLM client stub
        with patch("chuck_data.commands.scan_pii.LLMClient") as mock_llm_class:
            llm_stub = LLMClientStub()
            # Set up responses for multiple tables
            llm_stub.set_response_content(pii_response)
            mock_llm_class.return_value = llm_stub

            # Call real handle_command function (no mocking internal business logic)
            result = handle_command(
                self.client_stub, catalog_name="test_catalog", schema_name="test_schema"
            )

        # Verify results using real business logic
        self.assertTrue(result.success)
        self.assertIn("test_catalog.test_schema", result.message)
        self.assertEqual(result.data["catalog"], "test_catalog")
        self.assertEqual(result.data["schema"], "test_schema")

        # Verify console progress messages are displayed
        print_calls = [
            call[0][0]
            for call in mock_console.print.call_args_list
            if mock_console.print.called
        ]
        expected_progress_messages = [
            "[dim]Scanning test_catalog.test_schema.users...[/dim]",
            "[dim]Scanning test_catalog.test_schema.products...[/dim]",
        ]
        for expected_msg in expected_progress_messages:
            self.assertIn(expected_msg, print_calls)

    @patch("rich.console.Console")
    @patch("chuck_data.ui.tui._tui_instance", None)  # Ensure fallback path is used
    def test_progress_can_be_disabled_with_real_logic(self, mock_console_class):
        """Test that progress display can be disabled using real business logic."""
        # Setup mock console (external boundary)
        mock_console = MagicMock()
        mock_console_class.return_value = mock_console

        # Setup real databricks data
        self.client_stub.add_catalog("test_catalog")
        self.client_stub.add_schema("test_catalog", "test_schema")
        self.client_stub.add_table(
            "test_catalog",
            "test_schema",
            "users",
            columns=[{"name": "email", "type_name": "string"}],
        )

        # Configure LLM stub
        pii_response = '[{"name":"email","semantic":"email"}]'

        with patch("chuck_data.commands.scan_pii.LLMClient") as mock_llm_class:
            llm_stub = LLMClientStub()
            llm_stub.set_response_content(pii_response)
            mock_llm_class.return_value = llm_stub

            # Call with show_progress=False
            result = handle_command(
                self.client_stub,
                catalog_name="test_catalog",
                schema_name="test_schema",
                show_progress=False,
            )

        # Verify results
        self.assertTrue(result.success)

        # Verify no progress messages when disabled
        if mock_console.print.called:
            print_calls = [call[0][0] for call in mock_console.print.call_args_list]
            progress_messages = [
                msg for msg in print_calls if "Scanning" in msg and "[dim]" in msg
            ]
            self.assertEqual(
                len(progress_messages),
                0,
                "No progress messages should appear when show_progress=False",
            )

    def test_show_progress_parameter_in_definition(self):
        """Test that show_progress parameter is properly defined in command definition."""
        from chuck_data.commands.scan_pii import DEFINITION

        # Verify the parameter is defined
        self.assertIn("show_progress", DEFINITION.parameters)
        self.assertEqual(DEFINITION.parameters["show_progress"]["type"], "boolean")
        self.assertIn("progress", DEFINITION.parameters["show_progress"]["description"].lower())
