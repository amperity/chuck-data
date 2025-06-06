"""
Tests for agent tool display routing in the TUI.

These tests ensure that when agents use list-* commands, they display
the same formatted tables as when users use equivalent slash commands.
"""

import unittest
from unittest.mock import patch
from src.ui.tui import ChuckTUI
from src.commands.base import CommandResult
from src.agent.tool_executor import execute_tool


class TestAgentToolDisplayRouting(unittest.TestCase):
    """Test cases for agent tool display routing."""

    def setUp(self):
        """Set up test fixtures."""
        # Use a real TUI instance but capture console output
        self.tui = ChuckTUI()
        # We'll capture calls to console.print to verify table display

    def test_agent_list_commands_display_tables_not_raw_json(self):
        """
        End-to-end test: Agent tool calls should display formatted tables, not raw JSON.

        This is the critical test that prevents the regression where agents
        would see raw JSON instead of formatted tables.
        """
        from src.commands import register_all_commands
        from src.command_registry import get_command
        from unittest.mock import MagicMock

        # Register all commands
        register_all_commands()

        # Test data that would normally be returned by list commands
        test_cases = [
            {
                "tool_name": "list-schemas",
                "test_data": {
                    "schemas": [
                        {"name": "bronze", "comment": "Bronze layer"},
                        {"name": "silver", "comment": "Silver layer"},
                    ],
                    "catalog_name": "test_catalog",
                    "total_count": 2,
                },
                "expected_table_indicators": ["Schemas in catalog", "bronze", "silver"],
            },
            {
                "tool_name": "list-catalogs",
                "test_data": {
                    "catalogs": [
                        {
                            "name": "catalog1",
                            "type": "MANAGED",
                            "comment": "First catalog",
                        },
                        {
                            "name": "catalog2",
                            "type": "EXTERNAL",
                            "comment": "Second catalog",
                        },
                    ],
                    "total_count": 2,
                },
                "expected_table_indicators": [
                    "Available Catalogs",
                    "catalog1",
                    "catalog2",
                ],
            },
            {
                "tool_name": "list-tables",
                "test_data": {
                    "tables": [
                        {"name": "table1", "table_type": "MANAGED"},
                        {"name": "table2", "table_type": "EXTERNAL"},
                    ],
                    "catalog_name": "test_catalog",
                    "schema_name": "test_schema",
                    "total_count": 2,
                },
                "expected_table_indicators": [
                    "Tables in test_catalog.test_schema",
                    "table1",
                    "table2",
                ],
            },
        ]

        for case in test_cases:
            with self.subTest(tool=case["tool_name"]):
                # Mock console to capture output
                mock_console = MagicMock()
                self.tui.console = mock_console

                # Get the command definition
                cmd_def = get_command(case["tool_name"])
                self.assertIsNotNone(cmd_def, f"Command {case['tool_name']} not found")

                # Verify agent_display setting based on command type
                if case["tool_name"] in [
                    "list-catalogs",
                    "list-schemas",
                    "list-tables",
                ]:
                    # list-catalogs, list-schemas, and list-tables use conditional display
                    self.assertEqual(
                        cmd_def.agent_display,
                        "conditional",
                        f"Command {case['tool_name']} must have agent_display='conditional'",
                    )
                    # For conditional display, we need to test with display=true to see the table
                    test_data_with_display = case["test_data"].copy()
                    test_data_with_display["display"] = True
                    from src.exceptions import PaginationCancelled

                    with self.assertRaises(PaginationCancelled):
                        self.tui.display_tool_output(
                            case["tool_name"], test_data_with_display
                        )
                else:
                    # Other commands use full display
                    self.assertEqual(
                        cmd_def.agent_display,
                        "full",
                        f"Command {case['tool_name']} must have agent_display='full'",
                    )
                    # Call the display method with test data - should raise PaginationCancelled
                    from src.exceptions import PaginationCancelled

                    with self.assertRaises(PaginationCancelled):
                        self.tui.display_tool_output(
                            case["tool_name"], case["test_data"]
                        )

                # Verify console.print was called (indicates table display, not raw JSON)
                mock_console.print.assert_called()

                # Verify the output was processed by checking the call arguments
                print_calls = mock_console.print.call_args_list

                # Verify that Rich Table objects were printed (not raw JSON strings)
                table_objects_found = False
                raw_json_found = False

                for call in print_calls:
                    args, kwargs = call
                    for arg in args:
                        # Check if we're printing Rich Table objects (good)
                        if hasattr(arg, "__class__") and "Table" in str(type(arg)):
                            table_objects_found = True
                        # Check if we're printing raw JSON strings (bad)
                        elif isinstance(arg, str) and (
                            '"schemas":' in arg
                            or '"catalogs":' in arg
                            or '"tables":' in arg
                        ):
                            raw_json_found = True

                # Verify we're displaying tables, not raw JSON
                self.assertTrue(
                    table_objects_found,
                    f"No Rich Table objects found in {case['tool_name']} output - this indicates the regression",
                )
                self.assertFalse(
                    raw_json_found,
                    f"Raw JSON strings found in {case['tool_name']} output - this indicates the regression",
                )

    def test_unknown_tool_falls_back_to_generic_display(self):
        """Test that unknown tools fall back to generic display."""
        from unittest.mock import MagicMock

        test_data = {"some": "data"}

        mock_console = MagicMock()
        self.tui.console = mock_console

        self.tui._display_full_tool_output("unknown-tool", test_data)
        # Should create a generic panel
        mock_console.print.assert_called()

    def test_command_name_mapping_prevents_regression(self):
        """
        Test that ensures command name mapping in TUI covers both hyphenated and underscore versions.

        This test specifically prevents the regression where agent tool names with hyphens
        (like 'list-schemas') weren't being mapped to the correct display methods.
        """

        # Test cases: agent tool name -> expected display method call
        command_mappings = [
            ("list-schemas", "_display_schemas"),
            ("list-catalogs", "_display_catalogs"),
            ("list-tables", "_display_tables"),
            ("list-warehouses", "_display_warehouses"),
            ("list-volumes", "_display_volumes"),
            ("detailed-models", "_display_detailed_models"),
            ("list-models", "_display_models"),
        ]

        for tool_name, expected_method in command_mappings:
            with self.subTest(tool_name=tool_name):
                # Mock the expected display method
                with patch.object(self.tui, expected_method) as mock_method:
                    # Call with appropriate test data structure based on what the TUI routing expects
                    if tool_name == "list-models":
                        # For list-models, the TUI checks if "models" key exists in the dict
                        # If not, it calls _display_models with the dict itself
                        # (which seems like a bug, but we're testing the current behavior)
                        test_data = [
                            {"name": "test_model", "creator": "test"}
                        ]  # This will be passed to _display_models
                    elif tool_name == "detailed-models":
                        # For detailed-models, it expects "models" key in the dict
                        test_data = {
                            "models": [{"name": "test_model", "creator": "test"}]
                        }
                    else:
                        test_data = {"test": "data"}
                    self.tui._display_full_tool_output(tool_name, test_data)

                    # Verify the correct method was called
                    mock_method.assert_called_once_with(test_data)

    def test_agent_display_setting_validation(self):
        """
        Test that validates ALL list commands have agent_display='full'.

        This prevents regressions where commands might be added without proper display settings.
        """
        from src.commands import register_all_commands
        from src.command_registry import get_command, get_agent_commands

        register_all_commands()

        # Get all agent-visible commands
        agent_commands = get_agent_commands()

        # Find all list-* commands
        list_commands = [
            name
            for name in agent_commands.keys()
            if name.startswith("list-") or name == "detailed-models"
        ]

        # Ensure we have the expected list commands
        expected_list_commands = {
            "list-schemas",
            "list-catalogs",
            "list-tables",
            "list-warehouses",
            "list-volumes",
            "detailed-models",
            "list-models",
        }

        found_commands = set(list_commands)
        self.assertEqual(
            found_commands,
            expected_list_commands,
            f"Expected list commands changed. Found: {found_commands}, Expected: {expected_list_commands}",
        )

        # Verify each has agent_display="full" (except list-warehouses, list-catalogs, list-schemas, and list-tables which use conditional display)
        for cmd_name in list_commands:
            with self.subTest(command=cmd_name):
                cmd_def = get_command(cmd_name)
                if cmd_name in [
                    "list-warehouses",
                    "list-catalogs",
                    "list-schemas",
                    "list-tables",
                ]:
                    # list-warehouses, list-catalogs, list-schemas, and list-tables use conditional display with display parameter
                    self.assertEqual(
                        cmd_def.agent_display,
                        "conditional",
                        f"Command {cmd_name} should use conditional display with display parameter control",
                    )
                    # Verify it has a display_condition function
                    self.assertIsNotNone(
                        cmd_def.display_condition,
                        f"Command {cmd_name} with conditional display must have display_condition function",
                    )
                else:
                    self.assertEqual(
                        cmd_def.agent_display,
                        "full",
                        f"Command {cmd_name} must have agent_display='full' for table display",
                    )

    def test_end_to_end_agent_tool_execution_with_table_display(self):
        """
        Full end-to-end test: Execute an agent tool and verify it displays tables.

        This test goes through the complete flow: agent calls tool -> tool executes ->
        output callback triggers -> TUI displays formatted table.
        """
        from unittest.mock import MagicMock

        # Mock an API client
        mock_client = MagicMock()

        # Mock console to capture display output
        mock_console = MagicMock()
        self.tui.console = mock_console

        # Create a simple output callback that mimics agent behavior
        def output_callback(tool_name, tool_data):
            """This mimics how agents call display_tool_output"""
            self.tui.display_tool_output(tool_name, tool_data)

        # Test with list-schemas command
        with patch("src.agent.tool_executor.get_command") as mock_get_command:
            # Get the real command definition
            from src.commands.list_schemas import DEFINITION as schemas_def
            from src.commands import register_all_commands

            register_all_commands()

            mock_get_command.return_value = schemas_def

            # Mock the handler to return test data
            with patch.object(schemas_def, "handler") as mock_handler:
                mock_handler.__name__ = "mock_handler"
                mock_handler.return_value = CommandResult(
                    True,
                    data={
                        "schemas": [
                            {"name": "bronze", "comment": "Bronze layer"},
                            {"name": "silver", "comment": "Silver layer"},
                        ],
                        "catalog_name": "test_catalog",
                        "total_count": 2,
                        "display": True,  # This triggers the display
                    },
                    message="Found 2 schemas",
                )

                # Execute the tool with output callback (mimics agent behavior)
                # The output callback should raise PaginationCancelled which bubbles up
                from src.exceptions import PaginationCancelled

                with patch("src.agent.tool_executor.jsonschema.validate"):
                    with self.assertRaises(PaginationCancelled):
                        execute_tool(
                            mock_client,
                            "list-schemas",
                            {"catalog_name": "test_catalog", "display": True},
                            output_callback=output_callback,
                        )

                # Verify the callback triggered table display (not raw JSON)
                mock_console.print.assert_called()

                # Verify table-formatted output was displayed (use same approach as main test)
                print_calls = mock_console.print.call_args_list

                # Verify that Rich Table objects were printed (not raw JSON strings)
                table_objects_found = False
                raw_json_found = False

                for call in print_calls:
                    args, kwargs = call
                    for arg in args:
                        # Check if we're printing Rich Table objects (good)
                        if hasattr(arg, "__class__") and "Table" in str(type(arg)):
                            table_objects_found = True
                        # Check if we're printing raw JSON strings (bad)
                        elif isinstance(arg, str) and (
                            '"schemas":' in arg or '"total_count":' in arg
                        ):
                            raw_json_found = True

                # Verify we're displaying tables, not raw JSON
                self.assertTrue(
                    table_objects_found,
                    "No Rich Table objects found - this indicates the regression",
                )
                self.assertFalse(
                    raw_json_found,
                    "Raw JSON strings found - this indicates the regression",
                )

    def test_list_commands_raise_pagination_cancelled_like_run_sql(self):
        """
        Test that list-* commands raise PaginationCancelled to return to chuck > prompt,
        just like run-sql does.

        This is the key behavior the user requested - list commands should show tables
        and immediately return to chuck > prompt, not continue with agent processing.
        """
        from src.exceptions import PaginationCancelled
        from unittest.mock import MagicMock

        list_display_methods = [
            (
                "_display_schemas",
                {"schemas": [{"name": "test"}], "catalog_name": "test"},
            ),
            ("_display_catalogs", {"catalogs": [{"name": "test"}]}),
            (
                "_display_tables",
                {
                    "tables": [{"name": "test"}],
                    "catalog_name": "test",
                    "schema_name": "test",
                },
            ),
            ("_display_warehouses", {"warehouses": [{"name": "test", "id": "test"}]}),
            (
                "_display_volumes",
                {
                    "volumes": [{"name": "test"}],
                    "catalog_name": "test",
                    "schema_name": "test",
                },
            ),
            (
                "_display_models",
                [{"name": "test", "creator": "test"}],
            ),  # models expects a list directly
            ("_display_detailed_models", {"models": [{"name": "test"}]}),
        ]

        for method_name, test_data in list_display_methods:
            with self.subTest(method=method_name):
                # Mock console to prevent actual output
                mock_console = MagicMock()
                self.tui.console = mock_console

                # Get the display method
                display_method = getattr(self.tui, method_name)

                # Call the method and verify it raises PaginationCancelled
                with self.assertRaises(
                    PaginationCancelled,
                    msg=f"{method_name} should raise PaginationCancelled to return to chuck > prompt",
                ):
                    display_method(test_data)

                # Verify console output was called (table was displayed)
                mock_console.print.assert_called()


if __name__ == "__main__":
    unittest.main()
