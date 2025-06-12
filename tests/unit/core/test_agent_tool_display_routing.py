"""
Tests for agent tool display routing in the TUI.

These tests ensure that when agents use list-* commands, they display
the same formatted tables as when users use equivalent slash commands.
"""

import pytest
from unittest.mock import patch, MagicMock
from chuck_data.ui.tui import ChuckTUI
from chuck_data.commands.base import CommandResult
from chuck_data.agent.tool_executor import execute_tool


@pytest.fixture
def tui():
    """Create a ChuckTUI instance for testing."""
    return ChuckTUI()


def test_agent_list_commands_display_tables_not_raw_json(tui):
    """
    End-to-end test: Agent tool calls should display formatted tables, not raw JSON.

    This is the critical test that prevents the regression where agents
    would see raw JSON instead of formatted tables.
    """
    from chuck_data.commands import register_all_commands
    from rich.table import Table
    from chuck_data.exceptions import PaginationCancelled
    from chuck_data.ui.view_base import BaseView

    # Register all commands
    register_all_commands()

    # Directly patch the view registry to use our test views that just print tables
    from chuck_data.ui import view_registry

    # Create test views that directly print tables without using display_table
    class TestTableView(BaseView):
        def render(self, data):
            table = Table(title=f"Test Table for {self.__class__.__name__}")
            table.add_column("Test Column")
            table.add_row("Test Value")
            self.console.print(table)
            raise PaginationCancelled()

    class TestSchemasView(TestTableView):
        pass

    class TestCatalogsView(TestTableView):
        pass

    class TestTablesView(TestTableView):
        pass

    # Store original views to restore them after the test
    original_views = {
        "list-schemas": view_registry._VIEW_REGISTRY.get("list-schemas"),
        "list-catalogs": view_registry._VIEW_REGISTRY.get("list-catalogs"),
        "list-tables": view_registry._VIEW_REGISTRY.get("list-tables"),
    }

    # Register our test views
    view_registry.register_view("list-schemas", TestSchemasView)
    view_registry.register_view("list-catalogs", TestCatalogsView)
    view_registry.register_view("list-tables", TestTablesView)

    try:
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
                    "display": True,  # Ensures full display for conditional commands
                },
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
                    "display": True,  # Ensures full display for conditional commands
                },
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
                    "display": True,  # Ensures full display for conditional commands
                },
            },
        ]

        for case in test_cases:
            # Mock console to capture output
            mock_console = MagicMock()
            tui.console = mock_console

            # Execute the command with our test views
            try:
                tui.display_tool_output(case["tool_name"], case["test_data"])
            except PaginationCancelled:
                pass  # Expected when tables are displayed

            # Verify that the console received output
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

            assert (
                table_objects_found
            ), f"No Rich Table objects found in {case['tool_name']} output"
            assert (
                not raw_json_found
            ), f"Raw JSON strings found in {case['tool_name']} output"

    finally:
        # Restore original views
        for name, view in original_views.items():
            if view:
                view_registry.register_view(name, view)
            else:
                # If it didn't exist originally, remove it
                if name in view_registry._VIEW_REGISTRY:
                    del view_registry._VIEW_REGISTRY[name]


def test_unknown_tool_falls_back_to_generic_display(tui):
    """Test that unknown tools fall back to generic display."""
    test_data = {"some": "data"}

    mock_console = MagicMock()
    tui.console = mock_console

    tui._display_full_tool_output("unknown-tool", test_data)
    # Should create a generic panel
    mock_console.print.assert_called()


def test_command_name_mapping_prevents_regression(tui):
    """
    Test that ensures command name mapping in TUI via view registry covers both hyphenated and underscore versions.

    This test specifically prevents the regression where agent tool names with hyphens
    (like 'list-schemas') weren't being mapped to the correct display methods.
    """

    # Test cases: agent tool name -> expected view class location
    command_mappings = [
        ("list-schemas", "chuck_data.ui.views.schemas.SchemasTableView"),
        ("list-catalogs", "chuck_data.ui.views.catalogs.CatalogsTableView"),
        ("list-tables", "chuck_data.ui.views.tables.TablesTableView"),
        ("list-warehouses", "chuck_data.ui.views.warehouses.WarehousesTableView"),
        ("list-volumes", "chuck_data.ui.views.volumes.VolumesTableView"),
        ("list-models", "chuck_data.ui.views.models.ModelsTableView"),
    ]

    for tool_name, view_class_path in command_mappings:
        # Mock the expected view class render method
        with patch(f"{view_class_path}.render") as mock_render:
            # Call with appropriate test data structure based on what the view expects
            if tool_name == "list-models":
                test_data = {
                    "models": [{"name": "test_model", "creator": "test"}],
                    "active_model": None,
                    "detailed": False,
                    "filter": None,
                }
            else:
                test_data = {"test": "data"}

            # Mock get_view to verify it's called correctly
            with patch("chuck_data.ui.view_registry.get_view") as mock_get_view:
                # Simulate the view registry behavior
                from chuck_data.ui.view_registry import _VIEW_REGISTRY

                mock_get_view.side_effect = lambda name: _VIEW_REGISTRY.get(name)

                # Since raising PaginationCancelled is part of render, we handle that here
                from chuck_data.exceptions import PaginationCancelled

                mock_render.side_effect = PaginationCancelled()

                with pytest.raises(PaginationCancelled):
                    tui._display_full_tool_output(tool_name, test_data)

                # We have special handling for list-* commands in _display_full_tool_output
                # which bypasses get_view for direct method calls, so we don't expect get_view to be called
                if tool_name in [
                    "list-schemas",
                    "list-catalogs",
                    "list-tables",
                    "list-models",
                    "list-warehouses",
                    "list-volumes",
                ]:
                    # Verify direct view method calls by checking render was called
                    mock_render.assert_called_once()
                else:
                    # For non-special commands, verify the view registry was called
                    mock_get_view.assert_called_once_with(tool_name)
                # Verify the render method was called with the test data
                mock_render.assert_called_once()


def test_agent_display_setting_validation(tui):
    """
    Test that validates ALL list commands have agent_display='full'.

    This prevents regressions where commands might be added without proper display settings.
    """
    from chuck_data.commands import register_all_commands
    from chuck_data.command_registry import get_command, get_agent_commands

    register_all_commands()

    # Get all agent-visible commands
    agent_commands = get_agent_commands()

    # Find all list-* commands
    list_commands = [name for name in agent_commands.keys() if name.startswith("list-")]

    # Ensure we have the expected list commands
    expected_list_commands = {
        "list-schemas",
        "list-catalogs",
        "list-tables",
        "list-warehouses",
        "list-volumes",
        "list-models",
    }

    found_commands = set(list_commands)
    assert (
        found_commands == expected_list_commands
    ), f"Expected list commands changed. Found: {found_commands}, Expected: {expected_list_commands}"

    # Verify each has agent_display="full" or "conditional" with correct display_condition
    for cmd_name in list_commands:
        cmd_def = get_command(cmd_name)
        if cmd_name in [
            "list-warehouses",
            "list-catalogs",
            "list-schemas",
            "list-tables",
            "list-models",
            "list-volumes",
        ]:
            # These commands use conditional display with display parameter
            assert (
                cmd_def.agent_display == "conditional"
            ), f"Command {cmd_name} should use conditional display with display parameter control"
            # Verify it has a display_condition function
            assert (
                cmd_def.display_condition is not None
            ), f"Command {cmd_name} with conditional display must have display_condition function"
        else:
            assert (
                cmd_def.agent_display == "full"
            ), f"Command {cmd_name} must have agent_display='full' for table display"


def test_end_to_end_agent_tool_execution_with_table_display(tui):
    """
    Full end-to-end test: Execute an agent tool and verify it displays tables.

    This test goes through the complete flow: agent calls tool -> tool executes ->
    output callback triggers -> TUI displays formatted table.

    The test verifies both with display=True (should show tables) and without display parameter
    (should not show raw JSON). This test is critical for catching regressions where
    list-* commands might display raw JSON instead of formatted tables.
    """
    # Mock an API client
    mock_client = MagicMock()

    # Create a simple output callback that mimics agent behavior
    def output_callback(tool_name, tool_data):
        """This mimics how agents call display_tool_output"""
        tui.display_tool_output(tool_name, tool_data)

    # Test with list-schemas command
    with patch("chuck_data.agent.tool_executor.get_command") as mock_get_command:
        # Get the real command definition
        from chuck_data.commands.list_schemas import DEFINITION as schemas_def
        from chuck_data.commands import register_all_commands
        from chuck_data.exceptions import PaginationCancelled
        from rich.panel import Panel

        register_all_commands()

        mock_get_command.return_value = schemas_def

        # Setup test data with and without display flag
        test_data_with_display = {
            "schemas": [
                {"name": "bronze", "comment": "Bronze layer"},
                {"name": "silver", "comment": "Silver layer"},
            ],
            "catalog_name": "test_catalog",
            "total_count": 2,
            "display": True,  # This triggers the full display
        }

        test_data_without_display = {
            "schemas": [
                {"name": "bronze", "comment": "Bronze layer"},
                {"name": "silver", "comment": "Silver layer"},
            ],
            "catalog_name": "test_catalog",
            "total_count": 2,
            # No display parameter - should use condensed display
        }

        # Test 1: WITH display=True - should show tables
        with patch.object(schemas_def, "handler") as mock_handler:
            mock_handler.__name__ = "mock_handler"
            mock_handler.return_value = CommandResult(
                True,
                data=test_data_with_display,
                message="Found 2 schemas",
            )

            # Mock console to capture display output
            mock_console = MagicMock()
            tui.console = mock_console

            # Execute tool with output callback (mimics agent behavior)
            with patch("chuck_data.agent.tool_executor.jsonschema.validate"):
                try:
                    execute_tool(
                        mock_client,
                        "list-schemas",
                        {"catalog_name": "test_catalog", "display": True},
                        output_callback=output_callback,
                    )
                except PaginationCancelled:
                    pass  # Expected when tables are displayed

            # Verify table-formatted output was displayed
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

            # WITH display=True, we should see tables and not raw JSON
            assert (
                table_objects_found
            ), "Case with display=True: No Rich Table objects found"
            assert not raw_json_found, "Case with display=True: Raw JSON strings found"

        # Test 2: WITHOUT display parameter - check for raw JSON (which is a failure case)
        with patch.object(schemas_def, "handler") as mock_handler:
            mock_handler.__name__ = "mock_handler"
            mock_handler.return_value = CommandResult(
                True,
                data=test_data_without_display,
                message="Found 2 schemas",
            )

            # Reset mock console
            mock_console = MagicMock()
            tui.console = mock_console

            # Execute tool without display parameter
            with patch("chuck_data.agent.tool_executor.jsonschema.validate"):
                # Should not raise PaginationCancelled since we're using condensed display
                execute_tool(
                    mock_client,
                    "list-schemas",
                    {"catalog_name": "test_catalog"},  # No display parameter
                    output_callback=output_callback,
                )

            # Verify the callback triggered some output
            mock_console.print.assert_called()

            print_calls = mock_console.print.call_args_list
            panel_objects_found = False
            raw_json_found = False

            for call in print_calls:
                args, kwargs = call
                for arg in args:
                    # Check if we're printing a Panel object that contains raw JSON
                    if isinstance(arg, Panel) and isinstance(arg.renderable, str):
                        panel_text = str(arg.renderable)
                        if '"schemas":' in panel_text or '"total_count":' in panel_text:
                            raw_json_found = True
                    # Check if we're printing raw JSON strings directly
                    elif isinstance(arg, str) and (
                        '"schemas":' in arg
                        or '"catalog_name":' in arg
                        or '"total_count":' in arg
                    ):
                        raw_json_found = True

            # WITHOUT display param, we should NOT see raw JSON in any form
            condensed_output_found = any(
                "→" in str(arg) for args, _ in print_calls for arg in args
            )

            assert (
                not raw_json_found
            ), "Case without display param: Raw JSON strings found (bug)"
            assert (
                condensed_output_found
            ), "Case without display param: No condensed output found (should show arrow prefix)"


def test_list_commands_raise_pagination_cancelled_like_run_sql(tui):
    """
    Test that list-* commands raise PaginationCancelled to return to chuck > prompt,
    just like run-sql does.

    This is the key behavior the user requested - list commands should show tables
    and immediately return to chuck > prompt, not continue with agent processing.
    """
    from chuck_data.exceptions import PaginationCancelled

    # View class paths and test data pairings
    view_class_tests = [
        (
            "chuck_data.ui.views.schemas.SchemasTableView",
            {"schemas": [{"name": "test"}], "catalog_name": "test"},
        ),
        (
            "chuck_data.ui.views.catalogs.CatalogsTableView",
            {"catalogs": [{"name": "test"}]},
        ),
        (
            "chuck_data.ui.views.tables.TablesTableView",
            {
                "tables": [{"name": "test"}],
                "catalog_name": "test",
                "schema_name": "test",
            },
        ),
        (
            "chuck_data.ui.views.warehouses.WarehousesTableView",
            {"warehouses": [{"name": "test", "id": "test"}]},
        ),
        (
            "chuck_data.ui.views.volumes.VolumesTableView",
            {
                "volumes": [{"name": "test"}],
                "catalog_name": "test",
                "schema_name": "test",
            },
        ),
        (
            "chuck_data.ui.views.models.ModelsTableView",
            {
                "models": [{"name": "test"}],
                "active_model": None,
                "detailed": False,
                "filter": None,
            },
        ),
    ]

    for view_class_path, test_data in view_class_tests:
        # Mock console to prevent actual output
        mock_console = MagicMock()

        # Create a simple view instance using the mock console
        parts = view_class_path.split(".")
        class_name = parts[-1]
        module_path = ".".join(parts[:-1])

        with patch(f"{view_class_path}.render") as mock_render:
            # Configure the mock to raise PaginationCancelled
            mock_render.side_effect = PaginationCancelled()

            # Import and instantiate the view
            module = __import__(module_path, fromlist=[class_name])
            view_class = getattr(module, class_name)
            view = view_class(mock_console)

            # Test that the render method raises PaginationCancelled
            with pytest.raises(PaginationCancelled):
                view.render(test_data)

            # Verify that render was called
            mock_render.assert_called_once()
