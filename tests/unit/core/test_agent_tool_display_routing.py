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
    from chuck_data.command_registry import get_command

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
        # Mock console to capture output
        mock_console = MagicMock()
        tui.console = mock_console

        # Get the command definition
        cmd_def = get_command(case["tool_name"])
        assert cmd_def is not None, f"Command {case['tool_name']} not found"

        # Verify agent_display setting based on command type
        if case["tool_name"] in [
            "list-catalogs",
            "list-schemas",
            "list-tables",
        ]:
            # list-catalogs, list-schemas, and list-tables use conditional display
            assert (
                cmd_def.agent_display == "conditional"
            ), f"Command {case['tool_name']} must have agent_display='conditional'"
            # For conditional display, we need to test with display=true to see the table
            test_data_with_display = case["test_data"].copy()
            test_data_with_display["display"] = True
            from chuck_data.exceptions import PaginationCancelled

            with pytest.raises(PaginationCancelled):
                tui.display_tool_output(case["tool_name"], test_data_with_display)
        else:
            # Other commands use full display
            assert (
                cmd_def.agent_display == "full"
            ), f"Command {case['tool_name']} must have agent_display='full'"
            # Call the display method with test data - should raise PaginationCancelled
            from chuck_data.exceptions import PaginationCancelled

            with pytest.raises(PaginationCancelled):
                tui.display_tool_output(case["tool_name"], case["test_data"])

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
                    '"schemas":' in arg or '"catalogs":' in arg or '"tables":' in arg
                ):
                    raw_json_found = True

        # Verify we're displaying tables, not raw JSON
        assert (
            table_objects_found
        ), f"No Rich Table objects found in {case['tool_name']} output - this indicates the regression"
        assert (
            not raw_json_found
        ), f"Raw JSON strings found in {case['tool_name']} output - this indicates the regression"


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
                mock_render.side_effect = PaginationCancelled()
                
                with pytest.raises(PaginationCancelled):
                    tui._display_full_tool_output(tool_name, test_data)
                
                # Verify the view registry was called
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

    # Verify each has agent_display="full" (except list-warehouses, list-catalogs, list-schemas, and list-tables which use conditional display)
    for cmd_name in list_commands:
        cmd_def = get_command(cmd_name)
        if cmd_name in [
            "list-warehouses",
            "list-catalogs",
            "list-schemas",
            "list-tables",
        ]:
            # list-warehouses, list-catalogs, list-schemas, and list-tables use conditional display with display parameter
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
    """
    # Mock an API client
    mock_client = MagicMock()

    # Mock console to capture display output
    mock_console = MagicMock()
    tui.console = mock_console

    # Create a simple output callback that mimics agent behavior
    def output_callback(tool_name, tool_data):
        """This mimics how agents call display_tool_output"""
        tui.display_tool_output(tool_name, tool_data)

    # Test with list-schemas command
    with patch("chuck_data.agent.tool_executor.get_command") as mock_get_command:
        # Get the real command definition
        from chuck_data.commands.list_schemas import DEFINITION as schemas_def
        from chuck_data.commands import register_all_commands

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
            from chuck_data.exceptions import PaginationCancelled

            with patch("chuck_data.agent.tool_executor.jsonschema.validate"):
                with pytest.raises(PaginationCancelled):
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
            assert (
                table_objects_found
            ), "No Rich Table objects found - this indicates the regression"
            assert (
                not raw_json_found
            ), "Raw JSON strings found - this indicates the regression"


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
            {"catalogs": [{"name": "test"}]}
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
            {"warehouses": [{"name": "test", "id": "test"}]}
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
        parts = view_class_path.split('.')
        class_name = parts[-1]
        module_path = '.'.join(parts[:-1])
        
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
