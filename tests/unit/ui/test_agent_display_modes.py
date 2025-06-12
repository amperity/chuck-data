"""
Tests for agent display modes in TUI.

These tests focus specifically on the behavior of commands with different display parameters:
1. With display=True - should show formatted tables
2. Without display parameter - should use condensed display (never raw JSON)
3. With display=False - should use condensed display
"""

import pytest
from unittest.mock import patch, MagicMock, call
from chuck_data.ui.tui import ChuckTUI
from chuck_data.commands.base import CommandResult
from chuck_data.exceptions import PaginationCancelled


@pytest.fixture
def tui():
    """Create a ChuckTUI instance for testing."""
    return ChuckTUI()


@pytest.fixture
def mock_console():
    """Create a mock console for testing."""
    return MagicMock()


def test_list_commands_without_display_param_use_condensed_mode(tui, mock_console):
    """
    Test that list-* commands without display parameter use condensed display mode.

    This is a critical test to ensure that agent tools never show raw JSON when
    called without an explicit display parameter.
    """
    from chuck_data.commands import register_all_commands

    register_all_commands()

    # Set the mock console
    tui.console = mock_console

    # These are all the list-* commands that should support condensed display
    test_cases = [
        {
            "tool_name": "list-schemas",
            "test_data": {
                "schemas": [{"name": "bronze", "comment": "Bronze layer"}],
                "catalog_name": "test_catalog",
                "total_count": 1,
                # No display parameter
            },
        },
        {
            "tool_name": "list-catalogs",
            "test_data": {
                "catalogs": [{"name": "catalog1", "type": "MANAGED"}],
                "total_count": 1,
                # No display parameter
            },
        },
        {
            "tool_name": "list-tables",
            "test_data": {
                "tables": [{"name": "table1", "table_type": "MANAGED"}],
                "catalog_name": "test_catalog",
                "schema_name": "test_schema",
                "total_count": 1,
                # No display parameter
            },
        },
        {
            "tool_name": "list-models",
            "test_data": {
                "models": [{"name": "model1", "creator": "user"}],
                "active_model": None,
                # No display parameter
            },
        },
        {
            "tool_name": "list-warehouses",
            "test_data": {
                "warehouses": [{"name": "warehouse1", "id": "abc123"}],
                # No display parameter
            },
        },
        {
            "tool_name": "list-volumes",
            "test_data": {
                "volumes": [{"name": "volume1"}],
                "catalog_name": "test_catalog",
                "schema_name": "test_schema",
                # No display parameter
            },
        },
    ]

    # Create patches for all the direct method calls to avoid PaginationCancelled
    # This lets us test the routing without the display methods raising exceptions
    method_patches = {}
    # Map tool names to their actual method names
    method_mappings = {
        "list-schemas": "_display_schemas",
        "list-catalogs": "_display_catalogs",
        "list-tables": "_display_tables",
        "list-models": "_display_models_consolidated",
        "list-warehouses": "_display_warehouses",
        "list-volumes": "_display_volumes",
    }
    for tool_name, method_name in method_mappings.items():
        method_patches[tool_name] = patch.object(tui, method_name, return_value=None)

    # Create view patches to handle any views that might be called
    view_patches = [
        patch("chuck_data.ui.views.schemas.SchemasTableView.render"),
        patch("chuck_data.ui.views.catalogs.CatalogsTableView.render"),
        patch("chuck_data.ui.views.tables.TablesTableView.render"),
        patch("chuck_data.ui.views.models.ModelsTableView.render"),
        patch("chuck_data.ui.views.warehouses.WarehousesTableView.render"),
        patch("chuck_data.ui.views.volumes.VolumesTableView.render"),
    ]

    # Start all patches
    started_method_patches = {k: v.start() for k, v in method_patches.items()}
    started_view_patches = [p.start() for p in view_patches]

    try:
        for case in test_cases:
            # Reset mock console for each test case
            mock_console.reset_mock()

            # Call display_tool_output with each test case
            tui.display_tool_output(case["tool_name"], case["test_data"])

            # Verify console was used
            assert (
                mock_console.print.called
            ), f"Console print not called for {case['tool_name']}"

            # Check the actual calls to verify condensed output
            print_calls = mock_console.print.call_args_list

            # In condensed mode, we expect the output to contain "→" arrow indicator
            # and NOT contain any raw JSON strings
            arrow_prefix_found = False
            raw_json_found = False

            for call_obj in print_calls:
                args, _ = call_obj
                for arg in args:
                    arg_str = str(arg)
                    # Check for condensed format indicators
                    if "→" in arg_str:
                        arrow_prefix_found = True
                    # Check for raw JSON indicators
                    if any(
                        f'"{key}":' in arg_str
                        for key in [
                            "schemas",
                            "catalogs",
                            "tables",
                            "models",
                            "warehouses",
                            "volumes",
                        ]
                    ):
                        raw_json_found = True

            # Verify we got condensed output, not raw JSON
            assert (
                arrow_prefix_found
            ), f"Condensed output format (→) not found for {case['tool_name']}"
            assert (
                not raw_json_found
            ), f"Raw JSON found in output for {case['tool_name']}"

            # Also verify the direct method calls were NOT made
            # Condensed mode should use _display_condensed_tool_output, not the direct methods
            if case["tool_name"] in started_method_patches:
                method_mock = started_method_patches[case["tool_name"]]
                assert (
                    not method_mock.called
                ), f"Direct method called for {case['tool_name']} without display param"

    finally:
        # Stop all patches
        for patch_obj in started_method_patches.values():
            patch_obj.stop()
        for patch_obj in started_view_patches:
            patch_obj.stop()


def test_list_commands_with_display_false_use_condensed_mode(tui, mock_console):
    """
    Test that list-* commands with display=False explicitly use condensed display mode.
    """
    from chuck_data.commands import register_all_commands

    register_all_commands()

    # Set the mock console
    tui.console = mock_console

    # Test case with display=False explicitly set
    test_cases = [
        {
            "tool_name": "list-schemas",
            "test_data": {
                "schemas": [{"name": "bronze"}],
                "catalog_name": "test_catalog",
                "display": False,  # Explicitly set to false
            },
        },
        {
            "tool_name": "list-tables",
            "test_data": {
                "tables": [{"name": "table1"}],
                "catalog_name": "test_catalog",
                "schema_name": "test_schema",
                "display": False,  # Explicitly set to false
            },
        },
    ]

    # Test each case
    for case in test_cases:
        # Reset mock console for each test case
        mock_console.reset_mock()

        # Call display_tool_output with the test case
        tui.display_tool_output(case["tool_name"], case["test_data"])

        # Verify console was used
        assert (
            mock_console.print.called
        ), f"Console print not called for {case['tool_name']}"

        # Check for condensed output format
        condensed_output_found = False
        raw_json_found = False

        for call_obj in mock_console.print.call_args_list:
            args, _ = call_obj
            for arg in args:
                arg_str = str(arg)
                # Check for condensed format indicators
                if "→" in arg_str:
                    condensed_output_found = True
                # Check for raw JSON indicators
                if any(f'"{key}":' in arg_str for key in case["test_data"].keys()):
                    raw_json_found = True

        # Verify we got condensed output, not raw JSON
        assert (
            condensed_output_found
        ), f"Condensed output format not found for {case['tool_name']} with display=False"
        assert (
            not raw_json_found
        ), f"Raw JSON found in output for {case['tool_name']} with display=False"


def test_list_commands_with_empty_data_handled_gracefully(tui, mock_console):
    """
    Test that list-* commands with empty data lists use condensed display and handle it gracefully.
    """
    # Set the mock console
    tui.console = mock_console

    # Test cases with empty data lists
    test_cases = [
        {
            "tool_name": "list-schemas",
            "test_data": {
                "schemas": [],  # Empty list
                "catalog_name": "test_catalog",
                "total_count": 0,
            },
        },
        {
            "tool_name": "list-tables",
            "test_data": {
                "tables": [],  # Empty list
                "catalog_name": "test_catalog",
                "schema_name": "test_schema",
                "total_count": 0,
            },
        },
        {
            "tool_name": "list-catalogs",
            "test_data": {
                "catalogs": None,  # None instead of a list
                "total_count": 0,
            },
        },
    ]

    # Test each case
    for case in test_cases:
        # Reset mock console for each test case
        mock_console.reset_mock()

        # Call display_tool_output with the test case
        tui.display_tool_output(case["tool_name"], case["test_data"])

        # Verify console was used
        assert (
            mock_console.print.called
        ), f"Console print not called for {case['tool_name']} with empty data"

        # Verify no exceptions were thrown (if we got here, that's already true)
        # Check for error indicators in console output
        console_output = "".join(
            str(c[0][0]) for c in mock_console.print.call_args_list if c[0]
        )
        assert (
            "Error" not in console_output
        ), f"Error displayed for {case['tool_name']} with empty data"

        # In condensed mode, we expect a count of 0 to be shown
        items_count_shown = (
            "0 items" in console_output
            or "0 total" in console_output
            or "0" in console_output
        )
        assert (
            items_count_shown
        ), f"Count of 0 not shown for {case['tool_name']} with empty data"


def test_command_with_display_true_uses_full_display(tui, mock_console):
    """
    Test that commands with display=True explicitly use full display mode.

    This tests the specific path that should trigger the direct method calls.
    """
    # Set the mock console
    tui.console = mock_console

    # Since we need to test the routing logic to specific methods,
    # we'll patch those methods to avoid PaginationCancelled exceptions
    with (
        patch.object(tui, "_display_schemas") as mock_display_schemas,
        patch.object(tui, "_display_tables") as mock_display_tables,
        patch.object(tui, "_display_catalogs") as mock_display_catalogs,
        patch.object(tui, "_display_models_consolidated") as mock_display_models,
    ):

        # Test cases with display=True explicitly set
        tui.display_tool_output(
            "list-schemas",
            {
                "schemas": [{"name": "bronze"}],
                "catalog_name": "test_catalog",
                "display": True,
            },
        )
        mock_display_schemas.assert_called_once()

        tui.display_tool_output(
            "list-tables",
            {
                "tables": [{"name": "table1"}],
                "catalog_name": "test_catalog",
                "schema_name": "test_schema",
                "display": True,
            },
        )
        mock_display_tables.assert_called_once()

        tui.display_tool_output(
            "list-catalogs", {"catalogs": [{"name": "catalog1"}], "display": True}
        )
        mock_display_catalogs.assert_called_once()

        tui.display_tool_output(
            "list-models", {"models": [{"name": "model1"}], "display": True}
        )
        mock_display_models.assert_called_once()


def test_display_route_selection_logic(tui, mock_console):
    """
    Test the logic that decides whether to use full or condensed display.

    This ensures commands are correctly routed based on their agent_display setting
    and parameters.
    """
    from chuck_data.commands import register_all_commands

    register_all_commands()
    from chuck_data.command_registry import get_command

    # Set the mock console
    tui.console = mock_console

    with (
        patch.object(tui, "_display_full_tool_output") as mock_full,
        patch.object(tui, "_display_condensed_tool_output") as mock_condensed,
    ):

        # Test 1: Command with agent_display='full' should always use full display
        command_def = get_command(
            "list-models"
        )  # This should have agent_display='full'

        # Patch command_def to ensure agent_display is 'full'
        with patch.object(command_def, "agent_display", "full"):
            # Reset mocks
            mock_full.reset_mock()
            mock_condensed.reset_mock()

            # Call display_tool_output with agent_display='full' command
            tui.display_tool_output("list-models", {"models": []})

            # Should use full display
            mock_full.assert_called_once()
            mock_condensed.assert_not_called()

        # Test 2: Command with agent_display='condensed' should always use condensed display
        command_def = get_command(
            "status"
        )  # This should have agent_display='condensed'
        if command_def:
            # Patch command_def to ensure agent_display is 'condensed'
            with patch.object(command_def, "agent_display", "condensed"):
                # Reset mocks
                mock_full.reset_mock()
                mock_condensed.reset_mock()

                # Call display_tool_output with agent_display='condensed' command
                tui.display_tool_output("status", {"workspace_url": "test"})

                # Should use condensed display
                mock_full.assert_not_called()
                mock_condensed.assert_called_once()

        # Test 3: Command with agent_display='conditional' should use display parameter
        command_def = get_command(
            "list-schemas"
        )  # This should have agent_display='conditional'

        # Patch command_def to ensure agent_display is 'conditional'
        with patch.object(command_def, "agent_display", "conditional"):
            # First with display=True
            mock_full.reset_mock()
            mock_condensed.reset_mock()

            # Set up display_condition function to check for 'display' key
            def mock_condition(result):
                return result.get("display", False)

            with patch.object(command_def, "display_condition", mock_condition):
                # Call with display=True
                tui.display_tool_output(
                    "list-schemas", {"schemas": [], "display": True}
                )

                # Should use full display
                mock_full.assert_called_once()
                mock_condensed.assert_not_called()

                # Reset mocks for display=False test
                mock_full.reset_mock()
                mock_condensed.reset_mock()

                # Call with display=False
                tui.display_tool_output(
                    "list-schemas", {"schemas": [], "display": False}
                )

                # Should use condensed display
                mock_full.assert_not_called()
                mock_condensed.assert_called_once()


def test_condensed_display_key_metrics_shown(tui, mock_console):
    """
    Test that condensed display shows key metrics from the result.

    This ensures that users get useful information in condensed display mode,
    rather than just a simple "command executed" message.
    """
    # Set the mock console
    tui.console = mock_console

    # Test different metric extraction patterns
    test_cases = [
        {
            "tool_name": "list-schemas",
            "test_data": {
                "schemas": [{"name": "bronze"}, {"name": "silver"}],
                "total_count": 2,
            },
            "expected_metric": "2 items",
        },
        {
            "tool_name": "status",
            "test_data": {
                "workspace_url": "https://test.cloud.databricks.com",
                "connection_status": "Connected",
                "active_catalog": "my_catalog",
                "active_schema": "bronze",
            },
            "expected_metric": "workspace: test.cloud.databricks.com",
        },
        {
            "tool_name": "select-catalog",
            "test_data": {
                "catalog_name": "my_catalog",
                "step": "Catalog set (Name: my_catalog)",
            },
            "expected_metric": "Catalog set (Name: my_catalog)",
        },
    ]

    for case in test_cases:
        # Reset mock console for each test case
        mock_console.reset_mock()

        # Call display_tool_output with the test case
        tui.display_tool_output(case["tool_name"], case["test_data"])

        # Verify console was used
        assert (
            mock_console.print.called
        ), f"Console print not called for {case['tool_name']}"

        # Combine all console output into a single string for easier checking
        console_output = "".join(
            str(c[0][0]) if c[0] else "" for c in mock_console.print.call_args_list
        )

        # Check that expected metric is in the output
        assert (
            case["expected_metric"] in console_output
        ), f"Expected metric '{case['expected_metric']}' not found for {case['tool_name']}"
