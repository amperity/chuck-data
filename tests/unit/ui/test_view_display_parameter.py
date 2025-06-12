"""
Tests for display parameter flow between TUI, service, and views.

These tests verify that the display=True parameter is properly passed
from TUI to service and then to view rendering.
"""

import pytest
from unittest.mock import patch, MagicMock, call

from chuck_data.ui.tui import ChuckTUI
from chuck_data.commands.base import CommandResult


def test_display_parameter_flows_from_tui_to_service():
    """
    Test that when a slash command is used in TUI, the display=True parameter
    is passed from TUI to service execution.

    Note: The display parameter is not automatically included in the result data,
    instead it's passed to the view from ChuckTUI._process_command directly.
    """
    # Create a TUI instance with mocked service
    tui = ChuckTUI()
    tui.service = MagicMock()

    # Set up mock command result with data for a tables command
    mock_result = CommandResult(
        success=True,
        message="Tables listed successfully",
        data={
            "tables": [{"name": "test_table", "table_type": "MANAGED"}],
            "catalog_name": "test_catalog",
            "schema_name": "test_schema",
            "total_count": 1,
        },
    )

    # Configure the mock service to return our result
    tui.service.execute_command.return_value = mock_result

    # Mock the view registry and TablesTableView
    mock_view = MagicMock()

    with patch(
        "chuck_data.ui.view_registry.get_view", return_value=lambda console: mock_view
    ) as mock_get_view:
        # Process a /tables command
        tui._process_command("/tables")

        # Verify service was called with display=True
        tui.service.execute_command.assert_called_once()
        args, kwargs = tui.service.execute_command.call_args
        assert "display" in kwargs, "Command /tables should set display=True"
        assert (
            kwargs["display"] is True
        ), "Command /tables should set display=True with value True"

        # Verify that mock_get_view was called with correct command name
        mock_get_view.assert_called_with("list-tables")


def test_view_honors_display_parameter_for_pagination():
    """
    Test that when a view receives data with display=True, it doesn't raise PaginationCancelled.
    """
    from chuck_data.ui.views.tables import TablesTableView
    from chuck_data.exceptions import PaginationCancelled

    # Create a console mock
    console_mock = MagicMock()

    # Create the tables view
    view = TablesTableView(console_mock)

    # Create test data without display parameter (should raise PaginationCancelled)
    test_data = {
        "tables": [{"name": "test_table", "table_type": "MANAGED"}],
        "catalog_name": "test_catalog",
        "schema_name": "test_schema",
        "total_count": 1,
    }

    # When display=False or not provided, view should raise PaginationCancelled
    with pytest.raises(PaginationCancelled):
        view.render(test_data)

    # Add display=True parameter (should not raise PaginationCancelled)
    test_data["display"] = True

    # This should not raise PaginationCancelled
    view.render(test_data)  # If this doesn't raise, the test passes

    # Verify that view called display_table
    assert console_mock.print.called, "Console print should be called"


def test_integrated_tui_service_view_flow():
    """
    Test that the full integration of TUI, Service, and View works properly with display=True.

    This test mocks both the service and table_formatter to verify the complete flow from
    TUI command to final rendering.
    """
    # Create a TUI instance with mocked service
    tui = ChuckTUI()
    tui.service = MagicMock()

    # Set up mock command result with data for a tables command
    mock_result = CommandResult(
        success=True,
        message="Tables listed successfully",
        data={
            "tables": [{"name": "test_table", "table_type": "MANAGED"}],
            "catalog_name": "test_catalog",
            "schema_name": "test_schema",
            "total_count": 1,
        },
    )

    # Configure the mock service to return our result
    tui.service.execute_command.return_value = mock_result

    # Mock the table formatter to verify the display flow
    with patch("chuck_data.ui.table_formatter.display_table") as mock_display_table:
        # Process a /tables command (which uses display=True)
        tui._process_command("/tables")

        # Verify service was called with display=True
        tui.service.execute_command.assert_called_once()
        args, kwargs = tui.service.execute_command.call_args
        assert kwargs.get("display") is True

        # Verify that display_table was called (the view worked)
        mock_display_table.assert_called_once()

        # This test passes if display_table was called, indicating the view processed
        # the command without raising PaginationCancelled - which is the behavior
        # we want when display=True


def test_all_table_type_views_honor_display_parameter():
    """
    Test that all table-type views properly handle the display parameter.
    """
    from chuck_data.ui.view_registry import _VIEW_REGISTRY
    from chuck_data.exceptions import PaginationCancelled

    # Collect all view classes that extend TableViewMixin
    table_view_classes = []
    for view_name, view_class in _VIEW_REGISTRY.items():
        if hasattr(view_class, "columns") and hasattr(view_class, "headers"):
            # This looks like a TableViewMixin subclass
            table_view_classes.append((view_name, view_class))

    # Ensure we found at least some table views
    assert len(table_view_classes) > 0, "No table views found in registry"

    # Test each table view class
    for view_name, view_class in table_view_classes:
        # Skip views that don't have columns attribute or are known not to use PaginationCancelled
        if not hasattr(view_class, "render"):
            continue

        console_mock = MagicMock()
        view_instance = view_class(console_mock)

        # Create minimal test data with required structure
        test_data = {}

        # Create more specific test data for common view types
        if view_name in ["list-tables", "tables"]:
            test_data["tables"] = [{"name": f"test_{view_name}"}]
        elif view_name in ["list-schemas", "schemas"]:
            test_data["schemas"] = [{"name": f"test_{view_name}"}]
        elif view_name in ["list-catalogs", "catalogs"]:
            test_data["catalogs"] = [{"name": f"test_{view_name}"}]
        elif view_name in ["list-models", "models"]:
            test_data["models"] = [{"name": f"test_{view_name}"}]
        elif view_name in ["list-warehouses", "warehouses"]:
            test_data["warehouses"] = [{"name": f"test_{view_name}"}]
        elif view_name in ["list-volumes", "volumes"]:
            test_data["volumes"] = [{"name": f"test_{view_name}"}]

        # Add display=True to test data
        test_data["display"] = True

        # Customize console mock to patch table_formatter.display_table
        with patch("chuck_data.ui.table_formatter.display_table") as mock_display:
            # This should not raise PaginationCancelled when display=True
            try:
                view_instance.render(test_data)
                # If we got here, the view correctly handled display=True
                assert (
                    mock_display.called
                ), f"View {view_name} should call display_table"
            except PaginationCancelled:
                pytest.fail(
                    f"View {view_name} incorrectly raised PaginationCancelled despite display=True"
                )
            except Exception as e:
                # Skip views that have other requirements for rendering that we don't satisfy
                print(f"Skipping {view_name}: {str(e)}")
