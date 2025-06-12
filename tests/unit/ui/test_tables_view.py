"""
Tests for TablesTableView.

Tests the TablesTableView component's rendering behavior,
particularly focusing on handling display=True without raising PaginationCancelled.
"""

import pytest
from unittest.mock import MagicMock, patch
from rich.console import Console

# Import the class under test directly, but mock specific calls
from chuck_data.ui.views.tables import TablesTableView
from chuck_data.exceptions import PaginationCancelled


class TestTablesTableView:
    def test_render_empty_tables(self):
        """Test rendering when tables list is empty."""
        console = MagicMock(spec=Console)
        view = TablesTableView(console)

        # Prepare test data - explicitly set display=False to ensure PaginationCancelled is raised
        data = {
            "tables": [],
            "catalog_name": "test_catalog",
            "schema_name": "test_schema",
            "display": False,
        }

        # Empty tables should raise PaginationCancelled due to display=False
        with pytest.raises(PaginationCancelled):
            view.render(data)

        # Verify error message was printed
        console.print.assert_called_once()
        args = console.print.call_args[0][0]
        assert "No tables found in test_catalog.test_schema" in args

    def test_render_with_tables(self):
        """Test rendering with table data."""
        console = MagicMock(spec=Console)
        view = TablesTableView(console)

        # Prepare test data with some tables - explicitly set display=False
        data = {
            "tables": [
                {
                    "name": "table1",
                    "table_type": "MANAGED",
                    "columns": ["col1", "col2"],
                    "row_count": 100,
                    "created_at": "2025-01-01T00:00:00Z",
                    "updated_at": "2025-01-02T00:00:00Z",
                }
            ],
            "catalog_name": "test_catalog",
            "schema_name": "test_schema",
            "display": False,
        }

        # Verify display_table is called and PaginationCancelled is raised
        with patch("chuck_data.ui.table_formatter.display_table") as mock_display_table:
            with pytest.raises(PaginationCancelled):
                view.render(data)
            mock_display_table.assert_called_once()

    def test_render_with_display_flag(self):
        """Test rendering with display=True flag to suppress PaginationCancelled."""
        console = MagicMock(spec=Console)
        view = TablesTableView(console)

        # Prepare test data with display flag
        data = {
            "tables": [
                {
                    "name": "table1",
                    "table_type": "MANAGED",
                    "columns": ["col1", "col2"],
                    "row_count": 100,
                    "created_at": "2025-01-01T00:00:00Z",
                    "updated_at": "2025-01-02T00:00:00Z",
                }
            ],
            "catalog_name": "test_catalog",
            "schema_name": "test_schema",
            "display": True,  # This flag should suppress PaginationCancelled
        }

        # Mock display_table to prevent actual rendering
        with patch("chuck_data.ui.table_formatter.display_table"):
            # Should not raise PaginationCancelled when display=True
            view.render(data)  # This should not raise an exception
