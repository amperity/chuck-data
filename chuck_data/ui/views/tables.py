"""Table view for list-tables command."""

from __future__ import annotations
from typing import Any, List

from chuck_data.ui.view_base import BaseView, TableViewMixin
from chuck_data.ui.theme import TABLE_TITLE_STYLE, WARNING_STYLE
from chuck_data.ui.format_utils import format_timestamp, humanize_row_count
from chuck_data.ui.styles import table_type_style
from chuck_data.exceptions import PaginationCancelled


class TablesTableView(BaseView, TableViewMixin):
    columns = [
        "name",
        "table_type",
        "column_count",
        "row_count",
        "created_at",
        "updated_at",
    ]
    headers = [
        "Table Name",
        "Type",
        "# Cols",
        "Rows",
        "Created",
        "Last Updated",
    ]

    def render(self, data: dict[str, Any]) -> None:
        """
        Render tables data to console.
        
        Args:
            data: A dictionary containing table data and rendering options
                 If data["display"] is True, rendering will complete without raising PaginationCancelled
        """
        from chuck_data.ui.table_formatter import display_table

        tables: List[dict[str, Any]] = data.get("tables", [])
        catalog_name = data.get("catalog_name", "")
        schema_name = data.get("schema_name", "")
        total_count = data.get("total_count", len(tables))
        display_mode = data.get("display", False)  # If True, suppress PaginationCancelled

        if not tables:
            self.console.print(
                f"[{WARNING_STYLE}]No tables found in {catalog_name}.{schema_name}[/{WARNING_STYLE}]"
            )
            if not display_mode:
                raise PaginationCancelled()
            return

        for t in tables:
            t["column_count"] = len(t.get("columns", []))
            for ts in ("created_at", "updated_at"):
                if ts in t and t[ts]:
                    t[ts] = format_timestamp(t[ts])
            if "row_count" in t:
                t["row_count"] = humanize_row_count(t["row_count"])

        style_map = {
            "table_type": table_type_style,
            "column_count": lambda v: "dim" if v == 0 else None,
        }

        title = (
            f"Tables in {catalog_name}.{schema_name} ({total_count} total)"
            if data.get("method") == "unity_catalog"
            else "Available Tables"
        )

        display_table(
            console=self.console,
            data=tables,
            columns=self.columns,
            headers=self.headers,
            title=title,
            style_map=style_map,
            column_alignments={"# Cols": "right", "Rows": "right"},
            title_style=TABLE_TITLE_STYLE,
            show_lines=True,
        )

        # Only raise PaginationCancelled if display_mode is False
        if not display_mode:
            raise PaginationCancelled()


# auto-register
from chuck_data.ui import view_registry  # noqa: E402

view_registry.register_view("list-tables", TablesTableView)
view_registry.register_view("tables", TablesTableView)
