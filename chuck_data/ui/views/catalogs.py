"""Table view for list-catalogs command."""

from __future__ import annotations
from typing import Any

from chuck_data.ui.view_base import BaseView, TableViewMixin
from chuck_data.ui.theme import TABLE_TITLE_STYLE, WARNING_STYLE
from chuck_data.exceptions import PaginationCancelled


class CatalogsTableView(BaseView, TableViewMixin):
    columns = ["name", "type", "comment", "owner"]
    headers = ["Name", "Type", "Comment", "Owner"]

    def render(self, data: dict[str, Any]):
        """
        Render catalogs data to console.

        Args:
            data: A dictionary containing catalog data and rendering options
                 If data["display"] is True, rendering will complete without raising PaginationCancelled
        """
        from chuck_data.ui.table_formatter import display_table

        catalogs = data.get("catalogs", [])
        current_catalog = data.get("current_catalog")
        display_mode = data.get(
            "display", False
        )  # If True, suppress PaginationCancelled
        # Add debug to see if display flag is actually coming through
        print(
            f"DEBUG: CatalogsView display_mode={display_mode}, got display={data.get('display')}"
        )

        if not catalogs:
            self.console.print(f"[{WARNING_STYLE}]No catalogs found.[/{WARNING_STYLE}]")
            if not display_mode:
                raise PaginationCancelled()
            return

        def name_style(name):
            return "bold green" if name == current_catalog else None

        style_map = {"name": name_style}

        for cat in catalogs:
            if "type" in cat and cat["type"]:
                cat["type"] = cat["type"].lower()

        display_table(
            console=self.console,
            data=catalogs,
            columns=self.columns,
            headers=self.headers,
            title="Available Catalogs",
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=False,
        )

        if current_catalog:
            self.console.print(
                f"\nCurrent catalog: [bold green]{current_catalog}[/bold green]"
            )

        # Only raise PaginationCancelled if display_mode is False
        if not display_mode:
            raise PaginationCancelled()


# --- auto-registration -----------------
from chuck_data.ui import view_registry  # noqa: E402, I001

view_registry.register_view("list-catalogs", CatalogsTableView)
view_registry.register_view("catalogs", CatalogsTableView)
