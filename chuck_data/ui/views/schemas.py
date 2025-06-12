"""View for list-schemas command."""
from __future__ import annotations
from typing import Any
from chuck_data.ui.view_base import BaseView, TableViewMixin
from chuck_data.ui.theme import TABLE_TITLE_STYLE, WARNING_STYLE, SUCCESS_STYLE
from chuck_data.exceptions import PaginationCancelled


class SchemasTableView(BaseView, TableViewMixin):
    columns = ["name", "comment"]
    headers = ["Name", "Comment"]

    def render(self, data: dict[str, Any]):
        from chuck_data.ui.table_formatter import display_table

        schemas = data.get("schemas", [])
        catalog_name = data.get("catalog_name", "")
        current_schema = data.get("current_schema")

        if not schemas:
            self.console.print(
                f"[{WARNING_STYLE}]No schemas found in catalog '{catalog_name}'.[/{WARNING_STYLE}]"
            )
            raise PaginationCancelled()

        def name_style(name):
            return "bold green" if name == current_schema else None

        style_map = {"name": name_style}

        display_table(
            console=self.console,
            data=schemas,
            columns=self.columns,
            headers=self.headers,
            title=f"Schemas in catalog '{catalog_name}'",
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=False,
        )

        if current_schema:
            self.console.print(
                f"\nCurrent schema: [{SUCCESS_STYLE}]{current_schema}[/{SUCCESS_STYLE}]"
            )

        raise PaginationCancelled()


from chuck_data.ui import view_registry  # noqa: E402
view_registry.register_view("list-schemas", SchemasTableView)
view_registry.register_view("schemas", SchemasTableView)
