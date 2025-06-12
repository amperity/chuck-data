"""View for list-warehouses command."""
from __future__ import annotations
from typing import Any

from chuck_data.ui.view_base import BaseView, TableViewMixin
from chuck_data.ui.theme import TABLE_TITLE_STYLE, WARNING_STYLE, SUCCESS_STYLE
from chuck_data.ui.styles import warehouse_state_style
from chuck_data.exceptions import PaginationCancelled


class WarehousesTableView(BaseView, TableViewMixin):
    columns = ["name", "id", "size", "type", "state"]
    headers = ["Name", "ID", "Size", "Type", "State"]

    def render(self, data: dict[str, Any]):
        from chuck_data.ui.table_formatter import display_table

        warehouses = data.get("warehouses", [])
        current_warehouse_id = data.get("current_warehouse_id")

        if not warehouses:
            self.console.print(
                f"[{WARNING_STYLE}]No SQL warehouses found.[/{WARNING_STYLE}]"
            )
            # Raise PaginationCancelled to return to chuck > prompt immediately
            raise PaginationCancelled()

        # Process warehouse data for display
        processed_warehouses = []
        for warehouse in warehouses:
            # Determine warehouse type: show 'serverless' if serverless is enabled, otherwise show warehouse_type
            warehouse_type = (
                "serverless"
                if warehouse.get("enable_serverless_compute", False)
                else warehouse.get("warehouse_type", "").lower()
            )

            # Create a processed warehouse with formatted fields
            processed = {
                "name": warehouse.get("name", ""),
                "id": warehouse.get("id", ""),
                "size": warehouse.get("size", "").lower(),  # Lowercase size field
                "type": warehouse_type,
                "state": warehouse.get("state", "").lower(),  # Lowercase state field
            }
            processed_warehouses.append(processed)

        # Define styling function for name based on current warehouse
        def name_style(name, row):
            if row.get("id") == current_warehouse_id:
                return "bold green"
            return None

        # Define styling function for ID based on current warehouse
        def id_style(id_val):
            if id_val == current_warehouse_id:
                return "bold green"
            return None

        # Set up style map
        style_map = {
            "name": lambda name, row=None: name_style(name, row),
            "id": id_style,
            "state": warehouse_state_style,
        }

        # Display the table
        display_table(
            console=self.console,
            data=processed_warehouses,
            columns=self.columns,
            headers=self.headers,
            title="Available SQL Warehouses",
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=False,
        )

        # Display current warehouse ID if set
        if current_warehouse_id:
            self.console.print(
                f"\nCurrent SQL warehouse ID: [{SUCCESS_STYLE}]{current_warehouse_id}[/{SUCCESS_STYLE}]"
            )

        # Always raise PaginationCancelled when we actually display the table
        raise PaginationCancelled()


from chuck_data.ui import view_registry  # noqa: E402
view_registry.register_view("list-warehouses", WarehousesTableView)
view_registry.register_view("warehouses", WarehousesTableView)