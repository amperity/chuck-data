"""View for status command."""
from __future__ import annotations
from typing import Any

from chuck_data.ui.view_base import BaseView, TableViewMixin
from chuck_data.ui.theme import TABLE_TITLE_STYLE
from chuck_data.exceptions import PaginationCancelled


class StatusTableView(BaseView, TableViewMixin):
    columns = ["setting", "value"]
    headers = ["Setting", "Value"]

    def render(self, data: dict[str, Any]):
        from chuck_data.ui.table_formatter import display_table

        workspace_url = data.get("workspace_url", "Not set")
        active_catalog = data.get("active_catalog", "Not set")
        active_schema = data.get("active_schema", "Not set")
        active_model = data.get("active_model", "Not set")
        warehouse_id = data.get("warehouse_id", "Not set")
        connection_status = data.get("connection_status", "Unknown")

        # Prepare settings for display
        status_items = [
            {"setting": "Workspace URL", "value": workspace_url},
            {"setting": "Active Catalog", "value": active_catalog},
            {"setting": "Active Schema", "value": active_schema},
            {"setting": "Active Model", "value": active_model},
            {"setting": "Active Warehouse", "value": warehouse_id},
            {"setting": "Connection Status", "value": connection_status},
        ]

        # Define styling functions
        def value_style(value, row):
            setting = row.get("setting", "")

            # Special handling for connection status
            if setting == "Connection Status":
                if value == "Connected - token is valid":
                    return "green"
                elif "Invalid" in value or "Not connected" in value:
                    return "red"
                else:
                    return "yellow"
            # General styling for values
            elif value != "Not set":
                return "green"
            else:
                return "yellow"

        # Set up style map
        style_map = {"value": lambda value, row: value_style(value, row)}

        # Display the status table
        display_table(
            console=self.console,
            data=status_items,
            columns=self.columns,
            headers=self.headers,
            title="Current Configuration",
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=False,
        )

        # If permissions data is available, display it
        permissions_data = data.get("permissions")
        if permissions_data:
            self._display_permissions(permissions_data)

        # Raise PaginationCancelled to return to chuck > prompt immediately
        raise PaginationCancelled()
    
    def _display_permissions(self, permissions_data: dict[str, Any]) -> None:
        """
        Display detailed permission check results.

        Args:
            permissions_data: Dictionary of permission check results
        """
        from chuck_data.ui.table_formatter import display_table

        if not permissions_data:
            self.console.print(
                "[yellow]No permission data available.[/yellow]"
            )
            return

        # Format permission data for display
        formatted_permissions = []
        for resource, data in permissions_data.items():
            authorized = data.get("authorized", False)
            details = (
                data.get("details")
                if authorized
                else data.get("error", "Access denied")
            )
            api_path = data.get("api_path", "Unknown")

            # Create a dictionary for this permission
            resource_name = resource.replace("_", " ").title()
            permission_entry = {
                "resource": resource_name,
                "status": "Authorized" if authorized else "Denied",
                "details": details,
                "api_path": api_path,  # Store for reference in the endpoints section
                "authorized": authorized,  # Store for conditional styling
            }
            formatted_permissions.append(permission_entry)

        # Define styling function for status column
        def status_style(status, row):
            return "green" if row.get("authorized") else "red"

        # Set up style map
        style_map = {"status": status_style}

        # Display the permissions table
        display_table(
            console=self.console,
            data=formatted_permissions,
            columns=["resource", "status", "details"],
            headers=["Resource", "Status", "Details"],
            title="Databricks API Token Permissions",
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=True,
        )

        # Additional note about API endpoints
        self.console.print("\n[dim]API endpoints checked:[/dim]")
        for item in formatted_permissions:
            resource_name = item["resource"]
            api_path = item["api_path"]
            self.console.print(f"[dim]- {resource_name}: {api_path}[/dim]")


from chuck_data.ui import view_registry  # noqa: E402
view_registry.register_view("status", StatusTableView)