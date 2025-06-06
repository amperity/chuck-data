"""
Unified command output handling for both user and agent interfaces.

This module provides consistent formatting of command results for different consumers,
such as the user TUI interface or the LLM agent.
"""

from typing import Dict, Any
from rich.console import Console

from src.ui.table_formatter import display_table

from src.command_result import CommandResult
from src.ui.theme import (
    SUCCESS,
    WARNING,
    NEUTRAL,
    SUCCESS_STYLE,
    TABLE_TITLE_STYLE,
)


class OutputFormatter:
    """Format command results for different consumers (user TUI, agent, etc.)"""

    @staticmethod
    def _display_status(data: Dict[str, Any], console: Console) -> None:
        """Display current status information including connection status and permissions."""
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
        def value_style(row):
            setting = row.get("setting", "")
            value = row.get("value", "")

            # Special handling for connection status
            if setting == "Connection Status":
                if (
                    value == "Connected - token is valid"
                    or value == "Connected (client present)."
                ):
                    return "green"
                elif (
                    "Invalid" in value
                    or "Not connected" in value
                    or "error" in value.lower()
                ):
                    return "red"
                else:
                    return "yellow"
            # General styling for values
            elif value != "Not set":
                return "green"
            else:
                return "yellow"

        # Display the status table
        display_table(
            console=console,
            data=status_items,
            columns=["setting", "value"],
            headers=["Setting", "Value"],
            title="Current Configuration",
            style_map={"value": value_style},
            title_style=TABLE_TITLE_STYLE,
            show_lines=False,
        )

        # If permissions data is available, display it
        permissions_data = data.get("permissions")
        if permissions_data:
            # Display permissions information
            permissions_items = []
            for key, value in permissions_data.items():
                permissions_items.append({"permission": key, "status": value})

            # Define styling function for permission status
            def status_style(row):
                status = row.get("status", "")
                if status == "OK":
                    return "green"
                elif status == "ERROR":
                    return "red"
                else:
                    return "yellow"

            # Display the permissions table
            display_table(
                console=console,
                data=permissions_items,
                columns=["permission", "status"],
                headers=["Permission", "Status"],
                title="API Permissions",
                style_map={"status": status_style},
                title_style=TABLE_TITLE_STYLE,
                show_lines=False,
            )

    @staticmethod
    def format_for_agent(result: CommandResult) -> Dict[str, Any]:
        """
        Format a command result for agent consumption.

        Args:
            result: Command result to format

        Returns:
            Dictionary formatted for agent consumption
        """
        if not result.success:
            return {
                "error": (
                    str(result.error)
                    if result.error
                    else result.message or "Unknown error"
                )
            }

        # Start with a base response
        response = {"success": True}

        # Add the message if available
        if result.message:
            response["message"] = result.message

        # Add displayed_to_user flag for tools that show output directly
        if result.data and isinstance(result.data, dict):
            # For commands that display directly to user, indicate this to the agent
            # This is a temporary approach - eventually the registry would indicate
            # whether a command displays directly to the user

            # Copy all data from the result
            for key, value in result.data.items():
                response[key] = value

            # Add displayed_to_user flag for appropriate commands
            # Ideally this would come from the command registry
            response["displayed_to_user"] = True
        else:
            # If data isn't a dict, include it as-is
            response["data"] = result.data

        return response

    # --- Helper methods for specific command output formatting ---
    # These would contain the display logic currently in cli.py

    @staticmethod
    def _display_catalogs(data: Dict[str, Any], console: Console) -> None:
        """Display catalogs in a nicely formatted way."""
        catalogs = data.get("catalogs", [])
        current_catalog = data.get("current_catalog")

        if not catalogs:
            console.print(f"[{WARNING}]No catalogs found.[/{WARNING}]")
            return

        # Transform data for display
        display_data = []
        for catalog in catalogs:
            display_data.append(
                {
                    "name": catalog.get("name", ""),
                    "type": catalog.get("type", ""),
                    "comment": catalog.get("comment", ""),
                    "owner": catalog.get("owner", ""),
                }
            )

        # Define styling functions
        def name_style(value):
            if value == current_catalog:
                return "bold green"
            return None

        def type_style(value):
            if value.lower() == "managed":
                return "green"
            elif value.lower() == "external":
                return "blue"
            else:
                return "yellow"

        style_map = {
            "name": name_style,
            "type": type_style,
        }

        # Display the catalogs table
        display_table(
            console=console,
            data=display_data,
            columns=["name", "type", "comment", "owner"],
            headers=["Name", "Type", "Comment", "Owner"],
            title="Available Catalogs",
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=False,
        )

        # Display current catalog if set
        if current_catalog:
            console.print(
                f"\nCurrent catalog: [bold green]{current_catalog}[/bold green]"
            )

    @staticmethod
    def _display_schemas(data: Dict[str, Any], console: Console) -> None:
        """Display schemas in a nicely formatted way."""
        schemas = data.get("schemas", [])
        catalog_name = data.get("catalog_name", "")
        current_schema = data.get("current_schema")

        if not schemas:
            console.print(
                f"[{WARNING}]No schemas found in catalog '{catalog_name}'.[/{WARNING}]"
            )
            return

        # Define a style map for conditional formatting
        def style_name(row):
            if row.get("name") == current_schema:
                return f"[{SUCCESS_STYLE}]{row.get('name')}[/{SUCCESS_STYLE}]"
            return row.get("name")

        style_map = {
            "name": style_name,
        }

        # Display the schemas table
        display_table(
            console=console,
            data=schemas,
            columns=["name", "comment"],
            headers=["Name", "Comment"],
            title=f"Schemas in catalog '{catalog_name}'",
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=False,
        )

        # Display current schema if set
        if current_schema:
            console.print(
                f"\nCurrent schema: [{SUCCESS_STYLE}]{current_schema}[/{SUCCESS_STYLE}]"
            )

    @staticmethod
    def _display_tables(data: Dict[str, Any], console: Console) -> None:
        """Display tables in a nicely formatted way."""
        tables = data.get("tables", [])
        catalog_name = data.get("catalog_name", "")
        schema_name = data.get("schema_name", "")
        total_count = data.get("total_count", len(tables))

        if not tables:
            console.print(
                f"[{WARNING}]No tables found in {catalog_name}.{schema_name}[/{WARNING}]"
            )
            return

        # Define a style map for conditional formatting
        def format_date(row, col_name):
            date_str = row.get(col_name, "")
            if not date_str:
                return ""
            # Format could be improved based on actual date format in the data
            return date_str

        style_map = {
            "created": format_date,
            "updated": format_date,
        }

        # Display the tables
        display_table(
            console=console,
            data=tables,
            columns=["name", "table_type", "column_count", "created", "updated"],
            headers=["Table Name", "Type", "# Cols", "Created", "Last Updated"],
            title=f"Tables in {catalog_name}.{schema_name} ({total_count} total)",
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=True,
        )

    @staticmethod
    def _display_models(data: Dict[str, Any], console: Console) -> None:
        """Display models in a nicely formatted way."""
        models = data.get("models", [])
        current_model = data.get("current_model")
        is_detailed = data.get("is_detailed", False)

        if not models:
            console.print(f"[{WARNING}]No models found.[/{WARNING}]")
            return

        # Define a style map for conditional formatting
        def style_name(row):
            model_name = row.get("name")
            # Check if this is a recommended model
            is_recommended = model_name in [
                "databricks-meta-llama-3-3-70b-instruct",
                "databricks-claude-3-7-sonnet",
            ]

            if model_name == current_model:
                if is_recommended:
                    return f"[bold green]{model_name} (recommended)[/bold green]"
                return f"[bold green]{model_name}[/bold green]"
            elif is_recommended:
                return f"{model_name} [green](recommended)[/green]"
            return model_name

        style_map = {
            "name": style_name,
        }

        if is_detailed:
            # Display detailed models with more columns
            display_table(
                console=console,
                data=models,
                columns=["name", "creator", "created", "status", "description"],
                headers=["Model Name", "Created By", "Date", "Status", "Description"],
                title="Available Models",
                style_map=style_map,
                title_style=TABLE_TITLE_STYLE,
                show_lines=True,
            )
        else:
            # Display simple models list
            display_table(
                console=console,
                data=models,
                columns=["name", "created", "status"],
                headers=["Model Name", "Created", "Status"],
                title="Available Models",
                style_map=style_map,
                title_style=TABLE_TITLE_STYLE,
                show_lines=False,
            )

        # Display current model if set
        if current_model:
            console.print(
                f"\nCurrent model: [{SUCCESS_STYLE}]{current_model}[/{SUCCESS_STYLE}]"
            )

    @staticmethod
    def _display_warehouses(data: Dict[str, Any], console: Console) -> None:
        """Display warehouses in a nicely formatted way."""
        warehouses = data.get("warehouses", [])
        current_warehouse = data.get("current_warehouse")

        if not warehouses:
            console.print(f"[{WARNING}]No warehouses found.[/{WARNING}]")
            return

        # Define a style map for conditional formatting
        def style_name(row):
            if row.get("name") == current_warehouse:
                return f"[{SUCCESS_STYLE}]{row.get('name')}[/{SUCCESS_STYLE}]"
            return row.get("name")

        def style_state(row):
            state = row.get("state", "").lower()
            if state == "running":
                return f"[{SUCCESS}]{state}[/{SUCCESS}]"
            elif state == "stopped":
                return f"[{NEUTRAL}]{state}[/{NEUTRAL}]"
            else:
                return f"[{WARNING}]{state}[/{WARNING}]"

        style_map = {
            "name": style_name,
            "state": style_state,
        }

        # Display the warehouses
        display_table(
            console=console,
            data=warehouses,
            columns=["name", "size", "state", "auto_stop_mins", "created_by"],
            headers=["Warehouse Name", "Size", "State", "Auto Stop", "Created By"],
            title="Available Warehouses",
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=False,
        )

        # Display current warehouse if set
        if current_warehouse:
            console.print(
                f"\nCurrent warehouse: [{SUCCESS_STYLE}]{current_warehouse}[/{SUCCESS_STYLE}]"
            )

    @staticmethod
    def _display_volumes(data: Dict[str, Any], console: Console) -> None:
        """Display volumes in a nicely formatted way."""
        volumes = data.get("volumes", [])
        current_volume = data.get("current_volume")

        if not volumes:
            console.print(f"[{WARNING}]No volumes found.[/{WARNING}]")
            return

        # Define a style map for conditional formatting
        def style_name(row):
            if row.get("name") == current_volume:
                return f"[{SUCCESS_STYLE}]{row.get('name')}[/{SUCCESS_STYLE}]"
            return row.get("name")

        style_map = {
            "name": style_name,
        }

        # Display the volumes
        display_table(
            console=console,
            data=volumes,
            columns=["name", "type", "catalog", "schema", "owner", "created"],
            headers=["Volume Name", "Type", "Catalog", "Schema", "Owner", "Created"],
            title="Available Volumes",
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=False,
        )

        # Display current volume if set
        if current_volume:
            console.print(
                f"\nCurrent volume: [{SUCCESS_STYLE}]{current_volume}[/{SUCCESS_STYLE}]"
            )
