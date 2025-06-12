"""View for list-models command."""
from __future__ import annotations
from typing import Any

from chuck_data.ui.view_base import BaseView, TableViewMixin
from chuck_data.ui.theme import TABLE_TITLE_STYLE, SUCCESS_STYLE, INFO, WARNING, DIALOG_BORDER
from chuck_data.ui.styles import table_type_style
from chuck_data.exceptions import PaginationCancelled
from chuck_data.config import get_active_model


class ModelsTableView(BaseView, TableViewMixin):
    columns = ["name", "status", "creator"]
    headers = ["Model Name", "Status", "Creator"]

    def render(self, data: dict[str, Any]):
        from chuck_data.ui.table_formatter import display_table

        models = data.get("models", [])
        active_model = data.get("active_model", get_active_model())
        detailed = data.get("detailed", False)
        filter_text = data.get("filter")

        # If no models, display the help message
        if not models:
            self.console.print(
                f"[{WARNING_STYLE}]No models found in workspace.[/{WARNING_STYLE}]"
            )
            if data.get("message"):
                self.console.print("\n" + data.get("message"))
            # Raise PaginationCancelled to return to chuck > prompt immediately
            raise PaginationCancelled()

        # Display header with filter information if applicable
        title = "Available Models"
        if filter_text:
            title += f" matching '{filter_text}'"

        # Process model data for display
        processed_models = []
        for model in models:
            # Create a processed model entry
            processed = {
                "name": model.get("name", "N/A"),
                "creator": model.get("creator", "N/A"),
            }

            # Get state information
            state = model.get("state", {})
            ready_status = state.get("ready", "UNKNOWN").upper()
            processed["status"] = ready_status

            # Add detailed fields if requested
            if detailed:
                processed["endpoint_type"] = model.get("endpoint_type", "Unknown")
                processed["last_modified"] = model.get("last_updated", "Unknown")

                # Add any additional details from the details field
                details = model.get("details", {})
                if details:
                    for key, value in details.items():
                        # Only add if not already present and meaningful
                        if key not in processed and value is not None and key != "name":
                            processed[key] = value

            # Add to our list
            processed_models.append(processed)

        # Process model names to add recommended tag
        for model in processed_models:
            if model["name"] in [
                "databricks-claude-3-7-sonnet",
            ]:
                model["name"] = f"{model['name']} (recommended)"

        # Define column styling functions
        def status_style(status):
            if status == "READY":
                return "green"
            elif status == "NOT_READY" or status == "UNKNOWN":
                return "yellow"
            elif "ERROR" in status:
                return "red"
            return None

        # Define styling function for the name to highlight active model
        def name_style(name):
            if active_model and (
                name == active_model or name.startswith(active_model + " ")
            ):
                return "bold green"
            return "cyan"

        # Set up style map with appropriate styles for each column
        style_map = {
            "name": name_style,
            "status": status_style,
            "creator": lambda _: f"{INFO}",
            "endpoint_type": lambda _: f"{WARNING}",
            "last_modified": lambda _: f"{DIALOG_BORDER}",
        }

        # Define columns and headers based on detail level
        if detailed:
            columns = ["name", "status", "creator", "endpoint_type", "last_modified"]
            headers = ["Name", "Status", "Creator", "Type", "Last Modified"]
        else:
            columns = ["name", "status", "creator"]
            headers = ["Model Name", "Status", "Creator"]

        # Display the table using our formatter
        display_table(
            console=self.console,
            data=processed_models,
            columns=columns,
            headers=headers,
            title=title,
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=True,
            box_style="ROUNDED",
        )

        # Display current active model
        if active_model:
            self.console.print(
                f"\nCurrent model: [bold green]{active_model}[/bold green]"
            )
        else:
            self.console.print("\nCurrent model: [dim]None[/dim]")

        # Raise PaginationCancelled to return to chuck > prompt immediately
        # This prevents agent from continuing processing after detailed model display is complete
        raise PaginationCancelled()


from chuck_data.ui import view_registry  # noqa: E402
view_registry.register_view("list-models", ModelsTableView)
view_registry.register_view("models", ModelsTableView)
view_registry.register_view("detailed-models", ModelsTableView)