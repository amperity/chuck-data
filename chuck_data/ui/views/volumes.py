"""View for list-volumes command."""

from __future__ import annotations
from typing import Any

from chuck_data.ui.view_base import BaseView, TableViewMixin
from chuck_data.ui.theme import TABLE_TITLE_STYLE, WARNING_STYLE
from chuck_data.exceptions import PaginationCancelled


class VolumesTableView(BaseView, TableViewMixin):
    columns = ["name", "type", "comment"]
    headers = ["Name", "Type", "Comment"]

    def render(self, data: dict[str, Any]):
        from chuck_data.ui.table_formatter import display_table

        volumes = data.get("volumes", [])
        catalog_name = data.get("catalog_name", "")
        schema_name = data.get("schema_name", "")

        if not volumes:
            self.console.print(
                f"[{WARNING_STYLE}]No volumes found in {catalog_name}.{schema_name}.[/{WARNING_STYLE}]"
            )
            # Raise PaginationCancelled to return to chuck > prompt immediately
            raise PaginationCancelled()

        # Process volume data for display
        processed_volumes = []
        for volume in volumes:
            # Create a processed volume with normalized fields
            processed = {
                "name": volume.get("name", ""),
                "type": volume.get(
                    "volume_type", ""
                ).upper(),  # Use upper for consistency
                "comment": volume.get("comment", ""),
            }
            processed_volumes.append(processed)

        # Define styling for volume types
        def type_style(volume_type):
            if volume_type == "EXTERNAL":  # Example conditional styling
                return "yellow"
            elif volume_type == "MANAGED":  # Example conditional styling
                return "blue"
            return None

        # Set up style map
        style_map = {"type": type_style}

        # Display the table
        display_table(
            console=self.console,
            data=processed_volumes,
            columns=self.columns,
            headers=self.headers,
            title=f"Volumes in {catalog_name}.{schema_name}",
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=False,
        )

        # Raise PaginationCancelled to return to chuck > prompt immediately
        raise PaginationCancelled()


from chuck_data.ui import view_registry  # noqa: E402

view_registry.register_view("list-volumes", VolumesTableView)
view_registry.register_view("volumes", VolumesTableView)
