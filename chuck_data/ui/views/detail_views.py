"""Views for detail commands like table, catalog, and schema."""

from __future__ import annotations
from typing import Any, Dict

from chuck_data.ui.view_base import BaseView
from chuck_data.ui.theme import TABLE_TITLE_STYLE, WARNING_STYLE
from chuck_data.ui.table_formatter import display_table
from chuck_data.exceptions import PaginationCancelled


class TableDetailsView(BaseView):
    """View for detailed table information."""

    def render(self, data: Dict[str, Any]) -> None:
        """
        Render detailed information for a single table.

        Args:
            data: Dictionary containing table details and rendering options
                 If data["display"] is True, rendering will complete without raising PaginationCancelled
        """
        table = data.get("table", {})
        full_name = data.get("full_name", "")
        has_delta_metadata = data.get("has_delta_metadata", False)
        display_mode = data.get("display", False)  # If True, suppress PaginationCancelled

        if not table:
            self.console.print(
                f"[{WARNING_STYLE}]No table details available.[/{WARNING_STYLE}]"
            )
            if not display_mode:
                raise PaginationCancelled()
            return

        # Display table header
        self.console.print(
            f"\n[{TABLE_TITLE_STYLE}]Table Details: {full_name}[/{TABLE_TITLE_STYLE}]"
        )

        # Prepare basic information data
        basic_info = []
        properties = [
            ("Name", table.get("name", "")),
            ("Full Name", full_name),
            ("Type", table.get("table_type", "")),
            ("Format", table.get("data_source_format", "")),
            ("Storage Location", table.get("storage_location", "")),
            ("Owner", table.get("owner", "")),
            ("Created", table.get("created_at", "")),
            ("Created By", table.get("created_by", "")),
            ("Updated", table.get("updated_at", "")),
            ("Updated By", table.get("updated_by", "")),
            ("Comment", table.get("comment", "")),
        ]

        for prop, value in properties:
            if value:  # Only include non-empty values
                basic_info.append({"property": prop, "value": value})

        # Display basic information table
        self.console.print("\n[bold]Basic Information:[/bold]")
        display_table(
            console=self.console,
            data=basic_info,
            columns=["property", "value"],
            headers=["Property", "Value"],
            show_lines=False,
        )

        # Display columns if available
        columns_data = table.get("columns", [])
        if columns_data:
            # Prepare column data
            columns_for_display = []
            for column in columns_data:
                columns_for_display.append(
                    {
                        "name": column.get("name", ""),
                        "type": column.get("type_text", column.get("type", "")),
                        "nullable": "Yes" if column.get("nullable", False) else "No",
                        "comment": column.get("comment", ""),
                    }
                )

            # Display columns table
            self.console.print("\n[bold]Columns:[/bold]")
            display_table(
                console=self.console,
                data=columns_for_display,
                columns=["name", "type", "nullable", "comment"],
                headers=["Name", "Type", "Nullable", "Comment"],
                show_lines=False,
            )

        # Display properties if available
        properties_data = table.get("properties", {})
        if properties_data:
            # Prepare properties data
            props_for_display = []
            for prop, value in properties_data.items():
                # Skip empty values
                if value is None or value == "":
                    continue

                props_for_display.append({"property": prop, "value": value})

            # Display properties table
            self.console.print("\n[bold]Table Properties:[/bold]")
            display_table(
                console=self.console,
                data=props_for_display,
                columns=["property", "value"],
                headers=["Property", "Value"],
                show_lines=False,
            )

        # Display Delta metadata if available
        if has_delta_metadata and "delta" in table:
            delta_info = table.get("delta", {})

            # Prepare Delta metadata data
            delta_for_display = []
            delta_properties = [
                ("Format", delta_info.get("format", "")),
                ("ID", delta_info.get("id", "")),
                ("Last Updated", delta_info.get("last_updated", "")),
                ("Min Reader Version", delta_info.get("min_reader_version", "")),
                ("Min Writer Version", delta_info.get("min_writer_version", "")),
                ("Num Files", delta_info.get("num_files", "")),
                ("Size (Bytes)", delta_info.get("size_in_bytes", "")),
            ]

            for prop, value in delta_properties:
                if value:  # Only include non-empty values
                    delta_for_display.append({"property": prop, "value": value})

            # Display Delta metadata table
            if delta_for_display:  # Only if we have data to show
                self.console.print("\n[bold]Delta Metadata:[/bold]")
                display_table(
                    console=self.console,
                    data=delta_for_display,
                    columns=["property", "value"],
                    headers=["Property", "Value"],
                    show_lines=False,
                )
        
        # Only raise PaginationCancelled if display_mode is False
        if not display_mode:
            raise PaginationCancelled()


class CatalogDetailsView(BaseView):
    """View for detailed catalog information."""

    def render(self, data: Dict[str, Any]) -> None:
        """
        Render detailed information for a specific catalog.

        Args:
            data: Dictionary containing catalog details and rendering options
                 If data["display"] is True, rendering will complete without raising PaginationCancelled
        """
        catalog = data
        display_mode = data.get("display", False)  # If True, suppress PaginationCancelled

        if not catalog:
            self.console.print(
                f"[{WARNING_STYLE}]No catalog details available.[/{WARNING_STYLE}]"
            )
            if not display_mode:
                raise PaginationCancelled()
            return

        # Display catalog header
        catalog_name = catalog.get("name", "Unknown")
        self.console.print(
            f"\n[{TABLE_TITLE_STYLE}]Catalog Details: {catalog_name}[/{TABLE_TITLE_STYLE}]"
        )

        # Prepare basic information data
        basic_info = []
        properties = [
            ("Name", catalog.get("name", "")),
            ("Type", catalog.get("type", "")),
            ("Comment", catalog.get("comment", "")),
            ("Provider", catalog.get("provider", {}).get("name", "")),
            ("Storage Root", catalog.get("storage_root", "")),
            ("Storage Location", catalog.get("storage_location", "")),
            ("Owner", catalog.get("owner", "")),
            ("Created At", catalog.get("created_at", "")),
            ("Created By", catalog.get("created_by", "")),
            ("Options", str(catalog.get("options", {}))),
        ]

        for prop, value in properties:
            if value:  # Only include non-empty values
                basic_info.append({"property": prop, "value": value})

        # Display basic information table
        display_table(
            console=self.console,
            data=basic_info,
            columns=["property", "value"],
            headers=["Property", "Value"],
            show_lines=False,
        )
        
        # Only raise PaginationCancelled if display_mode is False
        if not display_mode:
            raise PaginationCancelled()


class SchemaDetailsView(BaseView):
    """View for detailed schema information."""

    def render(self, data: Dict[str, Any]) -> None:
        """
        Render detailed information for a specific schema.

        Args:
            data: Dictionary containing schema details and rendering options
                 If data["display"] is True, rendering will complete without raising PaginationCancelled
        """
        schema = data
        display_mode = data.get("display", False)  # If True, suppress PaginationCancelled

        if not schema:
            self.console.print(
                f"[{WARNING_STYLE}]No schema details available.[/{WARNING_STYLE}]"
            )
            if not display_mode:
                raise PaginationCancelled()
            return

        # Display schema header
        schema_name = schema.get("name", "Unknown")
        catalog_name = schema.get("catalog_name", "Unknown")
        full_name = f"{catalog_name}.{schema_name}"
        self.console.print(
            f"\n[{TABLE_TITLE_STYLE}]Schema Details: {full_name}[/{TABLE_TITLE_STYLE}]"
        )

        # Prepare basic information data
        basic_info = []
        properties = [
            ("Name", schema.get("name", "")),
            ("Full Name", schema.get("full_name", "")),
            ("Catalog Name", schema.get("catalog_name", "")),
            ("Comment", schema.get("comment", "")),
            ("Storage Root", schema.get("storage_root", "")),
            ("Storage Location", schema.get("storage_location", "")),
            ("Owner", schema.get("owner", "")),
            ("Created At", schema.get("created_at", "")),
            ("Created By", schema.get("created_by", "")),
        ]

        for prop, value in properties:
            if value:  # Only include non-empty values
                basic_info.append({"property": prop, "value": value})

        # Display basic information table
        display_table(
            console=self.console,
            data=basic_info,
            columns=["property", "value"],
            headers=["Property", "Value"],
            show_lines=False,
        )
        
        # Only raise PaginationCancelled if display_mode is False
        if not display_mode:
            raise PaginationCancelled()


# Register the views with the registry
from chuck_data.ui import view_registry  # noqa: E402

view_registry.register_view("table-details", TableDetailsView)
view_registry.register_view("catalog-details", CatalogDetailsView)
view_registry.register_view("schema-details", SchemaDetailsView)