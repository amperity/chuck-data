"""Table operations mixin for DatabricksClientStub."""


class TableStubMixin:
    """Mixin providing table operations for DatabricksClientStub."""

    def __init__(self):
        self.tables = {}  # (catalog, schema) -> [tables]
        self.list_tables_calls = []
        self.get_table_calls = []
        self.list_tables_error = None

    def list_tables(
        self,
        catalog_name,
        schema_name,
        max_results=None,
        page_token=None,
        include_delta_metadata=False,
        omit_columns=False,
        omit_properties=False,
        omit_username=False,
        include_browse=False,
        include_manifest_capabilities=False,
        **kwargs,
    ):
        """List tables in a schema."""
        if self.list_tables_error:
            raise self.list_tables_error

        self.list_tables_calls.append(
            (
                catalog_name,
                schema_name,
                max_results,
                page_token,
                include_delta_metadata,
                omit_columns,
                omit_properties,
                omit_username,
                include_browse,
                include_manifest_capabilities,
            )
        )
        key = (catalog_name, schema_name)
        tables = self.tables.get(key, [])
        return {"tables": tables, "next_page_token": None}

    def get_table(
        self,
        full_name,
        include_delta_metadata=False,
        include_browse=False,
        include_manifest_capabilities=False,
        full_table_name=None,
        **kwargs,
    ):
        """Get a specific table by full name."""
        self.get_table_calls.append(
            (
                full_name or full_table_name,
                include_delta_metadata,
                include_browse,
                include_manifest_capabilities,
            )
        )
        # Support both parameter names for compatibility
        table_name = full_name or full_table_name
        if not table_name:
            raise Exception("Table name is required")

        # Parse full_table_name and return table details
        parts = table_name.split(".")
        if len(parts) != 3:
            raise Exception("Invalid table name format")

        catalog, schema, table = parts
        key = (catalog, schema)
        tables = self.tables.get(key, [])
        table_info = next((t for t in tables if t["name"] == table), None)
        if not table_info:
            raise Exception(f"Table {table_name} not found")
        return table_info

    def add_table(
        self, catalog_name, schema_name, table_name, table_type="MANAGED", **kwargs
    ):
        """Add a table to the test data."""
        key = (catalog_name, schema_name)
        if key not in self.tables:
            self.tables[key] = []

        table = {
            "name": table_name,
            "full_name": f"{catalog_name}.{schema_name}.{table_name}",
            "table_type": table_type,
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "comment": kwargs.get("comment", ""),
            "created_at": kwargs.get("created_at", "2023-01-01T00:00:00Z"),
            "created_by": kwargs.get("created_by", "test.user@example.com"),
            "owner": kwargs.get("owner", "test.user@example.com"),
            "columns": kwargs.get("columns", []),
            "properties": kwargs.get("properties", {}),
            **kwargs,
        }
        self.tables[key].append(table)
        return table

    def set_list_tables_error(self, error):
        """Configure list_tables to raise error."""
        self.list_tables_error = error
