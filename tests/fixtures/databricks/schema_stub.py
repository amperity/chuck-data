"""Schema operations mixin for DatabricksClientStub."""


class SchemaStubMixin:
    """Mixin providing schema operations for DatabricksClientStub."""

    def __init__(self):
        self.schemas = {}  # catalog_name -> [schemas]
        self.list_schemas_calls = []
        self.get_schema_calls = []

    def list_schemas(
        self,
        catalog_name,
        include_browse=False,
        max_results=None,
        page_token=None,
        **kwargs,
    ):
        """List schemas in a catalog."""
        self.list_schemas_calls.append(
            (catalog_name, include_browse, max_results, page_token)
        )
        return {"schemas": self.schemas.get(catalog_name, [])}

    def get_schema(self, full_name):
        """Get a specific schema by full name."""
        self.get_schema_calls.append((full_name,))
        # Parse full_name in format "catalog_name.schema_name"
        parts = full_name.split(".")
        if len(parts) != 2:
            raise Exception("Invalid schema name format")

        catalog_name, schema_name = parts
        schemas = self.schemas.get(catalog_name, [])
        schema = next((s for s in schemas if s["name"] == schema_name), None)
        if not schema:
            raise Exception(f"Schema {full_name} not found")
        return schema

    def add_schema(self, catalog_name, schema_name, **kwargs):
        """Add a schema to the test data."""
        if catalog_name not in self.schemas:
            self.schemas[catalog_name] = []
        schema = {"name": schema_name, "catalog_name": catalog_name, **kwargs}
        self.schemas[catalog_name].append(schema)
        return schema
