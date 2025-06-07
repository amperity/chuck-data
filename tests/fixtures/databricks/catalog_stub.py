"""Catalog operations mixin for DatabricksClientStub."""


class CatalogStubMixin:
    """Mixin providing catalog operations for DatabricksClientStub."""

    def __init__(self):
        self.catalogs = []
        self.get_catalog_calls = []
        self.list_catalogs_calls = []

    def list_catalogs(self, include_browse=False, max_results=None, page_token=None):
        """List catalogs with optional parameters."""
        self.list_catalogs_calls.append((include_browse, max_results, page_token))
        return {"catalogs": self.catalogs}

    def get_catalog(self, catalog_name):
        """Get a specific catalog by name."""
        self.get_catalog_calls.append((catalog_name,))
        catalog = next((c for c in self.catalogs if c["name"] == catalog_name), None)
        if not catalog:
            raise Exception(f"Catalog {catalog_name} not found")
        return catalog

    def add_catalog(self, name, catalog_type="MANAGED", **kwargs):
        """Add a catalog to the test data."""
        catalog = {"name": name, "type": catalog_type, **kwargs}
        self.catalogs.append(catalog)
        return catalog
