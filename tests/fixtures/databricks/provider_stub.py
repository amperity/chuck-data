"""Databricks Data Provider Stub for testing."""

from typing import List, Dict, Any, Optional
from tests.fixtures.databricks.client import DatabricksClientStub


class DatabricksDataProviderStub:
    """Test stub for DatabricksDataProvider.

    This wraps the existing DatabricksClientStub to provide the DataProvider
    interface for testing. It follows the same pattern as DatabricksDataProvider
    which wraps DatabricksAPIClient.
    """

    def __init__(self, client: Optional[DatabricksClientStub] = None):
        """Initialize provider stub.

        Args:
            client: Optional DatabricksClientStub to use (creates new one if not provided)
        """
        self._client = client if client is not None else DatabricksClientStub()

    def validate_connection(self) -> bool:
        """Validate Databricks connection."""
        return self._client.validate_token()

    def list_catalogs(self, **kwargs) -> List[Dict[str, Any]]:
        """List Unity Catalog catalogs."""
        result = self._client.list_catalogs(**kwargs)
        return result.get("catalogs", [])

    def get_catalog(self, name: str) -> Optional[Dict[str, Any]]:
        """Get catalog by name."""
        try:
            return self._client.get_catalog(name)
        except Exception:
            return None

    def list_schemas(self, catalog: str, **kwargs) -> List[Dict[str, Any]]:
        """List schemas in a catalog."""
        result = self._client.list_schemas(catalog, **kwargs)
        return result.get("schemas", [])

    def get_schema(self, catalog: str, schema: str) -> Optional[Dict[str, Any]]:
        """Get schema details."""
        full_name = f"{catalog}.{schema}"
        try:
            return self._client.get_schema(full_name)
        except Exception:
            return None

    def list_tables(self, catalog: str, schema: str, **kwargs) -> List[Dict[str, Any]]:
        """List tables in a schema."""
        result = self._client.list_tables(catalog, schema, **kwargs)
        return result.get("tables", [])

    def get_table(
        self, catalog: str, schema: str, table: str
    ) -> Optional[Dict[str, Any]]:
        """Get table details."""
        full_name = f"{catalog}.{schema}.{table}"
        try:
            return self._client.get_table(full_name)
        except Exception:
            return None

    def execute_query(self, query: str, **kwargs) -> Dict[str, Any]:
        """Execute SQL query."""
        warehouse_id = kwargs.get("warehouse_id", "test-warehouse")
        return self._client.submit_sql_statement(query, warehouse_id, **kwargs)

    def get_provider_name(self) -> str:
        """Return provider name."""
        return "databricks"

    def get_capabilities(self) -> Dict[str, bool]:
        """Return Databricks capabilities."""
        return {
            "supports_sql": True,
            "supports_catalogs": True,
            "supports_schemas": True,
            "supports_tables": True,
            "supports_volumes": True,
            "supports_models": True,
            "supports_jobs": True,
            "supports_warehouses": True,
        }

    # ===== Pass-through methods for test setup =====
    # These allow tests to configure the stub state

    def add_catalog(self, name: str, **kwargs):
        """Add a test catalog."""
        self._client.add_catalog(name, **kwargs)

    def add_schema(self, name: str, catalog: str, **kwargs):
        """Add a test schema."""
        self._client.add_schema(catalog, name, **kwargs)

    def add_table(self, name: str, schema: str, catalog: str, **kwargs):
        """Add a test table."""
        self._client.add_table(catalog, schema, name, **kwargs)

    def add_model(self, name: str, **kwargs):
        """Add a test model endpoint."""
        self._client.add_model(name, **kwargs)

    def add_warehouse(self, warehouse_id: str, name: str, **kwargs):
        """Add a test warehouse."""
        self._client.add_warehouse(warehouse_id, name, **kwargs)

    def add_volume(self, name: str, catalog: str, schema: str, **kwargs):
        """Add a test volume."""
        self._client.add_volume(catalog, schema, name, **kwargs)

    def set_connection_valid(self, valid: bool):
        """Set connection validation result."""
        self._client.set_token_validation_result(valid)

    def reset(self):
        """Reset all stub data to initial state."""
        self._client.reset()

    # ===== Databricks-specific methods =====
    # These maintain compatibility with existing tests

    def list_volumes(self, catalog: str, schema: str, **kwargs):
        """List volumes (Databricks-specific)."""
        # Note: stub only takes catalog, not schema
        result = self._client.list_volumes(catalog, **kwargs)
        return result.get("volumes", [])

    def create_volume(self, catalog: str, schema: str, volume_name: str, **kwargs):
        """Create a volume (Databricks-specific)."""
        return self._client.create_volume(catalog, schema, volume_name, **kwargs)

    def list_models(self, **kwargs):
        """List model serving endpoints (Databricks-specific)."""
        return self._client.list_models(**kwargs)

    def get_model(self, model_name: str):
        """Get model serving endpoint details (Databricks-specific)."""
        return self._client.get_model(model_name)

    def list_warehouses(self, **kwargs):
        """List SQL warehouses (Databricks-specific)."""
        return self._client.list_warehouses(**kwargs)

    def get_warehouse(self, warehouse_id: str):
        """Get SQL warehouse details (Databricks-specific)."""
        return self._client.get_warehouse(warehouse_id)

    def create_warehouse(self, **kwargs):
        """Create a SQL warehouse (Databricks-specific)."""
        return self._client.create_warehouse(**kwargs)

    def submit_job_run(self, config_path: str, init_script_path: str, run_name: str):
        """Submit a job run (Databricks-specific)."""
        return self._client.submit_job_run(config_path, init_script_path, run_name)

    def get_job_run_status(self, run_id: str):
        """Get job run status (Databricks-specific)."""
        return self._client.get_job_run_status(run_id)

    def upload_file(
        self,
        path: str,
        file_path: str = None,
        content: str = None,
        overwrite: bool = True,
    ):
        """Upload file to Databricks (Databricks-specific)."""
        return self._client.upload_file(path, file_path, content, overwrite)

    def store_dbfs_file(self, path: str, contents: str, overwrite: bool = False):
        """Store file via DBFS API (Databricks-specific)."""
        return self._client.store_dbfs_file(path, contents, overwrite)

    def get_current_user(self):
        """Get current logged-in user (Databricks-specific)."""
        return self._client.get_current_user()
