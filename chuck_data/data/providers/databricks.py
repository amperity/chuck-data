"""Databricks Data Provider Implementation."""

from typing import Optional, List, Dict, Any
from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.config import get_workspace_url, get_databricks_token


class DatabricksDataProvider:
    """Databricks data provider implementation.

    Wraps the existing DatabricksAPIClient to implement the DataProvider protocol.
    This maintains backward compatibility while providing the new provider interface.
    """

    def __init__(
        self,
        workspace_url: Optional[str] = None,
        token: Optional[str] = None,
        client: Optional[DatabricksAPIClient] = None,
    ):
        """Initialize Databricks provider.

        Args:
            workspace_url: Databricks workspace URL (optional, falls back to config)
            token: Personal access token (optional, falls back to config)
            client: Existing DatabricksAPIClient for testing/injection (optional)

        Raises:
            ValueError: If workspace URL and token cannot be determined
        """
        if client is not None:
            # Use provided client for testing
            self._client = client
        else:
            # Resolve credentials from parameters or config
            url = workspace_url or get_workspace_url()
            tok = token or get_databricks_token()

            if not url or not tok:
                raise ValueError(
                    "Databricks workspace URL and token required. "
                    "Provide as parameters or configure in ~/.chuck_config.json"
                )

            self._client = DatabricksAPIClient(url, tok)

    def validate_connection(self) -> bool:
        """Validate Databricks connection.

        Returns:
            True if connection is valid, False otherwise
        """
        try:
            self._client.validate_token()
            return True
        except Exception:
            return False

    def list_catalogs(self, **kwargs) -> List[Dict[str, Any]]:
        """List Unity Catalog catalogs.

        Args:
            **kwargs: Additional options passed to DatabricksAPIClient.list_catalogs()
                - include_browse: Include browse permission details
                - max_results: Maximum number of results to return
                - page_token: Pagination token

        Returns:
            List of catalog dicts from Unity Catalog API
        """
        return self._client.list_catalogs(**kwargs)

    def get_catalog(self, name: str) -> Optional[Dict[str, Any]]:
        """Get catalog by name.

        Args:
            name: Catalog name

        Returns:
            Catalog details dict or None if not found
        """
        return self._client.get_catalog(name)

    def list_schemas(self, catalog: str, **kwargs) -> List[Dict[str, Any]]:
        """List schemas in a catalog.

        Args:
            catalog: Catalog name
            **kwargs: Additional options passed to DatabricksAPIClient.list_schemas()
                - max_results: Maximum number of results to return
                - page_token: Pagination token

        Returns:
            List of schema dicts from Unity Catalog API
        """
        return self._client.list_schemas(catalog, **kwargs)

    def get_schema(self, catalog: str, schema: str) -> Optional[Dict[str, Any]]:
        """Get schema details.

        Args:
            catalog: Catalog name
            schema: Schema name

        Returns:
            Schema details dict or None if not found
        """
        return self._client.get_schema(catalog, schema)

    def list_tables(self, catalog: str, schema: str, **kwargs) -> List[Dict[str, Any]]:
        """List tables in a schema.

        Args:
            catalog: Catalog name
            schema: Schema name
            **kwargs: Additional options passed to DatabricksAPIClient.list_tables()
                - include_delta_metadata: Include Delta table metadata
                - max_results: Maximum number of results to return
                - page_token: Pagination token

        Returns:
            List of table dicts from Unity Catalog API
        """
        return self._client.list_tables(catalog, schema, **kwargs)

    def get_table(
        self, catalog: str, schema: str, table: str
    ) -> Optional[Dict[str, Any]]:
        """Get table details.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name

        Returns:
            Table details dict or None if not found
        """
        return self._client.get_table(catalog, schema, table)

    def execute_query(self, query: str, **kwargs) -> Dict[str, Any]:
        """Execute SQL query using SQL warehouse.

        Args:
            query: SQL query string
            **kwargs: Additional execution options
                - warehouse_id: SQL warehouse ID (optional, uses config if not provided)
                - wait_timeout: Timeout for query completion
                - catalog: Catalog to use for query
                - schema: Schema to use for query

        Returns:
            Query results dict with status and data

        Raises:
            ValueError: If warehouse_id cannot be determined
        """
        warehouse_id = kwargs.get("warehouse_id")

        if not warehouse_id:
            # Fall back to config
            from chuck_data.config import get_config_manager

            config = get_config_manager().get_config()
            warehouse_id = config.warehouse_id

        if not warehouse_id:
            raise ValueError(
                "SQL warehouse ID required. Provide as kwarg or configure with /select-warehouse"
            )

        return self._client.submit_sql_statement(query, warehouse_id, **kwargs)

    def get_provider_name(self) -> str:
        """Return provider name.

        Returns:
            'databricks'
        """
        return "databricks"

    def get_capabilities(self) -> Dict[str, bool]:
        """Return Databricks capabilities.

        Returns:
            Dict mapping capability names to boolean values
        """
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

    # ===== Databricks-specific methods not in protocol =====
    # These maintain backward compatibility with existing code

    def list_volumes(self, catalog: str, schema: str, **kwargs):
        """List volumes in a schema (Databricks-specific).

        Args:
            catalog: Catalog name
            schema: Schema name
            **kwargs: Additional options

        Returns:
            List of volume dicts from Unity Catalog API
        """
        return self._client.list_volumes(catalog, schema, **kwargs)

    def create_volume(self, catalog: str, schema: str, volume_name: str, **kwargs):
        """Create a volume (Databricks-specific).

        Args:
            catalog: Catalog name
            schema: Schema name
            volume_name: Volume name
            **kwargs: Additional volume options

        Returns:
            Created volume details
        """
        return self._client.create_volume(catalog, schema, volume_name, **kwargs)

    def list_models(self, **kwargs):
        """List model serving endpoints (Databricks-specific).

        Args:
            **kwargs: Additional options

        Returns:
            List of model endpoint dicts
        """
        return self._client.list_models(**kwargs)

    def get_model(self, model_name: str):
        """Get model serving endpoint details (Databricks-specific).

        Args:
            model_name: Model endpoint name

        Returns:
            Model endpoint details dict
        """
        return self._client.get_model(model_name)

    def list_warehouses(self, **kwargs):
        """List SQL warehouses (Databricks-specific).

        Args:
            **kwargs: Additional options

        Returns:
            List of warehouse dicts
        """
        return self._client.list_warehouses(**kwargs)

    def get_warehouse(self, warehouse_id: str):
        """Get SQL warehouse details (Databricks-specific).

        Args:
            warehouse_id: Warehouse ID

        Returns:
            Warehouse details dict
        """
        return self._client.get_warehouse(warehouse_id)

    def create_warehouse(self, **kwargs):
        """Create a SQL warehouse (Databricks-specific).

        Args:
            **kwargs: Warehouse configuration options

        Returns:
            Created warehouse details
        """
        return self._client.create_warehouse(**kwargs)

    def submit_job_run(self, config_path: str, init_script_path: str, run_name: str):
        """Submit a job run (Databricks-specific).

        Args:
            config_path: Path to job configuration
            init_script_path: Path to init script
            run_name: Name for the job run

        Returns:
            Job run details dict with run_id
        """
        return self._client.submit_job_run(config_path, init_script_path, run_name)

    def get_job_run_status(self, run_id: str):
        """Get job run status (Databricks-specific).

        Args:
            run_id: Job run ID

        Returns:
            Job run status dict
        """
        return self._client.get_job_run_status(run_id)

    def upload_file(
        self,
        path: str,
        file_path: str = None,
        content: str = None,
        overwrite: bool = True,
    ):
        """Upload file to Databricks (Databricks-specific).

        Args:
            path: Target path in Databricks
            file_path: Local file path to upload
            content: File content as string
            overwrite: Whether to overwrite existing file

        Returns:
            Upload response dict
        """
        return self._client.upload_file(path, file_path, content, overwrite)

    def store_dbfs_file(self, path: str, contents: str, overwrite: bool = False):
        """Store file via DBFS API (Databricks-specific).

        Args:
            path: DBFS path
            contents: File contents as string
            overwrite: Whether to overwrite existing file

        Returns:
            Upload response dict
        """
        return self._client.store_dbfs_file(path, contents, overwrite)

    def get_current_user(self):
        """Get current logged-in user (Databricks-specific).

        Returns:
            User details dict
        """
        return self._client.get_current_user()

    def fetch_amperity_job_init(self, token: str, api_url: str):
        """Fetch Amperity job initialization script (Databricks-specific).

        Args:
            token: Amperity token
            api_url: Amperity API URL

        Returns:
            Job initialization script content
        """
        return self._client.fetch_amperity_job_init(token, api_url)

    def create_stitch_notebook(
        self,
        table_path: str,
        notebook_name: str,
        stitch_config: dict,
        datasources: list,
    ):
        """Create Stitch notebook (Databricks-specific).

        Args:
            table_path: Table path for Stitch
            notebook_name: Name for the notebook
            stitch_config: Stitch configuration dict
            datasources: List of data sources

        Returns:
            Created notebook details
        """
        return self._client.create_stitch_notebook(
            table_path, notebook_name, stitch_config, datasources
        )
