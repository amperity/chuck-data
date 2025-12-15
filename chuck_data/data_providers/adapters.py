"""Data Provider Adapters.

These adapters wrap existing clients to conform to the DataProvider protocol.
"""

from typing import List, Dict, Optional, Any
from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.clients.redshift import RedshiftAPIClient


class DatabricksProviderAdapter:
    """Adapter for DatabricksAPIClient to conform to DataProvider protocol."""

    def __init__(self, workspace_url: str, token: str):
        """Initialize Databricks provider adapter.

        Args:
            workspace_url: Databricks workspace URL
            token: Authentication token
        """
        self.client = DatabricksAPIClient(workspace_url=workspace_url, token=token)

    def validate_connection(self) -> bool:
        """Validate connection by attempting to list catalogs."""
        try:
            self.client.list_catalogs()
            return True
        except Exception:
            return False

    def list_databases(self) -> List[str]:
        """List Unity Catalog catalogs.

        Returns:
            List of catalog names
        """
        catalogs = self.client.list_catalogs()
        return [catalog["name"] for catalog in catalogs.get("catalogs", [])]

    def list_schemas(self, catalog: Optional[str] = None) -> List[str]:
        """List schemas in a catalog.

        Args:
            catalog: Catalog name (required for Databricks)

        Returns:
            List of schema names
        """
        if not catalog:
            raise ValueError("Databricks provider requires 'catalog' parameter")

        schemas = self.client.list_schemas(catalog_name=catalog)
        return [schema["name"] for schema in schemas.get("schemas", [])]

    def list_tables(
        self, catalog: Optional[str] = None, schema: Optional[str] = None, **kwargs: Any
    ) -> List[Dict]:
        """List tables in a schema.

        Args:
            catalog: Catalog name (required for Databricks)
            schema: Schema name (required for Databricks)
            **kwargs: Additional filters

        Returns:
            List of table metadata dictionaries
        """
        if not catalog or not schema:
            raise ValueError(
                "Databricks provider requires 'catalog' and 'schema' parameters"
            )

        tables = self.client.list_tables(catalog_name=catalog, schema_name=schema)
        return tables.get("tables", [])

    def get_table(
        self,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> Dict:
        """Get table metadata.

        Args:
            catalog: Catalog name (required for Databricks)
            schema: Schema name (required for Databricks)
            table: Table name (required for Databricks)

        Returns:
            Table metadata dictionary
        """
        if not catalog or not schema or not table:
            raise ValueError(
                "Databricks provider requires 'catalog', 'schema', and 'table' parameters"
            )

        full_name = f"{catalog}.{schema}.{table}"
        return self.client.get_table(full_name=full_name)

    def execute_query(
        self, query: str, catalog: Optional[str] = None, **kwargs: Any
    ) -> Dict:
        """Execute SQL query using Databricks SQL warehouse.

        Args:
            query: SQL query to execute
            catalog: Catalog name (optional)
            **kwargs: Additional parameters including:
                - warehouse_id (required): SQL warehouse ID
                - wait_timeout: How long to wait for query completion (default "30s")
                - on_wait_timeout: What to do on timeout ("CONTINUE" or "CANCEL")

        Returns:
            Dictionary containing query results

        Raises:
            ValueError: If warehouse_id is not provided
        """
        warehouse_id = kwargs.get("warehouse_id")
        if not warehouse_id:
            raise ValueError(
                "Databricks query execution requires 'warehouse_id' parameter"
            )

        return self.client.submit_sql_statement(
            sql_text=query,
            warehouse_id=warehouse_id,
            catalog=catalog,
            wait_timeout=kwargs.get("wait_timeout", "30s"),
            on_wait_timeout=kwargs.get("on_wait_timeout", "CONTINUE"),
        )


class RedshiftProviderAdapter:
    """Adapter for RedshiftAPIClient to conform to DataProvider protocol."""

    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region: str,
        cluster_identifier: Optional[str] = None,
        workgroup_name: Optional[str] = None,
        database: str = "dev",
        s3_bucket: Optional[str] = None,
        redshift_iam_role: Optional[str] = None,
        emr_cluster_id: Optional[str] = None,
    ):
        """Initialize Redshift provider adapter.

        Args:
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            region: AWS region
            cluster_identifier: Redshift cluster identifier (optional)
            workgroup_name: Redshift Serverless workgroup name (optional)
            database: Default database name
            s3_bucket: S3 bucket for intermediate storage
            redshift_iam_role: IAM role ARN for Redshift COPY/UNLOAD operations
            emr_cluster_id: EMR cluster ID (optional)
        """
        self.client = RedshiftAPIClient(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region=region,
            cluster_identifier=cluster_identifier,
            workgroup_name=workgroup_name,
            database=database,
            s3_bucket=s3_bucket,
        )
        # Store additional config not needed by client
        self.redshift_iam_role = redshift_iam_role
        self.emr_cluster_id = emr_cluster_id

    def validate_connection(self) -> bool:
        """Validate Redshift connection.

        Returns:
            True if connection is valid, False otherwise
        """
        return self.client.validate_connection()

    def list_databases(self) -> List[str]:
        """List Redshift databases.

        Returns:
            List of database names
        """
        return self.client.list_databases()

    def list_schemas(self, catalog: Optional[str] = None) -> List[str]:
        """List schemas in a database.

        Args:
            catalog: Database name (uses client default if not specified)

        Returns:
            List of schema names
        """
        return self.client.list_schemas(database=catalog)

    def list_tables(
        self, catalog: Optional[str] = None, schema: Optional[str] = None, **kwargs: Any
    ) -> List[Dict]:
        """List tables in a schema.

        Args:
            catalog: Database name (uses client default if not specified)
            schema: Schema pattern to filter (optional)
            **kwargs: Additional filters (e.g., table_pattern)

        Returns:
            List of table metadata dictionaries
        """
        result = self.client.list_tables(
            database=catalog, schema_pattern=schema, **kwargs
        )
        # Extract tables from response dictionary
        return result.get("tables", [])

    def get_table(
        self,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> Dict:
        """Get table metadata.

        Args:
            catalog: Database name (uses client default if not specified)
            schema: Schema name (required)
            table: Table name (required)

        Returns:
            Table metadata dictionary
        """
        return self.client.describe_table(database=catalog, schema=schema, table=table)

    def execute_query(
        self, query: str, catalog: Optional[str] = None, **kwargs: Any
    ) -> Dict:
        """Execute SQL query.

        Args:
            query: SQL query to execute
            catalog: Database name (uses client default if not specified)
            **kwargs: Additional parameters (e.g., wait=True)

        Returns:
            Dictionary containing query results
        """
        return self.client.execute_sql(sql=query, database=catalog, **kwargs)
