"""Data Provider Adapters.

These adapters wrap existing clients to conform to the DataProvider protocol.
"""

from typing import List, Dict, Optional, Any
from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.clients.redshift import RedshiftAPIClient
from chuck_data.data_providers.provider import DataProvider


class DatabricksProviderAdapter(DataProvider):
    """Adapter for DatabricksAPIClient to conform to DataProvider protocol.

    Implements the DataProvider protocol.
    """

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

    def tag_columns(
        self,
        tags: List[Dict[str, str]],
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict:
        """Apply semantic tags to columns using ALTER TABLE statements.

        Args:
            tags: List of tag dictionaries with keys:
                - table: Fully qualified table name (catalog.schema.table)
                - column: Column name
                - semantic_type: Semantic type (e.g., 'pii/email')
            catalog: Catalog name (not used, table names are already fully qualified)
            schema: Schema name (not used, table names are already fully qualified)
            **kwargs: Additional parameters including:
                - warehouse_id (required): SQL warehouse ID for executing ALTER TABLE statements

        Returns:
            Dictionary containing:
                - success: bool (True if all tags applied successfully)
                - tags_applied: int (number of tags successfully applied)
                - errors: List[Dict] (any errors that occurred)
        """
        warehouse_id = kwargs.get("warehouse_id")
        if not warehouse_id:
            raise ValueError("Databricks tag_columns requires 'warehouse_id' parameter")

        tags_applied = 0
        errors = []

        for tag in tags:
            table_name = tag.get("table")
            column_name = tag.get("column")
            semantic_type = tag.get("semantic_type")

            if not table_name or not column_name or not semantic_type:
                errors.append(
                    {
                        "table": table_name or "unknown",
                        "column": column_name or "unknown",
                        "error": "Missing table, column, or semantic_type",
                    }
                )
                continue

            # Construct ALTER TABLE statement
            sql = f"""
            ALTER TABLE {table_name}
            ALTER COLUMN {column_name}
            SET TAGS ('semantic' = '{semantic_type}')
            """

            try:
                result = self.client.submit_sql_statement(
                    sql_text=sql,
                    warehouse_id=warehouse_id,
                    wait_timeout=kwargs.get("wait_timeout", "30s"),
                )

                if result.get("status", {}).get("state") == "SUCCEEDED":
                    tags_applied += 1
                else:
                    # Extract error information
                    status = result.get("status", {})
                    error_info = status.get("error", {})

                    if isinstance(error_info, dict):
                        error_message = error_info.get("message", "Unknown SQL error")
                    else:
                        error_message = (
                            str(error_info) if error_info else "Unknown error"
                        )

                    errors.append(
                        {
                            "table": table_name,
                            "column": column_name,
                            "error": error_message,
                        }
                    )
            except Exception as e:
                errors.append(
                    {"table": table_name, "column": column_name, "error": str(e)}
                )

        return {
            "success": len(errors) == 0,
            "tags_applied": tags_applied,
            "errors": errors,
        }


class RedshiftProviderAdapter(DataProvider):
    """Adapter for RedshiftAPIClient to conform to DataProvider protocol.

    Implements the DataProvider protocol.
    """

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

    def tag_columns(
        self,
        tags: List[Dict[str, str]],
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict:
        """Store semantic tags in Redshift metadata table.

        Redshift doesn't support native column tags, so we create a metadata table
        (chuck_metadata.semantic_tags) to store semantic type information that can
        be queried by stitch and other tools.

        Args:
            tags: List of tag dictionaries with keys:
                - table: Table name (not fully qualified, just table name)
                - column: Column name
                - semantic_type: Semantic type (e.g., 'pii/email')
            catalog: Database name (uses client default if not specified)
            schema: Schema name (required for Redshift)
            **kwargs: Additional parameters (accepted for protocol compatibility but unused)

        Returns:
            Dictionary containing:
                - success: bool (True if all tags stored successfully)
                - tags_applied: int (number of tags successfully stored)
                - errors: List[Dict] (any errors that occurred)
        """
        import logging

        # kwargs accepted for protocol compatibility but unused for Redshift
        _ = kwargs

        if not schema:
            raise ValueError("Redshift tag_columns requires 'schema' parameter")

        database = catalog or self.client.database

        try:
            # Step 1: Create chuck_metadata schema if it doesn't exist
            create_schema_sql = "CREATE SCHEMA IF NOT EXISTS chuck_metadata"
            self.client.execute_sql(create_schema_sql, database=database)

            # Step 2: Create semantic_tags table if it doesn't exist
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS chuck_metadata.semantic_tags (
                database_name VARCHAR(256),
                schema_name VARCHAR(256),
                table_name VARCHAR(256),
                column_name VARCHAR(256),
                semantic_type VARCHAR(256),
                updated_at TIMESTAMP DEFAULT GETDATE(),
                PRIMARY KEY (database_name, schema_name, table_name, column_name)
            )
            """
            self.client.execute_sql(create_table_sql, database=database)

            # Step 3: Delete existing tags for this schema (to handle re-tagging)
            delete_sql = f"""
            DELETE FROM chuck_metadata.semantic_tags
            WHERE database_name = '{database}'
            AND schema_name = '{schema}'
            """
            self.client.execute_sql(delete_sql, database=database)

            # Step 4: Insert tags
            if not tags:
                return {"success": True, "tags_applied": 0, "errors": []}

            # Build INSERT statement with all values
            values_list = []
            for tag in tags:
                table_name = tag.get("table")
                column_name = tag.get("column")
                semantic_type = tag.get("semantic_type")

                if not table_name or not column_name or not semantic_type:
                    logging.warning(f"Skipping tag with missing fields: {tag}")
                    continue

                # Escape single quotes in values
                table_name = table_name.replace("'", "''")
                column_name = column_name.replace("'", "''")
                semantic_type = semantic_type.replace("'", "''")

                values_list.append(
                    f"('{database}', '{schema}', '{table_name}', '{column_name}', '{semantic_type}', GETDATE())"
                )

            if not values_list:
                return {"success": True, "tags_applied": 0, "errors": []}

            insert_sql = f"""
            INSERT INTO chuck_metadata.semantic_tags
                (database_name, schema_name, table_name, column_name, semantic_type, updated_at)
            VALUES {', '.join(values_list)}
            """

            self.client.execute_sql(insert_sql, database=database)

            return {"success": True, "tags_applied": len(values_list), "errors": []}

        except Exception as e:
            import traceback

            logging.error(f"Error storing PII tags in Redshift: {str(e)}")
            logging.error(traceback.format_exc())
            return {"success": False, "tags_applied": 0, "errors": [{"error": str(e)}]}
