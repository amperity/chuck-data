"""
Reusable AWS Redshift API client for authentication and requests.

This client uses boto3 and the Redshift Data API to provide interactive
browsing capabilities similar to DatabricksAPIClient for Unity Catalog.
"""

import logging
import time
from typing import Dict, List, Optional, Any

import boto3
from botocore.exceptions import ClientError, BotoCoreError


class RedshiftAPIClient:
    """Reusable AWS Redshift API client for authentication and metadata operations."""

    def __init__(
        self,
        region: str,
        cluster_identifier: Optional[str] = None,
        workgroup_name: Optional[str] = None,
        database: str = "dev",
        s3_bucket: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_profile: Optional[str] = None,
    ):
        """
        Initialize the Redshift API client.

        Args:
            region: AWS region (e.g., 'us-west-2')
            cluster_identifier: Redshift cluster identifier (for provisioned clusters)
            workgroup_name: Redshift Serverless workgroup name (alternative to cluster_identifier)
            database: Default database name
            s3_bucket: S3 bucket for intermediate storage (required for Spark-Redshift connector)
            aws_access_key_id: AWS access key ID (optional, will use boto3 credential discovery if not provided)
            aws_secret_access_key: AWS secret access key (optional, will use boto3 credential discovery if not provided)
            aws_profile: AWS profile name (optional, will use AWS_PROFILE env var if not provided)

        Note: Either cluster_identifier or workgroup_name must be provided.
              If aws_access_key_id and aws_secret_access_key are not provided,
              boto3 will automatically discover credentials from aws_profile,
              AWS_PROFILE env var, ~/.aws/credentials, IAM roles, etc.
        """
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_profile = aws_profile
        self.region = region
        self.cluster_identifier = cluster_identifier
        self.workgroup_name = workgroup_name
        self.database = database
        self.s3_bucket = s3_bucket

        # Validate that either cluster_identifier or workgroup_name is provided
        if not cluster_identifier and not workgroup_name:
            raise ValueError(
                "Either cluster_identifier or workgroup_name must be provided"
            )

        # Create boto3 session
        # Credential priority (boto3 standard):
        # 1. Explicit credentials passed as parameters
        # 2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        # 3. AWS profile
        # 4. Default credential chain (~/.aws/credentials, IAM roles, etc.)

        import os

        env_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        env_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        if aws_access_key_id and aws_secret_access_key:
            # Use explicit credentials passed as parameters
            session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region,
            )
        elif env_access_key and env_secret_key:
            # Use environment variable credentials (don't pass profile to allow env vars)
            session = boto3.Session(region_name=region)
        elif aws_profile:
            # Use session with profile
            session = boto3.Session(profile_name=aws_profile, region_name=region)
        else:
            # Use default credential chain
            session = boto3.Session(region_name=region)

        # Initialize boto3 clients from session
        self.redshift_data = session.client("redshift-data")
        self.redshift = session.client("redshift")
        self.s3 = session.client("s3")

    #
    # Connection validation methods
    #

    def validate_connection(self) -> bool:
        """
        Validate the Redshift connection by attempting to list databases.

        Returns:
            True if connection is valid, False otherwise
        """
        try:
            self.list_databases()
            return True
        except Exception as e:
            logging.debug(f"Connection validation failed: {e}")
            return False

    #
    # SQL execution methods
    #

    def execute_sql(
        self, sql: str, database: Optional[str] = None, wait: bool = True
    ) -> Dict:
        """
        Execute SQL using Redshift Data API.

        Args:
            sql: SQL statement to execute
            database: Database name (uses default if not specified)
            wait: Whether to wait for statement completion

        Returns:
            Dictionary containing statement_id and optionally results

        Raises:
            ValueError: If an error occurs during execution
        """
        db = database or self.database

        try:
            # Build request parameters
            params = {
                "Database": db,
                "Sql": sql,
            }

            # Add cluster identifier or workgroup name
            if self.cluster_identifier:
                params["ClusterIdentifier"] = self.cluster_identifier
            elif self.workgroup_name:
                params["WorkgroupName"] = self.workgroup_name

            response = self.redshift_data.execute_statement(**params)
            statement_id = response["Id"]

            if wait:
                return self._wait_for_statement(statement_id)

            return {"statement_id": statement_id}

        except ClientError as e:
            logging.debug(f"SQL execution error: {e}")
            raise ValueError(f"SQL execution failed: {e}")
        except BotoCoreError as e:
            logging.debug(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    def _wait_for_statement(self, statement_id: str, timeout: int = 300) -> Dict:
        """
        Wait for SQL statement to complete.

        Args:
            statement_id: Statement ID to wait for
            timeout: Maximum time to wait in seconds

        Returns:
            Dictionary containing statement result

        Raises:
            ValueError: If statement fails or times out
        """
        start_time = time.time()

        while True:
            try:
                response = self.redshift_data.describe_statement(Id=statement_id)
                status = response["Status"]

                if status == "FINISHED":
                    # Check if statement has results (SELECT queries return results, DDL statements don't)
                    has_result_set = response.get("HasResultSet", False)

                    if has_result_set:
                        # Get results for SELECT queries
                        result = self.redshift_data.get_statement_result(
                            Id=statement_id
                        )
                        return {
                            "statement_id": statement_id,
                            "status": status,
                            "result": result,
                        }
                    else:
                        # DDL statements (CREATE, DROP, INSERT, etc.) don't have results
                        return {
                            "statement_id": statement_id,
                            "status": status,
                            "result": None,
                        }
                elif status == "FAILED":
                    error = response.get("Error", "Unknown error")
                    raise ValueError(f"Statement failed: {error}")
                elif status == "ABORTED":
                    raise ValueError("Statement was aborted")

                # Check timeout
                if time.time() - start_time > timeout:
                    raise ValueError(f"Statement timed out after {timeout} seconds")

                # Wait before polling again
                time.sleep(1)

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                # ResourceNotFoundException means the query doesn't have results (DDL statement)
                if error_code == "ResourceNotFoundException":
                    # This is actually success for DDL statements
                    return {
                        "statement_id": statement_id,
                        "status": "FINISHED",
                        "result": None,
                    }
                logging.debug(f"Error waiting for statement: {e}")
                raise ValueError(f"Error waiting for statement: {e}")

    def get_statement_result(self, statement_id: str) -> Dict:
        """
        Retrieve query results for a statement.

        Args:
            statement_id: Statement ID to retrieve results for

        Returns:
            Dictionary containing query results
        """
        try:
            return self.redshift_data.get_statement_result(Id=statement_id)
        except ClientError as e:
            logging.debug(f"Error getting statement result: {e}")
            raise ValueError(f"Error getting statement result: {e}")

    #
    # Database/Schema/Table metadata methods (parallel to DatabricksAPIClient)
    #

    def list_databases(self, database: Optional[str] = None) -> Dict:
        """
        List Redshift databases.

        This is parallel to DatabricksAPIClient.list_catalogs().

        Args:
            database: Database name to connect to for listing (uses default if not specified)

        Returns:
            Dictionary containing databases list in format: {"databases": [{"name": "db1"}, ...]}

        Raises:
            ValueError: If an error occurs
        """
        db = database or self.database

        try:
            # Build request parameters
            params = {"Database": db}

            # Add cluster identifier or workgroup name
            if self.cluster_identifier:
                params["ClusterIdentifier"] = self.cluster_identifier
            elif self.workgroup_name:
                params["WorkgroupName"] = self.workgroup_name

            response = self.redshift_data.list_databases(**params)
            # Return in same format as Databricks for consistency
            return {"databases": [{"name": db} for db in response.get("Databases", [])]}

        except ClientError as e:
            logging.debug(f"Error listing databases: {e}")
            raise ValueError(f"Error listing databases: {e}")
        except BotoCoreError as e:
            logging.debug(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    def list_schemas(self, database: Optional[str] = None) -> Dict:
        """
        List schemas in a database.

        This is parallel to DatabricksAPIClient.list_schemas(catalog_name).

        Args:
            database: Database name (uses default if not specified)

        Returns:
            Dictionary containing schemas list in format: {"schemas": [{"name": "schema1"}, ...]}

        Raises:
            ValueError: If an error occurs
        """
        db = database or self.database

        try:
            # Build request parameters
            params = {"Database": db}

            # Add cluster identifier or workgroup name
            if self.cluster_identifier:
                params["ClusterIdentifier"] = self.cluster_identifier
            elif self.workgroup_name:
                params["WorkgroupName"] = self.workgroup_name

            response = self.redshift_data.list_schemas(**params)
            # Return in same format as Databricks for consistency
            return {
                "schemas": [{"name": schema} for schema in response.get("Schemas", [])]
            }

        except ClientError as e:
            logging.debug(f"Error listing schemas: {e}")
            raise ValueError(f"Error listing schemas: {e}")
        except BotoCoreError as e:
            logging.debug(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    def list_tables(
        self,
        database: Optional[str] = None,
        schema_pattern: Optional[str] = None,
        table_pattern: Optional[str] = None,
        omit_columns: bool = False,
    ) -> Dict[str, List[Dict]]:
        """
        List tables in a schema.

        This is parallel to DatabricksAPIClient.list_tables(catalog_name, schema_name).

        Args:
            database: Database name (uses default if not specified)
            schema_pattern: Schema pattern to filter (optional)
            table_pattern: Table pattern to filter (optional)
            omit_columns: Whether to omit column information (optional, for API parity with Databricks)

        Returns:
            Dictionary with "tables" key containing list of table metadata dictionaries

        Raises:
            ValueError: If an error occurs
        """
        db = database or self.database

        try:
            # Build request parameters
            params = {"Database": db}

            # Add cluster identifier or workgroup name
            if self.cluster_identifier:
                params["ClusterIdentifier"] = self.cluster_identifier
            elif self.workgroup_name:
                params["WorkgroupName"] = self.workgroup_name

            # Add optional filters
            if schema_pattern:
                params["SchemaPattern"] = schema_pattern
            if table_pattern:
                params["TablePattern"] = table_pattern

            # Note: omit_columns parameter is accepted for API compatibility with Databricks
            # but has no effect since Redshift's list_tables API doesn't return column information

            response = self.redshift_data.list_tables(**params)
            # Return in same format as Databricks for consistency
            return {"tables": response.get("Tables", [])}

        except ClientError as e:
            logging.debug(f"Error listing tables: {e}")
            raise ValueError(f"Error listing tables: {e}")
        except BotoCoreError as e:
            logging.debug(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    def describe_table(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> Dict:
        """
        Get table schema/metadata.

        This is parallel to DatabricksAPIClient.get_table(full_name).

        Args:
            database: Database name (uses default if not specified)
            schema: Schema name
            table: Table name

        Returns:
            Dictionary containing table metadata and column information

        Raises:
            ValueError: If an error occurs or required parameters are missing
        """
        if not schema or not table:
            raise ValueError("Both schema and table must be specified")

        db = database or self.database

        try:
            # Build request parameters
            params = {
                "Database": db,
                "Schema": schema,
                "Table": table,
            }

            # Add cluster identifier or workgroup name
            if self.cluster_identifier:
                params["ClusterIdentifier"] = self.cluster_identifier
            elif self.workgroup_name:
                params["WorkgroupName"] = self.workgroup_name

            response = self.redshift_data.describe_table(**params)
            return response

        except ClientError as e:
            logging.debug(f"Error describing table: {e}")
            raise ValueError(f"Error describing table: {e}")
        except BotoCoreError as e:
            logging.debug(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    #
    # S3 operations (for Spark-Redshift connector)
    #

    def upload_to_s3(self, local_path: str, s3_key: str) -> str:
        """
        Upload file to S3 bucket.

        Args:
            local_path: Local file path to upload
            s3_key: S3 key (path within bucket)

        Returns:
            S3 URI (s3://bucket/key)

        Raises:
            ValueError: If S3 bucket is not configured
        """
        if not self.s3_bucket:
            raise ValueError("S3 bucket not configured")

        try:
            self.s3.upload_file(local_path, self.s3_bucket, s3_key)
            return f"s3://{self.s3_bucket}/{s3_key}"
        except ClientError as e:
            logging.debug(f"Error uploading to S3: {e}")
            raise ValueError(f"Error uploading to S3: {e}")

    def list_s3_objects(self, prefix: str) -> List[str]:
        """
        List objects in S3 bucket.

        Args:
            prefix: S3 key prefix to filter

        Returns:
            List of S3 keys

        Raises:
            ValueError: If S3 bucket is not configured
        """
        if not self.s3_bucket:
            raise ValueError("S3 bucket not configured")

        try:
            response = self.s3.list_objects_v2(Bucket=self.s3_bucket, Prefix=prefix)
            return [obj["Key"] for obj in response.get("Contents", [])]
        except ClientError as e:
            logging.debug(f"Error listing S3 objects: {e}")
            raise ValueError(f"Error listing S3 objects: {e}")

    def read_semantic_tags(self, database: str, schema_name: str) -> Dict[str, Any]:
        """Read semantic tags from chuck_metadata.semantic_tags table.

        Args:
            database: Database name
            schema_name: Schema name

        Returns:
            Dict with 'success': bool, 'tags': list of dicts or 'error': str
        """
        try:
            query = f"""
            SELECT table_name, column_name, semantic_type
            FROM chuck_metadata.semantic_tags
            WHERE database_name = '{database}'
            AND schema_name = '{schema_name}'
            ORDER BY table_name, column_name
            """

            logging.info(f"Reading semantic tags with query: {query}")
            result = self.execute_sql(query, database=database)
            logging.info(
                f"Query result structure: {result.keys() if result else 'None'}"
            )
            logging.info(f"Full result: {result}")

            if not result.get("result"):
                logging.warning(f"No 'result' key in response: {result}")
                return {
                    "success": False,
                    "error": "No results returned from semantic_tags query",
                }

            rows = result["result"].get("Records", [])
            logging.info(f"Found {len(rows)} rows in semantic_tags table")
            tags = []
            for row in rows:
                tags.append(
                    {
                        "table": row[0]["stringValue"],
                        "column": row[1]["stringValue"],
                        "semantic": row[2]["stringValue"],
                    }
                )

            logging.info(f"Successfully parsed {len(tags)} semantic tags")
            return {"success": True, "tags": tags}

        except Exception as e:
            logging.error(f"Error reading semantic tags: {e}", exc_info=True)
            return {
                "success": False,
                "error": f"Failed to read semantic tags: {str(e)}",
            }

    def read_table_schemas(
        self, database: str, schema_name: str, semantic_tags: list
    ) -> Dict[str, Any]:
        """Read table schemas from Redshift to get all column definitions.

        Args:
            database: Database name
            schema_name: Schema name
            semantic_tags: List of semantic tag dicts with 'table', 'column', 'semantic' keys

        Returns:
            Dict with 'success': bool, 'tables': list of table dicts or 'error': str
        """
        try:
            table_names = list(set(tag["table"] for tag in semantic_tags))

            tables = []
            for table_name in table_names:
                logging.debug(f"Reading schema for {table_name}...")

                query = f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}'
                AND table_name = '{table_name}'
                ORDER BY ordinal_position
                """

                result = self.execute_sql(query, database=database)

                if not result.get("result"):
                    logging.warning(f"No columns found for {table_name}")
                    continue

                rows = result["result"].get("Records", [])
                columns = []
                for row in rows:
                    col_name = row[0]["stringValue"]
                    col_type = row[1]["stringValue"]

                    semantic = None
                    for tag in semantic_tags:
                        if tag["table"] == table_name and tag["column"] == col_name:
                            semantic = tag["semantic"]
                            break

                    columns.append(
                        {
                            "name": col_name,
                            "type": col_type,
                            "semantic": semantic,
                        }
                    )

                tables.append({"table_name": table_name, "columns": columns})

            return {"success": True, "tables": tables}

        except Exception as e:
            logging.error(f"Error reading table schemas: {e}", exc_info=True)
            return {
                "success": False,
                "error": f"Failed to read table schemas: {str(e)}",
            }
