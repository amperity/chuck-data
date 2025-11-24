"""
Reusable AWS Redshift API client for authentication and requests.

This client uses boto3 and the Redshift Data API to provide interactive
browsing capabilities similar to DatabricksAPIClient for Unity Catalog.
"""

import logging
import time
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError, BotoCoreError


class RedshiftAPIClient:
    """Reusable AWS Redshift API client for authentication and metadata operations."""

    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region: str,
        cluster_identifier: Optional[str] = None,
        workgroup_name: Optional[str] = None,
        database: str = "dev",
        s3_bucket: Optional[str] = None,
        emr_cluster_id: Optional[str] = None,
    ):
        """
        Initialize the Redshift API client.

        Args:
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            region: AWS region (e.g., 'us-west-2')
            cluster_identifier: Redshift cluster identifier (for provisioned clusters)
            workgroup_name: Redshift Serverless workgroup name (alternative to cluster_identifier)
            database: Default database name
            s3_bucket: S3 bucket for intermediate storage (required for Spark-Redshift connector)
            emr_cluster_id: EMR cluster ID (optional, for future EMR support)

        Note: Either cluster_identifier or workgroup_name must be provided.
        """
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region = region
        self.cluster_identifier = cluster_identifier
        self.workgroup_name = workgroup_name
        self.database = database
        self.s3_bucket = s3_bucket
        self.emr_cluster_id = emr_cluster_id

        # Validate that either cluster_identifier or workgroup_name is provided
        if not cluster_identifier and not workgroup_name:
            raise ValueError(
                "Either cluster_identifier or workgroup_name must be provided"
            )

        # Initialize boto3 clients
        self.redshift_data = boto3.client(
            "redshift-data",
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        self.redshift = boto3.client(
            "redshift",
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        self.s3 = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        self.emr = boto3.client(
            "emr",
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

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
                    # Get results
                    result = self.redshift_data.get_statement_result(Id=statement_id)
                    return {
                        "statement_id": statement_id,
                        "status": status,
                        "result": result,
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

    def list_databases(self) -> List[str]:
        """
        List Redshift databases.

        This is parallel to DatabricksAPIClient.list_catalogs().

        Returns:
            List of database names

        Raises:
            ValueError: If an error occurs
        """
        try:
            # Build request parameters
            params = {}

            # Add cluster identifier or workgroup name
            if self.cluster_identifier:
                params["ClusterIdentifier"] = self.cluster_identifier
            elif self.workgroup_name:
                params["WorkgroupName"] = self.workgroup_name

            response = self.redshift_data.list_databases(**params)
            return [db for db in response.get("Databases", [])]

        except ClientError as e:
            logging.debug(f"Error listing databases: {e}")
            raise ValueError(f"Error listing databases: {e}")
        except BotoCoreError as e:
            logging.debug(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    def list_schemas(self, database: Optional[str] = None) -> List[str]:
        """
        List schemas in a database.

        This is parallel to DatabricksAPIClient.list_schemas(catalog_name).

        Args:
            database: Database name (uses default if not specified)

        Returns:
            List of schema names

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
            return [schema for schema in response.get("Schemas", [])]

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
    ) -> List[Dict]:
        """
        List tables in a schema.

        This is parallel to DatabricksAPIClient.list_tables(catalog_name, schema_name).

        Args:
            database: Database name (uses default if not specified)
            schema_pattern: Schema pattern to filter (optional)
            table_pattern: Table pattern to filter (optional)

        Returns:
            List of dictionaries containing table metadata

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

            response = self.redshift_data.list_tables(**params)
            return response.get("Tables", [])

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

    #
    # EMR operations (for future EMR support)
    #

    def list_emr_clusters(self) -> List[Dict]:
        """
        List available EMR clusters for user selection during setup.

        Returns:
            List of dictionaries containing cluster information

        Raises:
            ValueError: If an error occurs
        """
        try:
            response = self.emr.list_clusters(
                ClusterStates=["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"]
            )
            return response.get("Clusters", [])
        except ClientError as e:
            logging.debug(f"Error listing EMR clusters: {e}")
            raise ValueError(f"Error listing EMR clusters: {e}")

    def verify_emr_cluster_running(self) -> str:
        """
        Verify that the configured EMR cluster exists and is running.

        Returns:
            Cluster state (e.g., 'RUNNING', 'WAITING')

        Raises:
            ValueError: If EMR cluster is not configured or cannot be verified
        """
        if not self.emr_cluster_id:
            raise ValueError("EMR cluster ID not configured")

        try:
            response = self.emr.describe_cluster(ClusterId=self.emr_cluster_id)
            return response["Cluster"]["Status"]["State"]
        except ClientError as e:
            logging.debug(f"Error verifying EMR cluster: {e}")
            raise ValueError(f"Unable to verify EMR cluster {self.emr_cluster_id}: {e}")

    def submit_emr_spark_job(
        self, jar_s3_path: str, main_class: str, args: List[str]
    ) -> Dict:
        """
        Submit Spark job to pre-existing EMR cluster.

        Note: Requires EMR cluster to be created and running before job submission.
        Users must provide cluster ID during setup.

        Args:
            jar_s3_path: S3 path to JAR file
            main_class: Main class to execute
            args: Arguments to pass to main class

        Returns:
            Dictionary containing cluster_id and step_id

        Raises:
            ValueError: If EMR cluster is not configured or not running
        """
        if not self.emr_cluster_id:
            raise ValueError(
                "EMR cluster ID not configured. Please set emr_cluster_id during initialization."
            )

        # Verify cluster is running
        cluster_status = self.verify_emr_cluster_running()
        if cluster_status not in ["RUNNING", "WAITING"]:
            raise ValueError(
                f"EMR cluster {self.emr_cluster_id} is not in RUNNING/WAITING state. Current state: {cluster_status}"
            )

        step_config = {
            "Name": "Chuck Stitch Job",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--class",
                    main_class,
                    "--master",
                    "yarn",
                    "--deploy-mode",
                    "cluster",
                    "--conf",
                    "spark.jars.packages=io.github.spark-redshift-community:spark-redshift_2.12:6.2.0",
                    "--conf",
                    f"spark.hadoop.fs.s3a.access.key={self.aws_access_key_id}",
                    "--conf",
                    f"spark.hadoop.fs.s3a.secret.key={self.aws_secret_access_key}",
                    jar_s3_path,
                    *args,
                ],
            },
        }

        try:
            response = self.emr.add_job_flow_steps(
                JobFlowId=self.emr_cluster_id, Steps=[step_config]
            )

            return {
                "cluster_id": self.emr_cluster_id,
                "step_id": response["StepIds"][0],
            }
        except ClientError as e:
            logging.debug(f"Error submitting EMR job: {e}")
            raise ValueError(f"Error submitting EMR job: {e}")

    def get_emr_step_status(self, step_id: str) -> Dict:
        """
        Check EMR step status on configured cluster.

        Args:
            step_id: Step ID to check

        Returns:
            Dictionary containing step information

        Raises:
            ValueError: If EMR cluster is not configured
        """
        if not self.emr_cluster_id:
            raise ValueError("EMR cluster ID not configured")

        try:
            response = self.emr.describe_step(
                ClusterId=self.emr_cluster_id, StepId=step_id
            )
            return response["Step"]
        except ClientError as e:
            logging.debug(f"Error getting EMR step status: {e}")
            raise ValueError(f"Error getting EMR step status: {e}")
