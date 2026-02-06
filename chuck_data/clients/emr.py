"""
Reusable AWS EMR API client for job submission and monitoring.

This client uses boto3 to provide EMR cluster management and job execution
capabilities similar to DatabricksAPIClient for Databricks workspaces.

Storage operations (S3) are handled by RedshiftAPIClient or S3Storage provider.
"""

import logging
import os
import shlex
import time
from typing import Dict, List, Optional, Any

import boto3
from botocore.exceptions import ClientError, BotoCoreError

from chuck_data.clients.amperity import get_amperity_url


class EMRAPIClient:
    """Reusable AWS EMR API client for cluster management and job execution."""

    def __init__(
        self,
        region: str,
        cluster_id: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_profile: Optional[str] = None,
    ):
        """
        Initialize the EMR API client.

        Args:
            region: AWS region (e.g., 'us-west-2')
            cluster_id: EMR cluster identifier (e.g., 'j-XXXXXXXXXXXXX')
                       Optional - can be set later or clusters can be created on-demand
            aws_access_key_id: AWS access key ID (optional, uses boto3 credential discovery if not provided)
            aws_secret_access_key: AWS secret access key (optional, uses boto3 credential discovery if not provided)
            aws_profile: AWS profile name from ~/.aws/credentials (optional)

        Note: Credentials are discovered via boto3's standard credential chain:
              1. Explicit credentials (aws_access_key_id, aws_secret_access_key)
              2. AWS profile (aws_profile parameter)
              3. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
              4. AWS credentials file (~/.aws/credentials)
              5. IAM roles (when running on EC2/ECS/Lambda/EMR)
        """
        self.region = region
        self.cluster_id = cluster_id
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_profile = aws_profile

        # Build boto3 client kwargs
        client_kwargs = {"region_name": region}

        # Credential priority (matching Redshift and boto3 standard):
        # 1. Explicit credentials passed as parameters
        # 2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        # 3. AWS profile
        # 4. Default credential chain (~/.aws/credentials, IAM roles, etc.)

        env_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        env_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        if aws_access_key_id and aws_secret_access_key:
            # Use explicit credentials passed as parameters
            client_kwargs["aws_access_key_id"] = aws_access_key_id
            client_kwargs["aws_secret_access_key"] = aws_secret_access_key
            self.emr = boto3.client("emr", **client_kwargs)
        elif env_access_key and env_secret_key:
            # Use environment variable credentials (don't pass profile to allow env vars)
            self.emr = boto3.client("emr", **client_kwargs)
        elif aws_profile:
            # Use session with profile
            session = boto3.Session(profile_name=aws_profile, region_name=region)
            self.emr = session.client("emr")
        else:
            # Use default credential chain
            self.emr = boto3.client("emr", **client_kwargs)

    #
    # Cluster management methods
    #

    def list_clusters(
        self,
        cluster_states: Optional[List[str]] = None,
        created_after: Optional[str] = None,
        created_before: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        List EMR clusters in the region.

        Args:
            cluster_states: Filter by cluster states (e.g., ['WAITING', 'RUNNING'])
                          If None, returns clusters in all states except TERMINATED
            created_after: Filter clusters created after this timestamp (ISO 8601)
            created_before: Filter clusters created before this timestamp (ISO 8601)

        Returns:
            List of cluster summaries

        Raises:
            ValueError: If an error occurs
        """
        try:
            params = {}
            if cluster_states:
                params["ClusterStates"] = cluster_states
            if created_after:
                params["CreatedAfter"] = created_after
            if created_before:
                params["CreatedBefore"] = created_before

            response = self.emr.list_clusters(**params)
            return response.get("Clusters", [])

        except ClientError as e:
            logging.debug(f"Error listing clusters: {e}")
            raise ValueError(f"Error listing clusters: {e}")
        except BotoCoreError as e:
            logging.debug(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    def describe_cluster(self, cluster_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get detailed information about an EMR cluster.

        Args:
            cluster_id: Cluster ID to describe. If None, uses self.cluster_id

        Returns:
            Cluster details including status, configuration, and metrics

        Raises:
            ValueError: If cluster_id is not provided and self.cluster_id is None
        """
        cid = cluster_id or self.cluster_id
        if not cid:
            raise ValueError("cluster_id must be provided or set in constructor")

        try:
            response = self.emr.describe_cluster(ClusterId=cid)
            return response.get("Cluster", {})

        except ClientError as e:
            logging.debug(f"Error describing cluster: {e}")
            raise ValueError(f"Error describing cluster: {e}")
        except BotoCoreError as e:
            logging.debug(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    def get_cluster_status(self, cluster_id: Optional[str] = None) -> str:
        """
        Get the current status of an EMR cluster.

        Args:
            cluster_id: Cluster ID to check. If None, uses self.cluster_id

        Returns:
            Cluster state string (e.g., 'STARTING', 'RUNNING', 'WAITING', 'TERMINATING', 'TERMINATED')

        Example:
            >>> client = EMRAPIClient(region='us-west-2', cluster_id='j-XXXXXXXXXXXXX')
            >>> status = client.get_cluster_status()
            >>> print(status)  # 'WAITING'
        """
        cluster = self.describe_cluster(cluster_id)
        return cluster.get("Status", {}).get("State", "UNKNOWN")

    def validate_connection(self, cluster_id: Optional[str] = None) -> bool:
        """
        Validate the EMR connection by attempting to describe the cluster.

        Args:
            cluster_id: Cluster ID to validate. If None, uses self.cluster_id

        Returns:
            True if connection is valid and cluster is accessible, False otherwise
        """
        try:
            self.describe_cluster(cluster_id)
            return True
        except Exception as e:
            logging.debug(f"Connection validation failed: {e}")
            return False

    def terminate_cluster(self, cluster_id: Optional[str] = None) -> bool:
        """
        Terminate an EMR cluster.

        Args:
            cluster_id: Cluster ID to terminate. If None, uses self.cluster_id

        Returns:
            True if termination was initiated successfully

        Raises:
            ValueError: If cluster_id is not provided and self.cluster_id is None
        """
        cid = cluster_id or self.cluster_id
        if not cid:
            raise ValueError("cluster_id must be provided or set in constructor")

        try:
            self.emr.terminate_job_flows(JobFlowIds=[cid])
            logging.info(f"Terminated EMR cluster: {cid}")
            return True

        except ClientError as e:
            logging.debug(f"Error terminating cluster: {e}")
            raise ValueError(f"Error terminating cluster: {e}")
        except BotoCoreError as e:
            logging.debug(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    #
    # Job execution methods (Steps API)
    #

    def add_job_flow_step(
        self,
        name: str,
        jar: str,
        main_class: Optional[str] = None,
        args: Optional[List[str]] = None,
        action_on_failure: str = "CONTINUE",
        cluster_id: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Add a Spark job step to an EMR cluster.

        Args:
            name: Step name
            jar: S3 path to JAR file or 'command-runner.jar' for script execution
            main_class: Main class to execute (for JAR files)
            args: Command-line arguments for the job
            action_on_failure: Action on failure ('CONTINUE', 'TERMINATE_CLUSTER', 'CANCEL_AND_WAIT')
            cluster_id: Cluster ID. If None, uses self.cluster_id
            properties: Additional Spark properties as key-value pairs

        Returns:
            Step ID (e.g., 's-XXXXXXXXXXXXX')

        Raises:
            ValueError: If cluster_id is not provided and self.cluster_id is None

        Example:
            >>> client = EMRAPIClient(region='us-west-2', cluster_id='j-XXXXXXXXXXXXX')
            >>> step_id = client.add_job_flow_step(
            ...     name='Stitch Job',
            ...     jar='s3://my-bucket/stitch.jar',
            ...     main_class='amperity.stitch_standalone.chuck_main',
            ...     args=['', 's3://my-bucket/config.json'],
            ...     action_on_failure='CONTINUE'
            ... )
            >>> print(f"Submitted step: {step_id}")
        """
        cid = cluster_id or self.cluster_id
        if not cid:
            raise ValueError("cluster_id must be provided or set in constructor")

        try:
            # Build Spark step configuration
            hadoop_jar_step = {
                "Jar": jar,
            }

            if main_class:
                hadoop_jar_step["MainClass"] = main_class

            if args:
                hadoop_jar_step["Args"] = args

            if properties:
                hadoop_jar_step["Properties"] = properties

            step = {
                "Name": name,
                "ActionOnFailure": action_on_failure,
                "HadoopJarStep": hadoop_jar_step,
            }

            response = self.emr.add_job_flow_steps(JobFlowId=cid, Steps=[step])
            step_id = response["StepIds"][0]

            logging.info(f"Added step {step_id} to cluster {cid}")
            return step_id

        except ClientError as e:
            logging.debug(f"Error adding step: {e}")
            raise ValueError(f"Error adding step: {e}")
        except BotoCoreError as e:
            logging.debug(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    def submit_bash_script(
        self,
        name: str,
        script_content: Optional[str] = None,
        script_s3_path: Optional[str] = None,
        env_vars: Optional[Dict[str, str]] = None,
        action_on_failure: str = "CONTINUE",
        cluster_id: Optional[str] = None,
    ) -> str:
        """
        Submit a bash script execution step to EMR cluster.

        The script can be provided either inline as content or as an S3 path.
        This is useful for executing init scripts, setup scripts, or other
        bash commands before running the main Spark job.

        Args:
            name: Step name
            script_content: Bash script content to execute (mutually exclusive with script_s3_path)
            script_s3_path: S3 path to bash script (mutually exclusive with script_content)
            env_vars: Optional dictionary of environment variables to set before script execution
                     Example: {'CHUCK_API_URL': 'https://api.amperity.com', 'DEBUG': 'true'}
            action_on_failure: Action on failure ('CONTINUE', 'TERMINATE_CLUSTER', 'CANCEL_AND_WAIT')
            cluster_id: Cluster ID. If None, uses self.cluster_id

        Returns:
            Step ID (e.g., 's-XXXXXXXXXXXXX')

        Raises:
            ValueError: If neither or both script_content and script_s3_path are provided

        Example:
            >>> # Execute inline script
            >>> client.submit_bash_script(
            ...     name='Setup Environment',
            ...     script_content='mkdir -p /opt/amperity && echo "Setup complete"'
            ... )
            >>> # Execute S3-hosted script with environment variables
            >>> client.submit_bash_script(
            ...     name='Run Init Script',
            ...     script_s3_path='s3://bucket/init.sh',
            ...     env_vars={'CHUCK_API_URL': 'https://api.amperity.com'}
            ... )
        """
        cid = cluster_id or self.cluster_id
        if not cid:
            raise ValueError("cluster_id must be provided or set in constructor")

        if not script_content and not script_s3_path:
            raise ValueError("Either script_content or script_s3_path must be provided")
        if script_content and script_s3_path:
            raise ValueError(
                "Only one of script_content or script_s3_path should be provided"
            )

        try:
            # Build environment variable exports if provided
            env_prefix = ""
            if env_vars:
                # Build export statements for each environment variable
                # Quote values to handle special characters safely
                exports = [f"export {key}='{value}'" for key, value in env_vars.items()]
                env_prefix = "; ".join(exports) + "; "

            # Build bash command
            if script_content:
                # Execute inline script content with environment variables
                full_command = (
                    f"{env_prefix}{script_content}" if env_prefix else script_content
                )
                bash_cmd = ["bash", "-c", full_command]
            else:
                # Download and execute script from S3 with environment variables
                script_execution = f"aws s3 cp {script_s3_path} - | bash"
                full_command = (
                    f"{env_prefix}{script_execution}"
                    if env_prefix
                    else script_execution
                )
                bash_cmd = ["bash", "-c", full_command]

            return self.add_job_flow_step(
                name=name,
                jar="command-runner.jar",
                args=bash_cmd,
                action_on_failure=action_on_failure,
                cluster_id=cid,
            )

        except Exception as e:
            logging.error(f"Error submitting bash script step: {e}", exc_info=True)
            raise

    def submit_spark_job(
        self,
        name: str,
        jar_path: str,
        main_class: str,
        args: Optional[List[str]] = None,
        spark_args: Optional[List[str]] = None,
        action_on_failure: str = "CONTINUE",
        cluster_id: Optional[str] = None,
    ) -> str:
        """
        Submit a Spark job using spark-submit via command-runner.jar.

        Args:
            name: Step name
            jar_path: S3 path to application JAR
            main_class: Main class to execute
            args: Application arguments
            spark_args: spark-submit arguments (e.g., ['--executor-memory', '4g'])
            action_on_failure: Action on failure ('CONTINUE', 'TERMINATE_CLUSTER', 'CANCEL_AND_WAIT')
            cluster_id: Cluster ID. If None, uses self.cluster_id

        Returns:
            Step ID (e.g., 's-XXXXXXXXXXXXX')

        Example:
            >>> client = EMRAPIClient(region='us-west-2', cluster_id='j-XXXXXXXXXXXXX')
            >>> step_id = client.submit_spark_job(
            ...     name='Stitch Job',
            ...     jar_path='s3://my-bucket/stitch.jar',
            ...     main_class='amperity.stitch_standalone.chuck_main',
            ...     args=['', 's3://my-bucket/config.json'],
            ...     spark_args=['--executor-memory', '4g', '--executor-cores', '2']
            ... )
        """
        # Build spark-submit command
        command_args = ["spark-submit"]

        # Add spark-submit arguments
        if spark_args:
            command_args.extend(spark_args)

        # Add main class and JAR
        command_args.extend(["--class", main_class, jar_path])

        # Add application arguments
        if args:
            command_args.extend(args)

        return self.add_job_flow_step(
            name=name,
            jar="command-runner.jar",
            args=command_args,
            action_on_failure=action_on_failure,
            cluster_id=cluster_id,
        )

    def submit_spark_redshift_job(
        self,
        name: str,
        jar_path: str,
        main_class: str,
        args: Optional[List[str]] = None,
        s3_temp_dir: Optional[str] = None,
        redshift_jdbc_url: Optional[str] = None,
        aws_iam_role: Optional[str] = None,
        spark_args: Optional[List[str]] = None,
        action_on_failure: str = "CONTINUE",
        cluster_id: Optional[str] = None,
    ) -> str:
        """
        Submit a Spark job with Spark-Redshift connector configuration.

        This method automatically configures the Spark job to use the Spark-Redshift
        connector for reading data from AWS Redshift. The connector handles:
        - JDBC connections to Redshift
        - S3 for temporary storage (UNLOAD/COPY operations)
        - IAM role authentication

        Args:
            name: Step name
            jar_path: S3 path to application JAR (e.g., Stitch JAR)
            main_class: Main class to execute (e.g., 'amperity.stitch_standalone.chuck_main')
            args: Application arguments (e.g., config path)
            s3_temp_dir: S3 path for temporary files (required for Redshift connector)
                        Format: 's3://bucket/path/' or 's3a://bucket/path/'
            redshift_jdbc_url: Redshift JDBC URL (optional, can be in config)
                              Format: 'jdbc:redshift://endpoint:5439/database'
            aws_iam_role: IAM role ARN for Redshift COPY/UNLOAD (optional)
            spark_args: Additional spark-submit arguments
            action_on_failure: Action on failure ('CONTINUE', 'TERMINATE_CLUSTER', 'CANCEL_AND_WAIT')
            cluster_id: Cluster ID. If None, uses self.cluster_id

        Returns:
            Step ID (e.g., 's-XXXXXXXXXXXXX')

        Raises:
            ValueError: If cluster_id is not provided and self.cluster_id is None

        Example:
            >>> client = EMRAPIClient(region='us-west-2', cluster_id='j-XXXXXXXXXXXXX')
            >>> step_id = client.submit_spark_redshift_job(
            ...     name='Stitch Job with Redshift',
            ...     jar_path='s3://my-bucket/stitch.jar',
            ...     main_class='amperity.stitch_standalone.chuck_main',
            ...     args=['', 's3://my-bucket/config.json'],
            ...     s3_temp_dir='s3://my-bucket/temp/',
            ...     redshift_jdbc_url='jdbc:redshift://my-cluster.redshift.amazonaws.com:5439/dev',
            ...     spark_args=['--executor-memory', '8g', '--executor-cores', '4']
            ... )
            >>> print(f"Submitted Redshift job: {step_id}")

        Notes:
            - The Spark-Redshift connector (spark-redshift_2.12:6.5.1-spark_3.5) should be installed
              on the EMR cluster via bootstrap actions or EMR release configuration
            - The connector uses S3 as temporary storage for efficient data transfer
            - IAM roles should have permissions for S3 and Redshift access
            - The application JAR receives the configuration with Redshift connection details
        """
        # Build spark-submit command with Redshift connector configuration
        # Start with spark-submit and build up the command parts
        spark_submit_parts = ["spark-submit"]

        # Add Spark-Redshift connector package
        # This will download the connector from Maven if not already present
        spark_submit_parts.extend(
            [
                "--packages",
                "io.github.spark-redshift-community:spark-redshift_2.12:6.5.1-spark_3.5,"
                "org.apache.spark:spark-avro_2.12:3.5.0",
            ]
        )

        # Configure S3 temporary directory for Redshift connector
        if s3_temp_dir:
            spark_submit_parts.extend(
                ["--conf", f"spark.hadoop.fs.s3a.tempdir={s3_temp_dir}"]
            )

        # Configure Redshift JDBC URL if provided
        if redshift_jdbc_url:
            spark_submit_parts.extend(
                ["--conf", f"spark.redshift.jdbc.url={redshift_jdbc_url}"]
            )

        # Configure IAM role for Redshift COPY/UNLOAD if provided
        if aws_iam_role:
            spark_submit_parts.extend(
                ["--conf", f"spark.redshift.aws_iam_role={aws_iam_role}"]
            )

        # Add Redshift-specific Spark configurations for performance
        spark_submit_parts.extend(
            [
                # Enable S3A for better S3 performance
                "--conf",
                "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
                # Configure S3A fast upload for large temporary files
                "--conf",
                "spark.hadoop.fs.s3a.fast.upload=true",
                # Increase S3A buffer size for better throughput
                "--conf",
                "spark.hadoop.fs.s3a.multipart.size=104857600",  # 100MB
            ]
        )

        # Add any additional spark-submit arguments from caller
        if spark_args:
            spark_submit_parts.extend(spark_args)

        # Add main class and JAR
        spark_submit_parts.extend(["--class", main_class, jar_path])

        # Add application arguments (e.g., config path)
        if args:
            spark_submit_parts.extend(args)

        # Build the full bash command with environment variable export
        # This ensures CHUCK_API_URL is available to the driver process
        spark_submit_cmd = " ".join(spark_submit_parts)
        chuck_api_url = f"https://{get_amperity_url()}"
        full_command = f"export CHUCK_API_URL={chuck_api_url} && {spark_submit_cmd}"

        # Wrap in bash -c for proper shell execution
        command_args = ["bash", "-c", full_command]

        return self.add_job_flow_step(
            name=name,
            jar="command-runner.jar",
            args=command_args,
            action_on_failure=action_on_failure,
            cluster_id=cluster_id,
        )

    def submit_spark_databricks_job(
        self,
        name: str,
        jar_path: str,
        main_class: str,
        args: Optional[List[str]] = None,
        databricks_jdbc_url: Optional[str] = None,
        databricks_catalog: Optional[str] = None,
        databricks_schema: Optional[str] = None,
        spark_args: Optional[List[str]] = None,
        action_on_failure: str = "CONTINUE",
        cluster_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Submit a Spark job with Databricks Unity Catalog connector configuration.

        This method configures Spark to read data from Databricks Unity Catalog using
        the Databricks JDBC connector. The connector allows EMR Spark jobs to access
        tables in Databricks Unity Catalog as if they were local Spark tables.

        Args:
            name: Human-readable name for the step
            jar_path: S3 path to application JAR (e.g., 's3://bucket/stitch.jar')
            main_class: Fully qualified main class name
            args: Application arguments (e.g., ['', 's3://bucket/config.json'])
            databricks_jdbc_url: Databricks JDBC connection URL
                Format: jdbc:databricks://workspace-host:443/default;httpPath=/sql/1.0/warehouses/warehouse-id;AuthMech=3;UID=token;PWD=token
            databricks_catalog: Databricks Unity Catalog name to access
            databricks_schema: Databricks schema within the catalog
            spark_args: Additional spark-submit arguments (e.g., ['--executor-memory', '8g'])
            action_on_failure: Action on failure ('CONTINUE', 'TERMINATE_CLUSTER', 'CANCEL_AND_WAIT')
            cluster_id: EMR cluster ID. If None, uses self.cluster_id

        Returns:
            Step ID (e.g., 's-XXXXXXXXXXXXX') or None if submission fails

        Example:
            >>> client = EMRAPIClient(region='us-west-2', cluster_id='j-XXX')
            >>> step_id = client.submit_spark_databricks_job(
            ...     name='Stitch with Databricks',
            ...     jar_path='s3://bucket/stitch.jar',
            ...     main_class='amperity.stitch_standalone.chuck_main',
            ...     args=['', 's3://bucket/config.json'],
            ...     databricks_jdbc_url='jdbc:databricks://workspace.cloud.databricks.com:443/default;...',
            ...     databricks_catalog='prod',
            ...     databricks_schema='customers',
            ...     spark_args=['--executor-memory', '8g']
            ... )

        Notes:
            - Requires Databricks JDBC driver and Spark connector packages
            - Authentication credentials are embedded in JDBC URL
            - The connector provides transparent access to Unity Catalog tables
            - The application JAR receives the configuration with Databricks connection details
        """
        # Build spark-submit command with Databricks connector configuration
        # Start with spark-submit and build up the command parts
        spark_submit_parts = ["spark-submit"]

        # Add Databricks JDBC connector packages
        # This will download the connector from Maven if not already present
        spark_submit_parts.extend(
            [
                "--packages",
                "com.databricks:databricks-jdbc:2.6.36",
                # ",com.databricks:spark-databricks_2.12:0.2.0",
            ]
        )

        # Configure Databricks JDBC URL if provided
        if databricks_jdbc_url:
            spark_submit_parts.extend(
                ["--conf", f"spark.databricks.jdbc.url={databricks_jdbc_url}"]
            )

        # Configure Databricks catalog if provided
        if databricks_catalog:
            spark_submit_parts.extend(
                ["--conf", f"spark.databricks.catalog={databricks_catalog}"]
            )

        # Configure Databricks schema if provided
        if databricks_schema:
            spark_submit_parts.extend(
                ["--conf", f"spark.databricks.schema={databricks_schema}"]
            )

        # Add Databricks-specific Spark configurations for performance
        spark_submit_parts.extend(
            [
                # Enable Databricks-optimized settings
                "--conf",
                "spark.databricks.delta.optimizeWrite.enabled=true",
                # Configure connection pooling
                "--conf",
                "spark.databricks.jdbc.pool.enabled=true",
                "--conf",
                "spark.databricks.jdbc.pool.maxSize=50",
            ]
        )

        # Add any additional spark-submit arguments from caller
        if spark_args:
            spark_submit_parts.extend(spark_args)

        # Add main class and JAR
        spark_submit_parts.extend(["--class", main_class, jar_path])

        # Add application arguments (e.g., config path)
        if args:
            spark_submit_parts.extend(args)

        # Build the full bash command with environment variable export
        # This ensures CHUCK_API_URL is available to the driver process
        # Use shlex.join to properly escape arguments containing special characters (e.g., UID= in JDBC URLs)
        spark_submit_cmd = shlex.join(spark_submit_parts)
        chuck_api_url = f"https://{get_amperity_url()}"
        full_command = (
            f"export CHUCK_API_URL={shlex.quote(chuck_api_url)} && {spark_submit_cmd}"
        )

        # Wrap in bash -c for proper shell execution
        command_args = ["bash", "-c", full_command]

        return self.add_job_flow_step(
            name=name,
            jar="command-runner.jar",
            args=command_args,
            action_on_failure=action_on_failure,
            cluster_id=cluster_id,
        )

    def describe_step(
        self, step_id: str, cluster_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get detailed information about a step.

        Args:
            step_id: Step ID to describe
            cluster_id: Cluster ID. If None, uses self.cluster_id

        Returns:
            Step details including status, timeline, and configuration

        Raises:
            ValueError: If cluster_id is not provided and self.cluster_id is None
        """
        cid = cluster_id or self.cluster_id
        if not cid:
            raise ValueError("cluster_id must be provided or set in constructor")

        try:
            response = self.emr.describe_step(ClusterId=cid, StepId=step_id)
            return response.get("Step", {})

        except ClientError as e:
            logging.debug(f"Error describing step: {e}")
            raise ValueError(f"Error describing step: {e}")
        except BotoCoreError as e:
            logging.debug(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    def get_step_status(
        self, step_id: str, cluster_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get the status of a step.

        Args:
            step_id: Step ID to check
            cluster_id: Cluster ID. If None, uses self.cluster_id

        Returns:
            Dictionary containing:
                - status: Step state ('PENDING', 'RUNNING', 'COMPLETED', 'CANCELLED', 'FAILED', 'INTERRUPTED')
                - state_message: Detailed status message
                - start_time: Start timestamp (if started)
                - end_time: End timestamp (if completed)
                - failure_reason: Failure details (if failed)

        Example:
            >>> status = client.get_step_status('s-XXXXXXXXXXXXX')
            >>> print(status['status'])  # 'COMPLETED'
            >>> print(status['start_time'])  # '2024-01-15T10:30:00Z'
        """
        step = self.describe_step(step_id, cluster_id)
        status_info = step.get("Status", {})
        timeline = status_info.get("Timeline", {})

        result = {
            "status": status_info.get("State", "UNKNOWN"),
            "state_message": status_info.get("StateChangeReason", {}).get(
                "Message", ""
            ),
        }

        # Add timeline information
        if "StartDateTime" in timeline:
            result["start_time"] = timeline["StartDateTime"].isoformat()
        if "EndDateTime" in timeline:
            result["end_time"] = timeline["EndDateTime"].isoformat()

        # Add failure reason if failed
        if result["status"] == "FAILED":
            failure_details = status_info.get("FailureDetails", {})
            result["failure_reason"] = failure_details.get("Reason", "Unknown")
            result["failure_message"] = failure_details.get("Message", "")
            result["log_file"] = failure_details.get("LogFile", "")

        return result

    def list_steps(
        self,
        cluster_id: Optional[str] = None,
        step_states: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        List steps for a cluster.

        Args:
            cluster_id: Cluster ID. If None, uses self.cluster_id
            step_states: Filter by step states (e.g., ['RUNNING', 'PENDING'])

        Returns:
            List of step summaries

        Raises:
            ValueError: If cluster_id is not provided and self.cluster_id is None
        """
        cid = cluster_id or self.cluster_id
        if not cid:
            raise ValueError("cluster_id must be provided or set in constructor")

        try:
            params = {"ClusterId": cid}
            if step_states:
                params["StepStates"] = step_states

            response = self.emr.list_steps(**params)
            return response.get("Steps", [])

        except ClientError as e:
            logging.debug(f"Error listing steps: {e}")
            raise ValueError(f"Error listing steps: {e}")
        except BotoCoreError as e:
            logging.debug(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    def cancel_step(self, step_id: str, cluster_id: Optional[str] = None) -> bool:
        """
        Cancel a running or pending step.

        Args:
            step_id: Step ID to cancel
            cluster_id: Cluster ID. If None, uses self.cluster_id

        Returns:
            True if cancellation was initiated successfully

        Raises:
            ValueError: If cluster_id is not provided and self.cluster_id is None

        Note:
            Only steps in PENDING or RUNNING state can be cancelled.
        """
        cid = cluster_id or self.cluster_id
        if not cid:
            raise ValueError("cluster_id must be provided or set in constructor")

        try:
            self.emr.cancel_steps(ClusterId=cid, StepIds=[step_id])
            logging.info(f"Cancelled step {step_id} on cluster {cid}")
            return True

        except ClientError as e:
            logging.debug(f"Error cancelling step: {e}")
            raise ValueError(f"Error cancelling step: {e}")
        except BotoCoreError as e:
            logging.debug(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    def wait_for_step(
        self,
        step_id: str,
        cluster_id: Optional[str] = None,
        timeout: int = 3600,
        poll_interval: int = 10,
    ) -> Dict[str, Any]:
        """
        Wait for a step to complete.

        Args:
            step_id: Step ID to wait for
            cluster_id: Cluster ID. If None, uses self.cluster_id
            timeout: Maximum time to wait in seconds (default: 3600 = 1 hour)
            poll_interval: Seconds between status checks (default: 10)

        Returns:
            Final step status dictionary

        Raises:
            ValueError: If step fails or timeout is reached
        """
        start_time = time.time()

        while True:
            status = self.get_step_status(step_id, cluster_id)
            state = status["status"]

            if state == "COMPLETED":
                return status
            elif state in ["FAILED", "CANCELLED", "INTERRUPTED"]:
                raise ValueError(
                    f"Step {step_id} {state.lower()}: {status.get('state_message', 'Unknown error')}"
                )

            # Check timeout
            if time.time() - start_time > timeout:
                raise ValueError(f"Step {step_id} timed out after {timeout} seconds")

            # Wait before next poll
            time.sleep(poll_interval)

    #
    # Utility methods
    #

    def get_cluster_dns(self, cluster_id: Optional[str] = None) -> str:
        """
        Get the master node public DNS name.

        Args:
            cluster_id: Cluster ID. If None, uses self.cluster_id

        Returns:
            Master node public DNS name

        Example:
            >>> dns = client.get_cluster_dns()
            >>> print(f"ssh hadoop@{dns}")
        """
        cluster = self.describe_cluster(cluster_id)
        return cluster.get("MasterPublicDnsName", "")

    def get_monitoring_url(self, cluster_id: Optional[str] = None) -> str:
        """
        Get the EMR console URL for monitoring the cluster.

        Args:
            cluster_id: Cluster ID. If None, uses self.cluster_id

        Returns:
            EMR console URL
        """
        cid = cluster_id or self.cluster_id
        if not cid:
            raise ValueError("cluster_id must be provided or set in constructor")

        return (
            f"https://{self.region}.console.aws.amazon.com/emr/home?"
            f"region={self.region}#/clusterDetails/{cid}"
        )
