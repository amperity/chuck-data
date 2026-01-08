"""EMR Compute Provider.

Runs Stitch jobs on Amazon EMR (Elastic MapReduce) clusters using Apache Spark.

This provider enables running Stitch identity resolution jobs on AWS EMR clusters,
allowing customers to leverage their existing AWS infrastructure for data processing.
"""

import logging
import json
from typing import Dict, Any, Optional

from chuck_data.compute_providers.provider import ComputeProvider
from chuck_data.clients.amperity import AmperityAPIClient
from chuck_data.clients.emr import EMRAPIClient
from chuck_data.config import get_amperity_token
from chuck_data.clients.amperity import get_amperity_url


def modify_init_script_for_emr(init_script_content: str, s3_jar_path: str) -> str:
    """Modify Amperity init script for EMR compatibility.

    Transforms the init script to:
    1. Download JAR to /tmp instead of /opt/amperity (no sudo required in EMR)
    2. Copy JAR from /tmp to S3 for Spark job execution
    3. Set proper permissions and error handling

    Args:
        init_script_content: Original init script content from Amperity API
        s3_jar_path: S3 path where JAR should be uploaded (e.g., s3://bucket/chuck/jars/job-123.jar)

    Returns:
        Modified init script content with EMR-compatible paths

    Example:
        >>> s3_path = "s3://my-bucket/chuck/jars/job-20231215.jar"
        >>> modified = modify_init_script_for_emr(original_script, s3_path)
        >>> assert "/tmp/amperity" in modified
        >>> assert s3_path in modified
    """
    # Replace /opt/amperity with /tmp/amperity (no sudo required)
    modified_script = init_script_content.replace(
        "mkdir -p /opt/amperity",
        "# EMR: Download to /tmp instead of /opt/amperity (no sudo required)\nmkdir -p /tmp/amperity",
    ).replace("/opt/amperity/job.jar", "/tmp/amperity/job.jar")

    # Add S3 upload command at the end
    modified_script += f"\n\n# Copy JAR to S3 for Spark job execution\naws s3 cp /tmp/amperity/job.jar {s3_jar_path}\necho 'JAR uploaded to {s3_jar_path}'\n"

    return modified_script


class EMRComputeProvider(ComputeProvider):
    """Run Stitch jobs on Amazon EMR clusters.

    This compute provider can process data from:
    - AWS Redshift (via Spark-Redshift connector)
    - Databricks Unity Catalog (via Databricks JDBC connector)
    - S3 data sources (via native Spark S3 support)

    The EMR provider follows the same interface as DatabricksComputeProvider,
    allowing seamless switching between compute environments while maintaining
    the same workflow for Stitch job execution.

    Architecture:
    - Uses boto3 for AWS API interactions
    - Submits Spark jobs via EMR Steps API
    - Stores job artifacts (manifests, init scripts) in S3
    - Configures Spark with necessary connectors for data sources
    - Monitors job execution via EMR API polling

    AWS Credentials:
    Uses boto3 credential discovery chain:
    1. AWS profiles (via aws_profile parameter)
    2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    3. AWS credentials file (~/.aws/credentials)
    4. IAM roles (when running on EC2/ECS/Lambda/EMR)
    5. AWS SSO

    Future Implementation Notes:
    - Will require EMR API client wrapper (similar to DatabricksAPIClient)
    - Will use S3Storage provider for artifact uploads
    - Will integrate with Amperity backend for job tracking
    - Will support both persistent EMR clusters and on-demand job flows
    - Will handle PII scanning similar to Databricks provider
    - Will generate EMR-compatible Stitch configuration
    """

    def __init__(
        self,
        region: str,
        storage_provider: Any,
        cluster_id: Optional[str] = None,
        aws_profile: Optional[str] = None,
        s3_bucket: Optional[str] = None,
        **kwargs,
    ):
        """Initialize EMR compute provider.

        Args:
            region: AWS region (e.g., 'us-west-2')
            storage_provider: StorageProvider instance for uploading artifacts to S3.
                            Must be provided - use ProviderFactory.create_storage_provider('s3', config)
            cluster_id: EMR cluster ID (e.g., 'j-XXXXXXXXXXXXX')
                       If None, provider can create clusters on-demand (future)
            aws_profile: AWS profile name from ~/.aws/credentials
                        If None, uses default boto3 credential chain
            s3_bucket: S3 bucket for storing job artifacts (manifests, logs, init scripts)
                      Required for job execution
            **kwargs: Additional configuration options:
                - instance_type: EC2 instance type for workers (default: 'm5.xlarge')
                - instance_count: Number of worker nodes (default: 3)
                - iam_role: EMR service role ARN
                - ec2_key_name: EC2 key pair for SSH access
                - log_uri: S3 path for EMR logs
                - spark_version: Spark version (default: latest)
                - bootstrap_actions: List of bootstrap action configurations
                - spark_jars: Additional JARs to include (e.g., Stitch JAR)
                - spark_packages: Maven packages (e.g., 'io.github.spark-redshift-community:spark-redshift_2.12:6.5.1-spark_3.5')

        Note: AWS credentials are discovered via boto3's standard credential chain.
              See class docstring for credential resolution order.

        Examples:
            >>> # Use ProviderFactory to create EMR compute provider (recommended)
            >>> from chuck_data.provider_factory import ProviderFactory
            >>> provider = ProviderFactory.create_compute_provider(
            ...     "aws_emr",
            ...     {
            ...         "region": "us-west-2",
            ...         "cluster_id": "j-XXXXXXXXXXXXX",
            ...         "s3_bucket": "my-stitch-bucket"
            ...     }
            ... )

            >>> # Manual creation (not recommended - use factory instead)
            >>> storage_provider = ProviderFactory.create_storage_provider("s3", {
            ...     "region": "us-west-2",
            ...     "aws_profile": "production"
            ... })
            >>> provider = EMRComputeProvider(
            ...     region='us-west-2',
            ...     storage_provider=storage_provider,
            ...     cluster_id='j-XXXXXXXXXXXXX',
            ...     s3_bucket='my-stitch-bucket'
            ... )
        """
        if storage_provider is None:
            raise ValueError(
                "storage_provider is required. Use ProviderFactory.create_storage_provider('s3', config) "
                "to create the appropriate S3 storage provider instance."
            )

        self.region = region
        self.cluster_id = cluster_id
        self.aws_profile = aws_profile
        self.s3_bucket = s3_bucket
        self.config = kwargs
        self.storage_provider = storage_provider

        # Initialize EMR client
        self.emr_client = EMRAPIClient(
            region=region, cluster_id=cluster_id, aws_profile=aws_profile
        )

        logging.info(
            f"Initialized EMRComputeProvider (region={region}, "
            f"cluster_id={cluster_id or 'on-demand'}, "
            f"s3_bucket={s3_bucket})"
        )

    def prepare_stitch_job(
        self,
        manifest: Dict[str, Any],
        data_provider: Any,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Prepare job artifacts for Stitch execution on EMR.

        This method will perform the following steps (when implemented):

        1. **PII Scanning**: Use LLM client to scan data source for PII columns
           - Similar to DatabricksComputeProvider PII scanning workflow
           - Identifies columns containing personal information
           - Generates semantic tags for Stitch configuration

        2. **Stitch Configuration Generation**:
           - Creates Stitch job manifest with discovered PII tables
           - Configures field-level semantic tags (email, phone, address, etc.)
           - Sets up data source connectors:
             * For Redshift: Spark-Redshift connector configuration
             * For Databricks: JDBC connector configuration
             * For S3: Native Spark S3 configuration

        3. **Artifact Upload to S3**:
           - Uploads Stitch configuration JSON to S3
           - Uploads cluster bootstrap scripts (if needed)
           - Uploads Stitch JAR and dependencies
           - Uses S3Storage provider for file operations

        4. **EMR Step Configuration**:
           - Generates EMR step definition for Spark job
           - Configures Spark properties (memory, cores, shuffle partitions)
           - Sets up connector-specific Spark configurations
           - Prepares job monitoring and logging

        5. **Amperity Integration**:
           - Fetches Amperity job initialization script
           - Registers job with Amperity backend for tracking
           - Obtains job-id for status lookups

        Data Provider Integration:
        - **Redshift data**: Manifest includes Redshift JDBC connection details,
          job will use Spark-Redshift connector to read data
        - **Databricks data**: Manifest includes Databricks JDBC connection,
          job will use Databricks JDBC driver to access Unity Catalog
        - **S3 data**: Manifest includes S3 paths, native Spark S3 support

        Args:
            manifest: Stitch configuration with tables and semantic tags
                     Can be empty dict if PII scanning generates the configuration
            data_provider: Data source provider (RedshiftProviderAdapter, DatabricksProviderAdapter)
                          Used to discover available tables and schemas
            config: Job configuration including:
                - target_catalog/target_schema: Where to find source data
                - llm_client: LLM provider for PII scanning (required)
                - s3_bucket: S3 bucket for artifacts (overrides constructor value)
                - stitch_jar_path: S3 path to Stitch JAR (optional)
                - spark_config: Additional Spark configuration (optional)
                - bootstrap_actions: EMR bootstrap actions (optional)

        Returns:
            Preparation results with:
                - success: Boolean indicating success/failure
                - stitch_config: Generated Stitch configuration
                - metadata: Job metadata including:
                    * s3_config_path: S3 path to uploaded config
                    * s3_jar_path: S3 path to Stitch JAR
                    * emr_step_definition: EMR step configuration
                    * job_id: Amperity job ID
                    * pii_scan_output: PII scanning results
                    * unsupported_columns: Columns excluded from Stitch
                - error: Error message if preparation failed

        Raises:
            NotImplementedError: Full implementation coming in future PR

        Example future usage:
            >>> provider = EMRComputeProvider(region='us-west-2', cluster_id='j-XXX', s3_bucket='my-bucket')
            >>> redshift_provider = RedshiftProviderAdapter(...)
            >>> config = {
            ...     'target_catalog': 'my_database',
            ...     'target_schema': 'public',
            ...     'llm_client': llm_client
            ... }
            >>> result = provider.prepare_stitch_job(
            ...     manifest={},
            ...     data_provider=redshift_provider,
            ...     config=config
            ... )
            >>> print(result['stitch_config'])  # Generated Stitch configuration
            >>> print(result['metadata']['s3_config_path'])  # S3 path to config
        """
        raise NotImplementedError(
            "EMRComputeProvider.prepare_stitch_job() will be implemented in a future PR. "
            "This method will handle PII scanning, Stitch configuration generation, "
            "artifact uploads to S3, and EMR step preparation. "
            "See docstring for detailed implementation plan."
        )

    def launch_stitch_job(self, preparation: Dict[str, Any]) -> Dict[str, Any]:
        """Launch the Stitch job on EMR cluster.

        Args:
            preparation: Results from prepare_stitch_job() containing:
                - success: Must be True to proceed
                - stitch_config: Stitch configuration
                - metadata: Job metadata with S3 paths and connector configuration

        Returns:
            Job execution results with:
                - success: Boolean indicating launch success
                - message: Summary message with job details
                - step_id: EMR step ID (e.g., 's-XXXXXXXXXXXXX')
                - cluster_id: EMR cluster ID
                - job_id: Amperity job ID
                - s3_config_path: S3 path to configuration file
                - monitoring_url: EMR console URL for job monitoring
                - unsupported_columns: Columns excluded due to unsupported types
                - error: Error message if launch failed
        """
        if not preparation.get("success"):
            return {
                "success": False,
                "error": "Cannot launch job: preparation failed",
                "preparation_error": preparation.get("error"),
            }

        stitch_config = preparation["stitch_config"]
        metadata = preparation["metadata"]

        try:
            # Extract metadata
            stitch_job_name = metadata["stitch_job_name"]
            s3_config_path = metadata["s3_config_path"]
            s3_jar_path = metadata.get("s3_jar_path")
            main_class = metadata.get(
                "main_class", "amperity.stitch_standalone.generic_main"
            )
            unsupported_columns = metadata.get("unsupported_columns", [])

            # Get connector configuration (Redshift or Databricks)
            s3_temp_dir = metadata.get("s3_temp_dir")
            redshift_jdbc_url = metadata.get("redshift_jdbc_url")
            aws_iam_role = metadata.get("aws_iam_role")
            databricks_jdbc_url = metadata.get("databricks_jdbc_url")
            databricks_catalog = metadata.get("databricks_catalog")
            databricks_schema = metadata.get("databricks_schema")

            # Write Stitch config to S3
            config_content_json = json.dumps(stitch_config, indent=2)
            try:
                upload_success = self.storage_provider.upload_file(
                    path=s3_config_path, content=config_content_json, overwrite=True
                )
                if not upload_success:
                    return {
                        "success": False,
                        "error": f"Failed to write Stitch config to '{s3_config_path}'",
                    }
                logging.debug(f"Stitch config written to {s3_config_path}")
            except Exception as e:
                return {
                    "success": False,
                    "error": f"Failed to write Stitch config '{s3_config_path}': {str(e)}",
                }

            # Validate cluster is accessible
            try:
                if not self.emr_client.validate_connection():
                    return {
                        "success": False,
                        "error": f"Cannot connect to EMR cluster {self.cluster_id}. Please verify the cluster exists and is accessible.",
                    }

                cluster_status = self.emr_client.get_cluster_status()
                if cluster_status not in ["WAITING", "RUNNING"]:
                    return {
                        "success": False,
                        "error": f"EMR cluster {self.cluster_id} is in state '{cluster_status}'. Cluster must be WAITING or RUNNING to submit jobs.",
                    }
            except Exception as e:
                return {
                    "success": False,
                    "error": f"Error validating EMR cluster: {str(e)}",
                }

            # Submit init script execution step to download the JAR
            try:
                # Get init script path from metadata
                init_script_s3_path = metadata.get("init_script_path")

                if init_script_s3_path:
                    # Build environment variables for init script
                    # Similar to Databricks spark_env_vars

                    env_vars = {
                        "CHUCK_API_URL": f"https://{get_amperity_url()}",
                    }

                    logging.info(f"Submitting init script step: {init_script_s3_path}")
                    logging.debug(f"Environment variables: {env_vars}")

                    init_step_id = self.emr_client.submit_bash_script(
                        name=f"Download Stitch JAR: {stitch_job_name}",
                        script_s3_path=init_script_s3_path,
                        env_vars=env_vars,
                        action_on_failure="CANCEL_AND_WAIT",
                    )
                    s3_jar_path = metadata.get("s3_jar_path", "S3")
                    logging.info(
                        f"Init script step submitted: {init_step_id}. JAR will be downloaded to /tmp and uploaded to {s3_jar_path}"
                    )
                else:
                    logging.warning(
                        "No init_script_path found in metadata. JAR download may fail."
                    )

            except Exception as e:
                return {
                    "success": False,
                    "error": f"Failed to submit init script step: {str(e)}",
                }

            # Submit Spark job to EMR
            try:
                # Build Spark arguments for the job
                spark_args = metadata.get("spark_args", [])
                if not spark_args:
                    # Default Spark configuration
                    spark_args = [
                        "--executor-memory",
                        "8g",
                        "--executor-cores",
                        "4",
                        "--driver-memory",
                        "4g",
                    ]

                # Job arguments (empty string for tenant, config path)
                job_args = ["", s3_config_path]

                # Use S3 JAR path (uploaded by init script from /tmp)
                # Fall back to local path for backward compatibility
                jar_path = metadata.get("s3_jar_path", "/opt/amperity/job.jar")

                # Check data source type and use appropriate connector
                if redshift_jdbc_url and s3_temp_dir:
                    # Use submit_spark_redshift_job for Redshift data sources
                    step_id = self.emr_client.submit_spark_redshift_job(
                        name=f"Stitch Setup: {stitch_job_name}",
                        jar_path=jar_path,
                        main_class=main_class,
                        args=job_args,
                        s3_temp_dir=s3_temp_dir,
                        redshift_jdbc_url=redshift_jdbc_url,
                        aws_iam_role=aws_iam_role,
                        spark_args=spark_args,
                        action_on_failure="CONTINUE",
                    )
                elif databricks_jdbc_url:
                    # Use submit_spark_databricks_job for Databricks Unity Catalog data sources
                    step_id = self.emr_client.submit_spark_databricks_job(
                        name=f"Stitch Setup: {stitch_job_name}",
                        jar_path=jar_path,
                        main_class=main_class,
                        args=job_args,
                        databricks_jdbc_url=databricks_jdbc_url,
                        databricks_catalog=databricks_catalog,
                        databricks_schema=databricks_schema,
                        spark_args=spark_args,
                        action_on_failure="CONTINUE",
                    )
                else:
                    # Use standard submit_spark_job for other data sources
                    step_id = self.emr_client.submit_spark_job(
                        name=f"Stitch Setup: {stitch_job_name}",
                        jar_path=jar_path,
                        main_class=main_class,
                        args=job_args,
                        spark_args=spark_args,
                        action_on_failure="CONTINUE",
                    )

                if not step_id:
                    return {
                        "success": False,
                        "error": "Failed to launch job (no step_id returned)",
                    }

                logging.info(
                    f"Launched EMR step {step_id} on cluster {self.cluster_id}"
                )

            except Exception as e:
                return {
                    "success": False,
                    "error": f"Failed to launch Stitch job: {str(e)}",
                }

            # Record job submission with Amperity backend
            try:
                amperity_token = metadata.get("amperity_token") or get_amperity_token()
                job_id = metadata.get("job_id")

                if amperity_token and job_id:
                    amperity_client = AmperityAPIClient()
                    amperity_client.record_job_submission(
                        databricks_run_id=step_id,  # Reuse field for EMR step ID
                        token=amperity_token,
                        job_id=job_id,
                    )
                    logging.info(
                        f"Recorded job submission: job_id={job_id}, step_id={step_id}"
                    )
            except Exception as e:
                # Log warning but don't fail the launch
                logging.warning(f"Failed to record job submission with Amperity: {e}")

            # Get monitoring URL
            monitoring_url = self.emr_client.get_monitoring_url()

            return {
                "success": True,
                "message": f"Stitch job launched successfully on EMR cluster {self.cluster_id}",
                "step_id": step_id,
                "cluster_id": self.cluster_id,
                "job_id": job_id if job_id else None,
                "s3_config_path": s3_config_path,
                "stitch_job_name": stitch_job_name,
                "monitoring_url": monitoring_url,
                "unsupported_columns": unsupported_columns,
            }

        except Exception as e:
            logging.error(f"Error launching Stitch job: {e}", exc_info=True)
            return {"success": False, "error": f"Unexpected error: {str(e)}"}

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get job status from EMR.

        Args:
            job_id: EMR step identifier (e.g., 's-XXXXXXXXXXXXX')

        Returns:
            Job status information including:
                - success: Boolean indicating if status was retrieved
                - step_id: EMR step ID
                - cluster_id: EMR cluster ID
                - status: Unified job status (PENDING, RUNNING, SUCCESS, FAILED, CANCELLED)
                - state_message: Detailed state message from EMR
                - start_time: Job start timestamp (ISO 8601) if available
                - end_time: Job end timestamp if completed (ISO 8601)
                - failure_reason: Failure details if job failed
                - monitoring_url: EMR console URL
                - full_status: Complete status data from EMR
                - error: Error message if retrieval failed
        """
        try:
            # Get step status from EMR
            status_data = self.emr_client.get_step_status(job_id)

            # Map EMR states to unified status
            # EMR states: PENDING, RUNNING, COMPLETED, CANCELLED, FAILED, INTERRUPTED
            emr_status = status_data.get("status", "UNKNOWN")
            status_map = {
                "PENDING": "PENDING",
                "RUNNING": "RUNNING",
                "COMPLETED": "SUCCESS",
                "CANCELLED": "CANCELLED",
                "FAILED": "FAILED",
                "INTERRUPTED": "FAILED",
            }
            unified_status = status_map.get(emr_status, emr_status)

            result = {
                "success": True,
                "step_id": job_id,
                "cluster_id": self.cluster_id,
                "status": unified_status,
                "emr_status": emr_status,
                "state_message": status_data.get("state_message", ""),
                "monitoring_url": self.emr_client.get_monitoring_url(),
                "full_status": status_data,
            }

            # Add timeline information if available
            if "start_time" in status_data:
                result["start_time"] = status_data["start_time"]
            if "end_time" in status_data:
                result["end_time"] = status_data["end_time"]

            # Add failure information if job failed
            if emr_status in ["FAILED", "INTERRUPTED"]:
                result["failure_reason"] = status_data.get("failure_reason", "Unknown")
                if "failure_message" in status_data:
                    result["failure_message"] = status_data["failure_message"]
                if "log_file" in status_data:
                    result["log_uri"] = status_data["log_file"]

            return result

        except Exception as e:
            logging.error(
                f"Error getting job status for {job_id}: {str(e)}", exc_info=True
            )
            return {"success": False, "error": f"Failed to get job status: {str(e)}"}

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running EMR step.

        Args:
            job_id: EMR step identifier (e.g., 's-XXXXXXXXXXXXX')

        Returns:
            True if cancellation succeeded, False otherwise

        Note:
            Cancellation is asynchronous - the step state transitions to CANCELLED
            but may take a few seconds to fully terminate the running Spark job.
            Use get_job_status() to verify cancellation completed.
        """
        try:
            # Check if step is in a cancellable state
            status = self.get_job_status(job_id)
            if not status.get("success"):
                logging.error(f"Cannot get status for step {job_id}, cannot cancel")
                return False

            emr_status = status.get("emr_status", "")
            if emr_status not in ["PENDING", "RUNNING"]:
                logging.warning(
                    f"Step {job_id} is in state '{emr_status}', cannot cancel. "
                    f"Only PENDING or RUNNING steps can be cancelled."
                )
                return False

            # Cancel the step using EMR client
            success = self.emr_client.cancel_step(job_id)

            if success:
                logging.info(f"Successfully initiated cancellation for step {job_id}")
            else:
                logging.error(f"Failed to cancel step {job_id}")

            return success

        except Exception as e:
            logging.error(f"Error cancelling job {job_id}: {str(e)}", exc_info=True)
            return False
