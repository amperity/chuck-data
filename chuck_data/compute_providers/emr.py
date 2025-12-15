"""EMR Compute Provider.

Runs Stitch jobs on Amazon EMR clusters.
"""

from typing import Dict, Any, Optional


class EMRComputeProvider:
    """Run Stitch jobs on Amazon EMR clusters.

    This compute provider can process data from:
    - AWS Redshift (via Spark-Redshift connector)
    - Databricks Unity Catalog (via Databricks JDBC connector)

    Note: This is a stub implementation for PR 1.
    Full implementation or detailed scaffolding will come in PR 4.

    Uses boto3 credential discovery chain:
    - AWS profiles (via aws_profile parameter)
    - IAM roles
    - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    - ~/.aws/credentials
    """

    def __init__(
        self,
        region: str,
        cluster_id: Optional[str] = None,
        aws_profile: Optional[str] = None,
        **kwargs,
    ):
        """Initialize EMR compute provider.

        Args:
            region: AWS region (e.g., 'us-west-2')
            cluster_id: EMR cluster ID (optional, can create on-demand)
            aws_profile: AWS profile name from ~/.aws/credentials (optional)
            **kwargs: Additional configuration options (e.g., s3_bucket, iam_role)

        Note: AWS credentials are discovered via boto3's standard credential chain:
              1. Explicit profile (if aws_profile provided)
              2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
              3. AWS credentials file (~/.aws/credentials)
              4. IAM role (when running on EC2/ECS/Lambda)
        """
        self.region = region
        self.cluster_id = cluster_id
        self.aws_profile = aws_profile
        self.config = kwargs

    def prepare_stitch_job(
        self,
        manifest: Dict[str, Any],
        data_provider: Any,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Prepare job artifacts for Stitch execution.

        Uploads manifests and init scripts via data_provider methods:
        - Redshift data → data_provider.upload_manifest() to S3
        - Databricks data → data_provider.upload_manifest() (if needed)

        Args:
            manifest: Stitch configuration
            data_provider: Data source provider (handles uploads to appropriate storage)
            config: Job configuration

        Returns:
            Preparation results

        Raises:
            NotImplementedError: Stub implementation
        """
        raise NotImplementedError(
            "EMRComputeProvider.prepare_stitch_job() " "will be implemented in PR 4"
        )

    def launch_stitch_job(self, preparation: Dict[str, Any]) -> Dict[str, Any]:
        """Launch the Stitch job on EMR.

        Args:
            preparation: Results from prepare_stitch_job()

        Returns:
            Job execution results

        Raises:
            NotImplementedError: Stub implementation
        """
        raise NotImplementedError(
            "EMRComputeProvider.launch_stitch_job() " "will be implemented in PR 4"
        )

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get job status.

        Args:
            job_id: Job identifier

        Returns:
            Job status information

        Raises:
            NotImplementedError: Stub implementation
        """
        raise NotImplementedError(
            "EMRComputeProvider.get_job_status() " "will be implemented in PR 4"
        )

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job.

        Args:
            job_id: Job identifier

        Returns:
            True if cancellation succeeded

        Raises:
            NotImplementedError: Stub implementation
        """
        raise NotImplementedError(
            "EMRComputeProvider.cancel_job() " "will be implemented in PR 4"
        )
