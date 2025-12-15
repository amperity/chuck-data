"""Databricks Compute Provider.

Runs Stitch jobs on Databricks clusters.
"""

from typing import Dict, Any


class DatabricksComputeProvider:
    """Run Stitch jobs on Databricks clusters.

    This compute provider can process data from:
    - Databricks Unity Catalog (direct access)
    - AWS Redshift (via Spark-Redshift connector)

    Note: This is a stub implementation for PR 1.
    Full implementation will come in PR 3.
    """

    def __init__(self, workspace_url: str, token: str, **kwargs):
        """Initialize Databricks compute provider.

        Args:
            workspace_url: Databricks workspace URL
            token: Authentication token
            **kwargs: Additional configuration options
        """
        self.workspace_url = workspace_url
        self.token = token
        self.config = kwargs

    def prepare_stitch_job(
        self,
        manifest: Dict[str, Any],
        data_provider: Any,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Prepare job artifacts for Stitch execution.

        Uploads manifests and init scripts via data_provider methods:
        - Databricks data → data_provider.upload_manifest() to /Volumes
        - Redshift data → data_provider.upload_manifest() to S3

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
            "DatabricksComputeProvider.prepare_stitch_job() "
            "will be implemented in PR 3"
        )

    def launch_stitch_job(self, preparation: Dict[str, Any]) -> Dict[str, Any]:
        """Launch the Stitch job on Databricks.

        Args:
            preparation: Results from prepare_stitch_job()

        Returns:
            Job execution results

        Raises:
            NotImplementedError: Stub implementation
        """
        raise NotImplementedError(
            "DatabricksComputeProvider.launch_stitch_job() "
            "will be implemented in PR 3"
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
            "DatabricksComputeProvider.get_job_status() " "will be implemented in PR 3"
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
            "DatabricksComputeProvider.cancel_job() " "will be implemented in PR 3"
        )
