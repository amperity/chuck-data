"""Compute Provider Protocol.

Defines the interface that all compute providers must implement.
"""

from typing import Protocol, Dict, Any, Optional


class ComputeProvider(Protocol):
    """Protocol for compute providers.

    Compute providers define WHERE Stitch jobs execute (Databricks clusters, EMR clusters).
    This is independent of where data comes from (DataProvider).

    A Stitch job can:
    - Read from Databricks → Run on Databricks
    - Read from Redshift → Run on Databricks
    - Read from Redshift → Run on EMR (future)
    - Read from Databricks → Run on EMR (future)
    """

    def prepare_stitch_job(
        self,
        manifest: Dict[str, Any],
        data_provider: Any,  # DataProvider type
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Prepare job artifacts for Stitch execution.

        This method prepares everything needed to run a Stitch job:
        - Upload manifest and init scripts via data_provider methods
        - Create job definitions (Databricks job, EMR step, etc.)
        - Configure data connectors based on data_provider type

        The data_provider handles uploads:
        - Databricks data → Upload to /Volumes
        - Redshift data → Upload to S3

        Args:
            manifest: Stitch configuration with tables and semantic tags
            data_provider: Where the data comes from (Databricks/Redshift)
                          Also handles uploading artifacts to appropriate storage
            config: Job configuration (cluster size, etc.)

        Returns:
            Dictionary containing preparation results (job_id, paths, etc.)
        """
        ...

    def launch_stitch_job(self, preparation: Dict[str, Any]) -> Dict[str, Any]:
        """Launch the Stitch job on this compute platform.

        Args:
            preparation: Results from prepare_stitch_job()

        Returns:
            Dictionary containing job execution results (run_id, status, etc.)
        """
        ...

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get the status of a running or completed job.

        Args:
            job_id: Job identifier

        Returns:
            Dictionary containing job status information
        """
        ...

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job.

        Args:
            job_id: Job identifier

        Returns:
            True if cancellation succeeded
        """
        ...
