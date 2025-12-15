"""Storage Provider Protocol.

Defines the interface that all storage providers must implement.
"""

from typing import Protocol


class StorageProvider(Protocol):
    """Protocol for storage providers that upload artifacts.

    Storage providers are responsible for uploading job artifacts (manifests,
    init scripts) to their respective storage backends. They are used by compute
    providers to store configuration files needed for job execution.

    Only upload_file is required. Download and list operations are not needed
    because storage providers are only used to upload manifest files and cluster
    init scripts. The actual data reading happens through the data providers
    (Unity Catalog, Redshift), not through storage.
    """

    def upload_file(self, content: str, path: str, overwrite: bool = True) -> bool:
        """Upload a file to the storage provider.

        Args:
            content: File content as a string
            path: Destination path in the storage system
                  - For Databricks: /Volumes/{catalog}/{schema}/{volume}/{filename}
                  - For S3: s3://{bucket}/{key}
            overwrite: Whether to overwrite existing files (default: True)

        Returns:
            True if upload succeeded, False otherwise

        Raises:
            Exception: If upload fails with detailed error message
        """
        ...
