"""Databricks Volume Storage Provider.

Handles uploading files to Databricks Unity Catalog Volumes.
"""

import logging
from typing import Optional

from chuck_data.clients.databricks import DatabricksAPIClient


class DatabricksVolumeStorage:
    """Upload files to Databricks Unity Catalog Volumes.

    This storage provider uploads job artifacts (manifests, init scripts) to
    Databricks Volumes, which are accessible from Databricks clusters.

    Unity Catalog Volumes provide a file system interface for storing and
    accessing files from within Databricks workspaces.
    """

    def __init__(
        self,
        workspace_url: str,
        token: str,
        client: Optional[DatabricksAPIClient] = None,
    ):
        """Initialize Databricks Volume Storage.

        Args:
            workspace_url: Databricks workspace URL
            token: Authentication token
            client: Optional existing DatabricksAPIClient to reuse
                   If not provided, a new client will be created
        """
        self.workspace_url = workspace_url
        self.token = token
        self.client = client or DatabricksAPIClient(
            workspace_url=workspace_url, token=token
        )

    def upload_file(self, content: str, path: str, overwrite: bool = True) -> bool:
        """Upload a file to Databricks Volumes.

        Args:
            content: File content as a string
            path: Volume path (e.g., /Volumes/catalog/schema/volume/file.txt)
            overwrite: Whether to overwrite existing files (default: True)

        Returns:
            True if upload succeeded, False otherwise

        Raises:
            Exception: If upload fails with detailed error message

        Example:
            >>> storage = DatabricksVolumeStorage(
            ...     workspace_url="https://my-workspace.databricks.com",
            ...     token="dapi..."
            ... )
            >>> success = storage.upload_file(
            ...     content='{"key": "value"}',
            ...     path="/Volumes/catalog/schema/chuck/config.json"
            ... )
            >>> assert success == True
        """
        try:
            logging.debug(f"Uploading file to Databricks Volume: {path}")
            success = self.client.upload_file(
                path=path, content=content, overwrite=overwrite
            )

            if success:
                logging.info(f"Successfully uploaded file to {path}")
            else:
                logging.error(f"Failed to upload file to {path}")

            return success

        except Exception as e:
            logging.error(f"Error uploading file to {path}: {str(e)}", exc_info=True)
            raise Exception(
                f"Failed to upload file to Databricks Volume {path}: {str(e)}"
            ) from e
