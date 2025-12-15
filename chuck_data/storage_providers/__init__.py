"""Storage Provider Abstractions.

Storage providers handle uploading artifacts (manifests, init scripts) to different
storage backends. They are used by compute providers to store job configuration files.

Note: Only upload_file is needed. Storage providers are used to upload manifest files
and cluster init scripts to the appropriate location:
- Databricks: /Volumes
- AWS/EMR: S3

Download and list operations are not needed for the Stitch workflow.
"""

from chuck_data.storage_providers.protocol import StorageProvider
from chuck_data.storage_providers.databricks import DatabricksVolumeStorage
from chuck_data.storage_providers.s3 import S3Storage

__all__ = [
    "StorageProvider",
    "DatabricksVolumeStorage",
    "S3Storage",
]
