"""Storage Provider Abstractions.

Storage providers handle uploading artifacts (manifests, init scripts) to different
storage backends. They are used by compute providers to store job configuration files.

Each data provider has a natural storage backend:
- Databricks: /Volumes  (DatabricksVolumeStorage)
- Redshift:   S3        (S3Storage)
- Snowflake:  Snowflake internal stage  (SnowflakeStorageProvider)

Only upload_file is required. The stitch-standalone JAR reads the uploaded
manifest from whichever path the storage provider produces.
"""

from chuck_data.storage_providers.protocol import StorageProvider
from chuck_data.storage_providers.databricks import DatabricksVolumeStorage
from chuck_data.storage_providers.s3 import S3Storage
from chuck_data.storage_providers.snowflake import SnowflakeStorageProvider

__all__ = [
    "StorageProvider",
    "DatabricksVolumeStorage",
    "S3Storage",
    "SnowflakeStorageProvider",
]
