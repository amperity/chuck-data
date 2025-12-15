"""S3 Storage Provider.

Handles uploading files to Amazon S3.
"""

import logging
from typing import Optional

import boto3
from botocore.exceptions import ClientError


class S3Storage:
    """Upload files to Amazon S3.

    This storage provider uploads job artifacts (manifests, init scripts) to
    S3, which are accessible from EMR clusters and other AWS services.

    Uses boto3 for S3 operations with standard AWS credential chain:
    1. Explicit credentials (if provided)
    2. AWS profiles (via aws_profile parameter)
    3. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    4. AWS credentials file (~/.aws/credentials)
    5. IAM roles (when running on EC2/ECS/Lambda/EMR)
    """

    def __init__(
        self,
        region: str = "us-east-1",
        aws_profile: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ):
        """Initialize S3 Storage.

        Args:
            region: AWS region (default: us-east-1)
            aws_profile: AWS profile name from ~/.aws/credentials (optional)
            aws_access_key_id: AWS access key ID (optional, for explicit credentials)
            aws_secret_access_key: AWS secret access key (optional, for explicit credentials)

        Note: If aws_access_key_id and aws_secret_access_key are provided, they
              will be used. Otherwise, boto3's credential chain is used.
        """
        self.region = region
        self.aws_profile = aws_profile

        # Create boto3 session
        if aws_profile:
            session = boto3.Session(profile_name=aws_profile, region_name=region)
        elif aws_access_key_id and aws_secret_access_key:
            session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region,
            )
        else:
            session = boto3.Session(region_name=region)

        self.s3_client = session.client("s3")

    def upload_file(self, content: str, path: str, overwrite: bool = True) -> bool:
        """Upload a file to S3.

        Args:
            content: File content as a string
            path: S3 path (e.g., s3://bucket/path/to/file.txt)
            overwrite: Whether to overwrite existing files (default: True)
                      Note: S3 PutObject always overwrites by default

        Returns:
            True if upload succeeded, False otherwise

        Raises:
            Exception: If upload fails with detailed error message

        Example:
            >>> storage = S3Storage(region="us-west-2")
            >>> success = storage.upload_file(
            ...     content='{"key": "value"}',
            ...     path="s3://my-bucket/stitch/config.json"
            ... )
            >>> assert success == True
        """
        # Parse S3 path
        if not path.startswith("s3://"):
            raise ValueError(f"S3 path must start with 's3://': {path}")

        # Remove s3:// prefix and split into bucket and key
        path_without_prefix = path[5:]  # Remove 's3://'
        parts = path_without_prefix.split("/", 1)

        if len(parts) != 2:
            raise ValueError(
                f"Invalid S3 path format (expected s3://bucket/key): {path}"
            )

        bucket, key = parts

        try:
            logging.debug(f"Uploading file to S3: s3://{bucket}/{key}")

            # Upload file content as string
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=content.encode("utf-8"),
                ContentType="text/plain",
            )

            logging.info(f"Successfully uploaded file to s3://{bucket}/{key}")
            return True

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_msg = e.response.get("Error", {}).get("Message", str(e))
            logging.error(
                f"S3 ClientError uploading to s3://{bucket}/{key}: "
                f"{error_code} - {error_msg}",
                exc_info=True,
            )
            raise Exception(
                f"Failed to upload file to S3 s3://{bucket}/{key}: "
                f"{error_code} - {error_msg}"
            ) from e

        except Exception as e:
            logging.error(
                f"Error uploading file to s3://{bucket}/{key}: {str(e)}", exc_info=True
            )
            raise Exception(
                f"Failed to upload file to S3 s3://{bucket}/{key}: {str(e)}"
            ) from e
