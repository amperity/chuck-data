"""Unit tests for S3Storage."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError
from chuck_data.storage_providers.s3 import S3Storage


class TestS3StorageInit:
    """Tests for S3Storage initialization."""

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_can_instantiate_with_default_credentials(self, mock_boto3):
        """Test that S3Storage can be instantiated with default credentials."""
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session

        storage = S3Storage(region="us-west-2")

        assert storage.region == "us-west-2"
        assert storage.aws_profile is None
        mock_boto3.Session.assert_called_once_with(region_name="us-west-2")
        mock_session.client.assert_called_once_with("s3")

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_can_instantiate_with_aws_profile(self, mock_boto3):
        """Test that S3Storage can be instantiated with AWS profile."""
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session

        storage = S3Storage(region="us-east-1", aws_profile="production")

        assert storage.region == "us-east-1"
        assert storage.aws_profile == "production"
        mock_boto3.Session.assert_called_once_with(
            profile_name="production", region_name="us-east-1"
        )

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_can_instantiate_with_explicit_credentials(self, mock_boto3):
        """Test that S3Storage can be instantiated with explicit credentials."""
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session

        storage = S3Storage(
            region="us-west-2",
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )

        mock_boto3.Session.assert_called_once_with(
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            region_name="us-west-2",
        )

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_default_region_is_us_east_1(self, mock_boto3):
        """Test that default region is us-east-1."""
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session

        storage = S3Storage()

        assert storage.region == "us-east-1"
        mock_boto3.Session.assert_called_once_with(region_name="us-east-1")


class TestUploadFile:
    """Tests for upload_file method."""

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_upload_file_success(self, mock_boto3):
        """Test successful file upload to S3."""
        mock_session = Mock()
        mock_s3_client = Mock()
        mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = mock_s3_client

        storage = S3Storage(region="us-west-2")

        result = storage.upload_file(
            content="test content",
            path="s3://my-bucket/path/to/file.txt",
        )

        assert result is True
        mock_s3_client.put_object.assert_called_once_with(
            Bucket="my-bucket",
            Key="path/to/file.txt",
            Body=b"test content",
            ContentType="text/plain",
        )

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_upload_file_with_nested_path(self, mock_boto3):
        """Test file upload with deeply nested path."""
        mock_session = Mock()
        mock_s3_client = Mock()
        mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = mock_s3_client

        storage = S3Storage(region="us-west-2")

        result = storage.upload_file(
            content="test content",
            path="s3://my-bucket/a/b/c/d/file.json",
        )

        assert result is True
        mock_s3_client.put_object.assert_called_once_with(
            Bucket="my-bucket",
            Key="a/b/c/d/file.json",
            Body=b"test content",
            ContentType="text/plain",
        )

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_upload_file_raises_on_invalid_path_format(self, mock_boto3):
        """Test that upload_file raises ValueError for invalid path format."""
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session

        storage = S3Storage(region="us-west-2")

        with pytest.raises(ValueError) as exc_info:
            storage.upload_file(
                content="test content",
                path="/local/path/file.txt",
            )

        assert "must start with 's3://'" in str(exc_info.value)

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_upload_file_raises_on_missing_key(self, mock_boto3):
        """Test that upload_file raises ValueError when key is missing."""
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session

        storage = S3Storage(region="us-west-2")

        with pytest.raises(ValueError) as exc_info:
            storage.upload_file(
                content="test content",
                path="s3://my-bucket",
            )

        assert "Invalid S3 path format" in str(exc_info.value)

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_upload_file_handles_client_error(self, mock_boto3):
        """Test that upload_file properly handles ClientError."""
        mock_session = Mock()
        mock_s3_client = Mock()
        mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = mock_s3_client

        # Simulate ClientError
        error_response = {
            "Error": {
                "Code": "NoSuchBucket",
                "Message": "The specified bucket does not exist",
            }
        }
        mock_s3_client.put_object.side_effect = ClientError(error_response, "PutObject")

        storage = S3Storage(region="us-west-2")

        with pytest.raises(Exception) as exc_info:
            storage.upload_file(
                content="test content",
                path="s3://non-existent-bucket/file.txt",
            )

        assert "Failed to upload file to S3" in str(exc_info.value)
        assert "NoSuchBucket" in str(exc_info.value)
        assert "The specified bucket does not exist" in str(exc_info.value)

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_upload_file_handles_generic_exception(self, mock_boto3):
        """Test that upload_file properly handles generic exceptions."""
        mock_session = Mock()
        mock_s3_client = Mock()
        mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = mock_s3_client

        mock_s3_client.put_object.side_effect = Exception("Network timeout")

        storage = S3Storage(region="us-west-2")

        with pytest.raises(Exception) as exc_info:
            storage.upload_file(
                content="test content",
                path="s3://my-bucket/file.txt",
            )

        assert "Failed to upload file to S3" in str(exc_info.value)
        assert "Network timeout" in str(exc_info.value)

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_upload_file_with_json_content(self, mock_boto3):
        """Test uploading JSON manifest content."""
        mock_session = Mock()
        mock_s3_client = Mock()
        mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = mock_s3_client

        storage = S3Storage(region="us-west-2")

        json_content = '{"tables": [{"name": "customers", "semantic_tags": ["email"]}]}'
        result = storage.upload_file(
            content=json_content,
            path="s3://stitch-artifacts/manifests/manifest.json",
        )

        assert result is True
        mock_s3_client.put_object.assert_called_once_with(
            Bucket="stitch-artifacts",
            Key="manifests/manifest.json",
            Body=json_content.encode("utf-8"),
            ContentType="text/plain",
        )

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_upload_file_with_unicode_content(self, mock_boto3):
        """Test uploading content with unicode characters."""
        mock_session = Mock()
        mock_s3_client = Mock()
        mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = mock_s3_client

        storage = S3Storage(region="us-west-2")

        unicode_content = "Hello 世界 🌍"
        result = storage.upload_file(
            content=unicode_content,
            path="s3://my-bucket/unicode.txt",
        )

        assert result is True
        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args
        assert call_args[1]["Body"] == unicode_content.encode("utf-8")

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_overwrite_parameter_ignored(self, mock_boto3):
        """Test that overwrite parameter is accepted but S3 always overwrites."""
        mock_session = Mock()
        mock_s3_client = Mock()
        mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = mock_s3_client

        storage = S3Storage(region="us-west-2")

        # S3 always overwrites, overwrite=False should still succeed
        result = storage.upload_file(
            content="test content",
            path="s3://my-bucket/file.txt",
            overwrite=False,
        )

        assert result is True
        mock_s3_client.put_object.assert_called_once()


class TestS3StorageInterface:
    """Tests for S3Storage interface compatibility."""

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_conforms_to_storage_provider_protocol(self, mock_boto3):
        """Test that S3Storage implements StorageProvider protocol."""
        from chuck_data.storage_providers.protocol import StorageProvider
        import inspect

        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session

        storage = S3Storage(region="us-west-2")

        # Check that upload_file method exists
        assert hasattr(storage, "upload_file")
        assert callable(storage.upload_file)

        # Check method signature
        sig = inspect.signature(storage.upload_file)
        params = list(sig.parameters.keys())
        assert "content" in params
        assert "path" in params
        assert "overwrite" in params

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_upload_file_returns_bool(self, mock_boto3):
        """Test that upload_file returns a boolean."""
        mock_session = Mock()
        mock_s3_client = Mock()
        mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = mock_s3_client

        storage = S3Storage(region="us-west-2")

        result = storage.upload_file(
            content="test",
            path="s3://bucket/key",
        )

        assert isinstance(result, bool)


class TestS3PathParsing:
    """Tests for S3 path parsing logic."""

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_parses_simple_s3_path(self, mock_boto3):
        """Test parsing simple s3:// path."""
        mock_session = Mock()
        mock_s3_client = Mock()
        mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = mock_s3_client

        storage = S3Storage()
        storage.upload_file(content="test", path="s3://bucket/key")

        call_args = mock_s3_client.put_object.call_args[1]
        assert call_args["Bucket"] == "bucket"
        assert call_args["Key"] == "key"

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_parses_nested_s3_path(self, mock_boto3):
        """Test parsing nested s3:// path."""
        mock_session = Mock()
        mock_s3_client = Mock()
        mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = mock_s3_client

        storage = S3Storage()
        storage.upload_file(
            content="test", path="s3://my-bucket/path/to/nested/file.json"
        )

        call_args = mock_s3_client.put_object.call_args[1]
        assert call_args["Bucket"] == "my-bucket"
        assert call_args["Key"] == "path/to/nested/file.json"

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_rejects_path_without_s3_prefix(self, mock_boto3):
        """Test that paths without s3:// prefix are rejected."""
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session

        storage = S3Storage()

        with pytest.raises(ValueError) as exc_info:
            storage.upload_file(content="test", path="bucket/key")

        assert "must start with 's3://'" in str(exc_info.value)

    @patch("chuck_data.storage_providers.s3.boto3")
    def test_rejects_path_with_only_bucket(self, mock_boto3):
        """Test that paths with only bucket (no key) are rejected."""
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session

        storage = S3Storage()

        with pytest.raises(ValueError) as exc_info:
            storage.upload_file(content="test", path="s3://bucket-only")

        assert "Invalid S3 path format" in str(exc_info.value)
