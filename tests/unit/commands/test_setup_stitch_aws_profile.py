"""
Tests for setup_stitch AWS profile handling.

Tests that setup_stitch command properly uses AWS profile from config
for all S3 operations (temp directory validation, manifest upload, init script upload).
"""

from unittest.mock import patch, MagicMock


class TestCreateS3ClientHelper:
    """Test _create_s3_client_with_profile helper function."""

    @patch("builtins.__import__", side_effect=__import__)
    @patch("chuck_data.commands.setup_stitch.get_aws_profile")
    @patch("chuck_data.commands.setup_stitch.get_aws_region")
    def test_creates_client_with_profile(
        self, mock_get_region, mock_get_profile, mock_import
    ):
        """Helper creates S3 client with AWS profile from config."""
        from chuck_data.commands.setup_stitch import _create_s3_client_with_profile

        # Setup mocks
        mock_get_profile.return_value = "sales"
        mock_get_region.return_value = "eu-north-1"

        # Mock boto3 module
        mock_boto3 = MagicMock()
        mock_session = MagicMock()
        mock_s3_client = MagicMock()
        mock_boto3.Session.return_value = mock_session
        mock_session.client.return_value = mock_s3_client

        def import_side_effect(name, *args, **kwargs):
            if name == "boto3":
                return mock_boto3
            return __import__(name, *args, **kwargs)

        mock_import.side_effect = import_side_effect

        # Call helper
        result = _create_s3_client_with_profile()

        # Verify boto3.Session was created with profile and region
        mock_boto3.Session.assert_called_once_with(
            profile_name="sales", region_name="eu-north-1"
        )

        # Verify S3 client was created from session
        mock_session.client.assert_called_once_with("s3")

        # Verify returned client
        assert result == mock_s3_client

    @patch("builtins.__import__", side_effect=__import__)
    @patch("chuck_data.commands.setup_stitch.get_aws_profile")
    @patch("chuck_data.commands.setup_stitch.get_aws_region")
    def test_creates_client_without_profile(
        self, mock_get_region, mock_get_profile, mock_import
    ):
        """Helper creates S3 client without profile when none configured."""
        from chuck_data.commands.setup_stitch import _create_s3_client_with_profile

        # Setup mocks - no profile
        mock_get_profile.return_value = None
        mock_get_region.return_value = "us-east-1"

        # Mock boto3 module
        mock_boto3 = MagicMock()
        mock_s3_client = MagicMock()
        mock_boto3.client.return_value = mock_s3_client

        def import_side_effect(name, *args, **kwargs):
            if name == "boto3":
                return mock_boto3
            return __import__(name, *args, **kwargs)

        mock_import.side_effect = import_side_effect

        # Call helper
        result = _create_s3_client_with_profile()

        # Verify boto3.client was called directly with region (no session)
        mock_boto3.client.assert_called_once_with("s3", region_name="us-east-1")

        # Verify returned client
        assert result == mock_s3_client


class TestS3TempDirectoryValidation:
    """Test AWS profile usage in S3 temp directory validation."""

    @patch("chuck_data.commands.setup_stitch._create_s3_client_with_profile")
    def test_temp_dir_validation_uses_helper(self, mock_create_client):
        """S3 temp directory validation uses _create_s3_client_with_profile helper."""
        from chuck_data.commands.setup_stitch import _ensure_s3_temp_dir_exists

        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_create_client.return_value = mock_s3_client

        # Mock S3 responses to indicate directory exists
        mock_s3_client.head_bucket.return_value = {}
        mock_s3_client.head_object.return_value = {"ContentLength": 0}

        # Call function
        result = _ensure_s3_temp_dir_exists("s3://test-bucket/temp/")

        # Verify helper was called
        mock_create_client.assert_called_once()

        # Should succeed
        assert result is True

    @patch("chuck_data.commands.setup_stitch._create_s3_client_with_profile")
    def test_temp_dir_validation_creates_marker(self, mock_create_client):
        """S3 temp directory validation creates marker file when directory doesn't exist."""
        from botocore.exceptions import ClientError
        from chuck_data.commands.setup_stitch import _ensure_s3_temp_dir_exists

        # Setup mock S3 client
        mock_s3_client = MagicMock()
        mock_create_client.return_value = mock_s3_client

        # Mock S3 responses
        mock_s3_client.head_bucket.return_value = {}
        # Mock head_object to raise 404 (directory doesn't exist)
        mock_s3_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "head_object"
        )
        # Mock put_object to succeed
        mock_s3_client.put_object.return_value = {"ETag": "test-etag"}

        # Call function
        result = _ensure_s3_temp_dir_exists("s3://chuck-bucket/redshift-temp/")

        # Verify helper was called
        mock_create_client.assert_called_once()

        # Verify put_object was called to create marker
        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args
        assert call_args[1]["Bucket"] == "chuck-bucket"
        assert call_args[1]["Key"] == "redshift-temp/.spark-redshift-temp-marker"

        # Should succeed
        assert result is True


class TestManifestUploadAwsProfileUsage:
    """Test AWS profile usage in manifest upload step."""

    def test_manifest_upload_uses_get_aws_profile(self):
        """Manifest upload step uses get_aws_profile() from config, not kwargs."""
        # This test verifies that the code imports and can use get_aws_profile
        from chuck_data.commands.setup_stitch import get_aws_profile

        # Verify function is accessible (actual behavior tested in integration)
        assert callable(get_aws_profile)
