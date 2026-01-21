"""
Unit tests for wizard steps AWS profile functionality.

Tests cover:
- AWS profile handling in RedshiftClusterSelectionStep
- AWS profile handling in S3BucketInputStep
- Credential priority (explicit > profile > default)
"""

import pytest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

from chuck_data.commands.wizard import WizardState, WizardStep
from chuck_data.commands.wizard.steps import (
    RedshiftClusterSelectionStep,
    S3BucketInputStep,
)
from chuck_data.commands.wizard.validator import InputValidator


class TestRedshiftClusterSelectionWithProfile:
    """Test RedshiftClusterSelectionStep with AWS profile support."""

    @patch("chuck_data.clients.redshift.RedshiftAPIClient")
    @patch.dict("os.environ", {}, clear=True)
    def test_uses_aws_profile_from_state(self, mock_redshift_client):
        """Test that RedshiftClusterSelectionStep uses aws_profile from state."""
        # Setup
        validator = InputValidator()
        step = RedshiftClusterSelectionStep(validator)

        state = WizardState(
            current_step=WizardStep.REDSHIFT_CLUSTER_SELECTION,
            aws_region="us-west-2",
            aws_profile="sales-power",
        )

        # Mock successful connection
        mock_client_instance = Mock()
        mock_client_instance.list_databases.return_value = ["dev", "analytics"]
        mock_redshift_client.return_value = mock_client_instance

        # Execute
        result = step.handle_input("test-workgroup", state)

        # Verify
        assert result.success
        # Verify RedshiftAPIClient was called with aws_profile
        mock_redshift_client.assert_called_once()
        call_kwargs = mock_redshift_client.call_args[1]
        assert call_kwargs["aws_profile"] == "sales-power"
        assert call_kwargs["region"] == "us-west-2"

    @patch("chuck_data.clients.redshift.RedshiftAPIClient")
    @patch.dict(
        "os.environ",
        {"AWS_ACCESS_KEY_ID": "test-key", "AWS_SECRET_ACCESS_KEY": "test-secret"},
    )
    def test_explicit_creds_take_precedence_over_profile(self, mock_redshift_client):
        """Test that explicit credentials from env vars take precedence over profile."""
        # Setup
        validator = InputValidator()
        step = RedshiftClusterSelectionStep(validator)

        state = WizardState(
            current_step=WizardStep.REDSHIFT_CLUSTER_SELECTION,
            aws_region="us-west-2",
            aws_profile="sales-power",  # Should be ignored when explicit creds exist
        )

        # Mock successful connection
        mock_client_instance = Mock()
        mock_client_instance.list_databases.return_value = ["dev"]
        mock_redshift_client.return_value = mock_client_instance

        # Execute
        result = step.handle_input("test-cluster", state)

        # Verify
        assert result.success
        # Verify explicit credentials were passed
        call_kwargs = mock_redshift_client.call_args[1]
        assert call_kwargs["aws_access_key_id"] == "test-key"
        assert call_kwargs["aws_secret_access_key"] == "test-secret"
        # Profile should still be in config but explicit creds take precedence
        assert call_kwargs["aws_profile"] == "sales-power"

    @patch("chuck_data.clients.redshift.RedshiftAPIClient")
    @patch.dict("os.environ", {}, clear=True)
    def test_no_profile_uses_default_credential_chain(self, mock_redshift_client):
        """Test that without profile or explicit creds, default credential chain is used."""
        # Setup
        validator = InputValidator()
        step = RedshiftClusterSelectionStep(validator)

        state = WizardState(
            current_step=WizardStep.REDSHIFT_CLUSTER_SELECTION,
            aws_region="eu-north-1",
            aws_profile=None,  # No profile
        )

        # Mock successful connection
        mock_client_instance = Mock()
        mock_client_instance.list_databases.return_value = ["dev"]
        mock_redshift_client.return_value = mock_client_instance

        # Execute
        result = step.handle_input("test-workgroup", state)

        # Verify
        assert result.success
        # Verify no explicit credentials or profile were passed
        call_kwargs = mock_redshift_client.call_args[1]
        assert "aws_access_key_id" not in call_kwargs
        assert "aws_secret_access_key" not in call_kwargs
        assert call_kwargs.get("aws_profile") is None


class TestS3BucketInputWithProfile:
    """Test S3BucketInputStep with AWS profile support."""

    @patch("boto3.Session")
    @patch.dict("os.environ", {}, clear=True)
    def test_uses_aws_profile_from_state(self, mock_session_class):
        """Test that S3BucketInputStep uses aws_profile from state."""
        # Setup
        validator = InputValidator()
        step = S3BucketInputStep(validator)

        state = WizardState(
            current_step=WizardStep.S3_BUCKET_INPUT,
            aws_region="us-west-2",
            aws_profile="sales-power",
        )

        # Mock successful S3 access
        mock_session = Mock()
        mock_s3_client = Mock()
        mock_s3_client.list_objects_v2.return_value = {}
        mock_session.client.return_value = mock_s3_client
        mock_session_class.return_value = mock_session

        # Execute
        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = True
            with patch("chuck_data.ui.tui.get_chuck_service"):
                result = step.handle_input("my-bucket", state)

        # Verify
        assert result.success
        # Verify Session was created with profile_name
        mock_session_class.assert_called_once_with(
            profile_name="sales-power",
            region_name="us-west-2",
        )
        # Verify S3 client was created from session
        mock_session.client.assert_called_once_with("s3")

    @patch("boto3.Session")
    @patch.dict(
        "os.environ",
        {"AWS_ACCESS_KEY_ID": "test-key", "AWS_SECRET_ACCESS_KEY": "test-secret"},
    )
    def test_explicit_creds_take_precedence_over_profile(self, mock_session_class):
        """Test that explicit credentials from env vars take precedence over profile."""
        # Setup
        validator = InputValidator()
        step = S3BucketInputStep(validator)

        state = WizardState(
            current_step=WizardStep.S3_BUCKET_INPUT,
            aws_region="us-west-2",
            aws_profile="sales-power",  # Should be ignored when explicit creds exist
        )

        # Mock successful S3 access
        mock_session = Mock()
        mock_s3_client = Mock()
        mock_s3_client.list_objects_v2.return_value = {}
        mock_session.client.return_value = mock_s3_client
        mock_session_class.return_value = mock_session

        # Execute
        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = True
            with patch("chuck_data.ui.tui.get_chuck_service"):
                result = step.handle_input("my-bucket", state)

        # Verify
        assert result.success
        # Verify Session was created with explicit credentials (not profile)
        mock_session_class.assert_called_once_with(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region_name="us-west-2",
        )

    @patch("boto3.Session")
    @patch.dict("os.environ", {}, clear=True)
    def test_no_profile_uses_default_credential_chain(self, mock_session_class):
        """Test that without profile or explicit creds, default credential chain is used."""
        # Setup
        validator = InputValidator()
        step = S3BucketInputStep(validator)

        state = WizardState(
            current_step=WizardStep.S3_BUCKET_INPUT,
            aws_region="eu-north-1",
            aws_profile=None,  # No profile
        )

        # Mock successful S3 access
        mock_session = Mock()
        mock_s3_client = Mock()
        mock_s3_client.list_objects_v2.return_value = {}
        mock_session.client.return_value = mock_s3_client
        mock_session_class.return_value = mock_session

        # Execute
        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = True
            with patch("chuck_data.ui.tui.get_chuck_service"):
                result = step.handle_input("my-bucket", state)

        # Verify
        assert result.success
        # Verify Session was created with only region (default credential chain)
        mock_session_class.assert_called_once_with(region_name="eu-north-1")

    @patch("boto3.Session")
    @patch.dict("os.environ", {}, clear=True)
    def test_access_denied_with_wrong_profile(self, mock_session_class):
        """Test that AccessDenied error is returned when using wrong profile."""
        # Setup
        validator = InputValidator()
        step = S3BucketInputStep(validator)

        state = WizardState(
            current_step=WizardStep.S3_BUCKET_INPUT,
            aws_region="us-west-2",
            aws_profile="wrong-profile",
        )

        # Mock AccessDenied error
        mock_session = Mock()
        mock_s3_client = Mock()
        mock_s3_client.list_objects_v2.side_effect = ClientError(
            {
                "Error": {
                    "Code": "AccessDenied",
                    "Message": "User is not authorized to perform: s3:ListBucket",
                }
            },
            "ListObjectsV2",
        )
        mock_session.client.return_value = mock_s3_client
        mock_session_class.return_value = mock_session

        # Execute
        result = step.handle_input("my-bucket", state)

        # Verify
        assert not result.success
        assert "Cannot access S3 bucket" in result.message
        assert "AccessDenied" in result.message

    @patch("boto3.Session")
    @patch.dict("os.environ", {}, clear=True)
    def test_bucket_not_found(self, mock_session_class):
        """Test error handling when bucket doesn't exist."""
        # Setup
        validator = InputValidator()
        step = S3BucketInputStep(validator)

        state = WizardState(
            current_step=WizardStep.S3_BUCKET_INPUT,
            aws_region="us-west-2",
            aws_profile="sales-power",
        )

        # Mock bucket not found error
        mock_session = Mock()
        mock_s3_client = Mock()
        mock_s3_client.list_objects_v2.side_effect = ClientError(
            {
                "Error": {
                    "Code": "NoSuchBucket",
                    "Message": "The specified bucket does not exist",
                }
            },
            "ListObjectsV2",
        )
        mock_session.client.return_value = mock_s3_client
        mock_session_class.return_value = mock_session

        # Execute
        result = step.handle_input("nonexistent-bucket", state)

        # Verify
        assert not result.success
        assert "Cannot access S3 bucket" in result.message
        assert "NoSuchBucket" in result.message


class TestCredentialPriorityConsistency:
    """Test that credential priority is consistent across all AWS operations."""

    @patch("chuck_data.clients.redshift.RedshiftAPIClient")
    @patch("boto3.Session")
    @patch.dict(
        "os.environ",
        {"AWS_ACCESS_KEY_ID": "env-key", "AWS_SECRET_ACCESS_KEY": "env-secret"},
    )
    def test_same_credentials_used_for_redshift_and_s3(
        self, mock_session_class, mock_redshift_client
    ):
        """Test that the same credential source is used for both Redshift and S3."""
        validator = InputValidator()

        # Setup state with profile (but env vars should take precedence)
        state = WizardState(
            current_step=WizardStep.REDSHIFT_CLUSTER_SELECTION,
            aws_region="us-west-2",
            aws_profile="profile-that-should-be-ignored",
        )

        # Test Redshift step
        redshift_step = RedshiftClusterSelectionStep(validator)
        mock_client_instance = Mock()
        mock_client_instance.list_databases.return_value = ["dev"]
        mock_redshift_client.return_value = mock_client_instance

        redshift_result = redshift_step.handle_input("test-cluster", state)
        assert redshift_result.success

        # Verify Redshift used env credentials
        redshift_call_kwargs = mock_redshift_client.call_args[1]
        assert redshift_call_kwargs["aws_access_key_id"] == "env-key"
        assert redshift_call_kwargs["aws_secret_access_key"] == "env-secret"

        # Test S3 step with same state
        state.current_step = WizardStep.S3_BUCKET_INPUT
        s3_step = S3BucketInputStep(validator)

        mock_session = Mock()
        mock_s3_client = Mock()
        mock_s3_client.list_objects_v2.return_value = {}
        mock_session.client.return_value = mock_s3_client
        mock_session_class.return_value = mock_session

        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = True
            with patch("chuck_data.ui.tui.get_chuck_service"):
                s3_result = s3_step.handle_input("my-bucket", state)

        assert s3_result.success

        # Verify S3 used env credentials (same as Redshift)
        mock_session_class.assert_called_with(
            aws_access_key_id="env-key",
            aws_secret_access_key="env-secret",
            region_name="us-west-2",
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
