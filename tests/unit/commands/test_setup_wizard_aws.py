"""Tests for AWS-specific setup wizard steps (Redshift configuration)."""

import pytest
from unittest.mock import patch, MagicMock
from chuck_data.commands.wizard import (
    WizardStep,
    WizardState,
    InputValidator,
    WizardAction,
)
from chuck_data.commands.wizard.steps import (
    AWSRegionInputStep,
    RedshiftClusterSelectionStep,
    S3BucketInputStep,
    DataProviderSelectionStep,
    create_step,
)


class TestDataProviderSelectionWithAWS:
    """Test data provider selection with AWS routing."""

    def test_databricks_selection_routes_to_computation_provider(self):
        """Selecting Databricks routes to computation provider selection."""
        validator = InputValidator()
        step = DataProviderSelectionStep(validator)
        state = WizardState()

        result = step.handle_input("1", state)
        assert result.success
        assert result.next_step == WizardStep.COMPUTATION_PROVIDER_SELECTION
        assert "Databricks" in result.message
        assert result.data["data_provider"] == "databricks"

    def test_databricks_selection_by_name(self):
        """Selecting Databricks by name works."""
        validator = InputValidator()
        step = DataProviderSelectionStep(validator)
        state = WizardState()

        result = step.handle_input("databricks", state)
        assert result.success
        assert result.next_step == WizardStep.COMPUTATION_PROVIDER_SELECTION
        assert result.data["data_provider"] == "databricks"

    def test_aws_redshift_selection_routes_to_region_input(self):
        """Selecting AWS Redshift routes to AWS region configuration."""
        validator = InputValidator()
        step = DataProviderSelectionStep(validator)
        state = WizardState()

        result = step.handle_input("2", state)
        assert result.success
        assert result.next_step == WizardStep.AWS_REGION_INPUT
        assert "AWS Redshift" in result.message
        assert result.data["data_provider"] == "aws_redshift"

    def test_aws_redshift_selection_by_name_variations(self):
        """AWS Redshift can be selected by various name variations."""
        validator = InputValidator()
        step = DataProviderSelectionStep(validator)

        for name in ["aws_redshift", "aws redshift", "redshift"]:
            state = WizardState()
            result = step.handle_input(name, state)
            assert result.success, f"Failed with input: {name}"
            assert result.next_step == WizardStep.AWS_REGION_INPUT
            assert result.data["data_provider"] == "aws_redshift"

    def test_invalid_provider_selection(self):
        """Invalid provider selection shows error."""
        validator = InputValidator()
        step = DataProviderSelectionStep(validator)
        state = WizardState()

        result = step.handle_input("3", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "Invalid selection" in result.message

    def test_empty_provider_selection(self):
        """Empty provider selection shows error."""
        validator = InputValidator()
        step = DataProviderSelectionStep(validator)
        state = WizardState()

        result = step.handle_input("", state)
        assert not result.success
        assert result.action == WizardAction.RETRY


class TestAWSRegionInputStep:
    """Test AWS region input step."""

    def test_step_title(self):
        """Step has correct title."""
        validator = InputValidator()
        step = AWSRegionInputStep(validator)
        assert step.get_step_title() == "AWS Region Configuration"

    @patch.dict(
        "os.environ", {"AWS_PROFILE": "test-profile", "AWS_REGION": "us-west-2"}
    )
    def test_prompt_message_shows_env_vars(self):
        """Prompt message displays current AWS environment variables."""
        validator = InputValidator()
        step = AWSRegionInputStep(validator)
        state = WizardState()

        prompt = step.get_prompt_message(state)
        assert "AWS_PROFILE: test-profile" in prompt
        assert "AWS_REGION: us-west-2" in prompt
        assert "us-west-2" in prompt or "us-east-1" in prompt  # Example regions

    @patch.dict("os.environ", {}, clear=True)
    def test_prompt_message_without_env_vars(self):
        """Prompt message handles missing environment variables."""
        validator = InputValidator()
        step = AWSRegionInputStep(validator)
        state = WizardState()

        prompt = step.get_prompt_message(state)
        assert "not set" in prompt

    @patch("chuck_data.config.get_config_manager")
    def test_valid_region_input(self, mock_config):
        """Valid AWS region input is accepted."""
        mock_config.return_value.update.return_value = True

        validator = InputValidator()
        step = AWSRegionInputStep(validator)
        state = WizardState()

        result = step.handle_input("us-west-2", state)
        assert result.success
        assert result.next_step == WizardStep.REDSHIFT_CLUSTER_SELECTION
        assert result.data["aws_region"] == "us-west-2"
        assert "us-west-2" in result.message
        mock_config.return_value.update.assert_called_once_with(aws_region="us-west-2")

    @patch("chuck_data.config.get_config_manager")
    def test_various_valid_region_formats(self, mock_config):
        """Various AWS region formats are accepted."""
        mock_config.return_value.update.return_value = True

        validator = InputValidator()
        step = AWSRegionInputStep(validator)

        valid_regions = [
            "us-east-1",
            "us-west-2",
            "eu-west-1",
            "ap-southeast-2",
            "ca-central-1",
        ]

        for region in valid_regions:
            state = WizardState()
            result = step.handle_input(region, state)
            assert result.success, f"Failed for region: {region}"
            assert result.data["aws_region"] == region

    def test_empty_region_input(self):
        """Empty region input is rejected."""
        validator = InputValidator()
        step = AWSRegionInputStep(validator)
        state = WizardState()

        result = step.handle_input("", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "cannot be empty" in result.message

    def test_whitespace_only_region_input(self):
        """Whitespace-only region input is rejected."""
        validator = InputValidator()
        step = AWSRegionInputStep(validator)
        state = WizardState()

        result = step.handle_input("   ", state)
        assert not result.success
        assert result.action == WizardAction.RETRY

    def test_invalid_region_format(self):
        """Invalid region format is rejected."""
        validator = InputValidator()
        step = AWSRegionInputStep(validator)
        state = WizardState()

        invalid_regions = [
            "us west 2",  # spaces
            "us@west-2",  # special chars
            "us/west/2",  # slashes
        ]

        for region in invalid_regions:
            result = step.handle_input(region, state)
            assert not result.success, f"Should reject region: {region}"
            assert result.action == WizardAction.RETRY
            assert "Invalid region format" in result.message

    @patch("chuck_data.config.get_config_manager")
    def test_config_save_failure(self, mock_config):
        """Config save failure is handled gracefully."""
        mock_config.return_value.update.return_value = False

        validator = InputValidator()
        step = AWSRegionInputStep(validator)
        state = WizardState()

        result = step.handle_input("us-west-2", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "Failed to save" in result.message

    @patch("chuck_data.config.get_config_manager")
    def test_config_save_exception(self, mock_config):
        """Config save exception is handled gracefully."""
        mock_config.return_value.update.side_effect = Exception("Config error")

        validator = InputValidator()
        step = AWSRegionInputStep(validator)
        state = WizardState()

        result = step.handle_input("us-west-2", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "Error saving" in result.message


class TestRedshiftClusterSelectionStep:
    """Test Redshift cluster selection step."""

    def test_step_title(self):
        """Step has correct title."""
        validator = InputValidator()
        step = RedshiftClusterSelectionStep(validator)
        assert step.get_step_title() == "Redshift Cluster Selection"

    def test_prompt_message(self):
        """Prompt message includes both cluster and workgroup options."""
        validator = InputValidator()
        step = RedshiftClusterSelectionStep(validator)
        state = WizardState()

        prompt = step.get_prompt_message(state)
        assert "cluster identifier" in prompt.lower()
        assert "workgroup" in prompt.lower()

    def test_empty_identifier_input(self):
        """Empty cluster identifier is rejected."""
        validator = InputValidator()
        step = RedshiftClusterSelectionStep(validator)
        state = WizardState()
        state.aws_region = "us-west-2"

        result = step.handle_input("", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "cannot be empty" in result.message

    @patch("boto3.client")
    @patch("chuck_data.config.get_config_manager")
    def test_valid_serverless_workgroup(self, mock_config, mock_boto3):
        """Valid Serverless workgroup is accepted."""
        # Mock boto3 clients
        mock_redshift_data = MagicMock()
        mock_redshift_data.list_databases.return_value = {"Databases": ["dev", "test"]}

        mock_boto3.return_value = mock_redshift_data
        mock_config.return_value.update.return_value = True

        validator = InputValidator()
        step = RedshiftClusterSelectionStep(validator)
        state = WizardState()
        state.aws_region = "us-west-2"

        result = step.handle_input("my-workgroup", state)
        assert result.success
        assert result.next_step == WizardStep.S3_BUCKET_INPUT
        assert result.data["redshift_workgroup_name"] == "my-workgroup"
        assert "workgroup" in result.message.lower()

    @patch("boto3.client")
    @patch("chuck_data.config.get_config_manager")
    def test_valid_provisioned_cluster(self, mock_config, mock_boto3):
        """Valid provisioned cluster is accepted when workgroup lookup fails."""
        # Mock boto3 clients - serverless fails, provisioned succeeds
        call_count = [0]

        def boto3_client_side_effect(service_name, **kwargs):
            mock_client = MagicMock()
            # First call (serverless) fails, second call (provisioned) succeeds
            if call_count[0] == 0:
                call_count[0] += 1
                mock_client.list_databases.side_effect = Exception(
                    "Serverless not found"
                )
            else:
                mock_client.list_databases.return_value = {"Databases": ["dev", "test"]}
            return mock_client

        mock_boto3.side_effect = boto3_client_side_effect
        mock_config.return_value.update.return_value = True

        validator = InputValidator()
        step = RedshiftClusterSelectionStep(validator)
        state = WizardState()
        state.aws_region = "us-west-2"

        result = step.handle_input("my-cluster", state)
        assert result.success
        assert result.next_step == WizardStep.S3_BUCKET_INPUT
        assert result.data["redshift_cluster_identifier"] == "my-cluster"
        assert "cluster" in result.message.lower()

    @patch("boto3.client")
    def test_invalid_cluster_and_workgroup(self, mock_boto3):
        """Invalid cluster/workgroup (not found in either service) is rejected."""
        # Both lookups fail
        mock_client = MagicMock()
        mock_client.list_databases.side_effect = Exception("Not found")
        mock_boto3.return_value = mock_client

        validator = InputValidator()
        step = RedshiftClusterSelectionStep(validator)
        state = WizardState()
        state.aws_region = "us-west-2"

        result = step.handle_input("nonexistent", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert (
            "Failed to connect" in result.message
            or "not found" in result.message.lower()
        )

    @patch("boto3.client")
    @patch("chuck_data.config.get_config_manager")
    def test_config_save_failure(self, mock_config, mock_boto3):
        """Config save failure is handled gracefully."""
        mock_client = MagicMock()
        mock_client.list_databases.return_value = {"Databases": ["dev"]}
        mock_boto3.return_value = mock_client
        mock_config.return_value.update.return_value = False

        validator = InputValidator()
        step = RedshiftClusterSelectionStep(validator)
        state = WizardState()
        state.aws_region = "us-west-2"

        result = step.handle_input("test-workgroup", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "Failed to save" in result.message


class TestS3BucketInputStep:
    """Test S3 bucket configuration step."""

    def test_step_title(self):
        """Step has correct title."""
        validator = InputValidator()
        step = S3BucketInputStep(validator)
        assert step.get_step_title() == "S3 Bucket Configuration"

    def test_prompt_message(self):
        """Prompt message explains S3 bucket purpose."""
        validator = InputValidator()
        step = S3BucketInputStep(validator)
        state = WizardState()

        prompt = step.get_prompt_message(state)
        assert "S3 bucket" in prompt
        assert "Spark-Redshift" in prompt or "intermediate" in prompt.lower()

    def test_empty_bucket_input(self):
        """Empty bucket name is rejected."""
        validator = InputValidator()
        step = S3BucketInputStep(validator)
        state = WizardState()

        result = step.handle_input("", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "cannot be empty" in result.message

    def test_whitespace_only_bucket_input(self):
        """Whitespace-only bucket name is rejected."""
        validator = InputValidator()
        step = S3BucketInputStep(validator)
        state = WizardState()

        result = step.handle_input("   ", state)
        assert not result.success
        assert result.action == WizardAction.RETRY

    def test_invalid_bucket_name_format(self):
        """Invalid S3 bucket name format is rejected."""
        validator = InputValidator()
        step = S3BucketInputStep(validator)
        state = WizardState()

        invalid_buckets = [
            "bucket with spaces",
            "bucket@special",
            "bucket/slash",
        ]

        for bucket in invalid_buckets:
            result = step.handle_input(bucket, state)
            assert not result.success, f"Should reject bucket: {bucket}"
            assert result.action == WizardAction.RETRY
            assert "Invalid" in result.message

    @patch("boto3.client")
    @patch("chuck_data.config.get_config_manager")
    @patch("chuck_data.commands.wizard.steps.get_chuck_service")
    def test_valid_bucket_accessible(self, mock_service, mock_config, mock_boto3):
        """Valid and accessible S3 bucket is accepted."""
        # Mock S3 client
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {"Contents": []}
        mock_boto3.return_value = mock_s3

        mock_config.return_value.update.return_value = True
        mock_service.return_value = None  # No service to reinitialize

        validator = InputValidator()
        step = S3BucketInputStep(validator)
        state = WizardState()
        state.aws_region = "us-west-2"

        result = step.handle_input("my-test-bucket", state)
        assert result.success
        assert result.next_step == WizardStep.COMPUTATION_PROVIDER_SELECTION
        assert result.data["s3_bucket"] == "my-test-bucket"
        mock_s3.list_objects_v2.assert_called_once_with(
            Bucket="my-test-bucket", MaxKeys=1
        )

    @patch("boto3.client")
    @patch("chuck_data.config.get_config_manager")
    @patch("chuck_data.commands.wizard.steps.get_chuck_service")
    def test_valid_bucket_with_service_reinit(
        self, mock_service, mock_config, mock_boto3
    ):
        """Valid bucket triggers service reinitialization."""
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {"Contents": []}
        mock_boto3.return_value = mock_s3

        mock_config.return_value.update.return_value = True

        # Mock service that can be reinitialized
        mock_service_instance = MagicMock()
        mock_service_instance.reinitialize_client.return_value = True
        mock_service.return_value = mock_service_instance

        validator = InputValidator()
        step = S3BucketInputStep(validator)
        state = WizardState()
        state.aws_region = "us-west-2"

        result = step.handle_input("my-bucket", state)
        assert result.success
        mock_service_instance.reinitialize_client.assert_called_once()

    @patch("boto3.client")
    def test_inaccessible_bucket(self, mock_boto3):
        """Inaccessible S3 bucket is rejected."""
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.side_effect = Exception("Access Denied")
        mock_boto3.return_value = mock_s3

        validator = InputValidator()
        step = S3BucketInputStep(validator)
        state = WizardState()
        state.aws_region = "us-west-2"

        result = step.handle_input("inaccessible-bucket", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "Cannot access" in result.message
        assert "Access Denied" in result.message

    @patch("boto3.client")
    @patch("chuck_data.config.get_config_manager")
    @patch("chuck_data.commands.wizard.steps.get_chuck_service")
    def test_config_save_failure(self, mock_service, mock_config, mock_boto3):
        """Config save failure is handled gracefully."""
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {"Contents": []}
        mock_boto3.return_value = mock_s3

        mock_config.return_value.update.return_value = False
        mock_service.return_value = None

        validator = InputValidator()
        step = S3BucketInputStep(validator)
        state = WizardState()
        state.aws_region = "us-west-2"

        result = step.handle_input("my-bucket", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "Failed to save" in result.message

    @patch.dict(
        "os.environ",
        {"AWS_ACCESS_KEY_ID": "test_key", "AWS_SECRET_ACCESS_KEY": "test_secret"},
    )
    @patch("boto3.client")
    @patch("chuck_data.config.get_config_manager")
    @patch("chuck_data.commands.wizard.steps.get_chuck_service")
    def test_uses_explicit_aws_credentials(self, mock_service, mock_config, mock_boto3):
        """S3 client uses explicit AWS credentials from environment."""
        mock_s3 = MagicMock()
        mock_s3.list_objects_v2.return_value = {"Contents": []}
        mock_boto3.return_value = mock_s3

        mock_config.return_value.update.return_value = True
        mock_service.return_value = None

        validator = InputValidator()
        step = S3BucketInputStep(validator)
        state = WizardState()
        state.aws_region = "us-west-2"

        result = step.handle_input("my-bucket", state)
        assert result.success

        # Verify boto3.client was called with credentials
        call_args = mock_boto3.call_args
        assert call_args[0][0] == "s3"
        assert call_args[1]["region_name"] == "us-west-2"
        assert call_args[1]["aws_access_key_id"] == "test_key"
        assert call_args[1]["aws_secret_access_key"] == "test_secret"


class TestAWSWizardFlow:
    """Integration tests for complete AWS Redshift wizard flow."""

    def test_aws_step_factory_creation(self):
        """AWS steps can be created via factory."""
        validator = InputValidator()

        # Test creating AWS-specific steps
        aws_region_step = create_step(WizardStep.AWS_REGION_INPUT, validator)
        assert isinstance(aws_region_step, AWSRegionInputStep)

        cluster_step = create_step(WizardStep.REDSHIFT_CLUSTER_SELECTION, validator)
        assert isinstance(cluster_step, RedshiftClusterSelectionStep)

        s3_step = create_step(WizardStep.S3_BUCKET_INPUT, validator)
        assert isinstance(s3_step, S3BucketInputStep)

    def test_wizard_state_tracks_aws_configuration(self):
        """WizardState properly tracks AWS configuration."""
        state = WizardState()

        # Set data provider to AWS Redshift
        state.data_provider = "aws_redshift"
        assert state.data_provider == "aws_redshift"

        # Set AWS region
        state.aws_region = "us-west-2"
        assert state.aws_region == "us-west-2"

        # Set cluster identifier
        state.cluster_identifier = "my-cluster"
        assert state.cluster_identifier == "my-cluster"

        # Set S3 bucket
        state.s3_bucket = "my-bucket"
        assert state.s3_bucket == "my-bucket"

    def test_aws_redshift_path_differs_from_databricks(self):
        """AWS Redshift wizard path is different from Databricks."""
        validator = InputValidator()

        # Databricks path: DATA_PROVIDER -> COMPUTATION_PROVIDER -> WORKSPACE_URL
        databricks_step = DataProviderSelectionStep(validator)
        state_db = WizardState()
        result_db = databricks_step.handle_input("databricks", state_db)
        assert result_db.next_step == WizardStep.COMPUTATION_PROVIDER_SELECTION

        # AWS Redshift path: DATA_PROVIDER -> AWS_REGION -> CLUSTER -> S3 -> COMPUTATION_PROVIDER
        redshift_step = DataProviderSelectionStep(validator)
        state_rs = WizardState()
        result_rs = redshift_step.handle_input("aws_redshift", state_rs)
        assert result_rs.next_step == WizardStep.AWS_REGION_INPUT

        # Verify paths are different
        assert result_db.next_step != result_rs.next_step
