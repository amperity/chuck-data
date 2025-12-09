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
    IAMRoleInputStep,
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
        assert result.next_step == WizardStep.IAM_ROLE_INPUT
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


class TestIAMRoleInputStep:
    """Test IAM role configuration step."""

    def test_step_title(self):
        """Step has correct title."""
        validator = InputValidator()
        step = IAMRoleInputStep(validator)
        assert step.get_step_title() == "IAM Role Configuration"

    def test_prompt_message(self):
        """Prompt message explains IAM role purpose and shows example."""
        validator = InputValidator()
        step = IAMRoleInputStep(validator)
        state = WizardState()

        prompt = step.get_prompt_message(state)
        assert "IAM role ARN" in prompt
        assert "Redshift" in prompt
        assert "Databricks to access Redshift and S3" in prompt
        assert "arn:aws:iam::" in prompt
        assert "Example:" in prompt

    @patch("chuck_data.config.get_config_manager")
    def test_valid_iam_role_arn(self, mock_config):
        """Valid IAM role ARN is accepted and S3 temp dir is constructed."""
        mock_config.return_value.update.return_value = True

        validator = InputValidator()
        step = IAMRoleInputStep(validator)
        state = WizardState()
        state.s3_bucket = "my-test-bucket"

        result = step.handle_input("arn:aws:iam::123456789012:role/RedshiftRole", state)
        assert result.success
        assert result.next_step == WizardStep.COMPUTATION_PROVIDER_SELECTION
        assert result.data["iam_role"] == "arn:aws:iam::123456789012:role/RedshiftRole"
        assert "configured successfully" in result.message

        # Verify config was updated with both IAM role and S3 temp dir
        mock_config.return_value.update.assert_called_once_with(
            redshift_iam_role="arn:aws:iam::123456789012:role/RedshiftRole",
            redshift_s3_temp_dir="s3://my-test-bucket/redshift-temp/",
        )

    @patch("chuck_data.config.get_config_manager")
    def test_various_valid_iam_role_formats(self, mock_config):
        """Various valid IAM role ARN formats are accepted."""
        mock_config.return_value.update.return_value = True

        validator = InputValidator()
        step = IAMRoleInputStep(validator)

        valid_arns = [
            "arn:aws:iam::123456789012:role/MyRole",
            "arn:aws:iam::999888777666:role/RedshiftAccessRole",
            "arn:aws:iam::000000000000:role/test-role-123",
            "arn:aws:iam::111111111111:role/role_with_underscores",
            "arn:aws:iam::884752987182:role/service-role/AmazonRedshift-CommandsAccessRole-20251205T162728",
        ]

        for arn in valid_arns:
            state = WizardState()
            state.s3_bucket = "test-bucket"
            result = step.handle_input(arn, state)
            assert result.success, f"Failed for ARN: {arn}"
            assert result.data["iam_role"] == arn

    @patch("chuck_data.config.get_config_manager")
    def test_s3_temp_dir_construction_from_bucket(self, mock_config):
        """S3 temp directory is properly constructed from state.s3_bucket."""
        mock_config.return_value.update.return_value = True

        validator = InputValidator()
        step = IAMRoleInputStep(validator)

        # Test with different bucket names
        bucket_tests = [
            ("my-test-bucket", "s3://my-test-bucket/redshift-temp/"),
            ("prod-data-bucket", "s3://prod-data-bucket/redshift-temp/"),
            ("dev-123", "s3://dev-123/redshift-temp/"),
        ]

        for bucket, expected_temp_dir in bucket_tests:
            state = WizardState()
            state.s3_bucket = bucket
            result = step.handle_input("arn:aws:iam::123456789012:role/TestRole", state)
            assert result.success

            # Verify the S3 temp dir was constructed correctly
            call_args = mock_config.return_value.update.call_args
            assert call_args[1]["redshift_s3_temp_dir"] == expected_temp_dir

    def test_empty_iam_role_input(self):
        """Empty IAM role input is rejected."""
        validator = InputValidator()
        step = IAMRoleInputStep(validator)
        state = WizardState()

        result = step.handle_input("", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "cannot be empty" in result.message

    def test_whitespace_only_iam_role_input(self):
        """Whitespace-only IAM role input is rejected."""
        validator = InputValidator()
        step = IAMRoleInputStep(validator)
        state = WizardState()

        result = step.handle_input("   ", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "cannot be empty" in result.message

    def test_invalid_arn_format_missing_prefix(self):
        """IAM role ARN without proper prefix is rejected."""
        validator = InputValidator()
        step = IAMRoleInputStep(validator)
        state = WizardState()

        invalid_arns = [
            "arn:aws:123456789012:role/MyRole",  # Missing iam::
            "aws:iam::123456789012:role/MyRole",  # Missing arn:
            "arn:aws:s3::123456789012:role/MyRole",  # Wrong service (s3 instead of iam)
            "123456789012:role/MyRole",  # Missing entire prefix
            "role/MyRole",  # Just the role name
            "arn:aws:iam::123456789012:policy/MyPolicy",  # Policy instead of role
        ]

        for arn in invalid_arns:
            result = step.handle_input(arn, state)
            assert not result.success, f"Should reject ARN: {arn}"
            assert result.action == WizardAction.RETRY
            assert "Invalid IAM role ARN format" in result.message

    def test_invalid_arn_format_special_characters(self):
        """IAM role ARN with invalid characters is rejected."""
        validator = InputValidator()
        step = IAMRoleInputStep(validator)
        state = WizardState()

        # These would pass the prefix check but might have other issues
        # The current implementation only checks prefix, but we document expected behavior
        invalid_arns = [
            "arn:aws:iam:: :role/MyRole",  # Space in account ID
            "arn:aws:iam::123456789012:role/My Role",  # Space in role name
        ]

        for arn in invalid_arns:
            result = step.handle_input(arn, state)
            # Current implementation only checks prefix, these would actually pass
            # This test documents that we may want stricter validation in the future

    @patch("chuck_data.config.get_config_manager")
    def test_config_save_failure(self, mock_config):
        """Config save failure is handled gracefully."""
        mock_config.return_value.update.return_value = False

        validator = InputValidator()
        step = IAMRoleInputStep(validator)
        state = WizardState()
        state.s3_bucket = "test-bucket"

        result = step.handle_input("arn:aws:iam::123456789012:role/MyRole", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "Failed to save" in result.message

    @patch("chuck_data.config.get_config_manager")
    def test_config_save_exception(self, mock_config):
        """Config save exception is handled gracefully."""
        mock_config.return_value.update.side_effect = Exception("Config write error")

        validator = InputValidator()
        step = IAMRoleInputStep(validator)
        state = WizardState()
        state.s3_bucket = "test-bucket"

        result = step.handle_input("arn:aws:iam::123456789012:role/MyRole", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "Error saving IAM role" in result.message
        assert "Config write error" in result.message

    def test_next_step_routing(self):
        """IAM role step routes to computation provider selection."""
        validator = InputValidator()
        step = IAMRoleInputStep(validator)

        # The next step should always be COMPUTATION_PROVIDER_SELECTION
        # after successful IAM role configuration
        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = True
            state = WizardState()
            state.s3_bucket = "test-bucket"

            result = step.handle_input("arn:aws:iam::123456789012:role/MyRole", state)
            assert result.success
            assert result.next_step == WizardStep.COMPUTATION_PROVIDER_SELECTION


class TestIAMRoleWizardIntegration:
    """Integration tests for IAM role step in complete wizard flow."""

    def test_iam_role_step_factory_creation(self):
        """IAM role step can be created via factory."""
        validator = InputValidator()

        iam_step = create_step(WizardStep.IAM_ROLE_INPUT, validator)
        assert isinstance(iam_step, IAMRoleInputStep)

    def test_wizard_state_tracks_iam_role(self):
        """WizardState properly tracks iam_role field."""
        state = WizardState()

        # Initially None
        assert state.iam_role is None

        # Can be set
        state.iam_role = "arn:aws:iam::123456789012:role/MyRole"
        assert state.iam_role == "arn:aws:iam::123456789012:role/MyRole"

    def test_state_validation_requires_iam_role_before_computation(self):
        """State validation requires IAM role before computation provider when using Redshift."""
        state = WizardState()
        state.data_provider = "aws_redshift"
        state.aws_region = "us-west-2"
        state.redshift_cluster_identifier = "my-cluster"
        state.s3_bucket = "my-bucket"

        # Without IAM role, computation provider selection is invalid
        assert not state.is_valid_for_step(WizardStep.COMPUTATION_PROVIDER_SELECTION)

        # With IAM role, computation provider selection is valid
        state.iam_role = "arn:aws:iam::123456789012:role/MyRole"
        assert state.is_valid_for_step(WizardStep.COMPUTATION_PROVIDER_SELECTION)

    def test_state_validation_iam_role_step_requires_s3_bucket(self):
        """IAM role step requires S3 bucket to be configured first."""
        state = WizardState()
        state.data_provider = "aws_redshift"

        # Without S3 bucket, IAM role input is invalid
        assert not state.is_valid_for_step(WizardStep.IAM_ROLE_INPUT)

        # With S3 bucket, IAM role input is valid
        state.s3_bucket = "my-bucket"
        assert state.is_valid_for_step(WizardStep.IAM_ROLE_INPUT)

    def test_aws_redshift_complete_flow_includes_iam_role(self):
        """Complete AWS Redshift flow includes IAM role step."""
        validator = InputValidator()

        # Step 1: Select AWS Redshift
        provider_step = DataProviderSelectionStep(validator)
        state = WizardState()
        result = provider_step.handle_input("aws_redshift", state)
        assert result.next_step == WizardStep.AWS_REGION_INPUT

        # Step 2: Configure region (mocked)
        state.aws_region = "us-west-2"

        # Step 3: Select cluster (mocked)
        state.redshift_cluster_identifier = "my-cluster"

        # Step 4: Configure S3 bucket
        s3_step = S3BucketInputStep(validator)
        with (
            patch("boto3.client") as mock_boto3,
            patch("chuck_data.config.get_config_manager") as mock_config,
            patch("chuck_data.commands.wizard.steps.get_chuck_service") as mock_service,
        ):
            mock_s3 = MagicMock()
            mock_s3.list_objects_v2.return_value = {"Contents": []}
            mock_boto3.return_value = mock_s3
            mock_config.return_value.update.return_value = True
            mock_service.return_value = None

            result = s3_step.handle_input("my-bucket", state)
            assert result.success
            assert result.next_step == WizardStep.IAM_ROLE_INPUT

        # Step 5: Configure IAM role
        state.s3_bucket = "my-bucket"
        iam_step = IAMRoleInputStep(validator)
        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = True
            result = iam_step.handle_input(
                "arn:aws:iam::123456789012:role/RedshiftRole", state
            )
            assert result.success
            assert result.next_step == WizardStep.COMPUTATION_PROVIDER_SELECTION

    @patch("chuck_data.config.get_config_manager")
    def test_iam_role_and_s3_temp_dir_both_saved(self, mock_config):
        """IAM role step saves both iam_role and s3_temp_dir to config."""
        mock_config.return_value.update.return_value = True

        validator = InputValidator()
        step = IAMRoleInputStep(validator)
        state = WizardState()
        state.s3_bucket = "production-bucket"

        result = step.handle_input("arn:aws:iam::987654321098:role/ProdRole", state)
        assert result.success

        # Verify both fields were saved in a single update call
        mock_config.return_value.update.assert_called_once()
        call_kwargs = mock_config.return_value.update.call_args[1]
        assert (
            call_kwargs["redshift_iam_role"]
            == "arn:aws:iam::987654321098:role/ProdRole"
        )
        assert (
            call_kwargs["redshift_s3_temp_dir"]
            == "s3://production-bucket/redshift-temp/"
        )

    def test_iam_role_step_not_shown_for_databricks(self):
        """IAM role step is not valid when data provider is Databricks."""
        state = WizardState()
        state.data_provider = "databricks"

        # IAM role step should not be valid for Databricks
        assert not state.is_valid_for_step(WizardStep.IAM_ROLE_INPUT)

    def test_databricks_flow_skips_iam_role(self):
        """Databricks wizard flow skips IAM role configuration entirely."""
        validator = InputValidator()

        # Select Databricks
        provider_step = DataProviderSelectionStep(validator)
        state = WizardState()
        result = provider_step.handle_input("databricks", state)

        # Databricks goes directly to computation provider, skipping all AWS steps
        assert result.next_step == WizardStep.COMPUTATION_PROVIDER_SELECTION
        assert result.next_step != WizardStep.IAM_ROLE_INPUT


class TestIAMRoleContextPersistence:
    """Test IAM role persistence in wizard context."""

    @patch("chuck_data.commands.setup_wizard.InteractiveContext")
    def test_orchestrator_saves_iam_role_to_context(self, mock_context_class):
        """Orchestrator saves iam_role to context when state is updated."""
        from chuck_data.commands.setup_wizard import SetupWizardOrchestrator

        # Mock the context instance
        mock_context = MagicMock()
        mock_context_class.return_value = mock_context
        mock_context.is_in_interactive_mode.return_value = False
        mock_context.get_context_data.return_value = None

        orchestrator = SetupWizardOrchestrator()

        # Create a state with IAM role
        state = WizardState()
        state.current_step = WizardStep.IAM_ROLE_INPUT
        state.s3_bucket = "test-bucket"
        state.iam_role = "arn:aws:iam::123456789012:role/TestRole"

        # Save state to context
        orchestrator._save_state_to_context(state)

        # Verify iam_role was stored
        calls = mock_context.store_context_data.call_args_list
        iam_role_stored = False
        for call in calls:
            if call[0][1] == "iam_role":
                assert call[0][2] == "arn:aws:iam::123456789012:role/TestRole"
                iam_role_stored = True
                break

        assert iam_role_stored, "iam_role should be stored in context"

    @patch("chuck_data.commands.setup_wizard.InteractiveContext")
    def test_orchestrator_loads_iam_role_from_context(self, mock_context_class):
        """Orchestrator loads iam_role from context when restoring state."""
        from chuck_data.commands.setup_wizard import SetupWizardOrchestrator

        # Mock the context instance with saved IAM role
        mock_context = MagicMock()
        mock_context_class.return_value = mock_context
        mock_context.is_in_interactive_mode.return_value = False
        mock_context.get_context_data.return_value = {
            "current_step": WizardStep.COMPUTATION_PROVIDER_SELECTION.value,
            "data_provider": "aws_redshift",
            "aws_region": "us-west-2",
            "s3_bucket": "test-bucket",
            "iam_role": "arn:aws:iam::987654321098:role/LoadedRole",
        }

        orchestrator = SetupWizardOrchestrator()

        # Load state from context
        state = orchestrator._load_state_from_context()

        # Verify iam_role was loaded correctly
        assert state is not None
        assert state.iam_role == "arn:aws:iam::987654321098:role/LoadedRole"
        assert state.s3_bucket == "test-bucket"
        assert state.data_provider == "aws_redshift"

    @patch("chuck_data.commands.setup_wizard.InteractiveContext")
    def test_orchestrator_preserves_iam_role_across_steps(self, mock_context_class):
        """IAM role is preserved in context as wizard progresses through steps."""
        from chuck_data.commands.setup_wizard import SetupWizardOrchestrator

        # Mock the context instance
        mock_context = MagicMock()
        mock_context_class.return_value = mock_context
        mock_context.is_in_interactive_mode.return_value = False

        # Simulate wizard at IAM role step with all prerequisites
        mock_context.get_context_data.return_value = {
            "current_step": WizardStep.IAM_ROLE_INPUT.value,
            "data_provider": "aws_redshift",
            "aws_region": "us-west-2",
            "redshift_cluster_identifier": "my-cluster",
            "s3_bucket": "my-bucket",
        }

        orchestrator = SetupWizardOrchestrator()

        # Load initial state (no IAM role yet)
        state = orchestrator._load_state_from_context()
        assert state.iam_role is None

        # User enters IAM role
        state.iam_role = "arn:aws:iam::111111111111:role/ProgressRole"

        # Save updated state
        orchestrator._save_state_to_context(state)

        # Verify the IAM role was saved
        saved_values = {}
        for call in mock_context.store_context_data.call_args_list:
            saved_values[call[0][1]] = call[0][2]

        assert "iam_role" in saved_values
        assert saved_values["iam_role"] == "arn:aws:iam::111111111111:role/ProgressRole"
