"""
Unit tests for wizard step handlers.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from chuck_data.commands.wizard.steps import (
    AWSProfileInputStep,
    AWSRegionInputStep,
    RedshiftClusterSelectionStep,
    S3BucketInputStep,
    IAMRoleInputStep,
    ComputeProviderSelectionStep,
    DataProviderSelectionStep,
    create_step,
)
from chuck_data.commands.wizard.state import (
    WizardStep,
    WizardState,
    WizardAction,
    StepResult,
)
from chuck_data.commands.wizard.validator import InputValidator


@pytest.fixture
def validator():
    """Create a mock input validator."""
    return InputValidator()


class TestAWSProfileInputStep:
    """Tests for AWSProfileInputStep."""

    def test_step_title(self, validator):
        """Test step title."""
        step = AWSProfileInputStep(validator)
        assert step.get_step_title() == "AWS Profile Configuration"

    def test_prompt_message_shows_current_profile(self, validator):
        """Test prompt message displays current AWS_PROFILE."""
        step = AWSProfileInputStep(validator)
        state = WizardState()

        with patch.dict("os.environ", {"AWS_PROFILE": "production"}):
            prompt = step.get_prompt_message(state)
            assert "production" in prompt
            assert "AWS_PROFILE" in prompt

    def test_handle_input_valid_profile(self, validator):
        """Test handling valid AWS profile input."""
        step = AWSProfileInputStep(validator)
        state = WizardState(data_provider="aws_redshift")

        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = True

            result = step.handle_input("prod", state)

            assert result.success is True
            assert result.next_step == WizardStep.AWS_REGION_INPUT
            assert result.action == WizardAction.CONTINUE
            assert result.data == {"aws_profile": "prod"}
            mock_config.return_value.update.assert_called_once_with(aws_profile="prod")

    def test_handle_input_empty_defaults_to_default(self, validator):
        """Test empty input defaults to 'default' profile."""
        step = AWSProfileInputStep(validator)
        state = WizardState(data_provider="aws_redshift")

        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = True

            result = step.handle_input("", state)

            assert result.success is True
            assert result.data == {"aws_profile": "default"}
            mock_config.return_value.update.assert_called_once_with(
                aws_profile="default"
            )

    def test_handle_input_invalid_profile_name(self, validator):
        """Test handling invalid profile name."""
        step = AWSProfileInputStep(validator)
        state = WizardState(data_provider="aws_redshift")

        result = step.handle_input("invalid@profile!", state)

        assert result.success is False
        assert result.action == WizardAction.RETRY
        assert "Invalid profile name" in result.message

    def test_handle_input_config_save_failure(self, validator):
        """Test handling config save failure."""
        step = AWSProfileInputStep(validator)
        state = WizardState(data_provider="aws_redshift")

        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = False

            result = step.handle_input("prod", state)

            assert result.success is False
            assert result.action == WizardAction.RETRY
            assert "Failed to save AWS profile" in result.message


class TestAWSRegionInputStep:
    """Tests for AWSRegionInputStep."""

    def test_step_title(self, validator):
        """Test step title."""
        step = AWSRegionInputStep(validator)
        assert step.get_step_title() == "AWS Region Configuration"

    def test_handle_input_valid_region(self, validator):
        """Test handling valid AWS region."""
        step = AWSRegionInputStep(validator)
        state = WizardState(data_provider="aws_redshift", aws_profile="default")

        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = True

            result = step.handle_input("us-west-2", state)

            assert result.success is True
            assert result.next_step == WizardStep.AWS_ACCOUNT_ID_INPUT
            assert result.data == {"aws_region": "us-west-2"}

    def test_handle_input_empty_region(self, validator):
        """Test empty region is rejected."""
        step = AWSRegionInputStep(validator)
        state = WizardState(data_provider="aws_redshift", aws_profile="default")

        result = step.handle_input("", state)

        assert result.success is False
        assert result.action == WizardAction.RETRY
        assert "cannot be empty" in result.message

    def test_handle_input_invalid_region_format(self, validator):
        """Test invalid region format is rejected."""
        step = AWSRegionInputStep(validator)
        state = WizardState(data_provider="aws_redshift", aws_profile="default")

        result = step.handle_input("invalid@region!", state)

        assert result.success is False
        assert result.action == WizardAction.RETRY
        assert "Invalid region format" in result.message


class TestRedshiftClusterSelectionStep:
    """Tests for RedshiftClusterSelectionStep."""

    def test_step_title(self, validator):
        """Test step title."""
        step = RedshiftClusterSelectionStep(validator)
        assert step.get_step_title() == "Redshift Cluster Selection"

    def test_handle_input_empty_identifier(self, validator):
        """Test empty cluster identifier is rejected."""
        step = RedshiftClusterSelectionStep(validator)
        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
        )

        result = step.handle_input("", state)

        assert result.success is False
        assert result.action == WizardAction.RETRY
        assert "cannot be empty" in result.message

    def test_handle_input_missing_aws_region(self, validator):
        """Test handles missing aws_region gracefully."""
        step = RedshiftClusterSelectionStep(validator)
        state = WizardState(data_provider="aws_redshift", aws_profile="default")

        result = step.handle_input("my-cluster", state)

        assert result.success is False
        assert result.next_step == WizardStep.AWS_REGION_INPUT
        assert "AWS region not set" in result.message

    @patch("chuck_data.clients.redshift.RedshiftAPIClient")
    @patch("chuck_data.config.get_config_manager")
    def test_handle_input_serverless_workgroup_success(
        self, mock_config, mock_client_class, validator
    ):
        """Test successful connection to serverless workgroup."""
        step = RedshiftClusterSelectionStep(validator)
        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
        )

        # Mock successful connection
        mock_client = Mock()
        mock_client.list_databases.return_value = ["dev", "prod"]
        mock_client_class.return_value = mock_client
        mock_config.return_value.update.return_value = True

        result = step.handle_input("my-workgroup", state)

        assert result.success is True
        assert result.next_step == WizardStep.S3_BUCKET_INPUT
        assert result.data == {"redshift_workgroup_name": "my-workgroup"}

    @patch("chuck_data.clients.redshift.RedshiftAPIClient")
    @patch("chuck_data.config.get_config_manager")
    def test_handle_input_provisioned_cluster_fallback(
        self, mock_config, mock_client_class, validator
    ):
        """Test fallback to provisioned cluster when serverless fails."""
        step = RedshiftClusterSelectionStep(validator)
        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
        )

        # First call (serverless) fails, second call (provisioned) succeeds
        mock_client_serverless = Mock()
        mock_client_serverless.list_databases.side_effect = Exception(
            "Workgroup not found"
        )

        mock_client_provisioned = Mock()
        mock_client_provisioned.list_databases.return_value = ["dev"]

        mock_client_class.side_effect = [
            mock_client_serverless,
            mock_client_provisioned,
        ]
        mock_config.return_value.update.return_value = True

        result = step.handle_input("my-cluster", state)

        assert result.success is True
        assert result.data == {"redshift_cluster_identifier": "my-cluster"}


class TestS3BucketInputStep:
    """Tests for S3BucketInputStep."""

    def test_step_title(self, validator):
        """Test step title."""
        step = S3BucketInputStep(validator)
        assert step.get_step_title() == "S3 Bucket Configuration"

    def test_handle_input_empty_bucket(self, validator):
        """Test empty bucket name is rejected."""
        step = S3BucketInputStep(validator)
        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
            redshift_cluster_identifier="my-cluster",
        )

        result = step.handle_input("", state)

        assert result.success is False
        assert result.action == WizardAction.RETRY
        assert "cannot be empty" in result.message

    def test_handle_input_invalid_bucket_format(self, validator):
        """Test invalid bucket name format is rejected."""
        step = S3BucketInputStep(validator)
        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
            redshift_cluster_identifier="my-cluster",
        )

        result = step.handle_input("invalid@bucket!", state)

        assert result.success is False
        assert "Invalid S3 bucket name" in result.message

    @patch("chuck_data.config.get_config_manager")
    @patch("chuck_data.ui.tui.get_chuck_service")
    def test_handle_input_valid_bucket(self, mock_service, mock_config, validator):
        """Test valid bucket input."""
        step = S3BucketInputStep(validator)
        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
            redshift_cluster_identifier="my-cluster",
        )

        # Mock boto3 module
        with patch.dict("sys.modules", {"boto3": MagicMock()}):
            import sys

            mock_boto3 = sys.modules["boto3"]

            # Mock S3 client
            mock_s3 = Mock()
            mock_s3.list_objects_v2.return_value = {}
            mock_boto3.client.return_value = mock_s3
            mock_config.return_value.update.return_value = True
            mock_service.return_value = None  # No service to reinitialize

            result = step.handle_input("my-bucket", state)

            assert result.success is True
            assert result.next_step == WizardStep.IAM_ROLE_INPUT
            assert result.data == {"s3_bucket": "my-bucket"}

    def test_handle_input_bucket_access_denied(self, validator):
        """Test bucket access denied."""
        step = S3BucketInputStep(validator)
        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
            redshift_cluster_identifier="my-cluster",
        )

        # Mock boto3 module
        with patch.dict("sys.modules", {"boto3": MagicMock()}):
            import sys

            mock_boto3 = sys.modules["boto3"]

            # Mock S3 client to raise exception
            mock_s3 = Mock()
            mock_s3.list_objects_v2.side_effect = Exception("Access Denied")
            mock_boto3.client.return_value = mock_s3

            result = step.handle_input("my-bucket", state)

            assert result.success is False
            assert "Cannot access S3 bucket" in result.message


class TestIAMRoleInputStep:
    """Tests for IAMRoleInputStep."""

    def test_step_title(self, validator):
        """Test step title."""
        step = IAMRoleInputStep(validator)
        assert step.get_step_title() == "IAM Role Configuration"

    def test_handle_input_empty_role(self, validator):
        """Test empty IAM role is rejected."""
        step = IAMRoleInputStep(validator)
        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
            s3_bucket="my-bucket",
        )

        result = step.handle_input("", state)

        assert result.success is False
        assert "cannot be empty" in result.message

    def test_handle_input_invalid_arn_format_no_prefix(self, validator):
        """Test invalid ARN format (no arn:aws:iam prefix)."""
        step = IAMRoleInputStep(validator)
        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
            s3_bucket="my-bucket",
        )

        result = step.handle_input("invalid-arn", state)

        assert result.success is False
        assert "Must start with 'arn:aws:iam::'" in result.message

    def test_handle_input_invalid_arn_no_role_segment(self, validator):
        """Test invalid ARN format (no :role/ segment)."""
        step = IAMRoleInputStep(validator)
        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
            s3_bucket="my-bucket",
        )

        result = step.handle_input("arn:aws:iam::123456789012:user/test", state)

        assert result.success is False
        assert "Must contain ':role/'" in result.message

    @patch("chuck_data.config.get_config_manager")
    def test_handle_input_valid_iam_role(self, mock_config, validator):
        """Test valid IAM role ARN."""
        step = IAMRoleInputStep(validator)
        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
            s3_bucket="my-bucket",
        )

        mock_config.return_value.update.return_value = True

        iam_role = "arn:aws:iam::123456789012:role/RedshiftRole"
        result = step.handle_input(iam_role, state)

        assert result.success is True
        assert result.next_step == WizardStep.COMPUTE_PROVIDER_SELECTION
        assert result.data == {"iam_role": iam_role}
        # Verify both IAM role and S3 temp dir were saved
        mock_config.return_value.update.assert_called_once()
        call_kwargs = mock_config.return_value.update.call_args[1]
        assert call_kwargs["redshift_iam_role"] == iam_role
        assert call_kwargs["redshift_s3_temp_dir"] == "s3://my-bucket/redshift-temp/"

    @patch("chuck_data.config.get_config_manager")
    def test_handle_input_valid_iam_role_with_path(self, mock_config, validator):
        """Test valid IAM role ARN with path (e.g., service-role/)."""
        step = IAMRoleInputStep(validator)
        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
            s3_bucket="my-bucket",
        )

        mock_config.return_value.update.return_value = True

        iam_role = "arn:aws:iam::123456789012:role/service-role/RedshiftRole"
        result = step.handle_input(iam_role, state)

        assert result.success is True


class TestComputeProviderSelectionStep:
    """Tests for ComputeProviderSelectionStep."""

    def test_step_title(self, validator):
        """Test step title."""
        step = ComputeProviderSelectionStep(validator)
        assert step.get_step_title() == "Compute Provider Selection"

    def test_handle_input_databricks(self, validator):
        """Test selecting Databricks compute provider with Redshift data provider."""
        step = ComputeProviderSelectionStep(validator)
        state = WizardState(
            data_provider="aws_redshift",
            s3_bucket="my-bucket",
            iam_role="arn:aws:iam::123456789012:role/RedshiftRole",
        )

        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = True

            result = step.handle_input("1", state)

            assert result.success is True
            # With Redshift data provider, should ask for Instance Profile first
            assert result.next_step == WizardStep.INSTANCE_PROFILE_INPUT
            assert result.data == {"compute_provider": "databricks"}

    def test_handle_input_empty_defaults_to_databricks(self, validator):
        """Test empty input defaults to Databricks."""
        step = ComputeProviderSelectionStep(validator)
        state = WizardState(data_provider="databricks")

        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = True

            result = step.handle_input("", state)

            assert result.success is True
            assert result.data == {"compute_provider": "databricks"}

    def test_handle_input_databricks_by_name(self, validator):
        """Test selecting Databricks by name."""
        step = ComputeProviderSelectionStep(validator)
        state = WizardState(data_provider="databricks")

        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = True

            result = step.handle_input("databricks", state)

            assert result.success is True
            assert result.data == {"compute_provider": "databricks"}

    def test_handle_input_invalid_selection(self, validator):
        """Test invalid selection is rejected."""
        step = ComputeProviderSelectionStep(validator)
        state = WizardState(data_provider="databricks")

        result = step.handle_input("invalid", state)

        assert result.success is False
        assert result.action == WizardAction.RETRY


class TestDataProviderSelectionStep:
    """Tests for DataProviderSelectionStep."""

    def test_handle_input_databricks(self, validator):
        """Test selecting Databricks data provider."""
        step = DataProviderSelectionStep(validator)
        state = WizardState()

        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = True

            result = step.handle_input("1", state)

            assert result.success is True
            # After selecting Databricks, go to workspace URL to collect credentials
            assert result.next_step == WizardStep.WORKSPACE_URL
            assert result.data == {"data_provider": "databricks"}

    def test_handle_input_redshift_by_number(self, validator):
        """Test selecting Redshift data provider by number."""
        step = DataProviderSelectionStep(validator)
        state = WizardState()

        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = True

            result = step.handle_input("2", state)

            assert result.success is True
            assert result.next_step == WizardStep.AWS_PROFILE_INPUT
            assert result.data == {"data_provider": "aws_redshift"}

    def test_handle_input_redshift_by_name(self, validator):
        """Test selecting Redshift by name."""
        step = DataProviderSelectionStep(validator)
        state = WizardState()

        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = True

            for name in ["aws_redshift", "aws redshift", "redshift"]:
                result = step.handle_input(name, state)
                assert result.success is True
                assert result.data == {"data_provider": "aws_redshift"}

    def test_handle_input_invalid_selection(self, validator):
        """Test invalid selection is rejected."""
        step = DataProviderSelectionStep(validator)
        state = WizardState()

        result = step.handle_input("invalid", state)

        assert result.success is False
        assert result.action == WizardAction.RETRY
        assert "enter 1 (Databricks) or 2 (AWS Redshift)" in result.message


class TestStepFactory:
    """Tests for step factory."""

    def test_create_step_all_new_steps(self, validator):
        """Test factory can create all new wizard steps."""
        step_types = [
            WizardStep.AWS_PROFILE_INPUT,
            WizardStep.AWS_REGION_INPUT,
            WizardStep.REDSHIFT_CLUSTER_SELECTION,
            WizardStep.S3_BUCKET_INPUT,
            WizardStep.IAM_ROLE_INPUT,
            WizardStep.INSTANCE_PROFILE_INPUT,
            WizardStep.COMPUTE_PROVIDER_SELECTION,
        ]

        for step_type in step_types:
            step = create_step(step_type, validator)
            assert step is not None
            assert hasattr(step, "handle_input")
            assert hasattr(step, "get_prompt_message")
            assert hasattr(step, "get_step_title")

    def test_create_step_unknown_type_raises_error(self, validator):
        """Test factory raises error for unknown step type."""
        # Create a mock enum value that doesn't exist in the factory
        with patch("chuck_data.commands.wizard.steps.WizardStep") as mock_wizard_step:
            # Create a mock enum value that isn't handled by the factory
            mock_unknown_step = Mock()
            mock_unknown_step.name = "UNKNOWN_STEP"

            with pytest.raises(ValueError, match="Unknown step type"):
                create_step(mock_unknown_step, validator)


class TestInstanceProfileInputStep:
    """Tests for Instance Profile ARN input step."""

    def test_valid_instance_profile_arn(self, validator):
        """Test that valid Instance Profile ARN is accepted."""
        step = create_step(WizardStep.INSTANCE_PROFILE_INPUT, validator)
        state = WizardState()
        state.data_provider = "aws_redshift"
        state.iam_role = "arn:aws:iam::123456789012:role/RedshiftRole"

        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = True
            result = step.handle_input(
                "arn:aws:iam::123456789012:instance-profile/DatabricksProfile", state
            )

        assert result.success
        assert result.next_step == WizardStep.WORKSPACE_URL
        assert result.action == WizardAction.CONTINUE
        assert (
            result.data["instance_profile_arn"]
            == "arn:aws:iam::123456789012:instance-profile/DatabricksProfile"
        )

    def test_valid_instance_profile_arn_with_path(self, validator):
        """Test that Instance Profile ARN with path components is accepted."""
        step = create_step(WizardStep.INSTANCE_PROFILE_INPUT, validator)
        state = WizardState()
        state.data_provider = "aws_redshift"
        state.iam_role = "arn:aws:iam::123456789012:role/RedshiftRole"

        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = True
            result = step.handle_input(
                "arn:aws:iam::123456789012:instance-profile/databricks/DatabricksProfile",
                state,
            )

        assert result.success
        assert result.next_step == WizardStep.WORKSPACE_URL

    def test_invalid_instance_profile_arn_format(self, validator):
        """Test that invalid Instance Profile ARN format is rejected."""
        step = create_step(WizardStep.INSTANCE_PROFILE_INPUT, validator)
        state = WizardState()

        # Missing arn:aws:iam:: prefix
        result = step.handle_input("invalid-arn", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "Invalid Instance Profile ARN format" in result.message

        # Missing :instance-profile/ component (role instead)
        result = step.handle_input("arn:aws:iam::123456789012:role/TestRole", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert ":instance-profile/" in result.message

        # User instead of instance-profile
        result = step.handle_input("arn:aws:iam::123456789012:user/TestUser", state)
        assert not result.success
        assert result.action == WizardAction.RETRY

    def test_empty_instance_profile_arn(self, validator):
        """Test that empty Instance Profile ARN is rejected."""
        step = create_step(WizardStep.INSTANCE_PROFILE_INPUT, validator)
        state = WizardState()

        result = step.handle_input("", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "cannot be empty" in result.message

    def test_whitespace_trimming(self, validator):
        """Test that leading/trailing whitespace is trimmed."""
        step = create_step(WizardStep.INSTANCE_PROFILE_INPUT, validator)
        state = WizardState()
        state.data_provider = "aws_redshift"
        state.iam_role = "arn:aws:iam::123456789012:role/RedshiftRole"

        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = True
            result = step.handle_input(
                "  arn:aws:iam::123456789012:instance-profile/DatabricksProfile  ",
                state,
            )

        assert result.success
        assert (
            result.data["instance_profile_arn"]
            == "arn:aws:iam::123456789012:instance-profile/DatabricksProfile"
        )

    def test_config_save_failure(self, validator):
        """Test handling of config save failure."""
        step = create_step(WizardStep.INSTANCE_PROFILE_INPUT, validator)
        state = WizardState()
        state.data_provider = "aws_redshift"
        state.iam_role = "arn:aws:iam::123456789012:role/RedshiftRole"

        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.return_value = False
            result = step.handle_input(
                "arn:aws:iam::123456789012:instance-profile/DatabricksProfile", state
            )

        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "Failed to save" in result.message

    def test_config_save_exception(self, validator):
        """Test handling of exception during config save."""
        step = create_step(WizardStep.INSTANCE_PROFILE_INPUT, validator)
        state = WizardState()
        state.data_provider = "aws_redshift"
        state.iam_role = "arn:aws:iam::123456789012:role/RedshiftRole"

        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.update.side_effect = Exception("Database error")
            result = step.handle_input(
                "arn:aws:iam::123456789012:instance-profile/DatabricksProfile", state
            )

        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "Error saving" in result.message

    def test_step_title(self, validator):
        """Test that step has appropriate title."""
        step = create_step(WizardStep.INSTANCE_PROFILE_INPUT, validator)
        title = step.get_step_title()
        assert "Instance Profile" in title
        assert "Databricks" in title

    def test_prompt_message(self, validator):
        """Test that step has informative prompt message."""
        step = create_step(WizardStep.INSTANCE_PROFILE_INPUT, validator)
        state = WizardState()
        prompt = step.get_prompt_message(state)
        assert "Instance Profile ARN" in prompt
        assert "arn:aws:iam::" in prompt
        assert "instance-profile" in prompt
        assert "Databricks" in prompt


class TestEMRClusterIDInputStep:
    """Tests for EMR cluster ID input step."""

    def test_valid_emr_cluster_id(self, validator):
        """Test that valid EMR cluster ID is accepted."""
        step = create_step(WizardStep.EMR_CLUSTER_ID_INPUT, validator)
        state = WizardState()
        state.compute_provider = "aws_emr"
        state.aws_region = "us-west-2"
        state.aws_profile = "default"

        with patch("chuck_data.clients.emr.EMRAPIClient") as mock_emr:
            mock_client = MagicMock()
            mock_client.validate_connection.return_value = True
            mock_client.get_cluster_status.return_value = "WAITING"
            mock_emr.return_value = mock_client

            with patch("chuck_data.config.get_config_manager") as mock_config:
                mock_config.return_value.update.return_value = True
                result = step.handle_input("j-XXXXXXXXXXXXX", state)

        assert result.success
        assert result.next_step == WizardStep.LLM_PROVIDER_SELECTION
        assert result.action == WizardAction.CONTINUE
        assert result.data["emr_cluster_id"] == "j-XXXXXXXXXXXXX"

    def test_invalid_cluster_id_format(self, validator):
        """Test that cluster IDs not starting with 'j-' are rejected."""
        step = create_step(WizardStep.EMR_CLUSTER_ID_INPUT, validator)
        state = WizardState()
        state.compute_provider = "aws_emr"
        state.aws_region = "us-west-2"

        result = step.handle_input("invalid-cluster-id", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "start with 'j-'" in result.message

    def test_empty_cluster_id(self, validator):
        """Test that empty cluster ID is rejected."""
        step = create_step(WizardStep.EMR_CLUSTER_ID_INPUT, validator)
        state = WizardState()

        result = step.handle_input("", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "cannot be empty" in result.message

    def test_whitespace_trimming(self, validator):
        """Test that leading/trailing whitespace is trimmed."""
        step = create_step(WizardStep.EMR_CLUSTER_ID_INPUT, validator)
        state = WizardState()
        state.compute_provider = "aws_emr"
        state.aws_region = "us-west-2"
        state.aws_profile = "default"

        with patch("chuck_data.clients.emr.EMRAPIClient") as mock_emr:
            mock_client = MagicMock()
            mock_client.validate_connection.return_value = True
            mock_client.get_cluster_status.return_value = "WAITING"
            mock_emr.return_value = mock_client

            with patch("chuck_data.config.get_config_manager") as mock_config:
                mock_config.return_value.update.return_value = True
                result = step.handle_input("  j-XXXXXXXXXXXXX  ", state)

        assert result.success
        assert result.data["emr_cluster_id"] == "j-XXXXXXXXXXXXX"

    def test_connection_validation_failure(self, validator):
        """Test handling of EMR connection validation failure."""
        step = create_step(WizardStep.EMR_CLUSTER_ID_INPUT, validator)
        state = WizardState()
        state.compute_provider = "aws_emr"
        state.aws_region = "us-west-2"
        state.aws_profile = "default"

        with patch("chuck_data.clients.emr.EMRAPIClient") as mock_emr:
            mock_client = MagicMock()
            mock_client.validate_connection.return_value = False
            mock_emr.return_value = mock_client

            result = step.handle_input("j-XXXXXXXXXXXXX", state)

        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "Failed to connect" in result.message

    def test_missing_aws_region(self, validator):
        """Test that missing AWS region is handled."""
        step = create_step(WizardStep.EMR_CLUSTER_ID_INPUT, validator)
        state = WizardState()
        state.compute_provider = "aws_emr"
        state.aws_region = None

        result = step.handle_input("j-XXXXXXXXXXXXX", state)
        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "AWS region not found" in result.message

    def test_config_save_failure(self, validator):
        """Test handling of config save failure."""
        step = create_step(WizardStep.EMR_CLUSTER_ID_INPUT, validator)
        state = WizardState()
        state.compute_provider = "aws_emr"
        state.aws_region = "us-west-2"
        state.aws_profile = "default"

        with patch("chuck_data.clients.emr.EMRAPIClient") as mock_emr:
            mock_client = MagicMock()
            mock_client.validate_connection.return_value = True
            mock_client.get_cluster_status.return_value = "WAITING"
            mock_emr.return_value = mock_client

            with patch("chuck_data.config.get_config_manager") as mock_config:
                mock_config.return_value.update.return_value = False
                result = step.handle_input("j-XXXXXXXXXXXXX", state)

        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "Failed to save" in result.message

    def test_exception_during_validation(self, validator):
        """Test handling of exception during cluster validation."""
        step = create_step(WizardStep.EMR_CLUSTER_ID_INPUT, validator)
        state = WizardState()
        state.compute_provider = "aws_emr"
        state.aws_region = "us-west-2"
        state.aws_profile = "default"

        with patch("chuck_data.clients.emr.EMRAPIClient") as mock_emr:
            mock_emr.side_effect = Exception("AWS connection error")
            result = step.handle_input("j-XXXXXXXXXXXXX", state)

        assert not result.success
        assert result.action == WizardAction.RETRY
        assert "Error validating EMR cluster" in result.message

    def test_step_title(self, validator):
        """Test that step has appropriate title."""
        step = create_step(WizardStep.EMR_CLUSTER_ID_INPUT, validator)
        title = step.get_step_title()
        assert "EMR" in title
        assert "Cluster" in title

    def test_prompt_message(self, validator):
        """Test that step has informative prompt message."""
        step = create_step(WizardStep.EMR_CLUSTER_ID_INPUT, validator)
        state = WizardState()
        prompt = step.get_prompt_message(state)
        assert "EMR cluster ID" in prompt
        assert "j-" in prompt
