"""
Unit tests for wizard state management.
"""

import pytest
from chuck_data.commands.wizard.state import (
    WizardStep,
    WizardState,
    WizardAction,
    StepResult,
    WizardStateMachine,
)
from chuck_data.llm.provider import ModelInfo


class TestWizardState:
    """Tests for WizardState dataclass."""

    def test_default_state(self):
        """Test default wizard state initialization."""
        state = WizardState()
        assert state.current_step == WizardStep.AMPERITY_AUTH
        assert state.data_provider is None
        assert state.compute_provider is None
        assert state.aws_profile is None
        assert state.aws_region is None
        assert state.workspace_url is None
        assert state.error_message is None

    def test_state_with_databricks_fields(self):
        """Test state with Databricks-specific fields."""
        state = WizardState(
            data_provider="databricks",
            compute_provider="databricks",
            workspace_url="https://test.databricks.com",
            token="test-token",
        )
        assert state.data_provider == "databricks"
        assert state.compute_provider == "databricks"
        assert state.workspace_url == "https://test.databricks.com"
        assert state.token == "test-token"

    def test_state_with_redshift_fields(self):
        """Test state with AWS Redshift-specific fields."""
        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="prod",
            aws_region="us-west-2",
            redshift_cluster_identifier="my-cluster",
            s3_bucket="my-bucket",
            iam_role="arn:aws:iam::123456789012:role/RedshiftRole",
        )
        assert state.data_provider == "aws_redshift"
        assert state.aws_profile == "prod"
        assert state.aws_region == "us-west-2"
        assert state.redshift_cluster_identifier == "my-cluster"
        assert state.s3_bucket == "my-bucket"
        assert state.iam_role == "arn:aws:iam::123456789012:role/RedshiftRole"

    def test_is_valid_for_amperity_auth(self):
        """Test validation for AMPERITY_AUTH step."""
        state = WizardState()
        assert state.is_valid_for_step(WizardStep.AMPERITY_AUTH) is True

    def test_is_valid_for_data_provider_selection(self):
        """Test validation for DATA_PROVIDER_SELECTION step."""
        state = WizardState()
        assert state.is_valid_for_step(WizardStep.DATA_PROVIDER_SELECTION) is True

    def test_is_valid_for_aws_profile_input_requires_redshift(self):
        """Test AWS_PROFILE_INPUT requires aws_redshift data provider."""
        state = WizardState()
        assert state.is_valid_for_step(WizardStep.AWS_PROFILE_INPUT) is False

        state.data_provider = "aws_redshift"
        assert state.is_valid_for_step(WizardStep.AWS_PROFILE_INPUT) is True

    def test_is_valid_for_aws_region_input_requires_profile(self):
        """Test AWS_REGION_INPUT requires aws_profile to be set."""
        state = WizardState(data_provider="aws_redshift")
        assert state.is_valid_for_step(WizardStep.AWS_REGION_INPUT) is False

        state.aws_profile = "default"
        assert state.is_valid_for_step(WizardStep.AWS_REGION_INPUT) is True

    def test_is_valid_for_redshift_cluster_selection_requires_account_id(self):
        """Test REDSHIFT_CLUSTER_SELECTION requires aws_account_id."""
        state = WizardState(
            data_provider="aws_redshift", aws_profile="default", aws_region="us-west-2"
        )
        assert state.is_valid_for_step(WizardStep.REDSHIFT_CLUSTER_SELECTION) is False

        state.aws_account_id = "123456789012"
        assert state.is_valid_for_step(WizardStep.REDSHIFT_CLUSTER_SELECTION) is True

    def test_is_valid_for_s3_bucket_input_requires_cluster(self):
        """Test S3_BUCKET_INPUT requires cluster identifier or workgroup."""
        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
        )
        assert state.is_valid_for_step(WizardStep.S3_BUCKET_INPUT) is False

        state.redshift_cluster_identifier = "my-cluster"
        assert state.is_valid_for_step(WizardStep.S3_BUCKET_INPUT) is True

        # Test with workgroup instead
        state2 = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
            redshift_workgroup_name="my-workgroup",
        )
        assert state2.is_valid_for_step(WizardStep.S3_BUCKET_INPUT) is True

    def test_is_valid_for_iam_role_input_requires_s3_bucket(self):
        """Test IAM_ROLE_INPUT requires s3_bucket."""
        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
            redshift_cluster_identifier="my-cluster",
        )
        assert state.is_valid_for_step(WizardStep.IAM_ROLE_INPUT) is False

        state.s3_bucket = "my-bucket"
        assert state.is_valid_for_step(WizardStep.IAM_ROLE_INPUT) is True

    def test_is_valid_for_compute_provider_selection_databricks(self):
        """Test COMPUTE_PROVIDER_SELECTION for Databricks data provider."""
        state = WizardState(data_provider="databricks")
        assert state.is_valid_for_step(WizardStep.COMPUTE_PROVIDER_SELECTION) is True

    def test_is_valid_for_compute_provider_selection_redshift(self):
        """Test COMPUTE_PROVIDER_SELECTION for Redshift requires AWS config complete."""
        state = WizardState(data_provider="aws_redshift")
        assert state.is_valid_for_step(WizardStep.COMPUTE_PROVIDER_SELECTION) is False

        state.s3_bucket = "my-bucket"
        state.iam_role = "arn:aws:iam::123456789012:role/RedshiftRole"
        assert state.is_valid_for_step(WizardStep.COMPUTE_PROVIDER_SELECTION) is True

    def test_is_valid_for_workspace_url_requires_databricks_compute(self):
        """Test WORKSPACE_URL requires databricks compute provider."""
        state = WizardState()
        assert state.is_valid_for_step(WizardStep.WORKSPACE_URL) is False

        state.compute_provider = "databricks"
        assert state.is_valid_for_step(WizardStep.WORKSPACE_URL) is True

    def test_is_valid_for_token_input_requires_workspace_url(self):
        """Test TOKEN_INPUT requires workspace_url."""
        state = WizardState(compute_provider="databricks")
        assert state.is_valid_for_step(WizardStep.TOKEN_INPUT) is False

        state.workspace_url = "https://test.databricks.com"
        assert state.is_valid_for_step(WizardStep.TOKEN_INPUT) is True


class TestWizardStateMachine:
    """Tests for WizardStateMachine."""

    def test_can_transition_valid(self):
        """Test valid transitions are allowed."""
        machine = WizardStateMachine()
        assert (
            machine.can_transition(
                WizardStep.AMPERITY_AUTH, WizardStep.DATA_PROVIDER_SELECTION
            )
            is True
        )
        assert (
            machine.can_transition(
                WizardStep.DATA_PROVIDER_SELECTION, WizardStep.AWS_PROFILE_INPUT
            )
            is True
        )
        assert (
            machine.can_transition(
                WizardStep.AWS_PROFILE_INPUT, WizardStep.AWS_REGION_INPUT
            )
            is True
        )

    def test_can_transition_invalid(self):
        """Test invalid transitions are blocked."""
        machine = WizardStateMachine()
        assert (
            machine.can_transition(WizardStep.AMPERITY_AUTH, WizardStep.COMPLETE)
            is False
        )
        assert (
            machine.can_transition(WizardStep.AWS_PROFILE_INPUT, WizardStep.COMPLETE)
            is False
        )

    def test_can_transition_retry_same_step(self):
        """Test retrying the same step is allowed."""
        machine = WizardStateMachine()
        assert (
            machine.can_transition(
                WizardStep.AWS_PROFILE_INPUT, WizardStep.AWS_PROFILE_INPUT
            )
            is True
        )
        assert (
            machine.can_transition(
                WizardStep.AWS_REGION_INPUT, WizardStep.AWS_REGION_INPUT
            )
            is True
        )

    def test_can_transition_back_one_step(self):
        """Test going back one step is allowed."""
        machine = WizardStateMachine()
        assert (
            machine.can_transition(
                WizardStep.AWS_REGION_INPUT, WizardStep.AWS_PROFILE_INPUT
            )
            is True
        )
        assert (
            machine.can_transition(
                WizardStep.REDSHIFT_CLUSTER_SELECTION, WizardStep.AWS_ACCOUNT_ID_INPUT
            )
            is True
        )

    def test_transition_on_success_moves_forward(self):
        """Test successful step result moves to next step."""
        machine = WizardStateMachine()
        state = WizardState(current_step=WizardStep.AMPERITY_AUTH)

        result = StepResult(
            success=True,
            message="Auth complete",
            next_step=WizardStep.DATA_PROVIDER_SELECTION,
            action=WizardAction.CONTINUE,
        )

        new_state = machine.transition(state, result)
        assert new_state.current_step == WizardStep.DATA_PROVIDER_SELECTION
        assert new_state.error_message is None

    def test_transition_on_retry_stays_on_step(self):
        """Test retry action keeps current step."""
        machine = WizardStateMachine()
        state = WizardState(current_step=WizardStep.AWS_PROFILE_INPUT)

        result = StepResult(
            success=False,
            message="Invalid profile name",
            action=WizardAction.RETRY,
        )

        new_state = machine.transition(state, result)
        assert new_state.current_step == WizardStep.AWS_PROFILE_INPUT
        assert new_state.error_message == "Invalid profile name"

    def test_transition_applies_data_to_state(self):
        """Test transition applies data changes to state."""
        machine = WizardStateMachine()
        state = WizardState(
            current_step=WizardStep.AWS_PROFILE_INPUT, data_provider="aws_redshift"
        )

        result = StepResult(
            success=True,
            message="Profile saved",
            next_step=WizardStep.AWS_REGION_INPUT,
            action=WizardAction.CONTINUE,
            data={"aws_profile": "prod"},
        )

        new_state = machine.transition(state, result)
        assert new_state.aws_profile == "prod"
        assert new_state.current_step == WizardStep.AWS_REGION_INPUT

    def test_get_next_step_databricks_path(self):
        """Test get_next_step for Databricks data provider path."""
        machine = WizardStateMachine()
        state = WizardState(data_provider="databricks")

        assert (
            machine.get_next_step(WizardStep.DATA_PROVIDER_SELECTION, state)
            == WizardStep.COMPUTE_PROVIDER_SELECTION
        )

    def test_get_next_step_redshift_path(self):
        """Test get_next_step for Redshift data provider path."""
        machine = WizardStateMachine()
        state = WizardState(data_provider="aws_redshift")

        assert (
            machine.get_next_step(WizardStep.DATA_PROVIDER_SELECTION, state)
            == WizardStep.AWS_PROFILE_INPUT
        )
        assert (
            machine.get_next_step(WizardStep.AWS_PROFILE_INPUT, state)
            == WizardStep.AWS_REGION_INPUT
        )
        assert (
            machine.get_next_step(WizardStep.AWS_REGION_INPUT, state)
            == WizardStep.AWS_ACCOUNT_ID_INPUT
        )
        assert (
            machine.get_next_step(WizardStep.AWS_ACCOUNT_ID_INPUT, state)
            == WizardStep.REDSHIFT_CLUSTER_SELECTION
        )
        assert (
            machine.get_next_step(WizardStep.REDSHIFT_CLUSTER_SELECTION, state)
            == WizardStep.S3_BUCKET_INPUT
        )
        assert (
            machine.get_next_step(WizardStep.S3_BUCKET_INPUT, state)
            == WizardStep.IAM_ROLE_INPUT
        )
        assert (
            machine.get_next_step(WizardStep.IAM_ROLE_INPUT, state)
            == WizardStep.COMPUTE_PROVIDER_SELECTION
        )

    def test_get_next_step_compute_provider_to_databricks_config(self):
        """Test get_next_step from compute provider to Databricks config."""
        machine = WizardStateMachine()
        state = WizardState(compute_provider="databricks")

        assert (
            machine.get_next_step(WizardStep.COMPUTE_PROVIDER_SELECTION, state)
            == WizardStep.WORKSPACE_URL
        )

    def test_get_next_step_complete_path(self):
        """Test complete path through wizard."""
        machine = WizardStateMachine()
        state = WizardState()

        assert (
            machine.get_next_step(WizardStep.AMPERITY_AUTH, state)
            == WizardStep.DATA_PROVIDER_SELECTION
        )
        assert (
            machine.get_next_step(WizardStep.WORKSPACE_URL, state)
            == WizardStep.TOKEN_INPUT
        )
        assert (
            machine.get_next_step(WizardStep.TOKEN_INPUT, state)
            == WizardStep.LLM_PROVIDER_SELECTION
        )
        assert (
            machine.get_next_step(WizardStep.LLM_PROVIDER_SELECTION, state)
            == WizardStep.MODEL_SELECTION
        )
        assert (
            machine.get_next_step(WizardStep.MODEL_SELECTION, state)
            == WizardStep.USAGE_CONSENT
        )
        assert (
            machine.get_next_step(WizardStep.USAGE_CONSENT, state)
            == WizardStep.COMPLETE
        )


class TestStepResult:
    """Tests for StepResult dataclass."""

    def test_success_result(self):
        """Test successful step result."""
        result = StepResult(
            success=True,
            message="Step completed",
            next_step=WizardStep.AWS_REGION_INPUT,
            action=WizardAction.CONTINUE,
        )
        assert result.success is True
        assert result.message == "Step completed"
        assert result.next_step == WizardStep.AWS_REGION_INPUT
        assert result.action == WizardAction.CONTINUE

    def test_retry_result(self):
        """Test retry step result."""
        result = StepResult(
            success=False,
            message="Invalid input, please retry",
            action=WizardAction.RETRY,
        )
        assert result.success is False
        assert result.action == WizardAction.RETRY
        assert result.next_step is None

    def test_result_with_data(self):
        """Test step result with data payload."""
        result = StepResult(
            success=True,
            message="Region saved",
            next_step=WizardStep.REDSHIFT_CLUSTER_SELECTION,
            action=WizardAction.CONTINUE,
            data={"aws_region": "us-west-2"},
        )
        assert result.data == {"aws_region": "us-west-2"}
