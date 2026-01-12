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
        # For Databricks data provider, need credentials before compute provider selection
        state = WizardState(
            data_provider="databricks",
            workspace_url="https://test.databricks.com",
            token="test-token",
        )
        assert state.is_valid_for_step(WizardStep.COMPUTE_PROVIDER_SELECTION) is True

    def test_is_valid_for_compute_provider_selection_redshift(self):
        """Test COMPUTE_PROVIDER_SELECTION for Redshift requires AWS config complete."""
        state = WizardState(data_provider="aws_redshift")
        assert state.is_valid_for_step(WizardStep.COMPUTE_PROVIDER_SELECTION) is False

        state.s3_bucket = "my-bucket"
        state.iam_role = "arn:aws:iam::123456789012:role/RedshiftRole"
        # Now should be valid - instance_profile is collected AFTER compute provider selection
        assert state.is_valid_for_step(WizardStep.COMPUTE_PROVIDER_SELECTION) is True

    def test_is_valid_for_workspace_url_requires_data_provider(self):
        """Test WORKSPACE_URL requires data provider to be set."""
        state = WizardState()
        assert state.is_valid_for_step(WizardStep.WORKSPACE_URL) is False

        # For Databricks data provider, workspace URL is always valid
        state.data_provider = "databricks"
        assert state.is_valid_for_step(WizardStep.WORKSPACE_URL) is True

        # For Redshift data + Databricks compute, need instance profile first
        state2 = WizardState(
            data_provider="aws_redshift", compute_provider="databricks"
        )
        assert state2.is_valid_for_step(WizardStep.WORKSPACE_URL) is False

        state2.instance_profile_arn = (
            "arn:aws:iam::123456789012:instance-profile/Profile"
        )
        assert state2.is_valid_for_step(WizardStep.WORKSPACE_URL) is True

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
        # After IAM role, go to compute provider selection
        assert (
            machine.get_next_step(WizardStep.IAM_ROLE_INPUT, state)
            == WizardStep.COMPUTE_PROVIDER_SELECTION
        )

        # If Databricks compute is selected, ask for instance profile
        state.compute_provider = "databricks"
        assert (
            machine.get_next_step(WizardStep.COMPUTE_PROVIDER_SELECTION, state)
            == WizardStep.INSTANCE_PROFILE_INPUT
        )
        # After instance profile, go to workspace URL
        assert (
            machine.get_next_step(WizardStep.INSTANCE_PROFILE_INPUT, state)
            == WizardStep.WORKSPACE_URL
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


class TestStepNumbering:
    """Tests for dynamic step numbering functionality."""

    def test_default_step_number_is_one(self):
        """Test that wizard state starts with step number 1."""
        state = WizardState()
        assert state.step_number == 1
        assert state.visited_steps == []

    def test_step_number_increments_on_transition(self):
        """Test that step number increments when transitioning to new step."""
        machine = WizardStateMachine()
        state = WizardState(current_step=WizardStep.AMPERITY_AUTH)
        assert state.step_number == 1

        result = StepResult(
            success=True,
            message="Auth complete",
            next_step=WizardStep.DATA_PROVIDER_SELECTION,
            action=WizardAction.CONTINUE,
        )

        new_state = machine.transition(state, result)
        assert new_state.step_number == 2
        assert new_state.current_step == WizardStep.DATA_PROVIDER_SELECTION
        assert WizardStep.AMPERITY_AUTH in new_state.visited_steps

    def test_step_number_does_not_increment_on_retry(self):
        """Test that step number stays same when retrying current step."""
        machine = WizardStateMachine()
        state = WizardState(current_step=WizardStep.AWS_PROFILE_INPUT, step_number=3)

        result = StepResult(
            success=False,
            message="Invalid profile",
            action=WizardAction.RETRY,
        )

        new_state = machine.transition(state, result)
        assert new_state.step_number == 3
        assert new_state.current_step == WizardStep.AWS_PROFILE_INPUT
        assert len(new_state.visited_steps) == 0

    def test_step_number_does_not_increment_on_same_step(self):
        """Test step number doesn't increment when transitioning to same step."""
        machine = WizardStateMachine()
        state = WizardState(current_step=WizardStep.AWS_PROFILE_INPUT, step_number=5)

        result = StepResult(
            success=True,
            message="Retry same step",
            next_step=WizardStep.AWS_PROFILE_INPUT,
            action=WizardAction.CONTINUE,
        )

        new_state = machine.transition(state, result)
        assert new_state.step_number == 5
        assert new_state.current_step == WizardStep.AWS_PROFILE_INPUT

    def test_visited_steps_tracks_history(self):
        """Test that visited_steps correctly tracks step progression."""
        machine = WizardStateMachine()
        state = WizardState(current_step=WizardStep.AMPERITY_AUTH)

        # Step 1 -> Step 2
        result1 = StepResult(
            success=True,
            message="Auth complete",
            next_step=WizardStep.DATA_PROVIDER_SELECTION,
            action=WizardAction.CONTINUE,
        )
        state = machine.transition(state, result1)
        assert state.visited_steps == [WizardStep.AMPERITY_AUTH]
        assert state.step_number == 2

        # Step 2 -> Step 3
        result2 = StepResult(
            success=True,
            message="Provider selected",
            next_step=WizardStep.WORKSPACE_URL,
            action=WizardAction.CONTINUE,
            data={"data_provider": "databricks"},
        )
        state = machine.transition(state, result2)
        assert state.visited_steps == [
            WizardStep.AMPERITY_AUTH,
            WizardStep.DATA_PROVIDER_SELECTION,
        ]
        assert state.step_number == 3

    def test_databricks_only_path_step_numbers(self):
        """Test step numbering for Databricks-only setup path."""
        machine = WizardStateMachine()
        state = WizardState(current_step=WizardStep.AMPERITY_AUTH)

        steps = [
            (WizardStep.DATA_PROVIDER_SELECTION, {"data_provider": "databricks"}),
            (
                WizardStep.WORKSPACE_URL,
                {"workspace_url": "https://test.databricks.com"},
            ),
            (WizardStep.TOKEN_INPUT, {"token": "test-token"}),
            (WizardStep.COMPUTE_PROVIDER_SELECTION, {"compute_provider": "databricks"}),
            (WizardStep.LLM_PROVIDER_SELECTION, {"llm_provider": "databricks"}),
            (
                WizardStep.MODEL_SELECTION,
                {"selected_model": "databricks-claude-sonnet-4-5"},
            ),
            (WizardStep.USAGE_CONSENT, {"usage_consent": True}),
        ]

        expected_step_number = 1
        for next_step, data in steps:
            result = StepResult(
                success=True,
                message="Step complete",
                next_step=next_step,
                action=WizardAction.CONTINUE,
                data=data,
            )
            state = machine.transition(state, result)
            expected_step_number += 1
            assert state.step_number == expected_step_number, (
                f"Expected step number {expected_step_number} "
                f"at {next_step.value}, got {state.step_number}"
            )

        # Should end at step 8 (1 initial + 7 transitions)
        assert state.step_number == 8

    def test_redshift_databricks_compute_path_step_numbers(self):
        """Test step numbering for Redshift data + Databricks compute path."""
        machine = WizardStateMachine()
        state = WizardState(current_step=WizardStep.AMPERITY_AUTH)

        steps = [
            (WizardStep.DATA_PROVIDER_SELECTION, {"data_provider": "aws_redshift"}),
            (WizardStep.AWS_PROFILE_INPUT, {"aws_profile": "default"}),
            (WizardStep.AWS_REGION_INPUT, {"aws_region": "us-west-2"}),
            (WizardStep.AWS_ACCOUNT_ID_INPUT, {"aws_account_id": "123456789012"}),
            (
                WizardStep.REDSHIFT_CLUSTER_SELECTION,
                {"redshift_cluster_identifier": "my-cluster"},
            ),
            (WizardStep.S3_BUCKET_INPUT, {"s3_bucket": "my-bucket"}),
            (
                WizardStep.IAM_ROLE_INPUT,
                {"iam_role": "arn:aws:iam::123456789012:role/RedshiftRole"},
            ),
            (WizardStep.COMPUTE_PROVIDER_SELECTION, {"compute_provider": "databricks"}),
            (
                WizardStep.INSTANCE_PROFILE_INPUT,
                {
                    "instance_profile_arn": "arn:aws:iam::123456789012:instance-profile/DBProfile"
                },
            ),
            (
                WizardStep.WORKSPACE_URL,
                {"workspace_url": "https://test.databricks.com"},
            ),
            (WizardStep.TOKEN_INPUT, {"token": "test-token"}),
            (WizardStep.LLM_PROVIDER_SELECTION, {"llm_provider": "databricks"}),
            (
                WizardStep.MODEL_SELECTION,
                {"selected_model": "databricks-claude-sonnet-4-5"},
            ),
            (WizardStep.USAGE_CONSENT, {"usage_consent": True}),
        ]

        expected_step_number = 1
        for next_step, data in steps:
            result = StepResult(
                success=True,
                message="Step complete",
                next_step=next_step,
                action=WizardAction.CONTINUE,
                data=data,
            )
            state = machine.transition(state, result)
            expected_step_number += 1
            assert state.step_number == expected_step_number, (
                f"Expected step number {expected_step_number} "
                f"at {next_step.value}, got {state.step_number}"
            )

        # Should end at step 15 (1 initial + 14 transitions)
        assert state.step_number == 15

    def test_emr_compute_path_step_numbers(self):
        """Test step numbering for Databricks data + EMR compute path."""
        machine = WizardStateMachine()
        state = WizardState(current_step=WizardStep.AMPERITY_AUTH)

        steps = [
            (WizardStep.DATA_PROVIDER_SELECTION, {"data_provider": "databricks"}),
            (
                WizardStep.WORKSPACE_URL,
                {"workspace_url": "https://test.databricks.com"},
            ),
            (WizardStep.TOKEN_INPUT, {"token": "test-token"}),
            (WizardStep.COMPUTE_PROVIDER_SELECTION, {"compute_provider": "aws_emr"}),
            (WizardStep.AWS_PROFILE_INPUT, {"aws_profile": "default"}),
            (WizardStep.AWS_REGION_INPUT, {"aws_region": "us-west-2"}),
            (WizardStep.EMR_CLUSTER_ID_INPUT, {"emr_cluster_id": "j-XXXXXXXXXXXXX"}),
            (WizardStep.LLM_PROVIDER_SELECTION, {"llm_provider": "databricks"}),
            (
                WizardStep.MODEL_SELECTION,
                {"selected_model": "databricks-claude-sonnet-4-5"},
            ),
            (WizardStep.USAGE_CONSENT, {"usage_consent": True}),
        ]

        expected_step_number = 1
        for next_step, data in steps:
            result = StepResult(
                success=True,
                message="Step complete",
                next_step=next_step,
                action=WizardAction.CONTINUE,
                data=data,
            )
            state = machine.transition(state, result)
            expected_step_number += 1
            assert state.step_number == expected_step_number, (
                f"Expected step number {expected_step_number} "
                f"at {next_step.value}, got {state.step_number}"
            )

        # Should end at step 11 (1 initial + 10 transitions)
        assert state.step_number == 11

    def test_step_number_preserved_through_retry(self):
        """Test step number is preserved when user retries with errors."""
        machine = WizardStateMachine()
        state = WizardState(
            current_step=WizardStep.AWS_PROFILE_INPUT,
            step_number=3,
            data_provider="aws_redshift",  # Required for AWS_REGION_INPUT validation
        )

        # First attempt - error
        result1 = StepResult(
            success=False,
            message="Invalid profile",
            action=WizardAction.RETRY,
        )
        state = machine.transition(state, result1)
        assert state.step_number == 3
        assert state.error_message == "Invalid profile"

        # Second attempt - still error
        result2 = StepResult(
            success=False,
            message="Profile still invalid",
            action=WizardAction.RETRY,
        )
        state = machine.transition(state, result2)
        assert state.step_number == 3

        # Third attempt - success
        result3 = StepResult(
            success=True,
            message="Profile valid",
            next_step=WizardStep.AWS_REGION_INPUT,
            action=WizardAction.CONTINUE,
            data={"aws_profile": "prod"},
        )
        state = machine.transition(state, result3)
        assert state.step_number == 4
        assert state.current_step == WizardStep.AWS_REGION_INPUT

    def test_visited_steps_not_duplicated(self):
        """Test that visited_steps doesn't add duplicates on retry."""
        machine = WizardStateMachine()
        state = WizardState(
            current_step=WizardStep.AWS_PROFILE_INPUT,
            data_provider="aws_redshift",  # Required for AWS_REGION_INPUT validation
        )
        state.visited_steps = [
            WizardStep.AMPERITY_AUTH,
            WizardStep.DATA_PROVIDER_SELECTION,
        ]
        state.step_number = 3

        # Move forward
        result = StepResult(
            success=True,
            message="Profile saved",
            next_step=WizardStep.AWS_REGION_INPUT,
            action=WizardAction.CONTINUE,
            data={"aws_profile": "prod"},
        )
        state = machine.transition(state, result)

        # AWS_PROFILE_INPUT should be added once
        assert state.visited_steps.count(WizardStep.AWS_PROFILE_INPUT) == 1
        assert len(state.visited_steps) == 3
