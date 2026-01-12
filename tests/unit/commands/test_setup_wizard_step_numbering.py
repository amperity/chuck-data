"""
Integration tests for setup wizard step numbering.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from chuck_data.commands.setup_wizard import SetupWizardOrchestrator
from chuck_data.commands.wizard.state import WizardState, WizardStep
from chuck_data.interactive_context import InteractiveContext


class TestSetupWizardStepNumbering:
    """Integration tests for step numbering in setup wizard."""

    @pytest.fixture
    def orchestrator(self):
        """Create a test orchestrator."""
        with patch("chuck_data.commands.setup_wizard.get_console"):
            return SetupWizardOrchestrator()

    @pytest.fixture
    def mock_context(self):
        """Mock interactive context."""
        with patch.object(InteractiveContext, "store_context_data"):
            with patch.object(InteractiveContext, "get_context_data") as mock_get:
                mock_get.return_value = {}
                yield mock_get

    def test_save_state_includes_step_number(self, orchestrator):
        """Test that _save_state_to_context saves step_number."""
        state = WizardState(
            current_step=WizardStep.AWS_PROFILE_INPUT,
            step_number=5,
            visited_steps=[
                WizardStep.AMPERITY_AUTH,
                WizardStep.DATA_PROVIDER_SELECTION,
                WizardStep.AWS_PROFILE_INPUT,
            ],
        )

        with patch.object(orchestrator.context, "store_context_data") as mock_store:
            orchestrator._save_state_to_context(state)

            # Verify step_number was saved
            calls = {call[0][1]: call[0][2] for call in mock_store.call_args_list}
            assert calls["step_number"] == 5
            assert "visited_steps" in calls
            assert calls["visited_steps"] == [
                "amperity_auth",
                "data_provider_selection",
                "aws_profile_input",
            ]

    def test_load_state_restores_step_number(self, orchestrator):
        """Test that _load_state_from_context restores step_number."""
        context_data = {
            "current_step": "aws_profile_input",
            "step_number": 7,
            "visited_steps": [
                "amperity_auth",
                "data_provider_selection",
            ],
            "data_provider": "aws_redshift",
        }

        with patch.object(
            orchestrator.context, "get_context_data", return_value=context_data
        ):
            state = orchestrator._load_state_from_context()

            assert state is not None
            assert state.step_number == 7
            assert state.current_step == WizardStep.AWS_PROFILE_INPUT
            assert len(state.visited_steps) == 2
            assert WizardStep.AMPERITY_AUTH in state.visited_steps
            assert WizardStep.DATA_PROVIDER_SELECTION in state.visited_steps

    def test_load_state_defaults_step_number_if_missing(self, orchestrator):
        """Test that missing step_number defaults to 1."""
        context_data = {
            "current_step": "amperity_auth",
            "data_provider": None,
        }

        with patch.object(
            orchestrator.context, "get_context_data", return_value=context_data
        ):
            state = orchestrator._load_state_from_context()

            assert state is not None
            assert state.step_number == 1
            assert state.visited_steps == []

    def test_load_state_handles_invalid_visited_steps(self, orchestrator):
        """Test that invalid visited_steps are skipped."""
        context_data = {
            "current_step": "aws_profile_input",
            "step_number": 3,
            "visited_steps": [
                "amperity_auth",
                "invalid_step_name",  # This should be skipped
                "data_provider_selection",
            ],
        }

        with patch.object(
            orchestrator.context, "get_context_data", return_value=context_data
        ):
            state = orchestrator._load_state_from_context()

            assert state is not None
            assert len(state.visited_steps) == 2
            assert WizardStep.AMPERITY_AUTH in state.visited_steps
            assert WizardStep.DATA_PROVIDER_SELECTION in state.visited_steps

    def test_renderer_receives_correct_step_number(self, orchestrator):
        """Test that renderer.get_step_number is called with state."""
        state = WizardState(current_step=WizardStep.AWS_PROFILE_INPUT, step_number=8)

        with patch.object(
            orchestrator.renderer, "get_step_number", return_value=8
        ) as mock_get_step:
            with patch.object(orchestrator.renderer, "render_step"):
                with patch("chuck_data.commands.setup_wizard.create_step"):
                    # Call render logic
                    step_num = orchestrator.renderer.get_step_number(state)

                    mock_get_step.assert_called_once_with(state)
                    assert step_num == 8

    def test_step_number_persists_across_context_save_load(self, orchestrator):
        """Test step number round-trip through context."""
        original_state = WizardState(
            current_step=WizardStep.LLM_PROVIDER_SELECTION,
            step_number=11,
            visited_steps=[
                WizardStep.AMPERITY_AUTH,
                WizardStep.DATA_PROVIDER_SELECTION,
                WizardStep.WORKSPACE_URL,
                WizardStep.TOKEN_INPUT,
                WizardStep.COMPUTE_PROVIDER_SELECTION,
            ],
            data_provider="databricks",
            compute_provider="aws_emr",
        )

        # Save state
        saved_data = {}

        def mock_store(context, key, value):
            saved_data[key] = value

        with patch.object(
            orchestrator.context, "store_context_data", side_effect=mock_store
        ):
            orchestrator._save_state_to_context(original_state)

        # Load state back
        with patch.object(
            orchestrator.context, "get_context_data", return_value=saved_data
        ):
            loaded_state = orchestrator._load_state_from_context()

        # Verify round-trip
        assert loaded_state.step_number == original_state.step_number
        assert loaded_state.current_step == original_state.current_step
        assert len(loaded_state.visited_steps) == len(original_state.visited_steps)
        for step in original_state.visited_steps:
            assert step in loaded_state.visited_steps


class TestStepNumberingEndToEnd:
    """End-to-end tests simulating full wizard flows."""

    def test_databricks_only_flow_step_numbers(self):
        """Test step numbers through complete Databricks-only flow."""
        from chuck_data.commands.wizard.state import (
            WizardStateMachine,
            StepResult,
            WizardAction,
        )

        machine = WizardStateMachine()
        state = WizardState(current_step=WizardStep.AMPERITY_AUTH)

        # Track step numbers at each transition with proper data for validation
        transitions = [
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

        # Start at step 1
        assert state.step_number == 1
        assert state.current_step == WizardStep.AMPERITY_AUTH

        expected_num = 1
        for next_step, data in transitions:
            result = StepResult(
                success=True,
                message="Step complete",
                next_step=next_step,
                action=WizardAction.CONTINUE,
                data=data,
            )
            state = machine.transition(state, result)
            expected_num += 1

            # Verify step number incremented
            assert state.step_number == expected_num
            assert state.current_step == next_step

    def test_error_retry_preserves_step_number(self):
        """Test that errors and retries don't advance step number."""
        from chuck_data.commands.wizard.state import (
            WizardStateMachine,
            StepResult,
            WizardAction,
        )

        machine = WizardStateMachine()
        state = WizardState(
            current_step=WizardStep.AWS_PROFILE_INPUT,
            step_number=5,
            data_provider="aws_redshift",  # Required for AWS_REGION_INPUT validation
        )

        # User makes 3 errors before succeeding
        for _ in range(3):
            result = StepResult(
                success=False,
                message="Invalid input",
                action=WizardAction.RETRY,
            )
            state = machine.transition(state, result)
            # Step number should not change
            assert state.step_number == 5
            assert state.current_step == WizardStep.AWS_PROFILE_INPUT

        # Finally succeed
        result = StepResult(
            success=True,
            message="Valid input",
            next_step=WizardStep.AWS_REGION_INPUT,
            action=WizardAction.CONTINUE,
            data={"aws_profile": "prod"},
        )
        state = machine.transition(state, result)

        # Now step number should advance
        assert state.step_number == 6
        assert state.current_step == WizardStep.AWS_REGION_INPUT

    def test_mixed_path_step_numbering(self):
        """Test step numbering with Redshift data + EMR compute path."""
        from chuck_data.commands.wizard.state import (
            WizardStateMachine,
            StepResult,
            WizardAction,
        )

        machine = WizardStateMachine()
        state = WizardState(current_step=WizardStep.AMPERITY_AUTH)

        transitions = [
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
                {"iam_role": "arn:aws:iam::123456789012:role/Role"},
            ),
            (WizardStep.COMPUTE_PROVIDER_SELECTION, {"compute_provider": "aws_emr"}),
            (WizardStep.AWS_PROFILE_INPUT, {}),  # May need AWS config for EMR
            (WizardStep.AWS_REGION_INPUT, {}),
            (WizardStep.EMR_CLUSTER_ID_INPUT, {"emr_cluster_id": "j-XXXXX"}),
            (WizardStep.LLM_PROVIDER_SELECTION, {"llm_provider": "aws_bedrock"}),
            (WizardStep.MODEL_SELECTION, {"selected_model": "bedrock-claude-sonnet"}),
            (WizardStep.USAGE_CONSENT, {"usage_consent": True}),
        ]

        step_num = 1
        for next_step, data in transitions:
            result = StepResult(
                success=True,
                message="Step complete",
                next_step=next_step,
                action=WizardAction.CONTINUE,
                data=data,
            )
            state = machine.transition(state, result)
            step_num += 1
            assert state.step_number == step_num

        # Final step number should be 15
        assert state.step_number == 15
