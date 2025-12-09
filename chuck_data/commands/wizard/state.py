"""
Wizard state management for setup wizard.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any

from chuck_data.llm.provider import ModelInfo


class WizardStep(Enum):
    """Steps in the setup wizard."""

    AMPERITY_AUTH = "amperity_auth"
    DATA_PROVIDER_SELECTION = "data_provider_selection"
    # AWS Redshift-specific steps
    AWS_REGION_INPUT = "aws_region_input"
    REDSHIFT_CLUSTER_SELECTION = "redshift_cluster_selection"
    S3_BUCKET_INPUT = "s3_bucket_input"
    IAM_ROLE_INPUT = "iam_role_input"
    # Computation provider selection
    COMPUTATION_PROVIDER_SELECTION = "computation_provider_selection"
    # Databricks-specific steps
    WORKSPACE_URL = "workspace_url"
    TOKEN_INPUT = "token_input"
    # LLM provider and model selection
    LLM_PROVIDER_SELECTION = "llm_provider_selection"
    MODEL_SELECTION = "model_selection"
    USAGE_CONSENT = "usage_consent"
    COMPLETE = "complete"


class WizardAction(Enum):
    """Actions the wizard can take."""

    CONTINUE = "continue"
    RETRY = "retry"
    EXIT = "exit"
    COMPLETE = "complete"


@dataclass
class WizardState:
    """State of the setup wizard."""

    current_step: WizardStep = WizardStep.AMPERITY_AUTH
    # Provider selections
    data_provider: Optional[str] = None
    computation_provider: Optional[str] = None
    # Databricks-specific fields
    workspace_url: Optional[str] = None
    token: Optional[str] = None
    # AWS-specific fields
    aws_region: Optional[str] = None
    redshift_cluster_identifier: Optional[str] = None
    redshift_workgroup_name: Optional[str] = None
    s3_bucket: Optional[str] = None
    iam_role: Optional[str] = None
    emr_cluster_id: Optional[str] = None
    # LLM provider fields
    llm_provider: Optional[str] = None
    models: List[ModelInfo] = field(default_factory=list)
    selected_model: Optional[str] = None
    usage_consent: Optional[bool] = None
    error_message: Optional[str] = None

    def is_valid_for_step(self, step: WizardStep) -> bool:
        """Check if current state is valid for the given step."""
        if step == WizardStep.AMPERITY_AUTH:
            return True
        elif step == WizardStep.DATA_PROVIDER_SELECTION:
            return True  # Can always enter data provider selection
        # AWS Redshift-specific steps
        elif step == WizardStep.AWS_REGION_INPUT:
            return self.data_provider == "aws_redshift"
        elif step == WizardStep.REDSHIFT_CLUSTER_SELECTION:
            return self.data_provider == "aws_redshift" and self.aws_region is not None
        elif step == WizardStep.S3_BUCKET_INPUT:
            return self.data_provider == "aws_redshift" and (
                self.redshift_cluster_identifier is not None
                or self.redshift_workgroup_name is not None
            )
        elif step == WizardStep.IAM_ROLE_INPUT:
            return self.data_provider == "aws_redshift" and self.s3_bucket is not None
        # Computation provider selection
        elif step == WizardStep.COMPUTATION_PROVIDER_SELECTION:
            # For Databricks: just need data_provider set
            # For AWS Redshift: need AWS config complete (region, cluster, s3)
            if self.data_provider == "databricks":
                return True
            elif self.data_provider == "aws_redshift":
                return self.s3_bucket is not None and self.iam_role is not None
            return self.data_provider is not None
        # Databricks-specific steps
        elif step == WizardStep.WORKSPACE_URL:
            return self.computation_provider == "databricks"
        elif step == WizardStep.TOKEN_INPUT:
            return self.workspace_url is not None
        # LLM provider and model selection
        elif step == WizardStep.LLM_PROVIDER_SELECTION:
            # Need data provider configured before choosing LLM provider
            return self.data_provider is not None
        elif step == WizardStep.MODEL_SELECTION:
            # Need LLM provider selected
            return self.llm_provider is not None
        elif step == WizardStep.USAGE_CONSENT:
            return True  # Can skip to usage consent if no models available
        elif step == WizardStep.COMPLETE:
            return self.usage_consent is not None
        return False


@dataclass
class StepResult:
    """Result of processing a wizard step."""

    success: bool
    message: str
    next_step: Optional[WizardStep] = None
    action: WizardAction = WizardAction.CONTINUE
    data: Optional[Dict[str, Any]] = None


class WizardStateMachine:
    """State machine for managing wizard flow."""

    def __init__(self):
        self.valid_transitions = {
            WizardStep.AMPERITY_AUTH: [
                WizardStep.DATA_PROVIDER_SELECTION,
                WizardStep.AMPERITY_AUTH,
            ],
            WizardStep.DATA_PROVIDER_SELECTION: [
                WizardStep.AWS_REGION_INPUT,
                WizardStep.COMPUTATION_PROVIDER_SELECTION,
                WizardStep.DATA_PROVIDER_SELECTION,
            ],
            # AWS Redshift-specific steps
            WizardStep.AWS_REGION_INPUT: [
                WizardStep.REDSHIFT_CLUSTER_SELECTION,
                WizardStep.AWS_REGION_INPUT,
                WizardStep.DATA_PROVIDER_SELECTION,
            ],
            WizardStep.REDSHIFT_CLUSTER_SELECTION: [
                WizardStep.S3_BUCKET_INPUT,
                WizardStep.REDSHIFT_CLUSTER_SELECTION,
                WizardStep.AWS_REGION_INPUT,
            ],
            WizardStep.S3_BUCKET_INPUT: [
                WizardStep.IAM_ROLE_INPUT,
                WizardStep.S3_BUCKET_INPUT,
                WizardStep.REDSHIFT_CLUSTER_SELECTION,
            ],
            WizardStep.IAM_ROLE_INPUT: [
                WizardStep.COMPUTATION_PROVIDER_SELECTION,
                WizardStep.IAM_ROLE_INPUT,
                WizardStep.S3_BUCKET_INPUT,
            ],
            WizardStep.COMPUTATION_PROVIDER_SELECTION: [
                WizardStep.WORKSPACE_URL,
                WizardStep.LLM_PROVIDER_SELECTION,
                WizardStep.COMPUTATION_PROVIDER_SELECTION,
            ],
            WizardStep.WORKSPACE_URL: [
                WizardStep.TOKEN_INPUT,
                WizardStep.WORKSPACE_URL,
            ],
            WizardStep.TOKEN_INPUT: [
                WizardStep.LLM_PROVIDER_SELECTION,
                WizardStep.TOKEN_INPUT,
                WizardStep.WORKSPACE_URL,
            ],
            WizardStep.LLM_PROVIDER_SELECTION: [
                WizardStep.MODEL_SELECTION,
                WizardStep.LLM_PROVIDER_SELECTION,
            ],
            WizardStep.MODEL_SELECTION: [
                WizardStep.USAGE_CONSENT,
                WizardStep.MODEL_SELECTION,
            ],
            WizardStep.USAGE_CONSENT: [WizardStep.COMPLETE, WizardStep.USAGE_CONSENT],
            WizardStep.COMPLETE: [],
        }

    def can_transition(self, from_step: WizardStep, to_step: WizardStep) -> bool:
        """Check if transition is valid."""
        return to_step in self.valid_transitions.get(from_step, [])

    def transition(self, state: WizardState, result: StepResult) -> WizardState:
        """Apply step result to state and transition to next step."""
        if not result.success and result.action == WizardAction.RETRY:
            # Stay on current step for retry
            state.error_message = result.message
            return state

        if result.action == WizardAction.EXIT:
            # Exit the wizard
            return state

        # Set error message for failed steps, clear on successful steps
        if result.success:
            state.error_message = None
        elif result.message:
            # Preserve error message for failed steps that continue to next step
            state.error_message = result.message

        # Apply any data changes from the step result
        if result.data:
            import logging

            logging.info(
                f"WizardStateMachine.transition: Applying data from result: {result.data}"
            )
            for key, value in result.data.items():
                if hasattr(state, key):
                    logging.info(f"  Setting state.{key} = {value}")
                    setattr(state, key, value)
                else:
                    logging.warning(
                        f"  State does not have attribute '{key}', skipping"
                    )

        # Transition to next step if specified and valid
        if result.next_step and self.can_transition(
            state.current_step, result.next_step
        ):
            if state.is_valid_for_step(result.next_step):
                state.current_step = result.next_step
            else:
                # Invalid state for next step, set error
                import logging

                logging.error(
                    f"Invalid state for step {result.next_step.value}: "
                    f"data_provider={state.data_provider}, "
                    f"aws_region={state.aws_region}, "
                    f"redshift_cluster_identifier={state.redshift_cluster_identifier}, "
                    f"redshift_workgroup_name={state.redshift_workgroup_name}"
                )
                state.error_message = f"Invalid state for step {result.next_step.value}"

        return state

    def get_next_step(self, current_step: WizardStep, state: WizardState) -> WizardStep:
        """Determine the natural next step based on current step and state."""
        if current_step == WizardStep.AMPERITY_AUTH:
            return WizardStep.DATA_PROVIDER_SELECTION
        elif current_step == WizardStep.DATA_PROVIDER_SELECTION:
            # Route to provider-specific configuration
            if state.data_provider == "aws_redshift":
                return WizardStep.AWS_REGION_INPUT
            elif state.data_provider == "databricks":
                return WizardStep.COMPUTATION_PROVIDER_SELECTION
            return WizardStep.COMPUTATION_PROVIDER_SELECTION
        # AWS Redshift-specific steps
        elif current_step == WizardStep.AWS_REGION_INPUT:
            return WizardStep.REDSHIFT_CLUSTER_SELECTION
        elif current_step == WizardStep.REDSHIFT_CLUSTER_SELECTION:
            return WizardStep.S3_BUCKET_INPUT
        elif current_step == WizardStep.S3_BUCKET_INPUT:
            return WizardStep.IAM_ROLE_INPUT
        elif current_step == WizardStep.IAM_ROLE_INPUT:
            return WizardStep.COMPUTATION_PROVIDER_SELECTION
        elif current_step == WizardStep.COMPUTATION_PROVIDER_SELECTION:
            # For Databricks computation provider, go to workspace config
            if state.computation_provider == "databricks":
                return WizardStep.WORKSPACE_URL
            # For AWS EMR (future) or no computation provider needed, go to LLM
            return WizardStep.LLM_PROVIDER_SELECTION
        elif current_step == WizardStep.WORKSPACE_URL:
            return WizardStep.TOKEN_INPUT
        elif current_step == WizardStep.TOKEN_INPUT:
            # After data provider config, go to LLM provider selection
            return WizardStep.LLM_PROVIDER_SELECTION
        elif current_step == WizardStep.LLM_PROVIDER_SELECTION:
            return WizardStep.MODEL_SELECTION
        elif current_step == WizardStep.MODEL_SELECTION:
            return WizardStep.USAGE_CONSENT
        elif current_step == WizardStep.USAGE_CONSENT:
            return WizardStep.COMPLETE
        else:
            return WizardStep.COMPLETE
