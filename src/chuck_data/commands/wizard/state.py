"""
Wizard state management for setup wizard.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any


class WizardStep(Enum):
    """Steps in the setup wizard."""

    AMPERITY_AUTH = "amperity_auth"
    WORKSPACE_URL = "workspace_url"
    TOKEN_INPUT = "token_input"
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
    workspace_url: Optional[str] = None
    token: Optional[str] = None
    models: List[Dict[str, Any]] = field(default_factory=list)
    selected_model: Optional[str] = None
    usage_consent: Optional[bool] = None
    error_message: Optional[str] = None

    def is_valid_for_step(self, step: WizardStep) -> bool:
        """Check if current state is valid for the given step."""
        if step == WizardStep.AMPERITY_AUTH:
            return True
        elif step == WizardStep.WORKSPACE_URL:
            return True  # Can always enter workspace URL step
        elif step == WizardStep.TOKEN_INPUT:
            return self.workspace_url is not None
        elif step == WizardStep.MODEL_SELECTION:
            return self.workspace_url is not None and self.token is not None
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
                WizardStep.WORKSPACE_URL,
                WizardStep.AMPERITY_AUTH,
            ],
            WizardStep.WORKSPACE_URL: [
                WizardStep.TOKEN_INPUT,
                WizardStep.WORKSPACE_URL,
            ],
            WizardStep.TOKEN_INPUT: [
                WizardStep.MODEL_SELECTION,
                WizardStep.USAGE_CONSENT,
                WizardStep.TOKEN_INPUT,
                WizardStep.WORKSPACE_URL,
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
            for key, value in result.data.items():
                if hasattr(state, key):
                    setattr(state, key, value)

        # Transition to next step if specified and valid
        if result.next_step and self.can_transition(
            state.current_step, result.next_step
        ):
            if state.is_valid_for_step(result.next_step):
                state.current_step = result.next_step
            else:
                # Invalid state for next step, set error
                state.error_message = f"Invalid state for step {result.next_step.value}"

        return state

    def get_next_step(self, current_step: WizardStep, state: WizardState) -> WizardStep:
        """Determine the natural next step based on current step and state."""
        if current_step == WizardStep.AMPERITY_AUTH:
            return WizardStep.WORKSPACE_URL
        elif current_step == WizardStep.WORKSPACE_URL:
            return WizardStep.TOKEN_INPUT
        elif current_step == WizardStep.TOKEN_INPUT:
            # Skip to usage consent if no models available
            return (
                WizardStep.MODEL_SELECTION if state.models else WizardStep.USAGE_CONSENT
            )
        elif current_step == WizardStep.MODEL_SELECTION:
            return WizardStep.USAGE_CONSENT
        elif current_step == WizardStep.USAGE_CONSENT:
            return WizardStep.COMPLETE
        else:
            return WizardStep.COMPLETE
