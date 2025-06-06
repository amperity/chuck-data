"""
Refactored setup wizard command with clean architecture.
"""

import logging
from typing import Optional, Any

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.commands.base import CommandResult
from chuck_data.interactive_context import InteractiveContext
from chuck_data.command_registry import CommandDefinition
from chuck_data.ui.tui import get_console

from chuck_data.commands.wizard import (
    WizardStep,
    WizardState,
    WizardStateMachine,
    WizardAction,
    WizardRenderer,
    InputValidator,
    create_step,
)


class SetupWizardOrchestrator:
    """Orchestrates the setup wizard flow."""

    def __init__(self):
        self.state_machine = WizardStateMachine()
        self.validator = InputValidator()
        self.renderer = WizardRenderer(get_console())
        self.context = InteractiveContext()

    def start_wizard(self) -> CommandResult:
        """Start the setup wizard from the beginning."""
        # Set up interactive context - use the TUI alias that the service recognizes
        self.context.set_active_context("/setup")

        # Initialize state
        state = WizardState(current_step=WizardStep.AMPERITY_AUTH)

        # Store state in context for persistence across calls
        self._save_state_to_context(state)

        # Handle the first step (Amperity auth)
        return self._process_step(state, "")

    def handle_interactive_input(self, input_text: str) -> CommandResult:
        """Handle interactive input for current step."""
        # Load state from context
        state = self._load_state_from_context()

        if not state:
            # Context lost, restart wizard
            return self.start_wizard()

        return self._process_step(state, input_text, render_step=False)

    def _process_step(
        self, state: WizardState, input_text: str, render_step: bool = True
    ) -> CommandResult:
        """Process a single wizard step."""
        try:
            # Store the current step before processing (this will be the "previous" step after transition)
            previous_step = state.current_step

            # Create step handler
            step = create_step(state.current_step, self.validator)

            # Render the step UI only if requested (e.g., when starting, not when processing input)
            if render_step:
                step_number = self.renderer.get_step_number(state.current_step)
                self.renderer.render_step(step, state, step_number)

            # Handle special case for Amperity auth (no input needed)
            if state.current_step == WizardStep.AMPERITY_AUTH and not input_text:
                result = step.handle_input("", state)
            else:
                # Handle user input
                result = step.handle_input(input_text, state)

            # Apply result to state
            state = self.state_machine.transition(state, result)

            # Handle different actions
            if result.action == WizardAction.EXIT:
                self._clear_context()
                return CommandResult(
                    success=False, message="exit_interactive:" + result.message
                )

            elif result.action == WizardAction.COMPLETE:
                self._clear_context()
                self.renderer.render_completion()
                return CommandResult(success=True, message=result.message)

            elif result.action == WizardAction.RETRY:
                # Stay in same step, save state and ask for input again
                self._save_state_to_context(state)

                # Re-render the step to show the error message to the user
                step_number = self.renderer.get_step_number(state.current_step)
                self.renderer.render_step(step, state, step_number, clear_screen=False)

                return CommandResult(success=False, message=result.message)

            else:  # CONTINUE
                # Save updated state and move to next step
                self._save_state_to_context(state)

                # Check if we're done
                if state.current_step == WizardStep.COMPLETE:
                    self._clear_context()
                    self.renderer.render_completion()
                    return CommandResult(
                        success=True, message="Setup wizard completed successfully!"
                    )

                # If we transitioned to a new step, render it immediately
                if result.next_step and result.next_step != WizardStep.COMPLETE:
                    # Only clear screen if the step was successful AND we're moving forward
                    should_clear = (
                        result.success
                        and self._should_clear_screen_after_step(previous_step)
                        and self._is_forward_progression(
                            previous_step, result.next_step
                        )
                    )

                    # Update state to the new step before rendering
                    state.current_step = result.next_step
                    next_step = create_step(result.next_step, self.validator)
                    next_step_number = self.renderer.get_step_number(result.next_step)
                    self.renderer.render_step(
                        next_step, state, next_step_number, clear_screen=should_clear
                    )
                    # Indicate that we rendered content so TUI knows not to show prompt immediately
                    return CommandResult(
                        success=True,
                        message="",  # Empty message to avoid duplicate display
                        data={"rendered_next_step": True},
                    )

                return CommandResult(success=True, message=result.message)

        except Exception as e:
            logging.error(f"Error in setup wizard: {e}")
            self._clear_context()
            return CommandResult(
                success=False, error=e, message=f"Setup wizard error: {e}"
            )

    def _save_state_to_context(self, state: WizardState):
        """Save wizard state to interactive context."""
        context_data = {
            "current_step": state.current_step.value,
            "workspace_url": state.workspace_url,
            "token": state.token,
            "models": state.models,
            "selected_model": state.selected_model,
            "usage_consent": state.usage_consent,
            "error_message": state.error_message,
        }

        for key, value in context_data.items():
            self.context.store_context_data("/setup", key, value)

    def _load_state_from_context(self) -> Optional[WizardState]:
        """Load wizard state from interactive context."""
        try:
            context_data = self.context.get_context_data("/setup")

            if not context_data:
                return None

            # Convert step value back to enum (handle both string and numeric formats)
            step_value = context_data.get(
                "current_step", WizardStep.AMPERITY_AUTH.value
            )
            try:
                if isinstance(step_value, int):
                    # Handle old numeric format from TUI
                    step_map = {
                        1: WizardStep.AMPERITY_AUTH,
                        2: WizardStep.WORKSPACE_URL,  # TUI used 2 for both URL and token
                        3: WizardStep.MODEL_SELECTION,
                        4: WizardStep.USAGE_CONSENT,
                    }
                    current_step = step_map.get(step_value, WizardStep.AMPERITY_AUTH)
                else:
                    current_step = WizardStep(step_value)
            except (ValueError, TypeError):
                current_step = WizardStep.AMPERITY_AUTH

            return WizardState(
                current_step=current_step,
                workspace_url=context_data.get("workspace_url"),
                token=context_data.get("token"),
                models=context_data.get("models", []),
                selected_model=context_data.get("selected_model"),
                usage_consent=context_data.get("usage_consent"),
                error_message=context_data.get("error_message"),
            )

        except Exception as e:
            logging.error(f"Error loading wizard state from context: {e}")
            return None

    def _should_clear_screen_after_step(self, completed_step: WizardStep) -> bool:
        """Determine if screen should be cleared after successful completion of a step."""
        # Clear screen after successful completion of steps 1, 3, and 4
        return completed_step in [
            WizardStep.AMPERITY_AUTH,  # Step 1
            WizardStep.TOKEN_INPUT,  # Step 3
            WizardStep.MODEL_SELECTION,  # Step 4
        ]

    def _is_forward_progression(
        self, from_step: WizardStep, to_step: WizardStep
    ) -> bool:
        """Check if we're moving forward in the wizard (not going back due to errors)."""
        step_order = [
            WizardStep.AMPERITY_AUTH,
            WizardStep.WORKSPACE_URL,
            WizardStep.TOKEN_INPUT,
            WizardStep.MODEL_SELECTION,
            WizardStep.USAGE_CONSENT,
            WizardStep.COMPLETE,
        ]

        try:
            from_index = step_order.index(from_step)
            to_index = step_order.index(to_step)
            return to_index > from_index
        except ValueError:
            # If steps not found in order, assume it's not forward progression
            return False

    def _clear_context(self):
        """Clear the wizard context."""
        self.context.clear_active_context("/setup")


def handle_command(
    client: Optional[DatabricksAPIClient], interactive_input: str = None, **kwargs: Any
) -> CommandResult:
    """
    Setup wizard command handler using the new architecture.

    Args:
        client: API client instance (can be None)
        interactive_input: Optional user input when in interactive mode

    Returns:
        CommandResult with setup status
    """
    orchestrator = SetupWizardOrchestrator()

    # Check if we're in interactive mode or starting fresh
    context = InteractiveContext()

    if context.is_in_interactive_mode() and context.current_command == "/setup":
        # Handle interactive input
        if interactive_input is None:
            interactive_input = ""
        return orchestrator.handle_interactive_input(interactive_input)
    else:
        # Start new wizard
        return orchestrator.start_wizard()


# Command definition for registration in the command registry

DEFINITION = CommandDefinition(
    name="setup-wizard",
    description="Interactive setup wizard for first-time configuration",
    handler=handle_command,
    parameters={},
    required_params=[],
    tui_aliases=["/setup", "/wizard"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=False,
    usage_hint="Example: /setup to start the interactive setup wizard",
    supports_interactive_input=True,
)
