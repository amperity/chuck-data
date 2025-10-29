"""
Step handlers for setup wizard.
"""

from abc import ABC, abstractmethod
import logging

from .state import WizardState, StepResult, WizardStep, WizardAction
from .validator import InputValidator

from chuck_data.clients.amperity import AmperityAPIClient
from chuck_data.config import (
    get_amperity_token,
    set_workspace_url,
    set_databricks_token,
    set_active_model,
    set_usage_tracking_consent,
    set_llm_provider,
)
from chuck_data.ui.tui import get_chuck_service


class SetupStep(ABC):
    """Base class for setup wizard steps."""

    def __init__(self, validator: InputValidator):
        self.validator = validator

    @abstractmethod
    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle user input for this step."""
        pass

    @abstractmethod
    def get_prompt_message(self, state: WizardState) -> str:
        """Get the prompt message for this step."""
        pass

    @abstractmethod
    def get_step_title(self) -> str:
        """Get the title for this step."""
        pass

    def should_hide_input(self, state: WizardState) -> bool:
        """Whether input should be hidden (for passwords/tokens)."""
        return False


class AmperityAuthStep(SetupStep):
    """Handle Amperity authentication."""

    def get_step_title(self) -> str:
        return "Amperity Authentication"

    def get_prompt_message(self, state: WizardState) -> str:
        return "Starting Amperity authentication..."

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle Amperity authentication - this step doesn't take input."""
        # Check if we already have a valid token
        existing_token = get_amperity_token()

        if existing_token:
            return StepResult(
                success=True,
                message="Amperity token already exists. Proceeding to provider selection.",
                next_step=WizardStep.PROVIDER_SELECTION,
                action=WizardAction.CONTINUE,
            )

        # Initialize the auth manager and start the flow
        try:
            auth_manager = AmperityAPIClient()
            success, message = auth_manager.start_auth()

            if not success:
                return StepResult(
                    success=False,
                    message=f"Error starting Amperity authentication: {message}",
                    action=WizardAction.RETRY,
                )

            # Block until authentication completes
            auth_success, auth_message = auth_manager.wait_for_auth_completion(
                poll_interval=1
            )

            if auth_success:
                return StepResult(
                    success=True,
                    message="Amperity authentication complete. Proceeding to provider selection.",
                    next_step=WizardStep.PROVIDER_SELECTION,
                    action=WizardAction.CONTINUE,
                )
            else:
                # Check if cancelled
                if "cancelled" in auth_message.lower():
                    return StepResult(
                        success=False,
                        message="Setup cancelled. Run /setup again when ready.",
                        action=WizardAction.EXIT,
                    )

                # Clean up error message
                clean_message = auth_message
                if auth_message.lower().startswith("authentication failed:"):
                    clean_message = auth_message.split(":", 1)[1].strip()

                return StepResult(
                    success=False,
                    message=f"Authentication failed: {clean_message}",
                    action=WizardAction.RETRY,
                )

        except Exception as e:
            logging.error(f"Error in Amperity authentication: {e}")
            return StepResult(
                success=False,
                message=f"Authentication error: {str(e)}",
                action=WizardAction.RETRY,
            )


class ProviderSelectionStep(SetupStep):
    """Handle LLM provider selection."""

    def get_step_title(self) -> str:
        return "LLM Provider Selection"

    def get_prompt_message(self, state: WizardState) -> str:
        return "Please select your LLM provider:\n  1. Databricks (default)\n  2. AWS Bedrock\nEnter the number or name of the provider:"

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle provider selection input."""
        # Normalize input
        input_normalized = input_text.strip().lower()

        # Map inputs to provider names
        if input_normalized in ["1", "databricks"]:
            provider = "databricks"
            next_step = WizardStep.WORKSPACE_URL
            message = "Databricks selected. Please enter your workspace URL."
            models = []
        elif input_normalized in ["2", "aws_bedrock", "aws", "bedrock"]:
            provider = "aws_bedrock"
            next_step = WizardStep.MODEL_SELECTION
            message = "AWS Bedrock selected. Fetching available models..."

            # Fetch AWS Bedrock models immediately
            try:
                from chuck_data.llm.providers.aws_bedrock import AWSBedrockProvider

                bedrock_provider = AWSBedrockProvider()
                models = bedrock_provider.list_models()
                logging.debug(f"Found {len(models)} Bedrock models")

                if not models:
                    return StepResult(
                        success=False,
                        message="No Bedrock models found. Please check your AWS credentials and try again.",
                        action=WizardAction.RETRY,
                    )

                message = "AWS Bedrock selected. Proceeding to model selection."
            except Exception as e:
                logging.error(f"Error listing Bedrock models: {e}")
                return StepResult(
                    success=False,
                    message=f"Error listing Bedrock models: {str(e)}. Please check your AWS credentials.",
                    action=WizardAction.RETRY,
                )
        else:
            return StepResult(
                success=False,
                message="Invalid selection. Please enter 1 (Databricks) or 2 (AWS Bedrock).",
                action=WizardAction.RETRY,
            )

        # Save provider to config
        try:
            success = set_llm_provider(provider)
            if not success:
                return StepResult(
                    success=False,
                    message="Failed to save provider selection. Please try again.",
                    action=WizardAction.RETRY,
                )

            return StepResult(
                success=True,
                message=message,
                next_step=next_step,
                action=WizardAction.CONTINUE,
                data={"llm_provider": provider, "models": models},
            )

        except Exception as e:
            logging.error(f"Error saving provider selection: {e}")
            return StepResult(
                success=False,
                message=f"Error saving provider selection: {str(e)}",
                action=WizardAction.RETRY,
            )


class WorkspaceUrlStep(SetupStep):
    """Handle workspace URL input."""

    def get_step_title(self) -> str:
        return "Databricks Workspace"

    def get_prompt_message(self, state: WizardState) -> str:
        return "Please enter your Databricks workspace URL (e.g., https://my-workspace.cloud.databricks.com)"

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle workspace URL input."""
        # Validate the input
        validation = self.validator.validate_workspace_url(input_text)

        if not validation.is_valid:
            return StepResult(
                success=False, message=validation.message, action=WizardAction.RETRY
            )

        # Store the validated URL
        return StepResult(
            success=True,
            message="Workspace URL validated. Now enter your Databricks token.",
            next_step=WizardStep.TOKEN_INPUT,
            action=WizardAction.CONTINUE,
            data={"workspace_url": validation.processed_value},
        )


class TokenInputStep(SetupStep):
    """Handle Databricks token input."""

    def get_step_title(self) -> str:
        return "Databricks Token"

    def get_prompt_message(self, state: WizardState) -> str:
        return "Please enter your Databricks API token:"

    def should_hide_input(self, state: WizardState) -> bool:
        return True  # Hide token input

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle token input."""
        if not state.workspace_url:
            return StepResult(
                success=False,
                message="Workspace URL not set. Please restart the wizard.",
                action=WizardAction.EXIT,
            )

        # Validate the token
        validation = self.validator.validate_token(input_text, state.workspace_url)

        if not validation.is_valid:
            return StepResult(
                success=False,
                message=f"{validation.message}. Please re-enter your workspace URL and token.",
                next_step=WizardStep.WORKSPACE_URL,
                action=WizardAction.CONTINUE,
            )

        try:
            # Save workspace URL and token
            url_success = set_workspace_url(state.workspace_url)
            if not url_success:
                return StepResult(
                    success=False,
                    message="Failed to save workspace URL. Please try again.",
                    action=WizardAction.RETRY,
                )

            token_success = set_databricks_token(validation.processed_value)
            if not token_success:
                return StepResult(
                    success=False,
                    message="Failed to save Databricks token. Please try again.",
                    action=WizardAction.RETRY,
                )

            # Reinitialize the service client
            service = get_chuck_service()
            if service:
                init_success = service.reinitialize_client()
                if not init_success:
                    logging.warning(
                        "Failed to reinitialize client, but credentials were saved"
                    )
                    return StepResult(
                        success=True,
                        message="Credentials saved but client reinitialization failed.",
                        next_step=WizardStep.USAGE_CONSENT,
                        action=WizardAction.CONTINUE,
                        data={"token": validation.processed_value, "models": []},
                    )

                # Try to list models using provider interface
                try:
                    from chuck_data.llm.providers.databricks import DatabricksProvider

                    # Create provider with the validated credentials
                    provider = DatabricksProvider(
                        workspace_url=state.workspace_url,
                        token=validation.processed_value,
                        client=service.client,
                    )
                    models = provider.list_models()
                    logging.debug(f"Found {len(models)} models")

                    if models:
                        return StepResult(
                            success=True,
                            message="Databricks configured. Select a model.",
                            next_step=WizardStep.MODEL_SELECTION,
                            action=WizardAction.CONTINUE,
                            data={
                                "token": validation.processed_value,
                                "models": models,
                            },
                        )
                    else:
                        return StepResult(
                            success=True,
                            message="No models found. Proceeding to usage consent.",
                            next_step=WizardStep.USAGE_CONSENT,
                            action=WizardAction.CONTINUE,
                            data={"token": validation.processed_value, "models": []},
                        )

                except Exception as e:
                    logging.error(f"Error listing models: {e}")
                    return StepResult(
                        success=True,
                        message="Error listing models. Proceeding to usage consent.",
                        next_step=WizardStep.USAGE_CONSENT,
                        action=WizardAction.CONTINUE,
                        data={"token": validation.processed_value, "models": []},
                    )
            else:
                return StepResult(
                    success=True,
                    message="Databricks configured. Proceeding to usage consent.",
                    next_step=WizardStep.USAGE_CONSENT,
                    action=WizardAction.CONTINUE,
                    data={"token": validation.processed_value, "models": []},
                )

        except Exception as e:
            logging.error(f"Error saving Databricks configuration: {e}")
            return StepResult(
                success=False,
                message=f"Error saving configuration: {str(e)}",
                action=WizardAction.RETRY,
            )


class ModelSelectionStep(SetupStep):
    """Handle model selection."""

    def get_step_title(self) -> str:
        return "LLM Model Selection"

    def get_prompt_message(self, state: WizardState) -> str:
        return "Please enter the number or name of the model you want to use:"

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle model selection input."""
        if not state.models:
            # No models available - need to go back
            if state.llm_provider == "databricks":
                return StepResult(
                    success=False,
                    message="No models available. Restarting wizard at workspace setup step.",
                    next_step=WizardStep.WORKSPACE_URL,
                    action=WizardAction.CONTINUE,
                )
            else:
                return StepResult(
                    success=False,
                    message="No models available. Please select a different provider.",
                    next_step=WizardStep.PROVIDER_SELECTION,
                    action=WizardAction.CONTINUE,
                )

        # Sort models the same way as display (default first)
        from chuck_data.constants import DEFAULT_MODELS

        default_models = DEFAULT_MODELS

        sorted_models = []

        # Add default models first
        for default_model in default_models:
            for model in state.models:
                if model["model_id"] == default_model:
                    sorted_models.append(model)
                    break

        # Add remaining models
        for model in state.models:
            if model["model_id"] not in default_models:
                sorted_models.append(model)

        # Validate the selection
        validation = self.validator.validate_model_selection(input_text, sorted_models)

        if not validation.is_valid:
            return StepResult(
                success=False, message=validation.message, action=WizardAction.RETRY
            )

        # Save the selected model
        try:
            success = set_active_model(validation.processed_value)

            if success:
                return StepResult(
                    success=True,
                    message=f"Model '{validation.processed_value}' selected. Proceeding to usage consent.",
                    next_step=WizardStep.USAGE_CONSENT,
                    action=WizardAction.CONTINUE,
                    data={"selected_model": validation.processed_value},
                )
            else:
                return StepResult(
                    success=False,
                    message="Failed to save model selection. Please try again.",
                    action=WizardAction.RETRY,
                )

        except Exception as e:
            logging.error(f"Error saving model selection: {e}")
            return StepResult(
                success=False,
                message=f"Error saving model selection: {str(e)}",
                action=WizardAction.RETRY,
            )


class UsageConsentStep(SetupStep):
    """Handle usage tracking consent."""

    def get_step_title(self) -> str:
        return "Usage Tracking Consent"

    def get_prompt_message(self, state: WizardState) -> str:
        return (
            "Do you consent to sharing your usage information with Amperity (yes/no)?"
        )

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle usage consent input."""
        # Validate the input
        validation = self.validator.validate_usage_consent(input_text)

        if not validation.is_valid:
            return StepResult(
                success=False, message=validation.message, action=WizardAction.RETRY
            )

        # Save the consent
        try:
            consent = validation.processed_value == "yes"
            success = set_usage_tracking_consent(consent)

            if success:
                if consent:
                    message = "Thank you for helping us make Chuck better! Setup wizard completed successfully!"
                else:
                    message = "We understand, Chuck will not share your usage with Amperity. Setup wizard completed successfully!"

                return StepResult(
                    success=True,
                    message=message,
                    next_step=WizardStep.COMPLETE,
                    action=WizardAction.COMPLETE,
                    data={"usage_consent": consent},
                )
            else:
                return StepResult(
                    success=False,
                    message="Failed to save usage tracking preference. Please try again.",
                    action=WizardAction.RETRY,
                )

        except Exception as e:
            logging.error(f"Error saving usage consent: {e}")
            return StepResult(
                success=False,
                message=f"Error saving usage consent: {str(e)}",
                action=WizardAction.RETRY,
            )


# Step factory
def create_step(step_type: WizardStep, validator: InputValidator) -> SetupStep:
    """Factory function to create step handlers."""
    step_map = {
        WizardStep.AMPERITY_AUTH: AmperityAuthStep,
        WizardStep.PROVIDER_SELECTION: ProviderSelectionStep,
        WizardStep.WORKSPACE_URL: WorkspaceUrlStep,
        WizardStep.TOKEN_INPUT: TokenInputStep,
        WizardStep.MODEL_SELECTION: ModelSelectionStep,
        WizardStep.USAGE_CONSENT: UsageConsentStep,
    }

    step_class = step_map.get(step_type)
    if not step_class:
        raise ValueError(f"Unknown step type: {step_type}")

    return step_class(validator)
