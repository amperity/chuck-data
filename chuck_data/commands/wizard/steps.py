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
                message="Amperity token already exists. Proceeding to data provider selection.",
                next_step=WizardStep.DATA_PROVIDER_SELECTION,
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
                    message="Amperity authentication complete. Proceeding to data provider selection.",
                    next_step=WizardStep.DATA_PROVIDER_SELECTION,
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


class DataProviderSelectionStep(SetupStep):
    """Handle data provider selection."""

    def get_step_title(self) -> str:
        return "Data Provider Selection"

    def get_prompt_message(self, state: WizardState) -> str:
        return "Please select your data provider:\n  1. Databricks (Unity Catalog)\nEnter the number or name of the provider:"

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle data provider selection input."""
        # Normalize input
        input_normalized = input_text.strip().lower()

        # Map inputs to provider names
        if input_normalized in ["1", "databricks"]:
            provider = "databricks"
            next_step = WizardStep.WORKSPACE_URL
            message = "Databricks selected. Please enter your workspace URL."
        else:
            return StepResult(
                success=False,
                message="Invalid selection. Please enter 1 (Databricks).",
                action=WizardAction.RETRY,
            )

        # Save provider to config
        try:
            from chuck_data.config import get_config_manager

            success = get_config_manager().update(data_provider=provider)
            if not success:
                return StepResult(
                    success=False,
                    message="Failed to save data provider selection. Please try again.",
                    action=WizardAction.RETRY,
                )

            return StepResult(
                success=True,
                message=message,
                next_step=next_step,
                action=WizardAction.CONTINUE,
                data={"data_provider": provider},
            )

        except Exception as e:
            logging.error(f"Error saving data provider selection: {e}")
            return StepResult(
                success=False,
                message=f"Error saving data provider selection: {str(e)}",
                action=WizardAction.RETRY,
            )


class LLMProviderSelectionStep(SetupStep):
    """Handle LLM provider selection."""

    def get_step_title(self) -> str:
        return "LLM Provider Selection"

    def get_prompt_message(self, state: WizardState) -> str:
        return "Please select your LLM provider:\n  1. Databricks (default)\n  2. AWS Bedrock\nEnter the number or name of the provider:"

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle LLM provider selection input."""
        # Normalize input
        input_normalized = input_text.strip().lower()

        models = []

        # Map inputs to provider names and fetch models
        if input_normalized in ["1", "databricks"]:
            provider = "databricks"
            message = "Databricks selected for LLM. Fetching available models..."

            # Fetch Databricks models
            try:
                from chuck_data.llm.providers.databricks import DatabricksProvider

                service = get_chuck_service()
                if not service or not service.client:
                    return StepResult(
                        success=False,
                        message="Databricks connection not available. Please restart setup.",
                        next_step=WizardStep.DATA_PROVIDER_SELECTION,
                        action=WizardAction.CONTINUE,
                    )

                databricks_provider = DatabricksProvider(
                    workspace_url=state.workspace_url,
                    token=state.token,
                    client=service.client,
                )
                models = databricks_provider.list_models()
                logging.info(f"Found {len(models)} Databricks models")

                if not models:
                    return StepResult(
                        success=False,
                        message="No Databricks models found. Please check your workspace configuration.",
                        action=WizardAction.RETRY,
                    )

                message = "Databricks selected for LLM. Proceeding to model selection."
            except Exception as e:
                logging.error(f"Error listing Databricks models: {e}", exc_info=True)
                return StepResult(
                    success=False,
                    message=f"Error listing Databricks models: {str(e)}",
                    action=WizardAction.RETRY,
                )

        elif input_normalized in ["2", "aws_bedrock", "aws", "bedrock"]:
            provider = "aws_bedrock"
            message = "AWS Bedrock selected for LLM. Fetching available models..."

            # Fetch AWS Bedrock models
            try:
                from chuck_data.llm.providers.aws_bedrock import AWSBedrockProvider
                import os

                # Show AWS configuration being used
                aws_profile = os.getenv("AWS_PROFILE", "not set")
                aws_region = os.getenv(
                    "AWS_REGION", "not set (defaulting to us-east-1)"
                )
                logging.info(f"AWS_PROFILE: {aws_profile}, AWS_REGION: {aws_region}")

                bedrock_provider = AWSBedrockProvider()
                models = bedrock_provider.list_models()
                logging.info(f"Found {len(models)} Bedrock models")

                if not models:
                    error_msg = (
                        "No Bedrock models found. Possible causes:\n"
                        f"  1. AWS credentials not configured (AWS_PROFILE={aws_profile}, AWS_REGION={aws_region})\n"
                        "  2. Need to request model access in AWS Bedrock console\n"
                        "  3. Using wrong AWS region\n\n"
                        "To fix:\n"
                        "  - Configure AWS SSO: aws sso login --profile your-profile\n"
                        "  - Set environment variables: export AWS_PROFILE=your-profile AWS_REGION=us-east-1\n"
                        "  - Enable Bedrock models at: https://console.aws.amazon.com/bedrock"
                    )
                    return StepResult(
                        success=False,
                        message=error_msg,
                        action=WizardAction.RETRY,
                    )

                message = "AWS Bedrock selected for LLM. Proceeding to model selection."
            except Exception as e:
                logging.error(f"Error listing Bedrock models: {e}", exc_info=True)

                # Check for common AWS errors
                error_msg = str(e)
                if (
                    "UnrecognizedClientException" in error_msg
                    or "InvalidSignatureException" in error_msg
                ):
                    helpful_msg = (
                        f"AWS credentials error: {error_msg}\n\n"
                        "This usually means expired credentials. To fix:\n"
                        "  1. Run: aws sso login --profile your-profile\n"
                        "  2. Set: export AWS_PROFILE=your-profile AWS_REGION=us-east-1\n"
                        "  3. Restart Chuck"
                    )
                elif "AccessDeniedException" in error_msg:
                    helpful_msg = (
                        f"AWS access denied: {error_msg}\n\n"
                        "You need to request access to Bedrock models:\n"
                        "  1. Go to: https://console.aws.amazon.com/bedrock\n"
                        "  2. Navigate to 'Model access' in left sidebar\n"
                        "  3. Request access for Claude, Llama, and Nova models"
                    )
                else:
                    helpful_msg = f"Error listing Bedrock models: {error_msg}"

                return StepResult(
                    success=False,
                    message=helpful_msg,
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
                    message="Failed to save LLM provider selection. Please try again.",
                    action=WizardAction.RETRY,
                )

            return StepResult(
                success=True,
                message=message,
                next_step=WizardStep.MODEL_SELECTION,
                action=WizardAction.CONTINUE,
                data={"llm_provider": provider, "models": models},
            )

        except Exception as e:
            logging.error(f"Error saving LLM provider selection: {e}")
            return StepResult(
                success=False,
                message=f"Error saving LLM provider selection: {str(e)}",
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

            # Data provider configuration complete, proceed to LLM provider selection
            return StepResult(
                success=True,
                message="Databricks data provider configured. Select your LLM provider.",
                next_step=WizardStep.LLM_PROVIDER_SELECTION,
                action=WizardAction.CONTINUE,
                data={"token": validation.processed_value},
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
        # Fetch models if not already loaded based on LLM provider
        if not state.models and state.llm_provider:
            try:
                if state.llm_provider == "databricks":
                    from chuck_data.llm.providers.databricks import DatabricksProvider

                    # Check if we have the required credentials
                    if not state.workspace_url or not state.token:
                        logging.error(
                            f"Missing Databricks credentials - workspace_url: {state.workspace_url}, token: {'present' if state.token else 'missing'}"
                        )
                        return StepResult(
                            success=False,
                            message="Missing Databricks credentials. Please restart the wizard.",
                            next_step=WizardStep.DATA_PROVIDER_SELECTION,
                            action=WizardAction.CONTINUE,
                        )

                    service = get_chuck_service()
                    if not service or not service.client:
                        logging.error(
                            f"Service not initialized - service: {service}, client: {service.client if service else 'N/A'}"
                        )
                        return StepResult(
                            success=False,
                            message="Data provider connection not available. Please restart the wizard.",
                            next_step=WizardStep.DATA_PROVIDER_SELECTION,
                            action=WizardAction.CONTINUE,
                        )

                    provider = DatabricksProvider(
                        workspace_url=state.workspace_url,
                        token=state.token,
                        client=service.client,
                    )
                    state.models = provider.list_models()
                    logging.info(f"Found {len(state.models)} Databricks models")

                elif state.llm_provider == "aws_bedrock":
                    from chuck_data.llm.providers.aws_bedrock import AWSBedrockProvider

                    provider = AWSBedrockProvider()
                    state.models = provider.list_models()
                    logging.info(f"Found {len(state.models)} Bedrock models")

            except Exception as e:
                logging.error(
                    f"Error fetching models from {state.llm_provider}: {e}",
                    exc_info=True,
                )
                return StepResult(
                    success=False,
                    message=f"Error fetching models: {str(e)}. Please check your credentials.",
                    next_step=WizardStep.LLM_PROVIDER_SELECTION,
                    action=WizardAction.CONTINUE,
                )

        if not state.models:
            # No models available even after fetching
            return StepResult(
                success=False,
                message="No models available. Please select a different LLM provider.",
                next_step=WizardStep.LLM_PROVIDER_SELECTION,
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
        WizardStep.DATA_PROVIDER_SELECTION: DataProviderSelectionStep,
        WizardStep.WORKSPACE_URL: WorkspaceUrlStep,
        WizardStep.TOKEN_INPUT: TokenInputStep,
        WizardStep.LLM_PROVIDER_SELECTION: LLMProviderSelectionStep,
        WizardStep.MODEL_SELECTION: ModelSelectionStep,
        WizardStep.USAGE_CONSENT: UsageConsentStep,
    }

    step_class = step_map.get(step_type)
    if not step_class:
        raise ValueError(f"Unknown step type: {step_type}")

    return step_class(validator)
