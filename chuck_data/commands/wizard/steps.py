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

# Valid data provider + compute provider combinations
# These match the combinations allowed by chuck-api backend
VALID_PROVIDER_COMBINATIONS = {
    "databricks": ["databricks"],  # Databricks data → Databricks compute only
    "aws_redshift": [
        "databricks",
        "aws_emr",
    ],  # Redshift data → Databricks or EMR compute
}


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
        return "Please select your data provider:\n  1. Databricks (Unity Catalog)\n  2. AWS Redshift\nEnter the number or name of the provider:"

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle data provider selection input."""
        # Normalize input
        input_normalized = input_text.strip().lower()

        # Map inputs to provider names
        if input_normalized in ["1", "databricks"]:
            provider = "databricks"
            next_step = WizardStep.WORKSPACE_URL
            message = "Databricks selected. Please enter your Databricks workspace URL."
        elif input_normalized in ["2", "aws_redshift", "aws redshift", "redshift"]:
            provider = "aws_redshift"
            next_step = WizardStep.AWS_PROFILE_INPUT
            message = "AWS Redshift selected. Please configure AWS settings."
        else:
            return StepResult(
                success=False,
                message="Invalid selection. Please enter 1 (Databricks) or 2 (AWS Redshift).",
                action=WizardAction.RETRY,
            )

        # Save provider to config
        try:
            from chuck_data.config import set_data_provider

            success = set_data_provider(provider)
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


class ComputeProviderSelectionStep(SetupStep):
    """Handle compute provider selection."""

    def get_step_title(self) -> str:
        return "Compute Provider Selection"

    def get_prompt_message(self, state: WizardState) -> str:
        # Get valid compute providers for the selected data provider
        valid_providers = VALID_PROVIDER_COMBINATIONS.get(
            state.data_provider, ["databricks", "aws_emr"]
        )

        # Build dynamic prompt based on valid combinations
        if valid_providers == ["databricks"]:
            # Only Databricks is valid
            return (
                "Please select your compute provider:\n"
                "  1. Databricks\n"
                "Enter the number or name of the provider:"
            )
        elif valid_providers == ["aws_emr"]:
            # Only EMR is valid (shouldn't happen with current combinations, but handle it)
            return (
                "Please select your compute provider:\n"
                "  1. AWS EMR\n"
                "Enter the number or name of the provider:"
            )
        else:
            # Multiple options available
            return (
                "Please select your compute provider:\n"
                "  1. Databricks (default)\n"
                "  2. AWS EMR\n"
                "Enter the number or name of the provider:"
            )

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle compute provider selection input."""
        # Get valid compute providers for the selected data provider
        valid_providers = VALID_PROVIDER_COMBINATIONS.get(
            state.data_provider, ["databricks", "aws_emr"]
        )

        # Normalize input
        input_normalized = input_text.strip().lower()

        # Map inputs to provider names
        if input_normalized in ["1", "databricks", ""]:
            # Check if only one option is available and map accordingly
            if valid_providers == ["databricks"]:
                provider = "databricks"
            elif valid_providers == ["aws_emr"]:
                provider = "aws_emr"
            else:
                # Default to Databricks when "1" is entered with multiple options
                provider = "databricks"
        elif input_normalized in ["2", "emr", "aws_emr", "aws emr"]:
            provider = "aws_emr"
        else:
            return StepResult(
                success=False,
                message="Invalid selection. Please enter 1 (Databricks) or 2 (AWS EMR).",
                action=WizardAction.RETRY,
            )

        # Validate the selected provider is allowed for this data provider
        if provider not in valid_providers:
            valid_names = " or ".join(valid_providers)
            return StepResult(
                success=False,
                message=f"Invalid combination: {state.data_provider} data provider does not support {provider} compute provider. Valid options: {valid_names}",
                action=WizardAction.RETRY,
            )

        # Determine next step based on provider and data provider
        if provider == "databricks":
            # If data provider is AWS Redshift, need to collect Instance Profile ARN first
            if state.data_provider == "aws_redshift":
                next_step = WizardStep.INSTANCE_PROFILE_INPUT
                message = "Databricks selected for computation. Please enter the Instance Profile ARN for Databricks to access AWS services."
            elif state.data_provider == "databricks":
                # Databricks data + Databricks compute: creds already collected
                next_step = WizardStep.LLM_PROVIDER_SELECTION
                message = (
                    "Databricks selected for computation. Select your LLM provider."
                )
            else:
                # Other cases: need to collect Databricks creds
                next_step = WizardStep.WORKSPACE_URL
                message = "Databricks selected for computation. Please enter your workspace URL."
        elif provider == "aws_emr":
            # For EMR, we need AWS credentials regardless of data provider
            # If data provider is Databricks (not Redshift), we haven't collected AWS config yet
            if state.data_provider != "aws_redshift":
                # Need to collect AWS profile and region before EMR cluster ID
                next_step = WizardStep.AWS_PROFILE_INPUT
                message = (
                    "AWS EMR selected for computation. Please configure AWS settings."
                )
            else:
                # AWS config already collected during Redshift setup
                next_step = WizardStep.EMR_CLUSTER_ID_INPUT
                message = "AWS EMR selected for computation. Please enter your EMR cluster ID."
        else:
            return StepResult(
                success=False,
                message="Invalid compute provider selection.",
                action=WizardAction.RETRY,
            )

        # Save provider to config
        try:
            from chuck_data.config import get_config_manager

            success = get_config_manager().update(compute_provider=provider)
            if not success:
                return StepResult(
                    success=False,
                    message="Failed to save compute provider selection. Please try again.",
                    action=WizardAction.RETRY,
                )

            return StepResult(
                success=True,
                message=message,
                next_step=next_step,
                action=WizardAction.CONTINUE,
                data={"compute_provider": provider},
            )

        except Exception as e:
            logging.error(f"Error saving compute provider selection: {e}")
            return StepResult(
                success=False,
                message=f"Error saving compute provider selection: {str(e)}",
                action=WizardAction.RETRY,
            )


class EMRClusterIDInputStep(SetupStep):
    """Handle EMR cluster ID input."""

    def get_step_title(self) -> str:
        return "EMR Cluster ID"

    def get_prompt_message(self, state: WizardState) -> str:
        return (
            "Please enter your AWS EMR cluster ID.\n"
            "The cluster ID should be in the format: j-XXXXXXXXXXXXX\n"
            "EMR Cluster ID:"
        )

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle EMR cluster ID input."""
        cluster_id = input_text.strip()

        # Validate cluster ID format
        if not cluster_id:
            return StepResult(
                success=False,
                message="EMR cluster ID cannot be empty. Please enter a valid cluster ID.",
                action=WizardAction.RETRY,
            )

        # Basic format validation (EMR cluster IDs start with 'j-')
        if not cluster_id.startswith("j-"):
            return StepResult(
                success=False,
                message="Invalid EMR cluster ID format. Cluster IDs should start with 'j-'.",
                action=WizardAction.RETRY,
            )

        # Validate connection to EMR cluster
        try:
            from chuck_data.clients.emr import EMRAPIClient

            # Get AWS region from state (set during Redshift setup)
            aws_region = state.aws_region
            if not aws_region:
                return StepResult(
                    success=False,
                    message="AWS region not found. Please complete AWS configuration first.",
                    action=WizardAction.RETRY,
                )

            # Create EMR client and validate cluster
            emr_client = EMRAPIClient(
                region=aws_region,
                cluster_id=cluster_id,
                aws_profile=state.aws_profile,
            )

            # Validate connection
            if not emr_client.validate_connection():
                return StepResult(
                    success=False,
                    message=f"Failed to connect to EMR cluster {cluster_id}. Please check the cluster ID and try again.",
                    action=WizardAction.RETRY,
                )

            # Get cluster status to provide feedback
            cluster_status = emr_client.get_cluster_status()
            logging.info(f"EMR cluster {cluster_id} is in state: {cluster_status}")

            # Save to config
            from chuck_data.config import get_config_manager

            success = get_config_manager().update(emr_cluster_id=cluster_id)
            if not success:
                return StepResult(
                    success=False,
                    message="Failed to save EMR cluster ID. Please try again.",
                    action=WizardAction.RETRY,
                )

            return StepResult(
                success=True,
                message=f"EMR cluster {cluster_id} validated successfully (status: {cluster_status}). Proceeding to LLM provider selection.",
                next_step=WizardStep.LLM_PROVIDER_SELECTION,
                action=WizardAction.CONTINUE,
                data={"emr_cluster_id": cluster_id},
            )

        except Exception as e:
            logging.error(f"Error validating EMR cluster: {e}")
            return StepResult(
                success=False,
                message=f"Error validating EMR cluster: {str(e)}",
                action=WizardAction.RETRY,
            )


class LLMProviderSelectionStep(SetupStep):
    """Handle LLM provider selection."""

    def get_step_title(self) -> str:
        return "LLM Provider Selection"

    def get_prompt_message(self, state: WizardState) -> str:
        # Check which providers are available
        has_databricks_creds = bool(state.workspace_url and state.token)
        has_aws_config = bool(state.aws_profile and state.aws_region)

        # Build dynamic prompt based on available providers
        if has_databricks_creds and has_aws_config:
            return "Please select your LLM provider:\n  1. Databricks (default)\n  2. AWS Bedrock\nEnter the number or name of the provider:"
        elif has_databricks_creds:
            return "Please select your LLM provider:\n  1. Databricks"
        elif has_aws_config:
            return "Please select your LLM provider:\n  1. AWS Bedrock"
        else:
            return "Please select your LLM provider:\n  1. Databricks (default)\n  2. AWS Bedrock\nEnter the number or name of the provider:"

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle LLM provider selection input."""
        # Check which providers are available
        has_databricks_creds = bool(state.workspace_url and state.token)
        has_aws_config = bool(state.aws_profile and state.aws_region)

        # Auto-select if only one provider is available
        if has_databricks_creds and not has_aws_config:
            # Only Databricks available - auto-select it
            if not input_text or input_text.strip() in ["1", "databricks"]:
                input_normalized = "1"
            else:
                return StepResult(
                    success=False,
                    message="Only Databricks is available with your current configuration. Please enter 1.",
                    action=WizardAction.RETRY,
                )
        elif has_aws_config and not has_databricks_creds:
            # Only AWS available - auto-select it
            if not input_text or input_text.strip() in [
                "1",
                "aws_bedrock",
                "aws",
                "bedrock",
            ]:
                input_normalized = "2"  # Map to AWS since it's the only option
            else:
                return StepResult(
                    success=False,
                    message="Only AWS Bedrock is available with your current configuration. Please enter 1.",
                    action=WizardAction.RETRY,
                )
        else:
            # Both available or neither - use normal input
            input_normalized = input_text.strip().lower()

        models = []

        # Map inputs to provider names and fetch models
        if input_normalized in ["1", "databricks"]:
            provider = "databricks"
            message = "Databricks selected for LLM. Fetching available models..."

            # Fetch Databricks models
            try:
                from chuck_data.llm.providers.databricks import DatabricksProvider
                from chuck_data.clients.databricks import DatabricksAPIClient

                # Only use the service client if it's a DatabricksAPIClient
                # If the data provider is Redshift, we need to create a Databricks client for LLM
                service = get_chuck_service()
                databricks_client = None

                if (
                    service
                    and service.client
                    and isinstance(service.client, DatabricksAPIClient)
                ):
                    databricks_client = service.client

                # For Databricks LLM provider, we need workspace_url and token
                if not state.workspace_url or not state.token:
                    return StepResult(
                        success=False,
                        message="Databricks credentials not available. AWS Bedrock is the only supported LLM provider for your configuration. Please select option 2.",
                        action=WizardAction.RETRY,
                    )

                databricks_provider = DatabricksProvider(
                    workspace_url=state.workspace_url,
                    token=state.token,
                    client=databricks_client,
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

            # Determine next step based on data provider
            if state.data_provider == "databricks":
                # For Databricks data provider, go to compute provider selection
                next_step = WizardStep.COMPUTE_PROVIDER_SELECTION
                message = (
                    "Databricks credentials configured. Select your compute provider."
                )
            else:
                # For other cases (e.g., Redshift data + Databricks compute),
                # go to LLM provider selection
                next_step = WizardStep.LLM_PROVIDER_SELECTION
                message = "Databricks credentials configured. Select your LLM provider."

            return StepResult(
                success=True,
                message=message,
                next_step=next_step,
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


class AWSProfileInputStep(SetupStep):
    """Handle AWS profile input for AWS services (Redshift or EMR)."""

    def get_step_title(self) -> str:
        return "AWS Profile Configuration"

    def get_prompt_message(self, state: WizardState) -> str:
        import os

        current_profile = os.getenv("AWS_PROFILE", "not set")
        has_env_credentials = bool(
            os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY")
        )

        # Determine which service we're configuring for
        if state.data_provider == "aws_redshift":
            service_name = "Redshift"
        elif state.compute_provider == "aws_emr":
            service_name = "EMR"
        else:
            service_name = "AWS services"

        message = f"Current AWS_PROFILE: {current_profile}\n"
        if has_env_credentials:
            message += (
                "Detected AWS credentials in environment variables "
                "(AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)\n\n"
            )
        message += (
            f"Please enter the AWS profile name to use for {service_name} access\n"
            "(Press Enter to skip if using environment variables, "
            "or specify a profile from your ~/.aws/config):"
        )
        return message

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle AWS profile input."""
        import os

        profile = input_text.strip()

        # Check if environment variables are set
        has_env_credentials = bool(
            os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY")
        )

        # If empty and env credentials exist, skip profile (use None)
        if not profile:
            if has_env_credentials:
                # Skip profile, use environment variables
                profile = None
                message = (
                    "Using AWS credentials from environment variables. "
                    "Proceeding to region selection."
                )
            else:
                # Default to "default" profile if no env credentials
                profile = "default"
                message = f"AWS profile '{profile}' configured. Proceeding to region selection."
        else:
            # Validate provided profile name (alphanumeric, dash, underscore)
            if not profile.replace("-", "").replace("_", "").isalnum():
                return StepResult(
                    success=False,
                    message="Invalid profile name format. Please enter a valid AWS profile name.",
                    action=WizardAction.RETRY,
                )
            message = (
                f"AWS profile '{profile}' configured. Proceeding to region selection."
            )

        # Save profile to config (None is valid for env var credentials)
        try:
            from chuck_data.config import get_config_manager

            success = get_config_manager().update(aws_profile=profile)
            if not success:
                return StepResult(
                    success=False,
                    message="Failed to save AWS profile. Please try again.",
                    action=WizardAction.RETRY,
                )

            logging.info(
                f"AWS profile '{profile}' saved to config and will be added to state"
            )
            return StepResult(
                success=True,
                message=message,
                next_step=WizardStep.AWS_REGION_INPUT,
                action=WizardAction.CONTINUE,
                data={"aws_profile": profile},
            )

        except Exception as e:
            logging.error(f"Error saving AWS profile: {e}")
            return StepResult(
                success=False,
                message=f"Error saving AWS profile: {str(e)}",
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
                    from chuck_data.clients.databricks import DatabricksAPIClient

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

                    # Only use the service client if it's a DatabricksAPIClient
                    # If the data provider is Redshift, we need to create a Databricks client for LLM
                    service = get_chuck_service()
                    databricks_client = None

                    if (
                        service
                        and service.client
                        and isinstance(service.client, DatabricksAPIClient)
                    ):
                        databricks_client = service.client

                    provider = DatabricksProvider(
                        workspace_url=state.workspace_url,
                        token=state.token,
                        client=databricks_client,
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


class AWSRegionInputStep(SetupStep):
    """Handle AWS region input for AWS services (Redshift or EMR)."""

    def get_step_title(self) -> str:
        return "AWS Region Configuration"

    def get_prompt_message(self, state: WizardState) -> str:
        import os

        aws_profile = os.getenv("AWS_PROFILE", "not set")
        aws_region = os.getenv("AWS_REGION", "not set")

        # Determine which service we're configuring for
        if state.data_provider == "aws_redshift":
            service_name = "Redshift cluster"
        elif state.compute_provider == "aws_emr":
            service_name = "EMR cluster"
        else:
            service_name = "AWS resources"

        return (
            f"Current AWS_PROFILE: {aws_profile}\n"
            f"Current AWS_REGION: {aws_region}\n\n"
            f"Please enter the AWS region where your {service_name} is located\n"
            "(e.g., us-west-2, us-east-1, eu-west-1):"
        )

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle AWS region input."""
        region = input_text.strip()

        if not region:
            return StepResult(
                success=False,
                message="Region cannot be empty. Please enter a valid AWS region.",
                action=WizardAction.RETRY,
            )

        # Basic validation for AWS region format
        if not region.replace("-", "").replace("_", "").isalnum():
            return StepResult(
                success=False,
                message="Invalid region format. Please enter a valid AWS region (e.g., us-west-2).",
                action=WizardAction.RETRY,
            )

        # Save region to config
        try:
            from chuck_data.config import get_config_manager

            success = get_config_manager().update(aws_region=region)
            if not success:
                return StepResult(
                    success=False,
                    message="Failed to save AWS region. Please try again.",
                    action=WizardAction.RETRY,
                )

            logging.info(
                f"AWS region '{region}' saved to config and will be added to state"
            )

            # Determine next step based on what we're configuring
            if state.data_provider == "aws_redshift":
                # For Redshift, continue with full AWS setup
                next_step = WizardStep.AWS_ACCOUNT_ID_INPUT
                message = f"AWS region '{region}' configured. Proceeding to AWS account ID input."
            elif state.compute_provider == "aws_emr":
                # For EMR (with Databricks data provider), go directly to EMR cluster ID
                next_step = WizardStep.EMR_CLUSTER_ID_INPUT
                message = f"AWS region '{region}' configured. Proceeding to EMR cluster ID input."
            else:
                # Fallback to account ID input
                next_step = WizardStep.AWS_ACCOUNT_ID_INPUT
                message = f"AWS region '{region}' configured. Proceeding to AWS account ID input."

            return StepResult(
                success=True,
                message=message,
                next_step=next_step,
                action=WizardAction.CONTINUE,
                data={"aws_region": region},
            )

        except Exception as e:
            logging.error(f"Error saving AWS region: {e}")
            return StepResult(
                success=False,
                message=f"Error saving AWS region: {str(e)}",
                action=WizardAction.RETRY,
            )


class AWSAccountIdInputStep(SetupStep):
    """Handle AWS Account ID input for Redshift JDBC URL construction."""

    def get_step_title(self) -> str:
        return "AWS Account ID Configuration"

    def get_prompt_message(self, state: WizardState) -> str:
        return (
            "Please enter your AWS Account ID (12-digit number):\n"
            "(This is required to construct the Redshift JDBC URL)\n"
            "(You can find this in the AWS Console under your account settings)"
        )

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle AWS Account ID input."""
        account_id = input_text.strip()

        if not account_id:
            return StepResult(
                success=False,
                message="AWS Account ID cannot be empty. Please enter a 12-digit account ID.",
                action=WizardAction.RETRY,
            )

        # Validate account ID format (should be 12 digits)
        if not account_id.isdigit() or len(account_id) != 12:
            return StepResult(
                success=False,
                message="Invalid AWS Account ID format. Must be exactly 12 digits.",
                action=WizardAction.RETRY,
            )

        # Save account ID to config
        try:
            from chuck_data.config import get_config_manager

            success = get_config_manager().update(aws_account_id=account_id)
            if not success:
                return StepResult(
                    success=False,
                    message="Failed to save AWS Account ID. Please try again.",
                    action=WizardAction.RETRY,
                )

            logging.info(
                f"AWS Account ID '{account_id}' saved to config and will be added to state"
            )
            return StepResult(
                success=True,
                message=f"AWS Account ID configured. Proceeding to cluster selection.",
                next_step=WizardStep.REDSHIFT_CLUSTER_SELECTION,
                action=WizardAction.CONTINUE,
                data={"aws_account_id": account_id},
            )

        except Exception as e:
            logging.error(f"Error saving AWS Account ID: {e}")
            return StepResult(
                success=False,
                message=f"Error saving AWS Account ID: {str(e)}",
                action=WizardAction.RETRY,
            )


class RedshiftClusterSelectionStep(SetupStep):
    """Handle Redshift cluster selection."""

    def get_step_title(self) -> str:
        return "Redshift Cluster Selection"

    def get_prompt_message(self, state: WizardState) -> str:
        return (
            "Enter your Redshift cluster identifier or workgroup name:\n"
            "(For provisioned clusters: cluster-identifier)\n"
            "(For Serverless: workgroup-name)"
        )

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle Redshift cluster/workgroup selection."""
        identifier = input_text.strip()

        if not identifier:
            return StepResult(
                success=False,
                message="Cluster identifier/workgroup cannot be empty.",
                action=WizardAction.RETRY,
            )

        # Try both serverless workgroup and provisioned cluster
        # We'll try serverless first (more common for new deployments)

        # Get AWS credentials from environment (optional - boto3 will discover if not provided)
        import os

        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        # Validate connection to Redshift
        try:
            from chuck_data.clients.redshift import RedshiftAPIClient

            logging.info(
                f"RedshiftClusterSelectionStep: state.aws_region = {state.aws_region}"
            )

            if not state.aws_region:
                logging.error("aws_region is None in RedshiftClusterSelectionStep!")
                return StepResult(
                    success=False,
                    message="AWS region not set. Please go back and enter the region.",
                    next_step=WizardStep.AWS_REGION_INPUT,
                    action=WizardAction.CONTINUE,
                )

            client_config = {
                "region": state.aws_region,
            }

            # Add AWS profile if available
            if state.aws_profile:
                client_config["aws_profile"] = state.aws_profile

            # Add explicit credentials if provided
            if aws_access_key and aws_secret_key:
                client_config["aws_access_key_id"] = aws_access_key
                client_config["aws_secret_access_key"] = aws_secret_key

            # Try serverless workgroup first
            is_serverless = None
            connection_error = None

            # Attempt 1: Try as serverless workgroup
            try:
                logging.info(
                    f"Attempting to connect as serverless workgroup: {identifier}"
                )
                serverless_config = {**client_config, "workgroup_name": identifier}
                client = RedshiftAPIClient(**serverless_config)
                databases = client.list_databases()
                logging.info(
                    f"Successfully connected to Redshift Serverless workgroup. Found {len(databases)} databases."
                )
                is_serverless = True
            except Exception as serverless_error:
                logging.info(
                    f"Failed to connect as serverless workgroup: {serverless_error}"
                )
                connection_error = serverless_error

                # Attempt 2: Try as provisioned cluster
                try:
                    logging.info(
                        f"Attempting to connect as provisioned cluster: {identifier}"
                    )
                    cluster_config = {**client_config, "cluster_identifier": identifier}
                    client = RedshiftAPIClient(**cluster_config)
                    databases = client.list_databases()
                    logging.info(
                        f"Successfully connected to Redshift provisioned cluster. Found {len(databases)} databases."
                    )
                    is_serverless = False
                except Exception as cluster_error:
                    logging.info(
                        f"Failed to connect as provisioned cluster: {cluster_error}"
                    )
                    # Both attempts failed
                    error_msg = str(connection_error)

                    # Provide helpful error messages for common issues
                    if "AccessDenied" in error_msg or "not authorized" in error_msg:
                        helpful_msg = (
                            f"Access denied to Redshift workgroup/cluster '{identifier}'.\n"
                            "Possible causes:\n"
                            "  1. AWS credentials don't have Redshift Data API permissions\n"
                            "  2. The workgroup/cluster doesn't exist in this region\n"
                            f"  3. Your IAM user/role needs 'redshift-data:*' and 'redshift:DescribeClusters' permissions\n\n"
                            f"Detailed error: {error_msg}"
                        )
                    elif (
                        "not found" in error_msg.lower()
                        or "does not exist" in error_msg.lower()
                    ):
                        helpful_msg = (
                            f"Redshift workgroup/cluster '{identifier}' not found.\n"
                            f"Please verify:\n"
                            f"  1. The workgroup/cluster name is correct\n"
                            f"  2. It exists in region: {state.aws_region}\n"
                            f"  3. You have permission to access it\n\n"
                            f"Tried both serverless workgroup and provisioned cluster.\n"
                            f"Detailed error: {error_msg}"
                        )
                    else:
                        helpful_msg = f"Failed to connect to Redshift workgroup/cluster '{identifier}'.\n\nError: {error_msg}"

                    return StepResult(
                        success=False,
                        message=helpful_msg,
                        action=WizardAction.RETRY,
                    )

            # Save configuration
            from chuck_data.config import get_config_manager

            config_data = {}

            # Only save explicit credentials if they were provided
            if aws_access_key and aws_secret_key:
                config_data["aws_access_key_id"] = aws_access_key
                config_data["aws_secret_access_key"] = aws_secret_key

            if is_serverless:
                config_data["redshift_workgroup_name"] = identifier
                success = get_config_manager().update(**config_data)
                data = {"redshift_workgroup_name": identifier}
                msg_type = "workgroup"
            else:
                config_data["redshift_cluster_identifier"] = identifier
                success = get_config_manager().update(**config_data)
                data = {"redshift_cluster_identifier": identifier}
                msg_type = "cluster"

            if not success:
                return StepResult(
                    success=False,
                    message=f"Failed to save Redshift {msg_type} configuration. Please try again.",
                    action=WizardAction.RETRY,
                )

            return StepResult(
                success=True,
                message=f"Redshift {msg_type} '{identifier}' connected successfully. Proceeding to S3 configuration.",
                next_step=WizardStep.S3_BUCKET_INPUT,
                action=WizardAction.CONTINUE,
                data=data,
            )

        except Exception as e:
            logging.error(f"Error connecting to Redshift: {e}")
            return StepResult(
                success=False,
                message=f"Error connecting to Redshift: {str(e)}",
                action=WizardAction.RETRY,
            )


class S3BucketInputStep(SetupStep):
    """Handle S3 bucket configuration for Redshift."""

    def get_step_title(self) -> str:
        return "S3 Bucket Configuration"

    def get_prompt_message(self, state: WizardState) -> str:
        return (
            "Enter the S3 bucket name for intermediate storage:\n"
            "(Required for Spark-Redshift connector)\n"
            "Example: my-redshift-temp-bucket"
        )

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle S3 bucket input."""
        bucket = input_text.strip()

        if not bucket:
            return StepResult(
                success=False,
                message="S3 bucket name cannot be empty.",
                action=WizardAction.RETRY,
            )

        # Basic S3 bucket name validation
        if not bucket.replace("-", "").replace(".", "").isalnum():
            return StepResult(
                success=False,
                message="Invalid S3 bucket name format. Please enter a valid bucket name.",
                action=WizardAction.RETRY,
            )

        # Verify bucket exists and is accessible
        try:
            import boto3
            import os

            # Build boto3 session with the same credentials used for Redshift
            # Priority: explicit env credentials > aws_profile > default
            aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

            if aws_access_key and aws_secret_key:
                session = boto3.Session(
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=state.aws_region,
                )
            elif state.aws_profile:
                session = boto3.Session(
                    profile_name=state.aws_profile,
                    region_name=state.aws_region,
                )
            else:
                session = boto3.Session(region_name=state.aws_region)

            s3 = session.client("s3")

            # Try to list objects (with max 1) to verify access
            s3.list_objects_v2(Bucket=bucket, MaxKeys=1)

        except Exception as e:
            return StepResult(
                success=False,
                message=f"Cannot access S3 bucket '{bucket}': {str(e)}\nPlease verify the bucket exists and you have access.",
                action=WizardAction.RETRY,
            )

        # Save S3 bucket to config
        try:
            from chuck_data.config import get_config_manager

            success = get_config_manager().update(s3_bucket=bucket)
            if not success:
                return StepResult(
                    success=False,
                    message="Failed to save S3 bucket configuration. Please try again.",
                    action=WizardAction.RETRY,
                )

            # Reinitialize the service client with Redshift configuration
            service = get_chuck_service()
            if service:
                init_success = service.reinitialize_client()
                if not init_success:
                    logging.warning(
                        "Failed to reinitialize Redshift client, but credentials were saved"
                    )

            return StepResult(
                success=True,
                message=f"S3 bucket '{bucket}' configured successfully. Proceeding to IAM role configuration.",
                next_step=WizardStep.IAM_ROLE_INPUT,
                action=WizardAction.CONTINUE,
                data={"s3_bucket": bucket},
            )

        except Exception as e:
            logging.error(f"Error saving S3 bucket: {e}")
            return StepResult(
                success=False,
                message=f"Error saving S3 bucket: {str(e)}",
                action=WizardAction.RETRY,
            )


class IAMRoleInputStep(SetupStep):
    """Handle IAM role ARN configuration for Redshift."""

    def get_step_title(self) -> str:
        return "IAM Role Configuration"

    def get_prompt_message(self, state: WizardState) -> str:
        return (
            "Enter the IAM role ARN for Redshift:\n"
            "(Required for Databricks to access Redshift and S3)\n"
            "Example: arn:aws:iam::123456789012:role/RedshiftRole"
        )

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle IAM role ARN input."""
        iam_role = input_text.strip()

        if not iam_role:
            return StepResult(
                success=False,
                message="IAM role ARN cannot be empty.",
                action=WizardAction.RETRY,
            )

        # Basic IAM role ARN validation
        if not iam_role.startswith("arn:aws:iam::"):
            return StepResult(
                success=False,
                message="Invalid IAM role ARN format. Must start with 'arn:aws:iam::'",
                action=WizardAction.RETRY,
            )

        # Check that the ARN contains :role/ (allows for path components like service-role/)
        if ":role/" not in iam_role:
            return StepResult(
                success=False,
                message="Invalid IAM role ARN format. Must contain ':role/' followed by role name",
                action=WizardAction.RETRY,
            )

        # Save IAM role to config
        try:
            from chuck_data.config import get_config_manager

            # Construct S3 temp dir from bucket and default path
            s3_temp_dir = f"s3://{state.s3_bucket}/redshift-temp/"

            # Save both IAM role and construct S3 temp dir
            success = get_config_manager().update(
                redshift_iam_role=iam_role, redshift_s3_temp_dir=s3_temp_dir
            )

            if not success:
                return StepResult(
                    success=False,
                    message="Failed to save IAM role configuration. Please try again.",
                    action=WizardAction.RETRY,
                )

            return StepResult(
                success=True,
                message=f"IAM role configured successfully. Proceeding to compute provider selection.",
                next_step=WizardStep.COMPUTE_PROVIDER_SELECTION,
                action=WizardAction.CONTINUE,
                data={"iam_role": iam_role},
            )

        except Exception as e:
            logging.error(f"Error saving IAM role: {e}")
            return StepResult(
                success=False,
                message=f"Error saving IAM role: {str(e)}",
                action=WizardAction.RETRY,
            )


class InstanceProfileInputStep(SetupStep):
    """Handle Databricks Instance Profile ARN configuration for AWS access."""

    def get_step_title(self) -> str:
        return "Databricks Instance Profile Configuration"

    def get_prompt_message(self, state: WizardState) -> str:
        return (
            "Enter the AWS Instance Profile ARN for Databricks:\n"
            "(Required for Databricks clusters to access AWS services like Redshift and S3)\n"
            "Example: arn:aws:iam::123456789012:instance-profile/DatabricksInstanceProfile"
        )

    def handle_input(self, input_text: str, state: WizardState) -> StepResult:
        """Handle Instance Profile ARN input."""
        instance_profile_arn = input_text.strip()

        if not instance_profile_arn:
            return StepResult(
                success=False,
                message="Instance Profile ARN cannot be empty.",
                action=WizardAction.RETRY,
            )

        # Basic Instance Profile ARN validation
        if not instance_profile_arn.startswith("arn:aws:iam::"):
            return StepResult(
                success=False,
                message="Invalid Instance Profile ARN format. Must start with 'arn:aws:iam::'",
                action=WizardAction.RETRY,
            )

        # Check that the ARN contains :instance-profile/
        if ":instance-profile/" not in instance_profile_arn:
            return StepResult(
                success=False,
                message="Invalid Instance Profile ARN format. Must contain ':instance-profile/' followed by profile name",
                action=WizardAction.RETRY,
            )

        # Save Instance Profile ARN to config
        try:
            from chuck_data.config import get_config_manager

            success = get_config_manager().update(
                databricks_instance_profile_arn=instance_profile_arn
            )

            if not success:
                return StepResult(
                    success=False,
                    message="Failed to save Instance Profile ARN configuration. Please try again.",
                    action=WizardAction.RETRY,
                )

            return StepResult(
                success=True,
                message=f"Instance Profile ARN configured successfully. Proceeding to Databricks workspace configuration.",
                next_step=WizardStep.WORKSPACE_URL,
                action=WizardAction.CONTINUE,
                data={"instance_profile_arn": instance_profile_arn},
            )

        except Exception as e:
            logging.error(f"Error saving Instance Profile ARN: {e}")
            return StepResult(
                success=False,
                message=f"Error saving Instance Profile ARN: {str(e)}",
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
        WizardStep.COMPUTE_PROVIDER_SELECTION: ComputeProviderSelectionStep,
        WizardStep.EMR_CLUSTER_ID_INPUT: EMRClusterIDInputStep,
        WizardStep.WORKSPACE_URL: WorkspaceUrlStep,
        WizardStep.TOKEN_INPUT: TokenInputStep,
        WizardStep.LLM_PROVIDER_SELECTION: LLMProviderSelectionStep,
        WizardStep.MODEL_SELECTION: ModelSelectionStep,
        WizardStep.AWS_PROFILE_INPUT: AWSProfileInputStep,
        WizardStep.AWS_REGION_INPUT: AWSRegionInputStep,
        WizardStep.AWS_ACCOUNT_ID_INPUT: AWSAccountIdInputStep,
        WizardStep.REDSHIFT_CLUSTER_SELECTION: RedshiftClusterSelectionStep,
        WizardStep.S3_BUCKET_INPUT: S3BucketInputStep,
        WizardStep.IAM_ROLE_INPUT: IAMRoleInputStep,
        WizardStep.INSTANCE_PROFILE_INPUT: InstanceProfileInputStep,
        WizardStep.USAGE_CONSENT: UsageConsentStep,
    }

    step_class = step_map.get(step_type)
    if not step_class:
        raise ValueError(f"Unknown step type: {step_type}")

    return step_class(validator)
