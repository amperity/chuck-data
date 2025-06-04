"""
Input validation for setup wizard steps.
"""

from dataclasses import dataclass
from typing import Optional, List, Dict, Any
import logging

from src.databricks.url_utils import (
    validate_workspace_url,
    normalize_workspace_url,
    detect_cloud_provider,
    get_full_workspace_url,
)


@dataclass
class ValidationResult:
    """Result of input validation."""

    is_valid: bool
    message: str
    processed_value: Optional[str] = None
    error_details: Optional[str] = None


class InputValidator:
    """Handles validation of user inputs for wizard steps."""

    def validate_workspace_url(self, url_input: str) -> ValidationResult:
        """Validate and process workspace URL input."""
        if not url_input or not url_input.strip():
            return ValidationResult(
                is_valid=False, message="Workspace URL cannot be empty"
            )

        url_input = url_input.strip()

        try:
            # First validate the raw input before processing
            is_raw_valid, raw_error = validate_workspace_url(url_input)

            if not is_raw_valid:
                return ValidationResult(
                    is_valid=False,
                    message=raw_error or "Invalid workspace URL format",
                )

            # If raw input is valid, process it
            normalized_id = normalize_workspace_url(url_input)
            cloud_provider = detect_cloud_provider(url_input)
            full_url = get_full_workspace_url(normalized_id, cloud_provider)

            return ValidationResult(
                is_valid=True,
                message="Workspace URL validated successfully",
                processed_value=full_url,
            )

        except Exception as e:
            logging.error(f"Error processing workspace URL: {e}")
            return ValidationResult(
                is_valid=False,
                message="Error processing workspace URL",
                error_details=str(e),
            )

    def validate_token(self, token: str, workspace_url: str) -> ValidationResult:
        """Validate Databricks token."""
        if not token or not token.strip():
            return ValidationResult(is_valid=False, message="Token cannot be empty")

        token = token.strip()

        try:
            # Validate token with Databricks API using the provided workspace URL
            from src.clients.databricks import DatabricksAPIClient

            client = DatabricksAPIClient(workspace_url, token)
            is_valid = client.validate_token()

            if not is_valid:
                return ValidationResult(
                    is_valid=False,
                    message="Invalid Databricks token - please check and try again",
                )

            return ValidationResult(
                is_valid=True,
                message="Token validated successfully",
                processed_value=token,
            )

        except Exception as e:
            logging.error(f"Error validating token: {e}")
            return ValidationResult(
                is_valid=False, message="Error validating token", error_details=str(e)
            )

    def validate_model_selection(
        self, model_input: str, models: List[Dict[str, Any]]
    ) -> ValidationResult:
        """Validate model selection input."""
        if not model_input or not model_input.strip():
            return ValidationResult(
                is_valid=False,
                message="Please select a model by entering its number or name",
            )

        if not models:
            return ValidationResult(
                is_valid=False, message="No models available for selection"
            )

        model_input = model_input.strip()

        # Try to interpret as an index first
        if model_input.isdigit():
            index = int(model_input) - 1  # Convert to 0-based index
            if 0 <= index < len(models):
                selected_model = models[index]["name"]
                return ValidationResult(
                    is_valid=True,
                    message=f"Model '{selected_model}' selected",
                    processed_value=selected_model,
                )
            else:
                return ValidationResult(
                    is_valid=False,
                    message=f"Invalid model number. Please enter a number between 1 and {len(models)}",
                )

        # Try to find by exact name (case-insensitive)
        for model in models:
            if model_input.lower() == model["name"].lower():
                return ValidationResult(
                    is_valid=True,
                    message=f"Model '{model['name']}' selected",
                    processed_value=model["name"],
                )

        # Try substring match
        matches = []
        for model in models:
            if model_input.lower() in model["name"].lower():
                matches.append(model["name"])

        if len(matches) == 1:
            return ValidationResult(
                is_valid=True,
                message=f"Model '{matches[0]}' selected",
                processed_value=matches[0],
            )
        elif len(matches) > 1:
            return ValidationResult(
                is_valid=False,
                message=f"Multiple models match '{model_input}'. Please be more specific or use a number",
                error_details=f"Matching models: {', '.join(matches)}",
            )

        return ValidationResult(
            is_valid=False,
            message=f"Model '{model_input}' not found. Please enter a valid model number or name",
        )

    def validate_usage_consent(self, response: str) -> ValidationResult:
        """Validate usage consent response."""
        if not response or not response.strip():
            return ValidationResult(
                is_valid=False, message="Please enter 'yes' or 'no'"
            )

        response = response.strip().lower()

        if response in ["yes", "y"]:
            return ValidationResult(
                is_valid=True,
                message="Usage tracking consent granted",
                processed_value="yes",
            )
        elif response in ["no", "n"]:
            return ValidationResult(
                is_valid=True,
                message="Usage tracking consent declined",
                processed_value="no",
            )
        else:
            return ValidationResult(
                is_valid=False, message="Please enter 'yes' or 'no'"
            )

    def detect_input_type(self, input_text: str, current_step) -> str:
        """Detect what type of input this is based on content and current step."""
        if not input_text or not input_text.strip():
            return "empty"

        input_text = input_text.strip()

        # For workspace URL step, detect URL-like input
        if (
            hasattr(current_step, "WORKSPACE_URL")
            and current_step == current_step.WORKSPACE_URL
        ):
            has_dots = "." in input_text
            has_databricks = "databricks" in input_text.lower()
            has_protocol = input_text.lower().startswith("http")

            if has_dots or has_databricks or has_protocol:
                return "url"
            else:
                return "token"  # Assume non-URL input is token

        return "text"
