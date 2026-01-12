"""
Tests for LLM provider filtering based on available credentials.
"""

import pytest
from unittest.mock import patch, MagicMock
from chuck_data.commands.wizard.state import WizardState, WizardStep
from chuck_data.commands.wizard.steps import LLMProviderSelectionStep
from chuck_data.commands.wizard.validator import InputValidator


class TestLLMProviderFiltering:
    """Tests for dynamic LLM provider selection based on credentials."""

    @pytest.fixture
    def step(self):
        """Create LLM provider selection step."""
        validator = InputValidator()
        return LLMProviderSelectionStep(validator)

    def test_prompt_shows_both_providers_when_both_available(self, step):
        """Test prompt shows both providers when both credentials are available."""
        state = WizardState(
            workspace_url="https://test.databricks.com",
            token="test-token",
            aws_profile="default",
            aws_region="us-west-2",
        )

        prompt = step.get_prompt_message(state)
        assert "1. Databricks (default)" in prompt
        assert "2. AWS Bedrock" in prompt
        assert "only option" not in prompt

    def test_prompt_shows_only_databricks_when_no_aws(self, step):
        """Test prompt shows only Databricks when AWS credentials missing."""
        state = WizardState(
            workspace_url="https://test.databricks.com",
            token="test-token",
            aws_profile=None,
            aws_region=None,
        )

        prompt = step.get_prompt_message(state)
        assert "1. Databricks" in prompt
        # Should not show option 2 when only Databricks is available
        assert "2." not in prompt

    def test_prompt_shows_only_aws_when_no_databricks(self, step):
        """Test prompt shows only AWS when Databricks credentials missing."""
        state = WizardState(
            workspace_url=None,
            token=None,
            aws_profile="default",
            aws_region="us-west-2",
        )

        prompt = step.get_prompt_message(state)
        assert "1. AWS Bedrock" in prompt
        # Should not show option 2 when only AWS is available
        assert "2." not in prompt

    def test_prompt_shows_both_when_neither_available(self, step):
        """Test prompt shows both options as fallback when neither configured."""
        state = WizardState(
            workspace_url=None,
            token=None,
            aws_profile=None,
            aws_region=None,
        )

        prompt = step.get_prompt_message(state)
        # Falls back to showing both options
        assert "1. Databricks (default)" in prompt
        assert "2. AWS Bedrick" in prompt or "2. AWS Bedrock" in prompt

    @patch("chuck_data.commands.wizard.steps.get_chuck_service")
    @patch("chuck_data.commands.wizard.steps.set_llm_provider")
    def test_auto_maps_selection_when_only_aws_available(
        self, mock_set_provider, mock_service, step
    ):
        """Test that selecting '1' maps to AWS when it's the only option."""
        state = WizardState(
            workspace_url=None,
            token=None,
            aws_profile="default",
            aws_region="us-west-2",
            data_provider="aws_redshift",
        )

        # Mock AWS Bedrock provider
        with patch(
            "chuck_data.llm.providers.aws_bedrock.AWSBedrockProvider"
        ) as mock_aws:
            mock_provider_instance = MagicMock()
            mock_provider_instance.list_models.return_value = [
                {"model_id": "bedrock-claude-sonnet", "name": "Claude Sonnet"}
            ]
            mock_aws.return_value = mock_provider_instance
            mock_set_provider.return_value = True

            result = step.handle_input("1", state)

            # Should succeed and select AWS
            assert result.success is True
            assert result.data["llm_provider"] == "aws_bedrock"
            mock_set_provider.assert_called_once_with("aws_bedrock")

    def test_error_when_selecting_databricks_without_creds(self, step):
        """Test error when trying to select Databricks without credentials."""
        state = WizardState(
            workspace_url=None,
            token=None,
            aws_profile="default",
            aws_region="us-west-2",
        )

        # Mock get_chuck_service to avoid side effects
        with patch("chuck_data.commands.wizard.steps.get_chuck_service"):
            # Try to select Databricks (which would be option 1 in normal flow)
            # But user types "databricks" explicitly
            result = step.handle_input("databricks", state)

            assert result.success is False
            assert "Only AWS Bedrock is available" in result.message
            assert result.action.value == "retry"

    @patch("chuck_data.commands.wizard.steps.get_chuck_service")
    @patch("chuck_data.commands.wizard.steps.set_llm_provider")
    @patch("chuck_data.llm.providers.databricks.DatabricksProvider")
    def test_auto_maps_selection_when_only_databricks_available(
        self, mock_databricks_class, mock_set_provider, mock_service, step
    ):
        """Test that selecting '1' maps to Databricks when it's the only option."""
        state = WizardState(
            workspace_url="https://test.databricks.com",
            token="test-token",
            aws_profile=None,
            aws_region=None,
            data_provider="databricks",
        )

        # Mock Databricks provider
        mock_provider_instance = MagicMock()
        mock_provider_instance.list_models.return_value = [
            {"model_id": "databricks-claude-sonnet", "name": "Claude Sonnet"}
        ]
        mock_databricks_class.return_value = mock_provider_instance
        mock_set_provider.return_value = True

        result = step.handle_input("1", state)

        # Should succeed and select Databricks
        assert result.success is True
        assert result.data["llm_provider"] == "databricks"
        mock_set_provider.assert_called_once_with("databricks")

    def test_rejects_invalid_selection_when_only_one_option(self, step):
        """Test that invalid input is rejected when only one option available."""
        state = WizardState(
            workspace_url="https://test.databricks.com",
            token="test-token",
            aws_profile=None,
            aws_region=None,
        )

        # Try to select option 2 when only option 1 exists
        result = step.handle_input("2", state)

        assert result.success is False
        assert "Only Databricks is available" in result.message
        assert result.action.value == "retry"


class TestLLMProviderSelectionEdgeCases:
    """Test edge cases for LLM provider selection."""

    @pytest.fixture
    def step(self):
        """Create LLM provider selection step."""
        validator = InputValidator()
        return LLMProviderSelectionStep(validator)

    def test_partial_databricks_creds_not_considered_available(self, step):
        """Test that partial Databricks credentials don't count as available."""
        # Missing token
        state1 = WizardState(
            workspace_url="https://test.databricks.com",
            token=None,
            aws_profile="default",
            aws_region="us-west-2",
        )

        prompt1 = step.get_prompt_message(state1)
        assert "1. AWS Bedrock" in prompt1
        # Should not show option 2 when only AWS is available
        assert "2." not in prompt1

        # Missing workspace_url
        state2 = WizardState(
            workspace_url=None,
            token="test-token",
            aws_profile="default",
            aws_region="us-west-2",
        )

        prompt2 = step.get_prompt_message(state2)
        assert "1. AWS Bedrock" in prompt2
        # Should not show option 2 when only AWS is available
        assert "2." not in prompt2

    def test_partial_aws_creds_not_considered_available(self, step):
        """Test that partial AWS credentials don't count as available."""
        # Missing region
        state1 = WizardState(
            workspace_url="https://test.databricks.com",
            token="test-token",
            aws_profile="default",
            aws_region=None,
        )

        prompt1 = step.get_prompt_message(state1)
        assert "1. Databricks" in prompt1
        # Should not show option 2 when only Databricks is available
        assert "2." not in prompt1

        # Missing profile
        state2 = WizardState(
            workspace_url="https://test.databricks.com",
            token="test-token",
            aws_profile=None,
            aws_region="us-west-2",
        )

        prompt2 = step.get_prompt_message(state2)
        assert "1. Databricks" in prompt2
        # Should not show option 2 when only Databricks is available
        assert "2." not in prompt2
