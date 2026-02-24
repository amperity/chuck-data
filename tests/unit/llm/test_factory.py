"""Tests for LLMProviderFactory."""

import pytest
import os
from unittest.mock import patch, MagicMock
from chuck_data.llm.factory import LLMProviderFactory


class TestLLMProviderFactory:
    """Test LLM provider factory behavior."""

    def test_get_available_providers(self):
        """Factory returns list of supported providers."""
        providers = LLMProviderFactory.get_available_providers()
        assert "databricks" in providers
        assert "aws_bedrock" in providers
        assert "mock" in providers

    def test_unknown_provider_raises_error(self):
        """Factory raises ValueError for unknown provider."""
        with pytest.raises(ValueError, match="Unknown provider"):
            LLMProviderFactory.create("nonexistent_provider")

    @patch.dict(os.environ, {}, clear=True)
    def test_provider_selection_precedence_default(self):
        """Default provider is databricks when no config."""
        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.get_config.return_value = MagicMock(
                llm_provider=None
            )

            provider_name = LLMProviderFactory._resolve_provider_name()
            assert provider_name == "databricks"

    @patch.dict(os.environ, {"CHUCK_LLM_PROVIDER": "mock"}, clear=True)
    def test_provider_selection_precedence_env_var(self):
        """Environment variable overrides config."""
        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.get_config.return_value = MagicMock(
                llm_provider="databricks"
            )

            provider_name = LLMProviderFactory._resolve_provider_name()
            assert provider_name == "mock"

    def test_provider_selection_precedence_explicit(self):
        """Explicit parameter has highest priority."""
        with patch.dict(os.environ, {"CHUCK_LLM_PROVIDER": "openai"}, clear=True):
            provider_name = LLMProviderFactory._resolve_provider_name("aws_bedrock")
            assert provider_name == "aws_bedrock"

    def test_get_provider_config_returns_empty_dict_on_error(self):
        """Provider config returns empty dict when config unavailable."""
        with patch(
            "chuck_data.config.get_config_manager", side_effect=Exception("No config")
        ):
            config = LLMProviderFactory._get_provider_config("databricks")
            assert config == {}

    def test_get_provider_config_active_model_uses_model_key(self):
        """active_model is passed as 'model', not 'model_id', so providers accept it."""
        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config_obj = MagicMock()
            mock_config_obj.llm_provider_config = {}
            mock_config_obj.active_model = "databricks-claude-3-7-sonnet"
            mock_config.return_value.get_config.return_value = mock_config_obj

            config = LLMProviderFactory._get_provider_config("databricks")

            assert config.get("model") == "databricks-claude-3-7-sonnet"
            assert "model_id" not in config

    def test_get_provider_config_active_model_overrides_provider_specific_model(self):
        """active_model overrides any model set in provider-specific config."""
        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config_obj = MagicMock()
            mock_config_obj.llm_provider_config = {"databricks": {"model": "old-model"}}
            mock_config_obj.active_model = "databricks-claude-3-7-sonnet"
            mock_config.return_value.get_config.return_value = mock_config_obj

            config = LLMProviderFactory._get_provider_config("databricks")

            assert config.get("model") == "databricks-claude-3-7-sonnet"

    def test_get_provider_config_no_active_model_returns_provider_config(self):
        """Without active_model, provider-specific config is returned unchanged."""
        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config_obj = MagicMock()
            mock_config_obj.llm_provider_config = {
                "databricks": {"model": "some-default-model"}
            }
            mock_config_obj.active_model = None
            mock_config.return_value.get_config.return_value = mock_config_obj

            config = LLMProviderFactory._get_provider_config("databricks")

            assert config.get("model") == "some-default-model"

    def test_create_databricks_provider_with_active_model(self):
        """Factory creates DatabricksProvider with active_model passed as 'model'."""
        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config_obj = MagicMock()
            mock_config_obj.llm_provider_config = {}
            mock_config_obj.active_model = "databricks-claude-3-7-sonnet"
            mock_config.return_value.get_config.return_value = mock_config_obj

            from chuck_data.llm.providers.databricks import DatabricksProvider

            provider = LLMProviderFactory.create("databricks")

            assert isinstance(provider, DatabricksProvider)
            assert provider.default_model == "databricks-claude-3-7-sonnet"

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_create_aws_bedrock_provider(self, mock_boto3):
        """Factory can create AWS Bedrock provider."""
        mock_boto3.client.return_value = MagicMock()

        provider = LLMProviderFactory.create("aws_bedrock")

        assert provider is not None
        # Verify it's the right type
        from chuck_data.llm.providers.aws_bedrock import AWSBedrockProvider

        assert isinstance(provider, AWSBedrockProvider)

    @patch("chuck_data.llm.providers.aws_bedrock.boto3")
    def test_create_aws_bedrock_provider_with_config(self, mock_boto3):
        """Factory passes configuration to AWS Bedrock provider."""
        mock_boto3.client.return_value = MagicMock()

        with patch("chuck_data.config.get_config_manager") as mock_config:
            # Mock config with AWS provider settings
            mock_config_obj = MagicMock()
            mock_config_obj.llm_provider = "aws_bedrock"
            mock_config_obj.llm_provider_config = {
                "aws_bedrock": {
                    "region": "us-west-2",
                    "model_id": "anthropic.claude-3-haiku-20240307-v1:0",
                }
            }
            mock_config_obj.active_model = None  # Explicitly set to None
            mock_config.return_value.get_config.return_value = mock_config_obj

            provider = LLMProviderFactory.create("aws_bedrock")

            assert provider is not None
            # Verify configuration was passed correctly
            from chuck_data.llm.providers.aws_bedrock import AWSBedrockProvider

            assert isinstance(provider, AWSBedrockProvider)
            assert provider.region == "us-west-2"
            assert provider.default_model == "anthropic.claude-3-haiku-20240307-v1:0"

    def test_create_aws_bedrock_without_boto3_raises_error(self):
        """Factory raises ImportError when boto3 not available."""
        with patch("chuck_data.llm.providers.aws_bedrock.boto3", None):
            with pytest.raises(ImportError, match="boto3"):
                LLMProviderFactory.create("aws_bedrock")
