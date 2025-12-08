"""LLM Provider Factory."""

import os
import logging
from typing import Optional
from chuck_data.llm.provider import LLMProvider


logger = logging.getLogger(__name__)


class LLMProviderFactory:
    """Creates LLM provider instances based on configuration.

    Provider selection precedence:
    1. Explicit provider_name parameter
    2. CHUCK_LLM_PROVIDER environment variable
    3. llm_provider in config file
    4. Default: "databricks"
    """

    _SUPPORTED_PROVIDERS = ["databricks", "aws_bedrock", "openai", "anthropic", "mock"]

    @staticmethod
    def create(provider_name: Optional[str] = None) -> LLMProvider:
        """Create LLM provider instance.

        Args:
            provider_name: Provider to use ("databricks", "aws_bedrock", "openai", "anthropic", "mock")

        Returns:
            Configured LLMProvider instance

        Raises:
            ValueError: Unknown provider
            ImportError: Missing provider dependencies
        """
        selected_provider = LLMProviderFactory._resolve_provider_name(provider_name)
        provider_config = LLMProviderFactory._get_provider_config(selected_provider)
        return LLMProviderFactory._instantiate_provider(
            selected_provider, provider_config
        )

    @staticmethod
    def _resolve_provider_name(explicit_name: Optional[str] = None) -> str:
        """Resolve provider name using precedence rules."""
        if explicit_name is not None:
            logger.debug(f"Using explicit provider: {explicit_name}")
            return explicit_name

        env_provider = os.getenv("CHUCK_LLM_PROVIDER")
        if env_provider is not None:
            logger.debug(f"Using provider from environment: {env_provider}")
            return env_provider

        try:
            from chuck_data.config import get_config_manager

            config = get_config_manager().get_config()
            if hasattr(config, "llm_provider") and config.llm_provider:
                logger.debug(f"Using provider from config: {config.llm_provider}")
                return config.llm_provider
        except Exception as e:
            logger.debug(f"Could not load provider from config: {e}")

        logger.debug("Using default provider: databricks")
        return "databricks"

    @staticmethod
    def _get_provider_config(provider_name: str) -> dict:
        """Get provider-specific configuration from config file."""
        config = {}

        try:
            from chuck_data.config import get_config_manager

            chuck_config = get_config_manager().get_config()
            if hasattr(chuck_config, "llm_provider_config"):
                provider_configs = chuck_config.llm_provider_config or {}
                config = provider_configs.get(provider_name, {})
                logger.debug(f"Loaded config for {provider_name}")
        except Exception as e:
            logger.debug(f"Could not load provider config: {e}")

        return config

    @staticmethod
    def _instantiate_provider(provider_name: str, config: dict) -> LLMProvider:
        """Instantiate provider with configuration."""
        if provider_name == "databricks":
            from chuck_data.llm.providers.databricks import DatabricksProvider

            return DatabricksProvider(**config)

        elif provider_name == "aws_bedrock":
            try:
                from chuck_data.llm.providers.aws_bedrock import AWSBedrockProvider

                return AWSBedrockProvider(**config)
            except ImportError as e:
                raise ImportError(
                    "AWS Bedrock provider requires boto3: pip install boto3"
                ) from e

        elif provider_name == "openai":
            try:
                from chuck_data.llm.providers.openai import OpenAIProvider  # type: ignore[reportMissingImports]

                return OpenAIProvider(**config)
            except ImportError as e:
                raise ImportError(
                    "OpenAI provider requires openai: pip install openai"
                ) from e

        elif provider_name == "anthropic":
            try:
                from chuck_data.llm.providers.anthropic import AnthropicProvider  # type: ignore[reportMissingImports]

                return AnthropicProvider(**config)
            except ImportError as e:
                raise ImportError(
                    "Anthropic provider requires anthropic: pip install anthropic"
                ) from e

        elif provider_name == "mock":
            from chuck_data.llm.providers.mock import MockProvider  # type: ignore[reportMissingImports]

            return MockProvider(**config)

        else:
            raise ValueError(
                f"Unknown provider '{provider_name}'. "
                f"Supported: {', '.join(LLMProviderFactory._SUPPORTED_PROVIDERS)}"
            )

    @staticmethod
    def get_available_providers() -> list:
        """Get list of supported provider names."""
        return LLMProviderFactory._SUPPORTED_PROVIDERS.copy()
