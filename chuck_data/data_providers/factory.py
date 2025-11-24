"""Data Provider Factory."""

import os
import logging
from typing import Optional
from chuck_data.data_providers.provider import DataProvider


logger = logging.getLogger(__name__)


class DataProviderFactory:
    """Creates data provider instances based on configuration.

    Provider selection precedence:
    1. Explicit provider_name parameter
    2. CHUCK_DATA_PROVIDER environment variable
    3. data_provider in config file
    4. Default: "databricks"
    """

    _SUPPORTED_PROVIDERS = ["databricks", "aws_redshift"]

    @staticmethod
    def create(provider_name: Optional[str] = None, **kwargs) -> DataProvider:
        """Create data provider instance.

        Args:
            provider_name: Provider to use ("databricks", "aws_redshift")
            **kwargs: Provider-specific configuration parameters

        Returns:
            Configured DataProvider instance

        Raises:
            ValueError: Unknown provider or missing required parameters
            ImportError: Missing provider dependencies
        """
        selected_provider = DataProviderFactory._resolve_provider_name(provider_name)
        provider_config = DataProviderFactory._get_provider_config(selected_provider)

        # Merge kwargs with config (kwargs take precedence)
        merged_config = {**provider_config, **kwargs}

        return DataProviderFactory._instantiate_provider(
            selected_provider, merged_config
        )

    @staticmethod
    def _resolve_provider_name(explicit_name: Optional[str] = None) -> str:
        """Resolve provider name using precedence rules."""
        if explicit_name is not None:
            logger.debug(f"Using explicit data provider: {explicit_name}")
            return explicit_name

        env_provider = os.getenv("CHUCK_DATA_PROVIDER")
        if env_provider is not None:
            logger.debug(f"Using data provider from environment: {env_provider}")
            return env_provider

        try:
            from chuck_data.config import get_config_manager

            config = get_config_manager().get_config()
            if hasattr(config, "data_provider") and config.data_provider:
                logger.debug(f"Using data provider from config: {config.data_provider}")
                return config.data_provider
        except Exception as e:
            logger.debug(f"Could not load data provider from config: {e}")

        logger.debug("Using default data provider: databricks")
        return "databricks"

    @staticmethod
    def _get_provider_config(provider_name: str) -> dict:
        """Get provider-specific configuration from config file."""
        config = {}

        try:
            from chuck_data.config import get_config_manager

            chuck_config = get_config_manager().get_config()
            if hasattr(chuck_config, "data_provider_config"):
                provider_configs = chuck_config.data_provider_config or {}
                config = provider_configs.get(provider_name, {})
                logger.debug(f"Loaded config for data provider {provider_name}")
        except Exception as e:
            logger.debug(f"Could not load data provider config: {e}")

        return config

    @staticmethod
    def _instantiate_provider(provider_name: str, config: dict) -> DataProvider:
        """Instantiate provider with configuration."""
        if provider_name == "databricks":
            try:
                from chuck_data.data_providers.adapters import DatabricksProviderAdapter

                # Databricks requires workspace_url and token
                if "workspace_url" not in config or "token" not in config:
                    raise ValueError(
                        "Databricks provider requires 'workspace_url' and 'token'"
                    )

                return DatabricksProviderAdapter(
                    workspace_url=config["workspace_url"], token=config["token"]
                )
            except ImportError as e:
                raise ImportError(
                    "Databricks provider requires databricks-sdk: pip install databricks-sdk"
                ) from e

        elif provider_name == "aws_redshift":
            try:
                from chuck_data.data_providers.adapters import RedshiftProviderAdapter

                # AWS Redshift requires specific parameters
                required_params = [
                    "aws_access_key_id",
                    "aws_secret_access_key",
                    "region",
                ]
                missing = [p for p in required_params if p not in config]
                if missing:
                    raise ValueError(
                        f"AWS Redshift provider requires: {', '.join(missing)}"
                    )

                # Either cluster_identifier or workgroup_name is required
                if (
                    "cluster_identifier" not in config
                    and "workgroup_name" not in config
                ):
                    raise ValueError(
                        "AWS Redshift provider requires either 'cluster_identifier' or 'workgroup_name'"
                    )

                return RedshiftProviderAdapter(**config)
            except ImportError as e:
                raise ImportError(
                    "AWS Redshift provider requires boto3: pip install boto3"
                ) from e

        else:
            raise ValueError(
                f"Unknown data provider '{provider_name}'. "
                f"Supported: {', '.join(DataProviderFactory._SUPPORTED_PROVIDERS)}"
            )

    @staticmethod
    def get_available_providers() -> list:
        """Get list of supported data provider names."""
        return DataProviderFactory._SUPPORTED_PROVIDERS.copy()
