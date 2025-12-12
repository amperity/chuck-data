"""Data Provider Factory.

Note: This is a stub implementation for PR 1.
Config file integration will be implemented in a later PR.
"""

import os
import logging
from typing import Optional
from chuck_data.data_providers.provider import DataProvider
from chuck_data.data_providers.adapters import (
    DatabricksProviderAdapter,
    RedshiftProviderAdapter,
)


logger = logging.getLogger(__name__)


class DataProviderFactory:
    """Creates data provider instances based on configuration.

    Provider selection precedence:
    1. Explicit provider_name parameter
    2. CHUCK_DATA_PROVIDER environment variable
    3. Default: "databricks"

    Note: Config file integration (data_provider field in config) will be
    implemented in a later PR.
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
        """
        selected_provider = DataProviderFactory._resolve_provider_name(provider_name)
        return DataProviderFactory._instantiate_provider(selected_provider, kwargs)

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

        logger.debug("Using default data provider: databricks")
        return "databricks"

    @staticmethod
    def _instantiate_provider(provider_name: str, config: dict) -> DataProvider:
        """Instantiate provider with configuration."""
        if provider_name == "databricks":
            # Databricks requires workspace_url and token
            workspace_url = config.get("workspace_url") or os.getenv(
                "DATABRICKS_WORKSPACE_URL"
            )
            token = config.get("token") or os.getenv("DATABRICKS_TOKEN")

            if not workspace_url or not token:
                raise ValueError(
                    "Databricks provider requires 'workspace_url' and 'token' "
                    "in config or DATABRICKS_WORKSPACE_URL and DATABRICKS_TOKEN env vars"
                )

            return DatabricksProviderAdapter(
                workspace_url=workspace_url,
                token=token,
            )

        elif provider_name == "aws_redshift":
            # AWS Redshift requires aws credentials, region, and either cluster_identifier or workgroup_name
            region = config.get("region") or os.getenv("AWS_REGION", "us-east-1")
            aws_access_key_id = config.get("aws_access_key_id") or os.getenv(
                "AWS_ACCESS_KEY_ID"
            )
            aws_secret_access_key = config.get("aws_secret_access_key") or os.getenv(
                "AWS_SECRET_ACCESS_KEY"
            )

            if not aws_access_key_id or not aws_secret_access_key:
                raise ValueError(
                    "AWS Redshift provider requires 'aws_access_key_id' and 'aws_secret_access_key' "
                    "in config or AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env vars"
                )

            if not config.get("cluster_identifier") and not config.get(
                "workgroup_name"
            ):
                raise ValueError(
                    "AWS Redshift provider requires either 'cluster_identifier' or 'workgroup_name'"
                )

            return RedshiftProviderAdapter(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region=region,
                cluster_identifier=config.get("cluster_identifier"),
                workgroup_name=config.get("workgroup_name"),
                database=config.get("database", "dev"),
                s3_bucket=config.get("s3_bucket"),
                redshift_iam_role=config.get("redshift_iam_role"),
                emr_cluster_id=config.get("emr_cluster_id"),
            )

        else:
            raise ValueError(
                f"Unknown data provider '{provider_name}'. "
                f"Supported: {', '.join(DataProviderFactory._SUPPORTED_PROVIDERS)}"
            )

    @staticmethod
    def get_available_providers() -> list:
        """Get list of supported data provider names."""
        return DataProviderFactory._SUPPORTED_PROVIDERS.copy()
