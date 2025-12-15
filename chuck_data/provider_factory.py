"""Provider Factory for creating provider types.

This factory creates instances of:
- Data Providers (where data lives): Databricks, Redshift
  Note: Data providers also handle uploading artifacts (manifests, init scripts)
        to their appropriate storage (Volumes for Databricks, S3 for Redshift)
- Compute Providers (where Stitch runs): Databricks, EMR
"""

from typing import Dict, Any, Optional
import os

from chuck_data.compute_providers import (
    ComputeProvider,
    DatabricksComputeProvider,
    EMRComputeProvider,
)
from chuck_data.data_providers import DataProvider, DataProviderFactory


class ProviderFactory:
    """Central factory for creating all provider types."""

    @staticmethod
    def create_data_provider(
        provider_type: Optional[str] = None, **kwargs
    ) -> DataProvider:
        """Create a data provider (where data lives).

        This is a convenience wrapper around DataProviderFactory.create().

        Args:
            provider_type: Type of data provider:
                - "databricks": Databricks Unity Catalog
                - "aws_redshift": AWS Redshift
                - None: Use default from env or config
            **kwargs: Provider-specific configuration

        Returns:
            DataProvider instance

        Raises:
            ValueError: If provider_type is not supported or required config is missing

        Examples:
            >>> provider = ProviderFactory.create_data_provider(
            ...     "databricks",
            ...     workspace_url="https://...",
            ...     token="..."
            ... )
        """
        return DataProviderFactory.create(provider_name=provider_type, **kwargs)

    @staticmethod
    def create_compute_provider(
        provider_type: str, config: Optional[Dict[str, Any]] = None
    ) -> ComputeProvider:
        """Create a compute provider (where Stitch jobs run).

        IMPORTANT: This is independent of the data provider!

        Args:
            provider_type: Type of compute provider:
                - "databricks": Runs on Databricks clusters
                - "aws_emr": Runs on EMR clusters
            config: Provider configuration dictionary

        Returns:
            ComputeProvider instance

        Raises:
            ValueError: If provider_type is not supported

        Examples:
            >>> provider = ProviderFactory.create_compute_provider(
            ...     "databricks",
            ...     {"workspace_url": "https://...", "token": "..."}
            ... )
        """
        if config is None:
            config = {}

        if provider_type == "databricks":
            workspace_url = config.get("workspace_url") or os.getenv(
                "DATABRICKS_WORKSPACE_URL"
            )
            token = config.get("token") or os.getenv("DATABRICKS_TOKEN")

            if not workspace_url or not token:
                raise ValueError(
                    "Databricks compute provider requires 'workspace_url' and 'token' "
                    "in config or DATABRICKS_WORKSPACE_URL and DATABRICKS_TOKEN env vars"
                )

            return DatabricksComputeProvider(
                workspace_url=workspace_url,
                token=token,
                **{
                    k: v
                    for k, v in config.items()
                    if k not in ["workspace_url", "token"]
                },
            )

        elif provider_type == "aws_emr":
            region = config.get("region") or os.getenv("AWS_REGION", "us-east-1")

            return EMRComputeProvider(
                region=region,
                cluster_id=config.get("cluster_id"),
                aws_profile=config.get("aws_profile") or os.getenv("AWS_PROFILE"),
                s3_bucket=config.get("s3_bucket"),
                **{
                    k: v
                    for k, v in config.items()
                    if k not in ["region", "cluster_id", "aws_profile", "s3_bucket"]
                },
            )

        else:
            raise ValueError(
                f"Unknown compute provider: {provider_type}. "
                f"Supported providers: databricks, aws_emr"
            )
