"""Unit tests for DataProviderFactory."""

import pytest
from chuck_data.data_providers.factory import DataProviderFactory
from chuck_data.data_providers.adapters import (
    DatabricksProviderAdapter,
    RedshiftProviderAdapter,
)


class TestDataProviderFactory:
    """Tests for DataProviderFactory."""

    def test_create_databricks_provider(self):
        """Test creating a Databricks data provider."""
        provider = DataProviderFactory.create(
            provider_name="databricks",
            workspace_url="https://test.databricks.com",
            token="test-token",
        )
        assert isinstance(provider, DatabricksProviderAdapter)
        assert provider.workspace_url == "https://test.databricks.com"
        assert provider.token == "test-token"

    def test_create_redshift_provider_with_cluster(self):
        """Test creating a Redshift data provider with cluster."""
        provider = DataProviderFactory.create(
            provider_name="aws_redshift",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )
        assert isinstance(provider, RedshiftProviderAdapter)
        assert provider.aws_access_key_id == "test-key"
        assert provider.aws_secret_access_key == "test-secret"
        assert provider.region == "us-west-2"
        assert provider.cluster_identifier == "test-cluster"

    def test_create_redshift_provider_with_workgroup(self):
        """Test creating a Redshift data provider with workgroup."""
        provider = DataProviderFactory.create(
            provider_name="aws_redshift",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            workgroup_name="test-workgroup",
        )
        assert isinstance(provider, RedshiftProviderAdapter)
        assert provider.aws_access_key_id == "test-key"
        assert provider.aws_secret_access_key == "test-secret"
        assert provider.region == "us-west-2"
        assert provider.workgroup_name == "test-workgroup"

    def test_create_databricks_provider_missing_config(self):
        """Test that missing required config raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            DataProviderFactory.create(provider_name="databricks")

        assert "workspace_url" in str(exc_info.value)
        assert "token" in str(exc_info.value)

    def test_create_redshift_provider_missing_credentials(self):
        """Test that missing AWS credentials raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            DataProviderFactory.create(
                provider_name="aws_redshift",
                region="us-west-2",
                cluster_identifier="test-cluster",
            )

        assert "aws_access_key_id" in str(exc_info.value)
        assert "aws_secret_access_key" in str(exc_info.value)

    def test_create_redshift_provider_missing_identifier(self):
        """Test that missing cluster_identifier or workgroup_name raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            DataProviderFactory.create(
                provider_name="aws_redshift",
                aws_access_key_id="test-key",
                aws_secret_access_key="test-secret",
                region="us-west-2",
            )

        assert "cluster_identifier" in str(exc_info.value) or "workgroup_name" in str(
            exc_info.value
        )

    def test_create_unknown_provider(self):
        """Test that unknown provider type raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            DataProviderFactory.create(provider_name="unknown")

        assert "Unknown data provider" in str(exc_info.value)
        assert "unknown" in str(exc_info.value)

    def test_create_databricks_provider_from_env(self, monkeypatch):
        """Test creating Databricks provider from environment variables."""
        monkeypatch.setenv("DATABRICKS_WORKSPACE_URL", "https://env.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "env-token")

        provider = DataProviderFactory.create(provider_name="databricks")
        assert provider.workspace_url == "https://env.databricks.com"
        assert provider.token == "env-token"

    def test_create_databricks_config_overrides_env(self, monkeypatch):
        """Test that config parameters override environment variables."""
        monkeypatch.setenv("DATABRICKS_WORKSPACE_URL", "https://env.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "env-token")

        provider = DataProviderFactory.create(
            provider_name="databricks",
            workspace_url="https://config.databricks.com",
            token="config-token",
        )
        assert provider.workspace_url == "https://config.databricks.com"
        assert provider.token == "config-token"

    def test_resolve_provider_name_explicit(self):
        """Test that explicit provider_name takes precedence."""
        provider = DataProviderFactory.create(
            provider_name="databricks",
            workspace_url="https://test.databricks.com",
            token="test-token",
        )
        assert isinstance(provider, DatabricksProviderAdapter)

    def test_resolve_provider_name_from_env(self, monkeypatch):
        """Test resolving provider name from CHUCK_DATA_PROVIDER env var."""
        monkeypatch.setenv("CHUCK_DATA_PROVIDER", "databricks")
        monkeypatch.setenv("DATABRICKS_WORKSPACE_URL", "https://test.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "test-token")

        provider = DataProviderFactory.create()
        assert isinstance(provider, DatabricksProviderAdapter)

    def test_resolve_provider_name_default(self, monkeypatch):
        """Test that default provider is databricks."""
        monkeypatch.delenv("CHUCK_DATA_PROVIDER", raising=False)
        monkeypatch.setenv("DATABRICKS_WORKSPACE_URL", "https://test.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "test-token")

        provider = DataProviderFactory.create()
        assert isinstance(provider, DatabricksProviderAdapter)

    def test_get_available_providers(self):
        """Test getting list of available providers."""
        providers = DataProviderFactory.get_available_providers()
        assert "databricks" in providers
        assert "aws_redshift" in providers
        assert len(providers) == 2
