"""Unit tests for ProviderFactory."""

import pytest
from chuck_data.provider_factory import ProviderFactory
from chuck_data.compute_providers import DatabricksComputeProvider, EMRComputeProvider
from chuck_data.storage_providers import DatabricksVolumeStorage, S3Storage
from chuck_data.data_providers.adapters import (
    DatabricksProviderAdapter,
    RedshiftProviderAdapter,
)


class TestProviderFactoryDataProviders:
    """Tests for ProviderFactory data provider creation."""

    def test_create_databricks_data_provider(self):
        """Test creating a Databricks data provider."""
        provider = ProviderFactory.create_data_provider(
            "databricks",
            workspace_url="https://test.databricks.com",
            token="test-token",
        )
        assert isinstance(provider, DatabricksProviderAdapter)
        assert provider.client is not None

    def test_create_redshift_data_provider(self):
        """Test creating a Redshift data provider."""
        provider = ProviderFactory.create_data_provider(
            "aws_redshift",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )
        assert isinstance(provider, RedshiftProviderAdapter)
        assert provider.client is not None
        assert provider.redshift_iam_role is None
        assert provider.emr_cluster_id is None


class TestProviderFactoryComputeProviders:
    """Tests for ProviderFactory compute provider creation."""

    def test_create_databricks_compute_provider(self):
        """Test creating a Databricks compute provider."""
        provider = ProviderFactory.create_compute_provider(
            "databricks",
            {"workspace_url": "https://test.databricks.com", "token": "test-token"},
        )
        assert isinstance(provider, DatabricksComputeProvider)
        assert provider.workspace_url == "https://test.databricks.com"
        assert provider.token == "test-token"

    def test_create_emr_compute_provider(self, monkeypatch):
        """Test creating an EMR compute provider."""
        from unittest.mock import Mock
        import chuck_data.storage_providers.s3 as s3_module

        # Mock boto3 to avoid requiring real AWS profile
        mock_boto3 = Mock()
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session
        monkeypatch.setattr(s3_module, "boto3", mock_boto3)

        provider = ProviderFactory.create_compute_provider(
            "aws_emr", {"region": "us-west-2", "aws_profile": "test-profile"}
        )
        assert isinstance(provider, EMRComputeProvider)
        assert provider.region == "us-west-2"
        assert provider.aws_profile == "test-profile"

    def test_create_emr_compute_provider_defaults(self, monkeypatch):
        """Test creating EMR compute provider with minimal config."""
        from unittest.mock import Mock
        import chuck_data.storage_providers.s3 as s3_module

        # Mock boto3 to avoid requiring real AWS credentials
        mock_boto3 = Mock()
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session
        monkeypatch.setattr(s3_module, "boto3", mock_boto3)

        # Clear AWS_REGION to test the default
        monkeypatch.delenv("AWS_REGION", raising=False)

        provider = ProviderFactory.create_compute_provider("aws_emr", {})
        assert isinstance(provider, EMRComputeProvider)
        assert provider.region == "us-east-1"  # Default region

    def test_create_compute_provider_unknown_type(self):
        """Test that unknown provider type raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            ProviderFactory.create_compute_provider("unknown", {})

        assert "Unknown compute provider: unknown" in str(exc_info.value)
        assert "databricks, aws_emr" in str(exc_info.value)

    def test_create_databricks_compute_missing_config(self):
        """Test that missing required config raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            ProviderFactory.create_compute_provider("databricks", {})

        assert "workspace_url" in str(exc_info.value)
        assert "token" in str(exc_info.value)

    def test_create_compute_provider_with_env_vars(self, monkeypatch):
        """Test that provider can be created from environment variables."""
        monkeypatch.setenv("DATABRICKS_WORKSPACE_URL", "https://env.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "env-token")

        provider = ProviderFactory.create_compute_provider("databricks")
        assert provider.workspace_url == "https://env.databricks.com"
        assert provider.token == "env-token"

    def test_create_compute_provider_config_overrides_env(self, monkeypatch):
        """Test that config takes precedence over environment variables."""
        monkeypatch.setenv("DATABRICKS_WORKSPACE_URL", "https://env.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "env-token")

        provider = ProviderFactory.create_compute_provider(
            "databricks",
            {"workspace_url": "https://config.databricks.com", "token": "config-token"},
        )
        assert provider.workspace_url == "https://config.databricks.com"
        assert provider.token == "config-token"


class TestProviderFactoryStorageProviders:
    """Tests for ProviderFactory storage provider creation."""

    def test_create_databricks_storage_provider(self):
        """Test creating a Databricks storage provider."""
        provider = ProviderFactory.create_storage_provider(
            "databricks",
            {"workspace_url": "https://test.databricks.com", "token": "test-token"},
        )
        assert isinstance(provider, DatabricksVolumeStorage)
        assert provider.workspace_url == "https://test.databricks.com"
        assert provider.token == "test-token"

    def test_create_s3_storage_provider(self, monkeypatch):
        """Test creating an S3 storage provider."""
        from unittest.mock import Mock
        import chuck_data.storage_providers.s3 as s3_module

        # Mock boto3 to avoid requiring real AWS credentials
        mock_boto3 = Mock()
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session
        monkeypatch.setattr(s3_module, "boto3", mock_boto3)

        provider = ProviderFactory.create_storage_provider(
            "s3", {"region": "us-west-2", "aws_profile": "test-profile"}
        )
        assert isinstance(provider, S3Storage)
        assert provider.region == "us-west-2"
        assert provider.aws_profile == "test-profile"

    def test_create_s3_storage_provider_defaults(self, monkeypatch):
        """Test creating S3 storage provider with minimal config."""
        from unittest.mock import Mock
        import chuck_data.storage_providers.s3 as s3_module

        # Mock boto3 to avoid requiring real AWS credentials
        mock_boto3 = Mock()
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session
        monkeypatch.setattr(s3_module, "boto3", mock_boto3)

        # Clear AWS_REGION to test the default
        monkeypatch.delenv("AWS_REGION", raising=False)

        provider = ProviderFactory.create_storage_provider("s3", {})
        assert isinstance(provider, S3Storage)
        assert provider.region == "us-east-1"  # Default region

    def test_create_storage_provider_unknown_type(self):
        """Test that unknown provider type raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            ProviderFactory.create_storage_provider("unknown", {})

        assert "Unknown storage provider: unknown" in str(exc_info.value)
        assert "databricks, s3" in str(exc_info.value)

    def test_create_databricks_storage_missing_config(self):
        """Test that missing required config raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            ProviderFactory.create_storage_provider("databricks", {})

        assert "workspace_url" in str(exc_info.value)
        assert "token" in str(exc_info.value)

    def test_create_storage_provider_with_env_vars(self, monkeypatch):
        """Test that storage provider can be created from environment variables."""
        monkeypatch.setenv("DATABRICKS_WORKSPACE_URL", "https://env.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "env-token")

        provider = ProviderFactory.create_storage_provider("databricks")
        assert provider.workspace_url == "https://env.databricks.com"
        assert provider.token == "env-token"

    def test_create_storage_provider_config_overrides_env(self, monkeypatch):
        """Test that config takes precedence over environment variables."""
        monkeypatch.setenv("DATABRICKS_WORKSPACE_URL", "https://env.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "env-token")

        provider = ProviderFactory.create_storage_provider(
            "databricks",
            {"workspace_url": "https://config.databricks.com", "token": "config-token"},
        )
        assert provider.workspace_url == "https://config.databricks.com"
        assert provider.token == "config-token"

    def test_create_s3_storage_with_explicit_credentials(self, monkeypatch):
        """Test creating S3 storage with explicit credentials."""
        from unittest.mock import Mock
        import chuck_data.storage_providers.s3 as s3_module

        # Mock boto3 to avoid requiring real AWS credentials
        mock_boto3 = Mock()
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session
        monkeypatch.setattr(s3_module, "boto3", mock_boto3)

        provider = ProviderFactory.create_storage_provider(
            "s3",
            {
                "region": "us-west-2",
                "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
                "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            },
        )
        assert isinstance(provider, S3Storage)
        assert provider.region == "us-west-2"
