"""Tests for DataProviderFactory."""

import pytest
import os
from unittest.mock import patch, MagicMock
from chuck_data.data_providers.factory import DataProviderFactory


class TestDataProviderFactory:
    """Test data provider factory behavior."""

    def test_get_available_providers(self):
        """Factory returns list of supported providers."""
        providers = DataProviderFactory.get_available_providers()
        assert "databricks" in providers
        assert "aws_redshift" in providers
        assert len(providers) == 2

    def test_unknown_provider_raises_error(self):
        """Factory raises ValueError for unknown provider."""
        with pytest.raises(ValueError, match="Unknown data provider"):
            DataProviderFactory.create("nonexistent_provider")

    @patch.dict(os.environ, {}, clear=True)
    def test_provider_selection_precedence_default(self):
        """Default provider is databricks when no config."""
        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.get_config.return_value = MagicMock(
                data_provider=None
            )

            provider_name = DataProviderFactory._resolve_provider_name()
            assert provider_name == "databricks"

    @patch.dict(os.environ, {"CHUCK_DATA_PROVIDER": "aws_redshift"}, clear=True)
    def test_provider_selection_precedence_env_var(self):
        """Environment variable overrides config."""
        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.get_config.return_value = MagicMock(
                data_provider="databricks"
            )

            provider_name = DataProviderFactory._resolve_provider_name()
            assert provider_name == "aws_redshift"

    def test_provider_selection_precedence_explicit(self):
        """Explicit parameter has highest priority."""
        with patch.dict(os.environ, {"CHUCK_DATA_PROVIDER": "databricks"}, clear=True):
            provider_name = DataProviderFactory._resolve_provider_name("aws_redshift")
            assert provider_name == "aws_redshift"

    def test_get_provider_config_returns_empty_dict_on_error(self):
        """Provider config returns empty dict when config unavailable."""
        with patch(
            "chuck_data.config.get_config_manager", side_effect=Exception("No config")
        ):
            config = DataProviderFactory._get_provider_config("databricks")
            assert config == {}

    def test_get_provider_config_loads_from_config_file(self):
        """Provider config loads correctly from config file."""
        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.get_config.return_value = MagicMock(
                data_provider_config={
                    "databricks": {
                        "workspace_url": "https://test.databricks.com",
                        "token": "test_token",
                    }
                }
            )

            config = DataProviderFactory._get_provider_config("databricks")
            assert config["workspace_url"] == "https://test.databricks.com"
            assert config["token"] == "test_token"

    @patch("chuck_data.data_providers.adapters.DatabricksAPIClient")
    def test_create_databricks_provider(self, mock_client_class):
        """Factory can create Databricks provider."""
        mock_client_class.return_value = MagicMock()

        provider = DataProviderFactory.create(
            "databricks",
            workspace_url="https://test.databricks.com",
            token="test_token",
        )

        assert provider is not None
        from chuck_data.data_providers.adapters import DatabricksProviderAdapter

        assert isinstance(provider, DatabricksProviderAdapter)
        mock_client_class.assert_called_once_with(
            workspace_url="https://test.databricks.com", token="test_token"
        )

    def test_create_databricks_without_workspace_url_raises_error(self):
        """Factory raises ValueError when Databricks missing workspace_url."""
        with pytest.raises(ValueError, match="workspace_url"):
            DataProviderFactory.create("databricks", token="test_token")

    def test_create_databricks_without_token_raises_error(self):
        """Factory raises ValueError when Databricks missing token."""
        with pytest.raises(ValueError, match="token"):
            DataProviderFactory.create(
                "databricks", workspace_url="https://test.databricks.com"
            )

    @patch("chuck_data.clients.redshift.boto3")
    @patch("chuck_data.clients.redshift.RedshiftAPIClient")
    def test_create_redshift_provider_with_cluster(self, mock_client_class, mock_boto3):
        """Factory can create Redshift provider with cluster."""
        mock_boto3.client.return_value = MagicMock()
        mock_client_class.return_value = MagicMock()

        provider = DataProviderFactory.create(
            "aws_redshift",
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region="us-east-1",
            cluster_identifier="my-cluster",
        )

        assert provider is not None
        from chuck_data.data_providers.adapters import RedshiftProviderAdapter

        assert isinstance(provider, RedshiftProviderAdapter)

    @patch("chuck_data.clients.redshift.boto3")
    @patch("chuck_data.clients.redshift.RedshiftAPIClient")
    def test_create_redshift_provider_with_workgroup(
        self, mock_client_class, mock_boto3
    ):
        """Factory can create Redshift provider with workgroup."""
        mock_boto3.client.return_value = MagicMock()
        mock_client_class.return_value = MagicMock()

        provider = DataProviderFactory.create(
            "aws_redshift",
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region="us-east-1",
            workgroup_name="my-workgroup",
        )

        assert provider is not None
        from chuck_data.data_providers.adapters import RedshiftProviderAdapter

        assert isinstance(provider, RedshiftProviderAdapter)

    def test_create_redshift_without_aws_access_key_raises_error(self):
        """Factory raises ValueError when Redshift missing aws_access_key_id."""
        with pytest.raises(ValueError, match="aws_access_key_id"):
            DataProviderFactory.create(
                "aws_redshift",
                aws_secret_access_key="test_secret",
                region="us-east-1",
                cluster_identifier="my-cluster",
            )

    def test_create_redshift_without_aws_secret_key_raises_error(self):
        """Factory raises ValueError when Redshift missing aws_secret_access_key."""
        with pytest.raises(ValueError, match="aws_secret_access_key"):
            DataProviderFactory.create(
                "aws_redshift",
                aws_access_key_id="test_key",
                region="us-east-1",
                cluster_identifier="my-cluster",
            )

    def test_create_redshift_without_region_raises_error(self):
        """Factory raises ValueError when Redshift missing region."""
        with pytest.raises(ValueError, match="region"):
            DataProviderFactory.create(
                "aws_redshift",
                aws_access_key_id="test_key",
                aws_secret_access_key="test_secret",
                cluster_identifier="my-cluster",
            )

    def test_create_redshift_without_cluster_or_workgroup_raises_error(self):
        """Factory raises ValueError when Redshift missing both cluster and workgroup."""
        with pytest.raises(ValueError, match="cluster_identifier.*workgroup_name"):
            DataProviderFactory.create(
                "aws_redshift",
                aws_access_key_id="test_key",
                aws_secret_access_key="test_secret",
                region="us-east-1",
            )

    @patch("chuck_data.config.get_config_manager")
    @patch("chuck_data.clients.redshift.boto3")
    @patch("chuck_data.clients.redshift.RedshiftAPIClient")
    def test_create_redshift_provider_with_config(
        self, mock_client_class, mock_boto3, mock_config
    ):
        """Factory passes configuration to Redshift provider."""
        mock_boto3.client.return_value = MagicMock()
        mock_client_class.return_value = MagicMock()

        # Mock config with Redshift provider settings
        mock_config.return_value.get_config.return_value = MagicMock(
            data_provider="aws_redshift",
            data_provider_config={
                "aws_redshift": {
                    "aws_access_key_id": "config_key",
                    "aws_secret_access_key": "config_secret",
                    "region": "us-west-2",
                    "cluster_identifier": "config-cluster",
                    "database": "prod",
                    "s3_bucket": "my-bucket",
                }
            },
        )

        provider = DataProviderFactory.create("aws_redshift")

        assert provider is not None
        from chuck_data.data_providers.adapters import RedshiftProviderAdapter

        assert isinstance(provider, RedshiftProviderAdapter)

    @patch.dict(os.environ, {}, clear=True)
    @patch("chuck_data.data_providers.adapters.DatabricksAPIClient")
    def test_create_uses_default_provider(self, mock_client_class):
        """Factory uses default provider when none specified."""
        mock_client_class.return_value = MagicMock()

        with patch("chuck_data.config.get_config_manager") as mock_config:
            mock_config.return_value.get_config.return_value = MagicMock(
                data_provider=None
            )

            provider = DataProviderFactory.create(
                workspace_url="https://test.databricks.com", token="test_token"
            )

            assert provider is not None
            from chuck_data.data_providers.adapters import DatabricksProviderAdapter

            assert isinstance(provider, DatabricksProviderAdapter)

    def test_kwargs_override_config(self):
        """Explicit kwargs override config file values."""
        with patch("chuck_data.config.get_config_manager") as mock_config:
            with patch(
                "chuck_data.data_providers.adapters.DatabricksAPIClient"
            ) as mock_client_class:
                # Set up mock client to capture init parameters
                mock_client = MagicMock()
                mock_client_class.return_value = mock_client

                mock_config.return_value.get_config.return_value = MagicMock(
                    data_provider="databricks",
                    data_provider_config={
                        "databricks": {
                            "workspace_url": "https://config.databricks.com",
                            "token": "config_token",
                        }
                    },
                )

                provider = DataProviderFactory.create(
                    "databricks",
                    workspace_url="https://override.databricks.com",
                    token="override_token",
                )

                assert provider is not None
                # Verify the override URL was passed to the adapter (which passes to client)
                mock_client_class.assert_called_once_with(
                    workspace_url="https://override.databricks.com",
                    token="override_token",
                )
