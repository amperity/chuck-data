"""Tests for Data Provider Factory."""

import pytest
import tempfile
from unittest.mock import patch
from chuck_data.data.factory import DataProviderFactory
from chuck_data.data.provider import DataProvider
from chuck_data.config import ConfigManager


class TestDataProviderFactory:
    """Tests for DataProviderFactory."""

    def test_factory_creates_databricks_provider_by_default(self):
        """Factory creates Databricks provider when no config specified."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(
                '{"workspace_url": "https://test.databricks.com", "databricks_token": "test-token"}'
            )
            temp_path = f.name

        with patch("chuck_data.config.get_config_manager") as mock_get_cm:
            mock_cm = ConfigManager(temp_path)
            mock_get_cm.return_value = mock_cm

            provider = DataProviderFactory.create()
            assert provider.get_provider_name() == "databricks"
            assert isinstance(provider, DataProvider)

    def test_factory_respects_explicit_provider_name(self):
        """Explicit provider_name parameter takes precedence."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(
                '{"workspace_url": "https://test.databricks.com", "databricks_token": "test-token"}'
            )
            temp_path = f.name

        with patch("chuck_data.config.get_config_manager") as mock_get_cm:
            mock_cm = ConfigManager(temp_path)
            mock_get_cm.return_value = mock_cm

            provider = DataProviderFactory.create(provider_name="databricks")
            assert provider.get_provider_name() == "databricks"

    def test_factory_resolves_from_environment_variable(self):
        """CHUCK_DATA_PROVIDER environment variable is respected."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(
                '{"workspace_url": "https://test.databricks.com", "databricks_token": "test-token"}'
            )
            temp_path = f.name

        with patch.dict("os.environ", {"CHUCK_DATA_PROVIDER": "databricks"}):
            with patch("chuck_data.config.get_config_manager") as mock_get_cm:
                mock_cm = ConfigManager(temp_path)
                mock_get_cm.return_value = mock_cm

                provider = DataProviderFactory.create()
                assert provider.get_provider_name() == "databricks"

    def test_factory_raises_error_for_unknown_provider(self):
        """Factory raises ValueError for unsupported provider."""
        with pytest.raises(ValueError, match="Unknown data provider 'nonexistent'"):
            DataProviderFactory.create(provider_name="nonexistent")

    def test_factory_lists_available_providers(self):
        """Factory returns list of supported providers."""
        providers = DataProviderFactory.get_available_providers()
        assert "databricks" in providers
        assert isinstance(providers, list)

    def test_factory_resolves_precedence_explicit_over_env(self):
        """Explicit provider_name takes precedence over environment."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(
                '{"workspace_url": "https://test.databricks.com", "databricks_token": "test-token"}'
            )
            temp_path = f.name

        with patch.dict("os.environ", {"CHUCK_DATA_PROVIDER": "something_else"}):
            with patch("chuck_data.config.get_config_manager") as mock_get_cm:
                mock_cm = ConfigManager(temp_path)
                mock_get_cm.return_value = mock_cm

                # Explicit parameter should win
                provider = DataProviderFactory.create(provider_name="databricks")
                assert provider.get_provider_name() == "databricks"

    def test_factory_loads_provider_specific_config(self):
        """Factory loads provider-specific configuration from config file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(
                """{
                "workspace_url": "https://test.databricks.com",
                "databricks_token": "test-token",
                "data_provider": "databricks",
                "data_provider_config": {
                    "databricks": {
                        "workspace_url": "https://configured.databricks.com",
                        "token": "configured-token"
                    }
                }
            }"""
            )
            temp_path = f.name

        with patch("chuck_data.config.get_config_manager") as mock_get_cm:
            mock_cm = ConfigManager(temp_path)
            mock_get_cm.return_value = mock_cm

            # Should load config from data_provider_config
            provider = DataProviderFactory.create()
            assert provider.get_provider_name() == "databricks"

    def test_factory_allows_kwargs_to_override_config(self):
        """Factory allows kwargs to override config values."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(
                """{
                "workspace_url": "https://config.databricks.com",
                "databricks_token": "config-token"
            }"""
            )
            temp_path = f.name

        with patch("chuck_data.config.get_config_manager") as mock_get_cm:
            mock_cm = ConfigManager(temp_path)
            mock_get_cm.return_value = mock_cm

            # Passing explicit kwargs should work
            provider = DataProviderFactory.create(
                provider_name="databricks",
                workspace_url="https://override.databricks.com",
                token="override-token",
            )
            assert provider.get_provider_name() == "databricks"
