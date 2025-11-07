"""Tests for DatabricksProvider."""

import pytest
from unittest.mock import patch, MagicMock
from chuck_data.llm.providers.databricks import DatabricksProvider


class TestDatabricksProviderListModels:
    """Test DatabricksProvider.list_models() method."""

    @patch("chuck_data.llm.providers.databricks.DatabricksAPIClient")
    @patch("chuck_data.llm.providers.databricks.get_databricks_token")
    @patch("chuck_data.llm.providers.databricks.get_workspace_url")
    def test_list_models_returns_model_info_list(
        self, mock_get_workspace, mock_get_token, mock_client_class
    ):
        """list_models() returns list of ModelInfo dicts."""
        # Setup mocks
        mock_get_workspace.return_value = "https://test.databricks.com"
        mock_get_token.return_value = "test-token"

        # Mock API client response
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.list_models.return_value = [
            {
                "name": "test-model-1",
                "state": {"ready": "READY"},
                "config": {
                    "served_entities": [{"entity_name": "databricks-meta-llama-3"}]
                },
            },
            {
                "name": "test-model-2",
                "state": {"ready": "NOT_READY"},
                "config": {"served_entities": [{"entity_name": "databricks-dbrx"}]},
            },
        ]

        # Create provider and call list_models
        provider = DatabricksProvider()
        models = provider.list_models()

        # Verify results
        assert len(models) == 2

        # Check first model
        assert models[0]["model_id"] == "test-model-1"
        assert models[0]["model_name"] == "test-model-1"
        assert models[0]["provider_name"] == "databricks"
        assert models[0]["state"] == "READY"
        assert models[0]["endpoint_type"] == "databricks-meta-llama-3"
        assert models[0]["supports_tool_use"] is True

        # Check second model
        assert models[1]["model_id"] == "test-model-2"
        assert models[1]["model_name"] == "test-model-2"
        assert models[1]["provider_name"] == "databricks"
        assert models[1]["state"] == "NOT_READY"
        assert models[1]["supports_tool_use"] is True

    @patch("chuck_data.llm.providers.databricks.DatabricksAPIClient")
    @patch("chuck_data.llm.providers.databricks.get_databricks_token")
    @patch("chuck_data.llm.providers.databricks.get_workspace_url")
    def test_list_models_empty_list(
        self, mock_get_workspace, mock_get_token, mock_client_class
    ):
        """list_models() returns empty list when no models available."""
        # Setup mocks
        mock_get_workspace.return_value = "https://test.databricks.com"
        mock_get_token.return_value = "test-token"

        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.list_models.return_value = []

        # Create provider and call list_models
        provider = DatabricksProvider()
        models = provider.list_models()

        # Verify empty list
        assert models == []

    @patch("chuck_data.llm.providers.databricks.DatabricksAPIClient")
    @patch("chuck_data.llm.providers.databricks.get_databricks_token")
    @patch("chuck_data.llm.providers.databricks.get_workspace_url")
    def test_list_models_no_served_entities(
        self, mock_get_workspace, mock_get_token, mock_client_class
    ):
        """list_models() filters models without tool calling support by default."""
        # Setup mocks
        mock_get_workspace.return_value = "https://test.databricks.com"
        mock_get_token.return_value = "test-token"

        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.list_models.return_value = [
            {
                "name": "test-model-with-entities",
                "state": {"ready": "READY"},
                "config": {"served_entities": [{"entity_name": "databricks-llama"}]},
            },
            {
                "name": "test-model-no-entities",
                "state": {"ready": "READY"},
                "config": {"served_entities": []},  # Empty entities = no tool calling
            },
        ]

        # Create provider and call list_models
        provider = DatabricksProvider()

        # Default: only tool-calling models (tool_calling_only=True)
        models = provider.list_models()

        # Should only return model with served entities
        assert len(models) == 1
        assert models[0]["model_id"] == "test-model-with-entities"
        assert models[0]["supports_tool_use"] is True

        # With tool_calling_only=False: should return all models
        all_models = provider.list_models(tool_calling_only=False)
        assert len(all_models) == 2
        assert all_models[0]["supports_tool_use"] is True
        assert all_models[1]["supports_tool_use"] is False

    @patch("chuck_data.llm.providers.databricks.DatabricksAPIClient")
    @patch("chuck_data.llm.providers.databricks.get_databricks_token")
    @patch("chuck_data.llm.providers.databricks.get_workspace_url")
    def test_list_models_uses_provider_credentials(
        self, mock_get_workspace, mock_get_token, mock_client_class
    ):
        """list_models() uses provider's workspace_url and token."""
        # Setup mocks
        mock_get_workspace.return_value = "https://default.databricks.com"
        mock_get_token.return_value = "default-token"

        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.list_models.return_value = []

        # Create provider with explicit credentials
        provider = DatabricksProvider(
            workspace_url="https://custom.databricks.com", token="custom-token"
        )
        provider.list_models()

        # Verify client was created with provider's credentials
        mock_client_class.assert_called_once_with(
            workspace_url="https://custom.databricks.com", token="custom-token"
        )

    @patch("chuck_data.llm.providers.databricks.DatabricksAPIClient")
    @patch("chuck_data.llm.providers.databricks.get_databricks_token")
    @patch("chuck_data.llm.providers.databricks.get_workspace_url")
    def test_list_models_api_error_propagates(
        self, mock_get_workspace, mock_get_token, mock_client_class
    ):
        """list_models() propagates API errors."""
        # Setup mocks
        mock_get_workspace.return_value = "https://test.databricks.com"
        mock_get_token.return_value = "test-token"

        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.list_models.side_effect = ValueError("API error")

        # Create provider and verify error propagates
        provider = DatabricksProvider()
        with pytest.raises(ValueError, match="API error"):
            provider.list_models()
