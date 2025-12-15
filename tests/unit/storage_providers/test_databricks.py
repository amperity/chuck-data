"""Unit tests for DatabricksVolumeStorage."""

import pytest
from unittest.mock import Mock, patch
from chuck_data.storage_providers.databricks import DatabricksVolumeStorage


class TestDatabricksVolumeStorageInit:
    """Tests for DatabricksVolumeStorage initialization."""

    def test_can_instantiate_with_credentials(self):
        """Test that DatabricksVolumeStorage can be instantiated with credentials."""
        storage = DatabricksVolumeStorage(
            workspace_url="https://test.cloud.databricks.com",
            token="test-token",
        )
        assert storage.workspace_url == "https://test.cloud.databricks.com"
        assert storage.token == "test-token"
        assert storage.client is not None

    def test_can_instantiate_with_existing_client(self):
        """Test that DatabricksVolumeStorage can reuse an existing client."""
        mock_client = Mock()
        storage = DatabricksVolumeStorage(
            workspace_url="https://test.cloud.databricks.com",
            token="test-token",
            client=mock_client,
        )
        assert storage.client is mock_client

    def test_creates_new_client_if_not_provided(self):
        """Test that a new DatabricksAPIClient is created when not provided."""
        with patch(
            "chuck_data.storage_providers.databricks.DatabricksAPIClient"
        ) as MockClient:
            storage = DatabricksVolumeStorage(
                workspace_url="https://test.cloud.databricks.com",
                token="test-token",
            )
            MockClient.assert_called_once_with(
                workspace_url="https://test.cloud.databricks.com",
                token="test-token",
            )
            assert storage.client == MockClient.return_value


class TestUploadFile:
    """Tests for upload_file method."""

    def test_upload_file_success(self):
        """Test successful file upload."""
        mock_client = Mock()
        mock_client.upload_file.return_value = True

        storage = DatabricksVolumeStorage(
            workspace_url="https://test.cloud.databricks.com",
            token="test-token",
            client=mock_client,
        )

        result = storage.upload_file(
            content="test content",
            path="/Volumes/catalog/schema/volume/test.json",
        )

        assert result is True
        mock_client.upload_file.assert_called_once_with(
            path="/Volumes/catalog/schema/volume/test.json",
            content="test content",
            overwrite=True,
        )

    def test_upload_file_with_overwrite_false(self):
        """Test file upload with overwrite=False."""
        mock_client = Mock()
        mock_client.upload_file.return_value = True

        storage = DatabricksVolumeStorage(
            workspace_url="https://test.cloud.databricks.com",
            token="test-token",
            client=mock_client,
        )

        result = storage.upload_file(
            content="test content",
            path="/Volumes/catalog/schema/volume/test.json",
            overwrite=False,
        )

        assert result is True
        mock_client.upload_file.assert_called_once_with(
            path="/Volumes/catalog/schema/volume/test.json",
            content="test content",
            overwrite=False,
        )

    def test_upload_file_failure(self):
        """Test file upload failure."""
        mock_client = Mock()
        mock_client.upload_file.return_value = False

        storage = DatabricksVolumeStorage(
            workspace_url="https://test.cloud.databricks.com",
            token="test-token",
            client=mock_client,
        )

        result = storage.upload_file(
            content="test content",
            path="/Volumes/catalog/schema/volume/test.json",
        )

        assert result is False

    def test_upload_file_raises_exception_on_client_error(self):
        """Test that exceptions from client are properly raised."""
        mock_client = Mock()
        mock_client.upload_file.side_effect = Exception("API Error")

        storage = DatabricksVolumeStorage(
            workspace_url="https://test.cloud.databricks.com",
            token="test-token",
            client=mock_client,
        )

        with pytest.raises(Exception) as exc_info:
            storage.upload_file(
                content="test content",
                path="/Volumes/catalog/schema/volume/test.json",
            )

        assert "Failed to upload file to Databricks Volume" in str(exc_info.value)
        assert "/Volumes/catalog/schema/volume/test.json" in str(exc_info.value)
        assert "API Error" in str(exc_info.value)

    def test_upload_file_with_json_content(self):
        """Test uploading JSON manifest content."""
        mock_client = Mock()
        mock_client.upload_file.return_value = True

        storage = DatabricksVolumeStorage(
            workspace_url="https://test.cloud.databricks.com",
            token="test-token",
            client=mock_client,
        )

        json_content = '{"tables": [{"name": "customers", "semantic_tags": ["email"]}]}'
        result = storage.upload_file(
            content=json_content,
            path="/Volumes/stitch/dev/artifacts/manifest.json",
        )

        assert result is True
        mock_client.upload_file.assert_called_once_with(
            path="/Volumes/stitch/dev/artifacts/manifest.json",
            content=json_content,
            overwrite=True,
        )

    def test_upload_file_with_script_content(self):
        """Test uploading init script content."""
        mock_client = Mock()
        mock_client.upload_file.return_value = True

        storage = DatabricksVolumeStorage(
            workspace_url="https://test.cloud.databricks.com",
            token="test-token",
            client=mock_client,
        )

        script_content = "#!/bin/bash\necho 'Initializing cluster'\n"
        result = storage.upload_file(
            content=script_content,
            path="/Volumes/stitch/dev/scripts/init.sh",
        )

        assert result is True
        mock_client.upload_file.assert_called_once_with(
            path="/Volumes/stitch/dev/scripts/init.sh",
            content=script_content,
            overwrite=True,
        )


class TestDatabricksVolumeStorageInterface:
    """Tests for DatabricksVolumeStorage interface compatibility."""

    def test_conforms_to_storage_provider_protocol(self):
        """Test that DatabricksVolumeStorage implements StorageProvider protocol."""
        from chuck_data.storage_providers.protocol import StorageProvider
        import inspect

        storage = DatabricksVolumeStorage(
            workspace_url="https://test.cloud.databricks.com",
            token="test-token",
        )

        # Check that upload_file method exists
        assert hasattr(storage, "upload_file")
        assert callable(storage.upload_file)

        # Check method signature
        sig = inspect.signature(storage.upload_file)
        params = list(sig.parameters.keys())
        assert "content" in params
        assert "path" in params
        assert "overwrite" in params

    def test_upload_file_returns_bool(self):
        """Test that upload_file returns a boolean."""
        mock_client = Mock()
        mock_client.upload_file.return_value = True

        storage = DatabricksVolumeStorage(
            workspace_url="https://test.cloud.databricks.com",
            token="test-token",
            client=mock_client,
        )

        result = storage.upload_file(
            content="test",
            path="/Volumes/test/test/test/test.txt",
        )

        assert isinstance(result, bool)
