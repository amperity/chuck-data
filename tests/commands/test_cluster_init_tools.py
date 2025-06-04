"""
Test cluster init tools upload functionality.
"""

import pytest
from unittest.mock import Mock, patch

from src.commands.cluster_init_tools import _helper_upload_cluster_init_logic


@pytest.fixture
def mock_client():
    """Create a mock Databricks client."""
    client = Mock()
    client.list_volumes.return_value = {"volumes": [{"name": "chuck"}]}
    client.upload_file.return_value = True
    return client


class TestUploadClusterInitLogic:
    """Test upload cluster init logic for stitch setup."""

    def test_upload_success(self, mock_client):
        """Test successful upload with versioned filename."""
        with patch("src.commands.cluster_init_tools.datetime") as mock_datetime:
            mock_datetime.datetime.now.return_value.strftime.return_value = (
                "2025-06-02_14-30"
            )

            result = _helper_upload_cluster_init_logic(
                client=mock_client,
                target_catalog="main",
                target_schema="default",
                init_script_content="#!/bin/bash\necho 'Hello World'",
            )

            assert result["success"] is True
            assert result["filename"] == "cluster_init-2025-06-02_14-30.sh"
            assert result["timestamp"] == "2025-06-02_14-30"
            assert (
                "/Volumes/main/default/chuck/cluster_init-2025-06-02_14-30.sh"
                == result["volume_path"]
            )

            mock_client.upload_file.assert_called_once_with(
                path="/Volumes/main/default/chuck/cluster_init-2025-06-02_14-30.sh",
                content="#!/bin/bash\necho 'Hello World'",
                overwrite=True,
            )

    def test_missing_catalog(self, mock_client):
        """Test error when catalog is missing."""
        result = _helper_upload_cluster_init_logic(
            client=mock_client,
            target_catalog="",
            target_schema="default",
            init_script_content="#!/bin/bash\necho 'test'",
        )

        assert "error" in result
        assert "catalog and schema are required" in result["error"]

    def test_missing_schema(self, mock_client):
        """Test error when schema is missing."""
        result = _helper_upload_cluster_init_logic(
            client=mock_client,
            target_catalog="main",
            target_schema="",
            init_script_content="#!/bin/bash\necho 'test'",
        )

        assert "error" in result
        assert "catalog and schema are required" in result["error"]

    def test_empty_script_content(self, mock_client):
        """Test error when script content is empty."""
        result = _helper_upload_cluster_init_logic(
            client=mock_client,
            target_catalog="main",
            target_schema="default",
            init_script_content="   ",
        )

        assert "error" in result
        assert "empty" in result["error"]

    def test_whitespace_only_script_content(self, mock_client):
        """Test error when script content is only whitespace."""
        result = _helper_upload_cluster_init_logic(
            client=mock_client,
            target_catalog="main",
            target_schema="default",
            init_script_content="\n\t  \n",
        )

        assert "error" in result
        assert "empty" in result["error"]

    def test_volume_creation_when_missing(self, mock_client):
        """Test volume creation when chuck volume doesn't exist."""
        mock_client.list_volumes.return_value = {"volumes": []}
        mock_client.create_volume.return_value = True

        with patch("src.commands.cluster_init_tools.datetime") as mock_datetime:
            mock_datetime.datetime.now.return_value.strftime.return_value = (
                "2025-06-02_14-30"
            )

            result = _helper_upload_cluster_init_logic(
                client=mock_client,
                target_catalog="main",
                target_schema="default",
                init_script_content="#!/bin/bash\necho 'test'",
            )

            mock_client.create_volume.assert_called_once_with(
                catalog_name="main", schema_name="default", name="chuck"
            )
            assert result["success"] is True

    def test_volume_creation_with_other_volumes_present(self, mock_client):
        """Test that chuck volume is created even when other volumes exist."""
        mock_client.list_volumes.return_value = {"volumes": [{"name": "other_volume"}]}
        mock_client.create_volume.return_value = True

        with patch("src.commands.cluster_init_tools.datetime") as mock_datetime:
            mock_datetime.datetime.now.return_value.strftime.return_value = (
                "2025-06-02_14-30"
            )

            result = _helper_upload_cluster_init_logic(
                client=mock_client,
                target_catalog="main",
                target_schema="default",
                init_script_content="#!/bin/bash\necho 'test'",
            )

            mock_client.create_volume.assert_called_once_with(
                catalog_name="main", schema_name="default", name="chuck"
            )
            assert result["success"] is True

    def test_list_volumes_error(self, mock_client):
        """Test error handling when listing volumes fails."""
        mock_client.list_volumes.side_effect = Exception("API Error")

        result = _helper_upload_cluster_init_logic(
            client=mock_client,
            target_catalog="main",
            target_schema="default",
            init_script_content="#!/bin/bash\necho 'test'",
        )

        assert "error" in result
        assert "Failed to list volumes" in result["error"]

    def test_create_volume_error(self, mock_client):
        """Test error handling when volume creation fails."""
        mock_client.list_volumes.return_value = {"volumes": []}
        mock_client.create_volume.side_effect = Exception("Create failed")

        result = _helper_upload_cluster_init_logic(
            client=mock_client,
            target_catalog="main",
            target_schema="default",
            init_script_content="#!/bin/bash\necho 'test'",
        )

        assert "error" in result
        assert "Failed to create volume 'chuck'" in result["error"]

    def test_create_volume_returns_false(self, mock_client):
        """Test error handling when volume creation returns False."""
        mock_client.list_volumes.return_value = {"volumes": []}
        mock_client.create_volume.return_value = False

        result = _helper_upload_cluster_init_logic(
            client=mock_client,
            target_catalog="main",
            target_schema="default",
            init_script_content="#!/bin/bash\necho 'test'",
        )

        assert "error" in result
        assert "Failed to create volume 'chuck'" in result["error"]

    def test_upload_file_error(self, mock_client):
        """Test error handling when file upload fails."""
        mock_client.upload_file.side_effect = Exception("Upload failed")

        result = _helper_upload_cluster_init_logic(
            client=mock_client,
            target_catalog="main",
            target_schema="default",
            init_script_content="#!/bin/bash\necho 'test'",
        )

        assert "error" in result
        assert "Failed to upload init script" in result["error"]

    def test_upload_file_returns_false(self, mock_client):
        """Test error handling when file upload returns False."""
        mock_client.upload_file.return_value = False

        result = _helper_upload_cluster_init_logic(
            client=mock_client,
            target_catalog="main",
            target_schema="default",
            init_script_content="#!/bin/bash\necho 'test'",
        )

        assert "error" in result
        assert "Failed to upload init script" in result["error"]

    def test_timestamped_filename_format(self, mock_client):
        """Test that the timestamped filename follows the correct format."""
        with patch("src.commands.cluster_init_tools.datetime") as mock_datetime:
            mock_datetime.datetime.now.return_value.strftime.return_value = (
                "2025-12-31_23-59"
            )

            result = _helper_upload_cluster_init_logic(
                client=mock_client,
                target_catalog="test_catalog",
                target_schema="test_schema",
                init_script_content="#!/bin/bash\necho 'test'",
            )

            assert result["success"] is True
            assert result["filename"] == "cluster_init-2025-12-31_23-59.sh"
            assert result["timestamp"] == "2025-12-31_23-59"
            assert "cluster_init-2025-12-31_23-59.sh" in result["volume_path"]

            # Verify datetime strftime was called with the correct format
            mock_datetime.datetime.now.return_value.strftime.assert_called_with(
                "%Y-%m-%d_%H-%M"
            )
