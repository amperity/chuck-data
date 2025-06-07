"""
Tests for catalog_selection command handler.

This module contains tests for the catalog selection command handler.
"""

from unittest.mock import patch

from chuck_data.commands.catalog_selection import handle_command
from chuck_data.config import get_active_catalog


def test_missing_catalog_name(databricks_client_stub, temp_config):
    """Test handling when catalog parameter is not provided."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_command(databricks_client_stub)
        assert not result.success
        assert "catalog parameter is required" in result.message


def test_successful_catalog_selection(databricks_client_stub, temp_config):
    """Test successful catalog selection."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Set up catalog in stub
        databricks_client_stub.add_catalog("test_catalog", catalog_type="MANAGED")

        # Call function
        result = handle_command(databricks_client_stub, catalog="test_catalog")

        # Verify results
        assert result.success
        assert "Active catalog is now set to 'test_catalog'" in result.message
        assert "Type: MANAGED" in result.message
        assert result.data["catalog_name"] == "test_catalog"
        assert result.data["catalog_type"] == "MANAGED"

        # Verify config was updated
        assert get_active_catalog() == "test_catalog"


def test_catalog_selection_with_verification_failure(
    databricks_client_stub, temp_config
):
    """Test catalog selection when verification fails."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Add some catalogs but not the one we're looking for (make sure names are very different)
        databricks_client_stub.add_catalog("xyz", catalog_type="MANAGED")

        # Call function with nonexistent catalog that won't fuzzy match
        result = handle_command(
            databricks_client_stub, catalog="completely_different_name"
        )

        # Verify results - should fail since catalog doesn't exist and no fuzzy match
        assert not result.success
        assert "No catalog found matching 'completely_different_name'" in result.message
        assert "Available catalogs: xyz" in result.message


def test_catalog_selection_exception(databricks_client_stub, temp_config):
    """Test catalog selection with unexpected exception."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Configure stub to fail on get_catalog
        def get_catalog_failing(catalog_name):
            raise Exception("Failed to set catalog")

        databricks_client_stub.get_catalog = get_catalog_failing

        # This should trigger the exception in the catalog verification
        result = handle_command(databricks_client_stub, catalog="test_catalog")

        # Should fail since get_catalog fails and no catalogs in list
        assert not result.success
        assert "No catalogs found in workspace" in result.message


def test_select_catalog_by_name(databricks_client_stub, temp_config):
    """Test catalog selection by name."""
    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.add_catalog("Test Catalog", catalog_type="MANAGED")

        result = handle_command(databricks_client_stub, catalog="Test Catalog")

        assert result.success
        assert "Active catalog is now set to 'Test Catalog'" in result.message


def test_select_catalog_fuzzy_matching(databricks_client_stub, temp_config):
    """Test catalog selection with fuzzy matching."""
    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.add_catalog(
            "Test Catalog Long Name", catalog_type="MANAGED"
        )

        result = handle_command(databricks_client_stub, catalog="Test")

        assert result.success
        assert "Test Catalog Long Name" in result.message
