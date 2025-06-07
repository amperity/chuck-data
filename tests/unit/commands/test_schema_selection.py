"""
Tests for schema_selection command handler.

This module contains tests for the schema selection command handler.
"""

from unittest.mock import patch

from chuck_data.commands.schema_selection import handle_command
from chuck_data.config import get_active_schema, set_active_catalog


def test_missing_schema_name(databricks_client_stub, temp_config):
    """Test handling when schema parameter is not provided."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_command(databricks_client_stub)
        assert not result.success
        assert "schema parameter is required" in result.message


def test_no_active_catalog(databricks_client_stub, temp_config):
    """Test handling when no active catalog is selected."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Don't set any active catalog in config

        # Call function
        result = handle_command(databricks_client_stub, schema="test_schema")

        # Verify results
        assert not result.success
        assert "No active catalog selected" in result.message


def test_successful_schema_selection(databricks_client_stub, temp_config):
    """Test successful schema selection."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Set up active catalog and test data
        set_active_catalog("test_catalog")
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "test_schema")

        # Call function
        result = handle_command(databricks_client_stub, schema="test_schema")

        # Verify results
        assert result.success
        assert "Active schema is now set to 'test_schema'" in result.message
        assert "in catalog 'test_catalog'" in result.message
        assert result.data["schema_name"] == "test_schema"
        assert result.data["catalog_name"] == "test_catalog"

        # Verify config was updated
        assert get_active_schema() == "test_schema"


def test_schema_selection_with_verification_failure(
    databricks_client_stub, temp_config
):
    """Test schema selection when no matching schema exists."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Set up active catalog but don't add the schema to stub
        set_active_catalog("test_catalog")
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema(
            "test_catalog", "completely_different_schema_name"
        )

        # Call function with non-existent schema that won't match via fuzzy matching
        result = handle_command(databricks_client_stub, schema="xyz_nonexistent_abc")

        # Verify results - should fail cleanly
        assert not result.success
        assert "No schema found matching 'xyz_nonexistent_abc'" in result.message
        assert "Available schemas:" in result.message


def test_schema_selection_exception(temp_config):
    """Test schema selection with list_schemas exception."""
    from tests.fixtures.databricks.client import DatabricksClientStub

    with patch("chuck_data.config._config_manager", temp_config):
        # Set up active catalog
        set_active_catalog("test_catalog")

        # Create a stub that raises an exception during list_schemas
        class FailingStub(DatabricksClientStub):
            def list_schemas(
                self,
                catalog_name,
                include_browse=False,
                max_results=None,
                page_token=None,
                **kwargs,
            ):
                raise Exception("Failed to list schemas")

        failing_stub = FailingStub()
        failing_stub.add_catalog("test_catalog")

        # Call function
        result = handle_command(failing_stub, schema="test_schema")

        # Should fail due to the exception
        assert not result.success
        assert "Failed to list schemas" in result.message
