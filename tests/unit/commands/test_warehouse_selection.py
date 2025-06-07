"""
Tests for warehouse_selection command handler.

This module contains tests for the warehouse selection command handler.
"""

from unittest.mock import patch

from chuck_data.commands.warehouse_selection import handle_command
from chuck_data.config import get_warehouse_id


def test_missing_warehouse_parameter(databricks_client_stub, temp_config):
    """Test handling when warehouse parameter is not provided."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_command(databricks_client_stub)
        assert not result.success
        assert "warehouse parameter is required" in result.message


def test_successful_warehouse_selection_by_id(databricks_client_stub, temp_config):
    """Test successful warehouse selection by ID."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Set up warehouse in stub
        databricks_client_stub.add_warehouse(
            name="Test Warehouse", state="RUNNING", size="2X-Small"
        )
        # The warehouse_id should be "warehouse_0" based on the stub implementation
        warehouse_id = "warehouse_0"

        # Call function with warehouse ID
        result = handle_command(databricks_client_stub, warehouse=warehouse_id)

        # Verify results
        assert result.success
        assert "Active SQL warehouse is now set to 'Test Warehouse'" in result.message
        assert f"(ID: {warehouse_id}" in result.message
        assert "State: RUNNING" in result.message
        assert result.data["warehouse_id"] == warehouse_id
        assert result.data["warehouse_name"] == "Test Warehouse"
        assert result.data["state"] == "RUNNING"

        # Verify config was updated
        assert get_warehouse_id() == warehouse_id


def test_warehouse_selection_with_verification_failure(
    databricks_client_stub, temp_config
):
    """Test warehouse selection when verification fails."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Add a warehouse to stub but call with different ID - will cause verification failure
        databricks_client_stub.add_warehouse(
            name="Production Warehouse", state="RUNNING", size="2X-Small"
        )

        # Call function with non-existent warehouse ID that won't match by name
        result = handle_command(
            databricks_client_stub, warehouse="xyz-completely-different-name"
        )

        # Verify results - should now fail when warehouse is not found
        assert not result.success
        assert (
            "No warehouse found matching 'xyz-completely-different-name'"
            in result.message
        )


def test_warehouse_selection_no_client(temp_config):
    """Test warehouse selection with no client available."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Call function with no client
        result = handle_command(None, warehouse="abc123")

        # Verify results - should now fail when no client is available
        assert not result.success
        assert "No API client available to verify warehouse" in result.message


def test_warehouse_selection_exception(temp_config):
    """Test warehouse selection with unexpected exception."""
    from tests.fixtures.databricks.client import DatabricksClientStub

    with patch("chuck_data.config._config_manager", temp_config):
        # Create a stub that raises an exception during warehouse verification
        class FailingStub(DatabricksClientStub):
            def get_warehouse(self, warehouse_id):
                raise Exception("Failed to set warehouse")

            def list_warehouses(self, **kwargs):
                raise Exception("Failed to list warehouses")

        failing_stub = FailingStub()

        # Call function
        result = handle_command(failing_stub, warehouse="abc123")

        # Should fail when both get_warehouse and list_warehouses fail
        assert not result.success
        assert "Failed to list warehouses" in result.message


def test_warehouse_selection_by_name(databricks_client_stub, temp_config):
    """Test warehouse selection by name parameter."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Set up warehouse in stub
        databricks_client_stub.add_warehouse(
            name="Test Warehouse", state="RUNNING", size="2X-Small"
        )

        # Call function with warehouse name
        result = handle_command(databricks_client_stub, warehouse="Test Warehouse")

        # Verify results
        assert result.success
        assert "Active SQL warehouse is now set to 'Test Warehouse'" in result.message
        assert result.data["warehouse_name"] == "Test Warehouse"


def test_warehouse_selection_fuzzy_matching(databricks_client_stub, temp_config):
    """Test warehouse selection with fuzzy name matching."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Set up warehouse in stub
        databricks_client_stub.add_warehouse(
            name="Starter Warehouse", state="RUNNING", size="2X-Small"
        )

        # Call function with partial name match
        result = handle_command(databricks_client_stub, warehouse="Starter")

        # Verify results
        assert result.success
        assert (
            "Active SQL warehouse is now set to 'Starter Warehouse'" in result.message
        )
        assert result.data["warehouse_name"] == "Starter Warehouse"
