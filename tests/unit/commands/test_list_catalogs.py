"""
Tests for list_catalogs command handler.

This module contains tests for the list_catalogs command handler.
"""

from unittest.mock import patch

from chuck_data.commands.list_catalogs import handle_command


def test_no_client():
    """Test handling when no client is provided."""
    result = handle_command(None)
    assert not result.success
    assert "No Databricks client available" in result.message


def test_successful_list_catalogs(databricks_client_stub, temp_config):
    """Test successful list catalogs."""
    client_stub = databricks_client_stub
    config_manager = temp_config

    # Set up test data using stub - this simulates external API
    client_stub.add_catalog(
        "catalog1",
        catalog_type="MANAGED",
        comment="Test catalog 1",
        provider={"name": "provider1"},
        created_at="2023-01-01",
    )
    client_stub.add_catalog(
        "catalog2",
        catalog_type="EXTERNAL",
        comment="Test catalog 2",
        provider={"name": "provider2"},
        created_at="2023-01-02",
    )

    # Call function with parameters - tests real command logic
    with patch("chuck_data.config._config_manager", config_manager):
        result = handle_command(client_stub, include_browse=True, max_results=50)

    # Verify results
    assert result.success
    assert len(result.data["catalogs"]) == 2
    assert result.data["total_count"] == 2
    assert "Found 2 catalog(s)." in result.message
    assert not result.data.get("display", True)  # Should default to False
    assert "current_catalog" in result.data

    # Verify catalog data
    catalog_names = [c["name"] for c in result.data["catalogs"]]
    assert "catalog1" in catalog_names
    assert "catalog2" in catalog_names


def test_successful_list_catalogs_with_pagination(databricks_client_stub):
    """Test successful list catalogs with pagination."""
    from tests.fixtures.databricks.client import DatabricksClientStub

    # For pagination testing, we need to modify the stub to return pagination token
    class PaginatingClientStub(DatabricksClientStub):
        def list_catalogs(
            self, include_browse=False, max_results=None, page_token=None
        ):
            result = super().list_catalogs(include_browse, max_results, page_token)
            # Add pagination token if page_token was provided
            if page_token:
                result["next_page_token"] = "abc123"
            return result

    paginating_stub = PaginatingClientStub()
    paginating_stub.add_catalog("catalog1", catalog_type="MANAGED")
    paginating_stub.add_catalog("catalog2", catalog_type="EXTERNAL")

    # Call function with page token
    result = handle_command(paginating_stub, page_token="xyz789")

    # Verify results
    assert result.success
    assert result.data["next_page_token"] == "abc123"
    assert "More catalogs available with page token: abc123" in result.message


def test_empty_catalog_list(databricks_client_stub):
    """Test handling when no catalogs are found."""
    # Use empty client stub (no catalogs added)
    client_stub = databricks_client_stub
    client_stub.catalogs.clear()  # Ensure it's empty

    # Call function
    result = handle_command(client_stub)

    # Verify results
    assert result.success
    assert "No catalogs found in this workspace." in result.message
    assert result.data["total_count"] == 0
    assert not result.data.get("display", True)
    assert "current_catalog" in result.data


def test_list_catalogs_exception():
    """Test list_catalogs with unexpected exception."""
    from tests.fixtures.databricks.client import DatabricksClientStub

    # Create a stub that raises an exception for list_catalogs
    class FailingClientStub(DatabricksClientStub):
        def list_catalogs(
            self, include_browse=False, max_results=None, page_token=None
        ):
            raise Exception("API error")

    failing_client = FailingClientStub()

    # Call function
    result = handle_command(failing_client)

    # Verify results
    assert not result.success
    assert "Failed to list catalogs" in result.message
    assert str(result.error) == "API error"


def test_list_catalogs_with_display_true(databricks_client_stub):
    """Test list catalogs with display=true shows table."""
    # Set up test data
    databricks_client_stub.add_catalog("Test Catalog", catalog_type="MANAGED")

    result = handle_command(databricks_client_stub, display=True)

    assert result.success
    assert result.data.get("display")
    assert len(result.data.get("catalogs", [])) == 1


def test_list_catalogs_with_display_false(databricks_client_stub):
    """Test list catalogs with display=false returns data without display."""
    # Set up test data
    databricks_client_stub.add_catalog("Test Catalog", catalog_type="MANAGED")

    result = handle_command(databricks_client_stub, display=False)

    assert result.success
    assert not result.data.get("display")
    assert len(result.data.get("catalogs", [])) == 1
