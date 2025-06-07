"""
Tests for list_tables command handler.

This module contains tests for the list_tables command handler.
"""

from unittest.mock import patch

from chuck_data.commands.list_tables import handle_command
from tests.fixtures.databricks.client import DatabricksClientStub


def test_no_client():
    """Test handling when no client is provided."""
    result = handle_command(None)
    assert not result.success
    assert "No Databricks client available" in result.message


def test_no_active_catalog(temp_config):
    """Test handling when no catalog is provided and no active catalog is set."""
    with patch("chuck_data.config._config_manager", temp_config):
        client_stub = DatabricksClientStub()
        # Don't set any active catalog in config

        result = handle_command(client_stub)
        assert not result.success
        assert "No catalog specified and no active catalog selected" in result.message


def test_no_active_schema(temp_config):
    """Test handling when no schema is provided and no active schema is set."""
    with patch("chuck_data.config._config_manager", temp_config):
        from chuck_data.config import set_active_catalog

        client_stub = DatabricksClientStub()
        # Set active catalog but not schema
        set_active_catalog("test_catalog")

        result = handle_command(client_stub)
        assert not result.success
        assert "No schema specified and no active schema selected" in result.message


def test_successful_list_tables_with_parameters(temp_config):
    """Test successful list tables with all parameters specified."""
    with patch("chuck_data.config._config_manager", temp_config):
        client_stub = DatabricksClientStub()
        # Set up test data using stub
        client_stub.add_catalog("test_catalog")
        client_stub.add_schema("test_catalog", "test_schema")
        client_stub.add_table(
            "test_catalog",
            "test_schema",
            "table1",
            table_type="MANAGED",
            comment="Test table 1",
            created_at="2023-01-01",
        )
        client_stub.add_table(
            "test_catalog",
            "test_schema",
            "table2",
            table_type="VIEW",
            comment="Test table 2",
            created_at="2023-01-02",
        )

        # Call function
        result = handle_command(
            client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
            include_delta_metadata=True,
            omit_columns=False,
        )

        # Verify results
        assert result.success
        assert len(result.data["tables"]) == 2
        assert result.data["total_count"] == 2
        assert result.data["catalog_name"] == "test_catalog"
        assert result.data["schema_name"] == "test_schema"
        assert "Found 2 table(s) in 'test_catalog.test_schema'" in result.message

        # Verify table data
        table_names = [t["name"] for t in result.data["tables"]]
        assert "table1" in table_names
        assert "table2" in table_names


def test_successful_list_tables_with_defaults(temp_config):
    """Test successful list tables using default active catalog and schema."""
    with patch("chuck_data.config._config_manager", temp_config):
        from chuck_data.config import set_active_catalog, set_active_schema

        client_stub = DatabricksClientStub()
        # Set up active catalog and schema
        set_active_catalog("active_catalog")
        set_active_schema("active_schema")

        # Set up test data
        client_stub.add_catalog("active_catalog")
        client_stub.add_schema("active_catalog", "active_schema")
        client_stub.add_table("active_catalog", "active_schema", "table1")

        # Call function with no catalog or schema parameters
        result = handle_command(client_stub)

        # Verify results
        assert result.success
        assert len(result.data["tables"]) == 1
        assert result.data["catalog_name"] == "active_catalog"
        assert result.data["schema_name"] == "active_schema"
        assert result.data["tables"][0]["name"] == "table1"


def test_empty_table_list(temp_config):
    """Test handling when no tables are found."""
    with patch("chuck_data.config._config_manager", temp_config):
        client_stub = DatabricksClientStub()
        # Set up catalog and schema but no tables
        client_stub.add_catalog("test_catalog")
        client_stub.add_schema("test_catalog", "test_schema")
        # Don't add any tables

        # Call function
        result = handle_command(
            client_stub, catalog_name="test_catalog", schema_name="test_schema"
        )

        # Verify results
        assert result.success
        assert "No tables found in schema 'test_catalog.test_schema'" in result.message


def test_list_tables_exception(temp_config):
    """Test list_tables with unexpected exception."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Create a stub that raises an exception for list_tables
        class FailingClientStub(DatabricksClientStub):
            def list_tables(self, *args, **kwargs):
                raise Exception("API error")

        failing_client = FailingClientStub()

        # Call function
        result = handle_command(
            failing_client, catalog_name="test_catalog", schema_name="test_schema"
        )

        # Verify results
        assert not result.success
        assert "Failed to list tables" in result.message
        assert str(result.error) == "API error"


def test_list_tables_with_display_true(temp_config):
    """Test list tables with display=true shows table."""
    with patch("chuck_data.config._config_manager", temp_config):
        client_stub = DatabricksClientStub()
        # Set up test data
        client_stub.add_catalog("test_catalog")
        client_stub.add_schema("test_catalog", "test_schema")
        client_stub.add_table("test_catalog", "test_schema", "test_table")

        result = handle_command(
            client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
            display=True,
        )

        assert result.success
        assert result.data.get("display")
        assert len(result.data.get("tables", [])) == 1


def test_list_tables_with_display_false(temp_config):
    """Test list tables with display=false returns data without display."""
    with patch("chuck_data.config._config_manager", temp_config):
        client_stub = DatabricksClientStub()
        # Set up test data
        client_stub.add_catalog("test_catalog")
        client_stub.add_schema("test_catalog", "test_schema")
        client_stub.add_table("test_catalog", "test_schema", "test_table")

        result = handle_command(
            client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
            display=False,
        )

        assert result.success
        assert not result.data.get("display")
        assert len(result.data.get("tables", [])) == 1
