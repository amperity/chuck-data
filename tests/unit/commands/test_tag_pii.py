"""Unit tests for tag_pii command."""

from unittest.mock import MagicMock, patch

from chuck_data.commands.tag_pii import handle_command, apply_semantic_tags
from chuck_data.commands.base import CommandResult
from chuck_data.config import (
    set_warehouse_id,
    set_active_catalog,
    set_active_schema,
)


def test_missing_table_name():
    """Test that missing table_name parameter is handled correctly."""
    result = handle_command(None, pii_columns=[{"name": "test", "semantic": "email"}])

    assert isinstance(result, CommandResult)
    assert not result.success
    assert "table_name parameter is required" in result.message


def test_missing_pii_columns():
    """Test that missing pii_columns parameter is handled correctly."""
    result = handle_command(None, table_name="test_table")

    assert isinstance(result, CommandResult)
    assert not result.success
    assert "pii_columns parameter is required" in result.message


def test_empty_pii_columns():
    """Test that empty pii_columns list is handled correctly."""
    result = handle_command(None, table_name="test_table", pii_columns=[])

    assert isinstance(result, CommandResult)
    assert not result.success
    assert "pii_columns parameter is required" in result.message


def test_missing_client():
    """Test that missing client is handled correctly."""
    result = handle_command(
        None,
        table_name="test_table",
        pii_columns=[{"name": "test", "semantic": "email"}],
    )

    assert isinstance(result, CommandResult)
    assert not result.success
    assert "Client is required for PII tagging" in result.message


def test_missing_warehouse_id(databricks_client_stub, temp_config):
    """Test that missing warehouse ID is handled correctly."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Don't set warehouse ID in config
        result = handle_command(
            databricks_client_stub,
            table_name="test_table",
            pii_columns=[{"name": "test", "semantic": "email"}],
        )

        assert isinstance(result, CommandResult)
        assert not result.success
        assert "No warehouse ID configured" in result.message


def test_missing_catalog_schema_for_simple_table_name(
    databricks_client_stub, temp_config
):
    """Test that missing catalog/schema for simple table name is handled."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_warehouse_id("warehouse123")
        # Don't set active catalog/schema

        result = handle_command(
            databricks_client_stub,
            table_name="simple_table",  # No dots, so needs catalog/schema
            pii_columns=[{"name": "test", "semantic": "email"}],
        )

        assert isinstance(result, CommandResult)
        assert not result.success
        assert "No active catalog and schema selected" in result.message


def test_table_not_found(databricks_client_stub, temp_config):
    """Test that table not found is handled correctly."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_warehouse_id("warehouse123")
        set_active_catalog("test_catalog")
        set_active_schema("test_schema")

        # Don't add the table to stub - will cause table not found
        result = handle_command(
            databricks_client_stub,
            table_name="nonexistent_table",
            pii_columns=[{"name": "test", "semantic": "email"}],
        )

        assert isinstance(result, CommandResult)
        assert not result.success
        assert (
            "Table test_catalog.test_schema.nonexistent_table not found"
            in result.message
        )


def test_apply_semantic_tags_success(databricks_client_stub):
    """Test successful application of semantic tags."""
    pii_columns = [
        {"name": "email_col", "semantic": "email"},
        {"name": "name_col", "semantic": "given-name"},
    ]

    results = apply_semantic_tags(
        databricks_client_stub, "catalog.schema.table", pii_columns, "warehouse123"
    )

    assert len(results) == 2
    assert all(r["success"] for r in results)
    assert results[0]["column"] == "email_col"
    assert results[0]["semantic_type"] == "email"
    assert results[1]["column"] == "name_col"
    assert results[1]["semantic_type"] == "given-name"


def test_apply_semantic_tags_missing_data(databricks_client_stub):
    """Test handling of missing column data in apply_semantic_tags."""
    pii_columns = [
        {"name": "email_col"},  # Missing semantic type
        {"semantic": "email"},  # Missing column name
        {"name": "good_col", "semantic": "phone"},  # Good data
    ]

    results = apply_semantic_tags(
        databricks_client_stub, "catalog.schema.table", pii_columns, "warehouse123"
    )

    assert len(results) == 3
    assert not results[0]["success"]  # Missing semantic type
    assert not results[1]["success"]  # Missing column name
    assert results[2]["success"]  # Good data

    assert "Missing column name or semantic type" in results[0]["error"]
    assert "Missing column name or semantic type" in results[1]["error"]


def test_apply_semantic_tags_sql_failure(databricks_client_stub):
    """Test handling of SQL execution failures."""

    # Configure stub to return SQL failure
    def failing_sql_submit(sql_text=None, sql=None, **kwargs):
        return {
            "status": {
                "state": "FAILED",
                "error": {"message": "SQL execution failed"},
            }
        }

    # Mock the submit_sql_statement method on the specific instance
    databricks_client_stub.submit_sql_statement = failing_sql_submit

    pii_columns = [{"name": "email_col", "semantic": "email"}]

    results = apply_semantic_tags(
        databricks_client_stub, "catalog.schema.table", pii_columns, "warehouse123"
    )

    assert len(results) == 1
    assert not results[0]["success"]
    assert "SQL execution failed" in results[0]["error"]


def test_apply_semantic_tags_exception():
    """Test handling of exceptions during SQL execution."""
    mock_client = MagicMock()
    mock_client.submit_sql_statement.side_effect = Exception("Connection error")

    pii_columns = [{"name": "email_col", "semantic": "email"}]

    results = apply_semantic_tags(
        mock_client, "catalog.schema.table", pii_columns, "warehouse123"
    )

    assert len(results) == 1
    assert not results[0]["success"]
    assert "Connection error" in results[0]["error"]
