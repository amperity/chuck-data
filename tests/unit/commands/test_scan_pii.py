"""
Tests for scan_pii command handler.

This module contains tests for the scan_pii command handler.
"""

import pytest
from unittest.mock import patch, MagicMock

from chuck_data.commands.scan_pii import handle_command
from tests.fixtures.llm import LLMClientStub


@pytest.fixture
def client():
    """Mock client fixture."""
    return MagicMock()

def test_missing_client():
    """Test handling when client is not provided."""
    result = handle_command(None)
    assert not result.success
    assert "Client is required" in result.message

@patch("chuck_data.commands.scan_pii.get_active_catalog")
@patch("chuck_data.commands.scan_pii.get_active_schema")
def test_missing_context(mock_get_active_schema, mock_get_active_catalog, client):
    """Test handling when catalog or schema is missing."""
    # Setup mocks
    mock_get_active_catalog.return_value = None
    mock_get_active_schema.return_value = None

    # Call function
    result = handle_command(client)

    # Verify results
    assert not result.success
    assert "Catalog and schema must be specified" in result.message

@patch("chuck_data.commands.scan_pii.LLMClient")
@patch("chuck_data.commands.scan_pii._helper_scan_schema_for_pii_logic")
def test_successful_scan(mock_helper_scan, mock_llm_client, client):
    """Test successful schema scan for PII."""
    # Setup mocks
    llm_client_stub = LLMClientStub()
    mock_llm_client.return_value = llm_client_stub

    mock_helper_scan.return_value = {
        "tables_successfully_processed": 5,
        "tables_scanned_attempted": 6,
        "tables_with_pii": 3,
        "total_pii_columns": 8,
        "catalog": "test_catalog",
        "schema": "test_schema",
        "results_detail": [
            {"full_name": "test_catalog.test_schema.table1", "has_pii": True},
            {"full_name": "test_catalog.test_schema.table2", "has_pii": True},
            {"full_name": "test_catalog.test_schema.table3", "has_pii": True},
            {"full_name": "test_catalog.test_schema.table4", "has_pii": False},
            {"full_name": "test_catalog.test_schema.table5", "has_pii": False},
        ],
    }

    # Call function
    result = handle_command(
        client, catalog_name="test_catalog", schema_name="test_schema"
    )

    # Verify results
    assert result.success
    assert result.data["tables_successfully_processed"] == 5
    assert result.data["tables_with_pii"] == 3
    assert result.data["total_pii_columns"] == 8
    assert "Scanned 5/6 tables" in result.message
    assert "Found 3 tables with 8 PII columns" in result.message
    mock_helper_scan.assert_called_once_with(
        client, llm_client_stub, "test_catalog", "test_schema"
    )

@patch("chuck_data.commands.scan_pii.get_active_catalog")
@patch("chuck_data.commands.scan_pii.get_active_schema")
@patch("chuck_data.commands.scan_pii.LLMClient")
@patch("chuck_data.commands.scan_pii._helper_scan_schema_for_pii_logic")
def test_scan_with_active_context(
    mock_helper_scan,
    mock_llm_client,
    mock_get_active_schema,
    mock_get_active_catalog,
    client,
):
    """Test schema scan using active catalog and schema."""
    # Setup mocks
    mock_get_active_catalog.return_value = "active_catalog"
    mock_get_active_schema.return_value = "active_schema"

    llm_client_stub = LLMClientStub()
    mock_llm_client.return_value = llm_client_stub

    mock_helper_scan.return_value = {
        "tables_successfully_processed": 3,
        "tables_scanned_attempted": 3,
        "tables_with_pii": 1,
        "total_pii_columns": 2,
    }

    # Call function without catalog/schema args
    result = handle_command(client)

    # Verify results
    assert result.success
    mock_helper_scan.assert_called_once_with(
        client, llm_client_stub, "active_catalog", "active_schema"
    )

@patch("chuck_data.commands.scan_pii.LLMClient")
@patch("chuck_data.commands.scan_pii._helper_scan_schema_for_pii_logic")
def test_scan_with_helper_error(mock_helper_scan, mock_llm_client, client):
    """Test handling when helper returns an error."""
    # Setup mocks
    llm_client_stub = LLMClientStub()
    mock_llm_client.return_value = llm_client_stub

    mock_helper_scan.return_value = {"error": "Failed to list tables"}

    # Call function
    result = handle_command(
        client, catalog_name="test_catalog", schema_name="test_schema"
    )

    # Verify results
    assert not result.success
    assert result.message == "Failed to list tables"

@patch("chuck_data.commands.scan_pii.LLMClient")
def test_scan_with_exception(mock_llm_client, client):
    """Test handling when an exception occurs."""
    # Setup mocks
    mock_llm_client.side_effect = Exception("LLM client error")

    # Call function
    result = handle_command(
        client, catalog_name="test_catalog", schema_name="test_schema"
    )

    # Verify results
    assert not result.success
    assert "Error during bulk PII scan" in result.message
    assert str(result.error) == "LLM client error"
