"""
Tests for the profiler module.
"""

import pytest
from unittest.mock import patch, MagicMock
from chuck_data.profiler import (
    list_tables,
    query_llm,
    generate_manifest,
    store_manifest,
    profile_table,
)


@pytest.fixture
def client():
    """Mock client fixture."""
    return MagicMock()


@pytest.fixture
def warehouse_id():
    """Warehouse ID fixture."""
    return "warehouse-123"


@patch("chuck_data.profiler.time.sleep")
def test_list_tables(mock_sleep, client, warehouse_id):
    """Test listing tables."""
    # Set up mock responses
    client.post.return_value = {"statement_id": "stmt-123"}

    # Mock the get call to return a completed query status
    client.get.return_value = {
        "status": {"state": "SUCCEEDED"},
        "result": {
            "data": [
                ["table1", "catalog1", "schema1"],
                ["table2", "catalog1", "schema2"],
            ]
        },
    }

    # Call the function
    result = list_tables(client, warehouse_id)

    # Check the result
    expected_tables = [
        {
            "table_name": "table1",
            "catalog_name": "catalog1",
            "schema_name": "schema1",
        },
        {
            "table_name": "table2",
            "catalog_name": "catalog1",
            "schema_name": "schema2",
        },
    ]
    assert result == expected_tables

    # Verify API calls
    client.post.assert_called_once()
    client.get.assert_called_once()


@patch("chuck_data.profiler.time.sleep")
def test_list_tables_polling(mock_sleep, client, warehouse_id):
    """Test polling behavior when listing tables."""
    # Set up mock responses
    client.post.return_value = {"statement_id": "stmt-123"}

    # Set up get to return PENDING then RUNNING then SUCCEEDED
    client.get.side_effect = [
        {"status": {"state": "PENDING"}},
        {"status": {"state": "RUNNING"}},
        {
            "status": {"state": "SUCCEEDED"},
            "result": {"data": [["table1", "catalog1", "schema1"]]},
        },
    ]

    # Call the function
    result = list_tables(client, warehouse_id)

    # Verify polling behavior
    assert len(client.get.call_args_list) == 3
    assert mock_sleep.call_count == 2

    # Check result
    assert len(result) == 1
    assert result[0]["table_name"] == "table1"


@patch("chuck_data.profiler.time.sleep")
def test_list_tables_failed_query(mock_sleep, client, warehouse_id):
    """Test list tables with failed SQL query."""
    # Set up mock responses
    client.post.return_value = {"statement_id": "stmt-123"}
    client.get.return_value = {"status": {"state": "FAILED"}}

    # Call the function
    result = list_tables(client, warehouse_id)

    # Verify it returns empty list on failure
    assert result == []


def test_generate_manifest():
    """Test generating a manifest."""
    # Test data
    table_info = {
        "catalog_name": "catalog1",
        "schema_name": "schema1",
        "table_name": "table1",
    }
    schema = [{"col_name": "id", "data_type": "integer"}]
    sample_data = {"columns": ["id"], "rows": [{"id": 1}, {"id": 2}]}
    pii_tags = ["id"]

    # Call the function
    result = generate_manifest(table_info, schema, sample_data, pii_tags)

    # Check the result
    assert result["table"] == table_info
    assert result["schema"] == schema
    assert result["pii_tags"] == pii_tags
    assert "profiling_timestamp" in result


@patch("chuck_data.profiler.time.sleep")
@patch("chuck_data.profiler.base64.b64encode")
def test_store_manifest(mock_b64encode, mock_sleep, client):
    """Test storing a manifest."""
    # Set up mock responses
    mock_b64encode.return_value = b"base64_encoded_data"
    client.post.return_value = {"success": True}

    # Test data
    manifest = {"table": {"name": "table1"}, "pii_tags": ["id"]}
    manifest_path = "/chuck/manifests/table1_manifest.json"

    # Call the function
    result = store_manifest(client, manifest_path, manifest)

    # Check the result
    assert result

    # Verify API call
    client.post.assert_called_once()
    assert client.post.call_args[0][0] == "/api/2.0/dbfs/put"
    # Verify the manifest path was passed correctly
    assert client.post.call_args[0][1]["path"] == manifest_path


@patch("chuck_data.profiler.store_manifest")
@patch("chuck_data.profiler.generate_manifest")
@patch("chuck_data.profiler.query_llm")
@patch("chuck_data.profiler.get_sample_data")
@patch("chuck_data.profiler.get_table_schema")
@patch("chuck_data.profiler.list_tables")
def test_profile_table_success(
    mock_list_tables,
    mock_get_schema,
    mock_get_sample,
    mock_query_llm,
    mock_generate_manifest,
    mock_store_manifest,
    client,
    warehouse_id,
):
    """Test successfully profiling a table."""
    # Set up mock responses
    table_info = {
        "catalog_name": "catalog1",
        "schema_name": "schema1",
        "table_name": "table1",
    }
    schema = [{"col_name": "id", "data_type": "integer"}]
    sample_data = {"column_names": ["id"], "rows": [{"id": 1}]}
    pii_tags = ["id"]
    manifest = {"table": table_info, "pii_tags": pii_tags}
    manifest_path = "/chuck/manifests/table1_manifest.json"

    mock_list_tables.return_value = [table_info]
    mock_get_schema.return_value = schema
    mock_get_sample.return_value = sample_data
    mock_query_llm.return_value = {"predictions": [{"pii_tags": pii_tags}]}
    mock_generate_manifest.return_value = manifest
    mock_store_manifest.return_value = True

    # Call the function without specific table (should use first table found)
    result = profile_table(client, warehouse_id, "test-model")

    # Check the result
    assert result == manifest_path

    # Verify the correct functions were called
    mock_list_tables.assert_called_once_with(client, warehouse_id)
    mock_get_schema.assert_called_once()
    mock_get_sample.assert_called_once()
    mock_query_llm.assert_called_once()
    mock_generate_manifest.assert_called_once()
    mock_store_manifest.assert_called_once()


def test_query_llm(client):
    """Test querying the LLM."""
    # Set up mock response
    client.post.return_value = {"predictions": [{"pii_tags": ["id"]}]}

    # Test data
    endpoint_name = "test-model"
    input_data = {
        "schema": [{"col_name": "id", "data_type": "integer"}],
        "sample_data": {"column_names": ["id"], "rows": [{"id": 1}]},
    }

    # Call the function
    result = query_llm(client, endpoint_name, input_data)

    # Check the result
    assert result == {"predictions": [{"pii_tags": ["id"]}]}

    # Verify API call
    client.post.assert_called_once()
    assert (
        client.post.call_args[0][0]
        == "/api/2.0/serving-endpoints/test-model/invocations"
    )
