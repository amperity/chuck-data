"""
Tests for the profiler module.
"""

import pytest
from unittest.mock import patch
from chuck_data.profiler import (
    list_tables,
    query_llm,
    generate_manifest,
    store_manifest,
    profile_table,
)


@pytest.fixture
def warehouse_id():
    """Warehouse ID fixture."""
    return "warehouse-123"


@patch("chuck_data.profiler.time.sleep")
def test_list_tables(mock_sleep, databricks_client_stub, warehouse_id):
    """Test listing tables."""
    # Set up external API responses
    databricks_client_stub.post.return_value = {"statement_id": "stmt-123"}

    # Mock the get call to return a completed query status
    databricks_client_stub.get.return_value = {
        "status": {"state": "SUCCEEDED"},
        "result": {
            "data": [
                ["table1", "catalog1", "schema1"],
                ["table2", "catalog1", "schema2"],
            ]
        },
    }

    # Call the function
    result = list_tables(databricks_client_stub, warehouse_id)

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
    databricks_client_stub.post.assert_called_once()
    databricks_client_stub.get.assert_called_once()


@patch("chuck_data.profiler.time.sleep")
def test_list_tables_polling(mock_sleep, databricks_client_stub, warehouse_id):
    """Test polling behavior when listing tables."""
    # Set up external API responses
    databricks_client_stub.post.return_value = {"statement_id": "stmt-123"}

    # Set up get to return PENDING then RUNNING then SUCCEEDED
    databricks_client_stub.get.side_effect = [
        {"status": {"state": "PENDING"}},
        {"status": {"state": "RUNNING"}},
        {
            "status": {"state": "SUCCEEDED"},
            "result": {"data": [["table1", "catalog1", "schema1"]]},
        },
    ]

    # Call the function
    result = list_tables(databricks_client_stub, warehouse_id)

    # Verify polling behavior
    assert len(databricks_client_stub.get.call_args_list) == 3
    assert mock_sleep.call_count == 2

    # Check result
    assert len(result) == 1
    assert result[0]["table_name"] == "table1"


@patch("chuck_data.profiler.time.sleep")
def test_list_tables_failed_query(mock_sleep, databricks_client_stub, warehouse_id):
    """Test list tables with failed SQL query."""
    # Set up external API responses
    databricks_client_stub.post.return_value = {"statement_id": "stmt-123"}
    databricks_client_stub.get.return_value = {"status": {"state": "FAILED"}}

    # Call the function
    result = list_tables(databricks_client_stub, warehouse_id)

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
def test_store_manifest(mock_b64encode, mock_sleep, databricks_client_stub):
    """Test storing a manifest."""
    # Set up external dependencies
    mock_b64encode.return_value = b"base64_encoded_data"
    databricks_client_stub.post.return_value = {"success": True}

    # Test data
    manifest = {"table": {"name": "table1"}, "pii_tags": ["id"]}
    manifest_path = "/chuck/manifests/table1_manifest.json"

    # Call the function
    result = store_manifest(databricks_client_stub, manifest_path, manifest)

    # Check the result
    assert result

    # Verify API call
    databricks_client_stub.post.assert_called_once()
    assert databricks_client_stub.post.call_args[0][0] == "/api/2.0/dbfs/put"
    # Verify the manifest path was passed correctly
    assert databricks_client_stub.post.call_args[0][1]["path"] == manifest_path


@patch("chuck_data.profiler.time.sleep")
def test_profile_table_success(mock_sleep, databricks_client_stub, warehouse_id):
    """Test successfully profiling a table."""
    # Use real profiler logic with external API stubbing
    table_info = {
        "catalog_name": "catalog1",
        "schema_name": "schema1",
        "table_name": "table1",
    }

    # Configure databricks_client_stub for all the API calls
    # Mock the SQL query execution for list_tables
    databricks_client_stub.post.side_effect = [
        {"statement_id": "stmt-123"},  # list_tables query
        {"statement_id": "stmt-456"},  # get_table_schema query
        {"statement_id": "stmt-789"},  # get_sample_data query
        {"predictions": [{"pii_tags": ["id"]}]},  # query_llm endpoint call
        {"success": True},  # store_manifest DBFS call
    ]

    databricks_client_stub.get.side_effect = [
        {  # list_tables result
            "status": {"state": "SUCCEEDED"},
            "result": {
                "data": [
                    ["table1", "catalog1", "schema1"],
                ]
            },
        },
        {  # get_table_schema result
            "status": {"state": "SUCCEEDED"},
            "result": {"data": [["id", "integer", "Y", None, None, None, None]]},
        },
        {  # get_sample_data result
            "status": {"state": "SUCCEEDED"},
            "result": {
                "data": [
                    ["id"],  # column headers
                    [1],  # data row
                    [2],  # data row
                ]
            },
        },
    ]

    # Call the function - should use real profiler logic
    result = profile_table(databricks_client_stub, warehouse_id, "test-model")

    # Verify the result is a manifest path
    assert result is not None
    assert "/chuck/manifests/" in result
    assert result.endswith("_manifest.json")

    # Verify API calls were made (external boundaries)
    assert (
        databricks_client_stub.post.call_count >= 4
    )  # list, schema, sample, llm calls
    assert databricks_client_stub.get.call_count >= 3  # polling for query results


def test_query_llm(databricks_client_stub):
    """Test querying the LLM."""
    # Set up external API response
    databricks_client_stub.post.return_value = {"predictions": [{"pii_tags": ["id"]}]}

    # Test data
    endpoint_name = "test-model"
    input_data = {
        "schema": [{"col_name": "id", "data_type": "integer"}],
        "sample_data": {"column_names": ["id"], "rows": [{"id": 1}]},
    }

    # Call the function
    result = query_llm(databricks_client_stub, endpoint_name, input_data)

    # Check the result
    assert result == {"predictions": [{"pii_tags": ["id"]}]}

    # Verify API call
    databricks_client_stub.post.assert_called_once()
    assert (
        databricks_client_stub.post.call_args[0][0]
        == "/api/2.0/serving-endpoints/test-model/invocations"
    )
