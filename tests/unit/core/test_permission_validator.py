"""Tests for the permission validator module."""

import pytest
from unittest.mock import patch, MagicMock, call

from chuck_data.databricks.permission_validator import (
    validate_all_permissions,
    check_basic_connectivity,
    check_unity_catalog,
    check_sql_warehouse,
    check_jobs,
    check_models,
    check_volumes,
)


@pytest.fixture
def mock_client():
    """Mock HTTP client fixture for testing individual functions."""
    return MagicMock()


@pytest.fixture
def databricks_client_stub():
    """DatabricksClientStub fixture for integration testing."""
    from tests.fixtures.databricks.client import DatabricksClientStub

    return DatabricksClientStub()


def test_validate_all_permissions_success(databricks_client_stub):
    """Test that validate_all_permissions works with all permissions granted."""
    # Set up successful responses for all permission checks
    databricks_client_stub.set_get_response(
        "/api/2.0/preview/scim/v2/Me", {"userName": "test_user"}
    )
    databricks_client_stub.set_get_response(
        "/api/2.1/unity-catalog/catalogs?max_results=1",
        {"catalogs": [{"name": "test_catalog"}]},
    )
    databricks_client_stub.set_get_response(
        "/api/2.0/sql/warehouses?page_size=1", {"warehouses": [{"id": "warehouse1"}]}
    )
    databricks_client_stub.set_get_response(
        "/api/2.1/jobs/list?limit=1", {"jobs": [{"job_id": "job1"}]}
    )
    databricks_client_stub.set_get_response(
        "/api/2.0/mlflow/registered-models/list?max_results=1",
        {"registered_models": [{"name": "model1"}]},
    )

    # Set up volume check responses (multi-step process)
    databricks_client_stub.set_get_response(
        "/api/2.1/unity-catalog/catalogs?max_results=1",
        {"catalogs": [{"name": "test_catalog"}]},
    )
    databricks_client_stub.set_get_response(
        "/api/2.1/unity-catalog/schemas?catalog_name=test_catalog&max_results=1",
        {"schemas": [{"name": "test_schema"}]},
    )
    databricks_client_stub.set_get_response(
        "/api/2.1/unity-catalog/volumes?catalog_name=test_catalog&schema_name=test_schema",
        {"volumes": [{"name": "test_volume"}]},
    )

    # Call the real function with real business logic
    result = validate_all_permissions(databricks_client_stub)

    # Verify result contains all categories with expected structure
    assert "basic_connectivity" in result
    assert "unity_catalog" in result
    assert "sql_warehouse" in result
    assert "jobs" in result
    assert "models" in result
    assert "volumes" in result

    # Verify all categories show as authorized
    assert result["basic_connectivity"]["authorized"]
    assert result["unity_catalog"]["authorized"]
    assert result["sql_warehouse"]["authorized"]
    assert result["jobs"]["authorized"]
    assert result["models"]["authorized"]
    assert result["volumes"]["authorized"]


def test_validate_all_permissions_with_failures(databricks_client_stub):
    """Test that validate_all_permissions handles permission failures correctly."""
    # Set up mixed success/failure responses
    databricks_client_stub.set_get_response(
        "/api/2.0/preview/scim/v2/Me", {"userName": "test_user"}
    )
    databricks_client_stub.set_get_error(
        "/api/2.1/unity-catalog/catalogs?max_results=1", Exception("Access denied")
    )
    databricks_client_stub.set_get_response(
        "/api/2.0/sql/warehouses?page_size=1", {"warehouses": []}
    )
    databricks_client_stub.set_get_error(
        "/api/2.1/jobs/list?limit=1", Exception("Forbidden")
    )
    databricks_client_stub.set_get_response(
        "/api/2.0/mlflow/registered-models/list?max_results=1",
        {"registered_models": []},
    )

    # Volumes will fail due to catalog access denial

    # Call the real function
    result = validate_all_permissions(databricks_client_stub)

    # Verify result structure
    assert "basic_connectivity" in result
    assert "unity_catalog" in result
    assert "sql_warehouse" in result
    assert "jobs" in result
    assert "models" in result
    assert "volumes" in result

    # Verify mixed authorization results
    assert result["basic_connectivity"]["authorized"]  # Should succeed
    assert not result["unity_catalog"]["authorized"]  # Should fail - access denied
    assert result["sql_warehouse"][
        "authorized"
    ]  # Should succeed - empty list still authorized
    assert not result["jobs"]["authorized"]  # Should fail - forbidden
    assert result["models"][
        "authorized"
    ]  # Should succeed - empty list still authorized
    assert not result["volumes"]["authorized"]  # Should fail - catalog access denied


@patch("logging.debug")
def test_check_basic_connectivity_success(mock_debug, mock_client):
    """Test basic connectivity check with successful response."""
    # Set up mock response
    mock_client.get.return_value = {"userName": "test_user"}

    # Call the function
    result = check_basic_connectivity(mock_client)

    # Verify the API was called correctly
    mock_client.get.assert_called_once_with("/api/2.0/preview/scim/v2/Me")

    # Verify the result
    assert result["authorized"]
    assert result["details"] == "Connected as test_user"
    assert result["api_path"] == "/api/2.0/preview/scim/v2/Me"

    # Verify logging occurred
    mock_debug.assert_not_called()  # No errors, so no debug logging


@patch("logging.debug")
def test_check_basic_connectivity_error(mock_debug, mock_client):
    """Test basic connectivity check with error."""
    # Set up mock response
    mock_client.get.side_effect = Exception("Connection failed")

    # Call the function
    result = check_basic_connectivity(mock_client)

    # Verify the API was called correctly
    mock_client.get.assert_called_once_with("/api/2.0/preview/scim/v2/Me")

    # Verify the result
    assert not result["authorized"]
    assert result["error"] == "Connection failed"
    assert result["api_path"] == "/api/2.0/preview/scim/v2/Me"

    # Verify logging occurred
    mock_debug.assert_called_once()


@patch("logging.debug")
def test_check_unity_catalog_success(mock_debug, mock_client):
    """Test Unity Catalog check with successful response."""
    # Set up mock response
    mock_client.get.return_value = {"catalogs": [{"name": "test_catalog"}]}

    # Call the function
    result = check_unity_catalog(mock_client)

    # Verify the API was called correctly
    mock_client.get.assert_called_once_with(
        "/api/2.1/unity-catalog/catalogs?max_results=1"
    )

    # Verify the result
    assert result["authorized"]
    assert result["details"] == "Unity Catalog access granted (1 catalogs visible)"
    assert result["api_path"] == "/api/2.1/unity-catalog/catalogs"

    # Verify logging occurred
    mock_debug.assert_not_called()


@patch("logging.debug")
def test_check_unity_catalog_empty(mock_debug, mock_client):
    """Test Unity Catalog check with empty response."""
    # Set up mock response
    mock_client.get.return_value = {"catalogs": []}

    # Call the function
    result = check_unity_catalog(mock_client)

    # Verify the API was called correctly
    mock_client.get.assert_called_once_with(
        "/api/2.1/unity-catalog/catalogs?max_results=1"
    )

    # Verify the result
    assert result["authorized"]
    assert result["details"] == "Unity Catalog access granted (0 catalogs visible)"
    assert result["api_path"] == "/api/2.1/unity-catalog/catalogs"

    # Verify logging occurred
    mock_debug.assert_not_called()


@patch("logging.debug")
def test_check_unity_catalog_error(mock_debug, mock_client):
    """Test Unity Catalog check with error."""
    # Set up mock response
    mock_client.get.side_effect = Exception("Access denied")

    # Call the function
    result = check_unity_catalog(mock_client)

    # Verify the API was called correctly
    mock_client.get.assert_called_once_with(
        "/api/2.1/unity-catalog/catalogs?max_results=1"
    )

    # Verify the result
    assert not result["authorized"]
    assert result["error"] == "Access denied"
    assert result["api_path"] == "/api/2.1/unity-catalog/catalogs"

    # Verify logging occurred
    mock_debug.assert_called_once()


@patch("logging.debug")
def test_check_sql_warehouse_success(mock_debug, mock_client):
    """Test SQL warehouse check with successful response."""
    # Set up mock response
    mock_client.get.return_value = {"warehouses": [{"id": "warehouse1"}]}

    # Call the function
    result = check_sql_warehouse(mock_client)

    # Verify the API was called correctly
    mock_client.get.assert_called_once_with("/api/2.0/sql/warehouses?page_size=1")

    # Verify the result
    assert result["authorized"]
    assert result["details"] == "SQL Warehouse access granted (1 warehouses visible)"
    assert result["api_path"] == "/api/2.0/sql/warehouses"

    # Verify logging occurred
    mock_debug.assert_not_called()


@patch("logging.debug")
def test_check_sql_warehouse_error(mock_debug, mock_client):
    """Test SQL warehouse check with error."""
    # Set up mock response
    mock_client.get.side_effect = Exception("Access denied")

    # Call the function
    result = check_sql_warehouse(mock_client)

    # Verify the API was called correctly
    mock_client.get.assert_called_once_with("/api/2.0/sql/warehouses?page_size=1")

    # Verify the result
    assert not result["authorized"]
    assert result["error"] == "Access denied"
    assert result["api_path"] == "/api/2.0/sql/warehouses"

    # Verify logging occurred
    mock_debug.assert_called_once()


@patch("logging.debug")
def test_check_jobs_success(mock_debug, mock_client):
    """Test jobs check with successful response."""
    # Set up mock response
    mock_client.get.return_value = {"jobs": [{"job_id": "job1"}]}

    # Call the function
    result = check_jobs(mock_client)

    # Verify the API was called correctly
    mock_client.get.assert_called_once_with("/api/2.1/jobs/list?limit=1")

    # Verify the result
    assert result["authorized"]
    assert result["details"] == "Jobs access granted (1 jobs visible)"
    assert result["api_path"] == "/api/2.1/jobs/list"

    # Verify logging occurred
    mock_debug.assert_not_called()


@patch("logging.debug")
def test_check_jobs_error(mock_debug, mock_client):
    """Test jobs check with error."""
    # Set up mock response
    mock_client.get.side_effect = Exception("Access denied")

    # Call the function
    result = check_jobs(mock_client)

    # Verify the API was called correctly
    mock_client.get.assert_called_once_with("/api/2.1/jobs/list?limit=1")

    # Verify the result
    assert not result["authorized"]
    assert result["error"] == "Access denied"
    assert result["api_path"] == "/api/2.1/jobs/list"

    # Verify logging occurred
    mock_debug.assert_called_once()


@patch("logging.debug")
def test_check_models_success(mock_debug, mock_client):
    """Test models check with successful response."""
    # Set up mock response
    mock_client.get.return_value = {"registered_models": [{"name": "model1"}]}

    # Call the function
    result = check_models(mock_client)

    # Verify the API was called correctly
    mock_client.get.assert_called_once_with(
        "/api/2.0/mlflow/registered-models/list?max_results=1"
    )

    # Verify the result
    assert result["authorized"]
    assert result["details"] == "ML Models access granted (1 models visible)"
    assert result["api_path"] == "/api/2.0/mlflow/registered-models/list"

    # Verify logging occurred
    mock_debug.assert_not_called()


@patch("logging.debug")
def test_check_models_error(mock_debug, mock_client):
    """Test models check with error."""
    # Set up mock response
    mock_client.get.side_effect = Exception("Access denied")

    # Call the function
    result = check_models(mock_client)

    # Verify the API was called correctly
    mock_client.get.assert_called_once_with(
        "/api/2.0/mlflow/registered-models/list?max_results=1"
    )

    # Verify the result
    assert not result["authorized"]
    assert result["error"] == "Access denied"
    assert result["api_path"] == "/api/2.0/mlflow/registered-models/list"

    # Verify logging occurred
    mock_debug.assert_called_once()


@patch("logging.debug")
def test_check_volumes_success_full_path(mock_debug, mock_client):
    """Test volumes check with successful response through the full path."""
    # Set up mock responses for the multi-step process
    catalog_response = {"catalogs": [{"name": "test_catalog"}]}
    schema_response = {"schemas": [{"name": "test_schema"}]}
    volume_response = {"volumes": [{"name": "test_volume"}]}

    # Configure the client mock to return different responses for different calls
    mock_client.get.side_effect = [
        catalog_response,
        schema_response,
        volume_response,
    ]

    # Call the function
    result = check_volumes(mock_client)

    # Verify the API calls were made correctly
    expected_calls = [
        call("/api/2.1/unity-catalog/catalogs?max_results=1"),
        call("/api/2.1/unity-catalog/schemas?catalog_name=test_catalog&max_results=1"),
        call(
            "/api/2.1/unity-catalog/volumes?catalog_name=test_catalog&schema_name=test_schema"
        ),
    ]
    assert mock_client.get.call_args_list == expected_calls

    # Verify the result
    assert result["authorized"]
    assert (
        result["details"]
        == "Volumes access granted in test_catalog.test_schema (1 volumes visible)"
    )
    assert result["api_path"] == "/api/2.1/unity-catalog/volumes"

    # Verify logging occurred
    mock_debug.assert_not_called()


@patch("logging.debug")
def test_check_volumes_no_catalogs(mock_debug, mock_client):
    """Test volumes check when no catalogs are available."""
    # Set up empty catalog response
    mock_client.get.return_value = {"catalogs": []}

    # Call the function
    result = check_volumes(mock_client)

    # Verify only the catalogs API was called
    mock_client.get.assert_called_once_with(
        "/api/2.1/unity-catalog/catalogs?max_results=1"
    )

    # Verify the result
    assert not result["authorized"]
    assert result["error"] == "No catalogs available to check volumes access"
    assert result["api_path"] == "/api/2.1/unity-catalog/volumes"

    # Verify logging occurred
    mock_debug.assert_not_called()


@patch("logging.debug")
def test_check_volumes_no_schemas(mock_debug, mock_client):
    """Test volumes check when no schemas are available."""
    # Set up mock responses
    catalog_response = {"catalogs": [{"name": "test_catalog"}]}
    schema_response = {"schemas": []}

    # Configure the client mock
    mock_client.get.side_effect = [catalog_response, schema_response]

    # Call the function
    result = check_volumes(mock_client)

    # Verify the APIs were called
    expected_calls = [
        call("/api/2.1/unity-catalog/catalogs?max_results=1"),
        call("/api/2.1/unity-catalog/schemas?catalog_name=test_catalog&max_results=1"),
    ]
    assert mock_client.get.call_args_list == expected_calls

    # Verify the result
    assert not result["authorized"]
    assert (
        result["error"]
        == "No schemas available in catalog 'test_catalog' to check volumes access"
    )
    assert result["api_path"] == "/api/2.1/unity-catalog/volumes"

    # Verify logging occurred
    mock_debug.assert_not_called()


@patch("logging.debug")
def test_check_volumes_error(mock_debug, mock_client):
    """Test volumes check with an API error."""
    # Set up mock response to raise exception
    mock_client.get.side_effect = Exception("Access denied")

    # Call the function
    result = check_volumes(mock_client)

    # Verify the API was called
    mock_client.get.assert_called_once_with(
        "/api/2.1/unity-catalog/catalogs?max_results=1"
    )

    # Verify the result
    assert not result["authorized"]
    assert result["error"] == "Access denied"
    assert result["api_path"] == "/api/2.1/unity-catalog/volumes"

    # Verify logging occurred
    mock_debug.assert_called_once()
