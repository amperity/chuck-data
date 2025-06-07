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
def client():
    """Mock client fixture."""
    return MagicMock()


def test_validate_all_permissions(client):
    """Test that validate_all_permissions calls all check functions."""
    with (
        patch(
            "chuck_data.databricks.permission_validator.check_basic_connectivity"
        ) as mock_basic,
        patch(
            "chuck_data.databricks.permission_validator.check_unity_catalog"
        ) as mock_catalog,
        patch(
            "chuck_data.databricks.permission_validator.check_sql_warehouse"
        ) as mock_warehouse,
        patch("chuck_data.databricks.permission_validator.check_jobs") as mock_jobs,
        patch("chuck_data.databricks.permission_validator.check_models") as mock_models,
        patch(
            "chuck_data.databricks.permission_validator.check_volumes"
        ) as mock_volumes,
    ):

        # Set return values for mock functions
        mock_basic.return_value = {"authorized": True}
        mock_catalog.return_value = {"authorized": True}
        mock_warehouse.return_value = {"authorized": True}
        mock_jobs.return_value = {"authorized": True}
        mock_models.return_value = {"authorized": True}
        mock_volumes.return_value = {"authorized": True}

        # Call the function
        result = validate_all_permissions(client)

        # Verify all check functions were called
        mock_basic.assert_called_once_with(client)
        mock_catalog.assert_called_once_with(client)
        mock_warehouse.assert_called_once_with(client)
        mock_jobs.assert_called_once_with(client)
        mock_models.assert_called_once_with(client)
        mock_volumes.assert_called_once_with(client)

        # Verify result contains all categories
        assert "basic_connectivity" in result
        assert "unity_catalog" in result
        assert "sql_warehouse" in result
        assert "jobs" in result
        assert "models" in result
        assert "volumes" in result


@patch("logging.debug")
def test_check_basic_connectivity_success(mock_debug, client):
    """Test basic connectivity check with successful response."""
    # Set up mock response
    client.get.return_value = {"userName": "test_user"}

    # Call the function
    result = check_basic_connectivity(client)

    # Verify the API was called correctly
    client.get.assert_called_once_with("/api/2.0/preview/scim/v2/Me")

    # Verify the result
    assert result["authorized"]
    assert result["details"] == "Connected as test_user"
    assert result["api_path"] == "/api/2.0/preview/scim/v2/Me"

    # Verify logging occurred
    mock_debug.assert_not_called()  # No errors, so no debug logging


@patch("logging.debug")
def test_check_basic_connectivity_error(mock_debug, client):
    """Test basic connectivity check with error."""
    # Set up mock response
    client.get.side_effect = Exception("Connection failed")

    # Call the function
    result = check_basic_connectivity(client)

    # Verify the API was called correctly
    client.get.assert_called_once_with("/api/2.0/preview/scim/v2/Me")

    # Verify the result
    assert not result["authorized"]
    assert result["error"] == "Connection failed"
    assert result["api_path"] == "/api/2.0/preview/scim/v2/Me"

    # Verify logging occurred
    mock_debug.assert_called_once()


@patch("logging.debug")
def test_check_unity_catalog_success(mock_debug, client):
    """Test Unity Catalog check with successful response."""
    # Set up mock response
    client.get.return_value = {"catalogs": [{"name": "test_catalog"}]}

    # Call the function
    result = check_unity_catalog(client)

    # Verify the API was called correctly
    client.get.assert_called_once_with("/api/2.1/unity-catalog/catalogs?max_results=1")

    # Verify the result
    assert result["authorized"]
    assert result["details"] == "Unity Catalog access granted (1 catalogs visible)"
    assert result["api_path"] == "/api/2.1/unity-catalog/catalogs"

    # Verify logging occurred
    mock_debug.assert_not_called()


@patch("logging.debug")
def test_check_unity_catalog_empty(mock_debug, client):
    """Test Unity Catalog check with empty response."""
    # Set up mock response
    client.get.return_value = {"catalogs": []}

    # Call the function
    result = check_unity_catalog(client)

    # Verify the API was called correctly
    client.get.assert_called_once_with("/api/2.1/unity-catalog/catalogs?max_results=1")

    # Verify the result
    assert result["authorized"]
    assert result["details"] == "Unity Catalog access granted (0 catalogs visible)"
    assert result["api_path"] == "/api/2.1/unity-catalog/catalogs"

    # Verify logging occurred
    mock_debug.assert_not_called()


@patch("logging.debug")
def test_check_unity_catalog_error(mock_debug, client):
    """Test Unity Catalog check with error."""
    # Set up mock response
    client.get.side_effect = Exception("Access denied")

    # Call the function
    result = check_unity_catalog(client)

    # Verify the API was called correctly
    client.get.assert_called_once_with("/api/2.1/unity-catalog/catalogs?max_results=1")

    # Verify the result
    assert not result["authorized"]
    assert result["error"] == "Access denied"
    assert result["api_path"] == "/api/2.1/unity-catalog/catalogs"

    # Verify logging occurred
    mock_debug.assert_called_once()


@patch("logging.debug")
def test_check_sql_warehouse_success(mock_debug, client):
    """Test SQL warehouse check with successful response."""
    # Set up mock response
    client.get.return_value = {"warehouses": [{"id": "warehouse1"}]}

    # Call the function
    result = check_sql_warehouse(client)

    # Verify the API was called correctly
    client.get.assert_called_once_with("/api/2.0/sql/warehouses?page_size=1")

    # Verify the result
    assert result["authorized"]
    assert result["details"] == "SQL Warehouse access granted (1 warehouses visible)"
    assert result["api_path"] == "/api/2.0/sql/warehouses"

    # Verify logging occurred
    mock_debug.assert_not_called()


@patch("logging.debug")
def test_check_sql_warehouse_error(mock_debug, client):
    """Test SQL warehouse check with error."""
    # Set up mock response
    client.get.side_effect = Exception("Access denied")

    # Call the function
    result = check_sql_warehouse(client)

    # Verify the API was called correctly
    client.get.assert_called_once_with("/api/2.0/sql/warehouses?page_size=1")

    # Verify the result
    assert not result["authorized"]
    assert result["error"] == "Access denied"
    assert result["api_path"] == "/api/2.0/sql/warehouses"

    # Verify logging occurred
    mock_debug.assert_called_once()


@patch("logging.debug")
def test_check_jobs_success(mock_debug, client):
    """Test jobs check with successful response."""
    # Set up mock response
    client.get.return_value = {"jobs": [{"job_id": "job1"}]}

    # Call the function
    result = check_jobs(client)

    # Verify the API was called correctly
    client.get.assert_called_once_with("/api/2.1/jobs/list?limit=1")

    # Verify the result
    assert result["authorized"]
    assert result["details"] == "Jobs access granted (1 jobs visible)"
    assert result["api_path"] == "/api/2.1/jobs/list"

    # Verify logging occurred
    mock_debug.assert_not_called()


@patch("logging.debug")
def test_check_jobs_error(mock_debug, client):
    """Test jobs check with error."""
    # Set up mock response
    client.get.side_effect = Exception("Access denied")

    # Call the function
    result = check_jobs(client)

    # Verify the API was called correctly
    client.get.assert_called_once_with("/api/2.1/jobs/list?limit=1")

    # Verify the result
    assert not result["authorized"]
    assert result["error"] == "Access denied"
    assert result["api_path"] == "/api/2.1/jobs/list"

    # Verify logging occurred
    mock_debug.assert_called_once()


@patch("logging.debug")
def test_check_models_success(mock_debug, client):
    """Test models check with successful response."""
    # Set up mock response
    client.get.return_value = {"registered_models": [{"name": "model1"}]}

    # Call the function
    result = check_models(client)

    # Verify the API was called correctly
    client.get.assert_called_once_with(
        "/api/2.0/mlflow/registered-models/list?max_results=1"
    )

    # Verify the result
    assert result["authorized"]
    assert result["details"] == "ML Models access granted (1 models visible)"
    assert result["api_path"] == "/api/2.0/mlflow/registered-models/list"

    # Verify logging occurred
    mock_debug.assert_not_called()


@patch("logging.debug")
def test_check_models_error(mock_debug, client):
    """Test models check with error."""
    # Set up mock response
    client.get.side_effect = Exception("Access denied")

    # Call the function
    result = check_models(client)

    # Verify the API was called correctly
    client.get.assert_called_once_with(
        "/api/2.0/mlflow/registered-models/list?max_results=1"
    )

    # Verify the result
    assert not result["authorized"]
    assert result["error"] == "Access denied"
    assert result["api_path"] == "/api/2.0/mlflow/registered-models/list"

    # Verify logging occurred
    mock_debug.assert_called_once()


@patch("logging.debug")
def test_check_volumes_success_full_path(mock_debug, client):
    """Test volumes check with successful response through the full path."""
    # Set up mock responses for the multi-step process
    catalog_response = {"catalogs": [{"name": "test_catalog"}]}
    schema_response = {"schemas": [{"name": "test_schema"}]}
    volume_response = {"volumes": [{"name": "test_volume"}]}

    # Configure the client mock to return different responses for different calls
    client.get.side_effect = [
        catalog_response,
        schema_response,
        volume_response,
    ]

    # Call the function
    result = check_volumes(client)

    # Verify the API calls were made correctly
    expected_calls = [
        call("/api/2.1/unity-catalog/catalogs?max_results=1"),
        call("/api/2.1/unity-catalog/schemas?catalog_name=test_catalog&max_results=1"),
        call(
            "/api/2.1/unity-catalog/volumes?catalog_name=test_catalog&schema_name=test_schema"
        ),
    ]
    assert client.get.call_args_list == expected_calls

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
def test_check_volumes_no_catalogs(mock_debug, client):
    """Test volumes check when no catalogs are available."""
    # Set up empty catalog response
    client.get.return_value = {"catalogs": []}

    # Call the function
    result = check_volumes(client)

    # Verify only the catalogs API was called
    client.get.assert_called_once_with("/api/2.1/unity-catalog/catalogs?max_results=1")

    # Verify the result
    assert not result["authorized"]
    assert result["error"] == "No catalogs available to check volumes access"
    assert result["api_path"] == "/api/2.1/unity-catalog/volumes"

    # Verify logging occurred
    mock_debug.assert_not_called()


@patch("logging.debug")
def test_check_volumes_no_schemas(mock_debug, client):
    """Test volumes check when no schemas are available."""
    # Set up mock responses
    catalog_response = {"catalogs": [{"name": "test_catalog"}]}
    schema_response = {"schemas": []}

    # Configure the client mock
    client.get.side_effect = [catalog_response, schema_response]

    # Call the function
    result = check_volumes(client)

    # Verify the APIs were called
    expected_calls = [
        call("/api/2.1/unity-catalog/catalogs?max_results=1"),
        call("/api/2.1/unity-catalog/schemas?catalog_name=test_catalog&max_results=1"),
    ]
    assert client.get.call_args_list == expected_calls

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
def test_check_volumes_error(mock_debug, client):
    """Test volumes check with an API error."""
    # Set up mock response to raise exception
    client.get.side_effect = Exception("Access denied")

    # Call the function
    result = check_volumes(client)

    # Verify the API was called
    client.get.assert_called_once_with("/api/2.1/unity-catalog/catalogs?max_results=1")

    # Verify the result
    assert not result["authorized"]
    assert result["error"] == "Access denied"
    assert result["api_path"] == "/api/2.1/unity-catalog/volumes"

    # Verify logging occurred
    mock_debug.assert_called_once()
