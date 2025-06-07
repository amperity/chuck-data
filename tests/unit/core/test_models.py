"""Unit tests for the models module."""

import pytest
from chuck_data.models import list_models, get_model


def test_list_models_success(databricks_client_stub):
    """Test successful retrieval of model list."""
    # Configure stub to return expected model list
    expected_models = [
        {"name": "model1", "state": "READY", "creation_timestamp": 1234567890},
        {"name": "model2", "state": "READY", "creation_timestamp": 1234567891},
    ]
    databricks_client_stub.models = expected_models

    models = list_models(databricks_client_stub)

    assert models == expected_models


def test_list_models_empty(databricks_client_stub):
    """Test retrieval with empty model list."""
    # Configure stub to return empty list
    databricks_client_stub.models = []

    models = list_models(databricks_client_stub)
    assert models == []


def test_list_models_http_error(databricks_client_stub):
    """Test failure with HTTP error."""
    # Configure stub to raise ValueError
    databricks_client_stub.set_list_models_error(
        ValueError("HTTP error occurred: 404 Not Found")
    )

    with pytest.raises(ValueError) as excinfo:
        list_models(databricks_client_stub)
    assert "Model serving API error" in str(excinfo.value)


def test_list_models_connection_error(databricks_client_stub):
    """Test failure due to connection error."""
    # Configure stub to raise ConnectionError
    databricks_client_stub.set_list_models_error(ConnectionError("Connection failed"))

    with pytest.raises(ConnectionError) as excinfo:
        list_models(databricks_client_stub)
    assert "Failed to connect to serving endpoint" in str(excinfo.value)


def test_get_model_success(databricks_client_stub):
    """Test successful retrieval of a specific model."""
    # Configure model detail
    model_detail = {
        "name": "databricks-llama-4-maverick",
        "creator": "user@example.com",
        "creation_timestamp": 1645123456789,
        "state": "READY",
    }
    databricks_client_stub.add_model(
        "databricks-llama-4-maverick",
        status="READY",
        creator="user@example.com",
        creation_timestamp=1645123456789,
    )

    # Call the function
    result = get_model(databricks_client_stub, "databricks-llama-4-maverick")

    # Verify results
    assert result["name"] == model_detail["name"]
    assert result["creator"] == model_detail["creator"]


def test_get_model_not_found(databricks_client_stub):
    """Test retrieval of a non-existent model."""
    # No model added, so get_model will return None

    # Call the function
    result = get_model(databricks_client_stub, "nonexistent-model")

    # Verify result is None
    assert result is None


def test_get_model_error(databricks_client_stub):
    """Test retrieval with a non-404 error."""
    # Configure stub to raise a 500 error
    databricks_client_stub.set_get_model_error(
        ValueError("HTTP error occurred: 500 Internal Server Error")
    )

    # Call the function and expect an exception
    with pytest.raises(ValueError) as excinfo:
        get_model(databricks_client_stub, "error-model")

    # Verify error handling
    assert "Model serving API error" in str(excinfo.value)


def test_get_model_connection_error(databricks_client_stub):
    """Test retrieval with connection error."""
    # Configure stub to raise a connection error
    databricks_client_stub.set_get_model_error(ConnectionError("Connection failed"))

    # Call the function and expect an exception
    with pytest.raises(ConnectionError) as excinfo:
        get_model(databricks_client_stub, "network-error-model")

    # Verify error handling
    assert "Failed to connect to serving endpoint" in str(excinfo.value)
