"""Tests for the DatabricksAPIClient class."""

import pytest
from unittest.mock import patch, MagicMock
import requests
from chuck_data.clients.databricks import DatabricksAPIClient


@pytest.fixture
def databricks_api_client():
    """Create a DatabricksAPIClient instance for testing."""
    workspace_url = "test-workspace"
    token = "fake-token"
    return DatabricksAPIClient(workspace_url, token)


def test_workspace_url_normalization():
    """Test that workspace URLs are normalized correctly."""
    test_cases = [
        ("workspace", "workspace"),
        ("https://workspace", "workspace"),
        ("http://workspace", "workspace"),
        ("workspace.cloud.databricks.com", "workspace"),
        ("https://workspace.cloud.databricks.com", "workspace"),
        ("https://workspace.cloud.databricks.com/", "workspace"),
        ("dbc-12345-ab", "dbc-12345-ab"),
        # Azure test cases
        ("adb-3856707039489412.12.azuredatabricks.net", "adb-3856707039489412.12"),
        (
            "https://adb-3856707039489412.12.azuredatabricks.net",
            "adb-3856707039489412.12",
        ),
        ("workspace.azuredatabricks.net", "workspace"),
        # GCP test cases
        ("workspace.gcp.databricks.com", "workspace"),
        ("https://workspace.gcp.databricks.com", "workspace"),
    ]

    for input_url, expected_url in test_cases:
        client = DatabricksAPIClient(input_url, "token")
        assert (
            client.workspace_url == expected_url
        ), f"URL should be normalized: {input_url} -> {expected_url}"


def test_azure_domain_detection_and_url_construction():
    """Test that Azure domains are detected correctly and URLs are constructed properly."""
    azure_client = DatabricksAPIClient(
        "adb-3856707039489412.12.azuredatabricks.net", "token"
    )

    # Check that cloud provider is detected correctly
    assert azure_client.cloud_provider == "Azure"
    assert azure_client.base_domain == "azuredatabricks.net"
    assert azure_client.workspace_url == "adb-3856707039489412.12"


def test_gcp_domain_detection_and_url_construction():
    """Test that GCP domains are detected correctly and URLs are constructed properly."""
    gcp_client = DatabricksAPIClient("workspace.gcp.databricks.com", "token")

    # Check that cloud provider is detected correctly
    assert gcp_client.cloud_provider == "GCP"
    assert gcp_client.base_domain == "gcp.databricks.com"
    assert gcp_client.workspace_url == "workspace"


@patch("chuck_data.clients.databricks.requests.get")
def test_get_success(mock_get, databricks_api_client):
    """Test successful GET request."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"key": "value"}
    mock_get.return_value = mock_response

    response = databricks_api_client.get("/test-endpoint")
    assert response == {"key": "value"}
    mock_get.assert_called_once_with(
        "https://test-workspace.cloud.databricks.com/test-endpoint",
        headers={
            "Authorization": "Bearer fake-token",
            "User-Agent": "amperity",
        },
    )


@patch("chuck_data.clients.databricks.requests.get")
def test_get_http_error(mock_get, databricks_api_client):
    """Test GET request with HTTP error."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "HTTP 404"
    )
    mock_response.text = "Not Found"
    mock_get.return_value = mock_response

    with pytest.raises(ValueError) as exc_info:
        databricks_api_client.get("/test-endpoint")

    assert "HTTP error occurred" in str(exc_info.value)
    assert "Not Found" in str(exc_info.value)


@patch("chuck_data.clients.databricks.requests.get")
def test_get_connection_error(mock_get, databricks_api_client):
    """Test GET request with connection error."""
    mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")

    with pytest.raises(ConnectionError) as exc_info:
        databricks_api_client.get("/test-endpoint")

    assert "Connection error occurred" in str(exc_info.value)


@patch("chuck_data.clients.databricks.requests.post")
def test_post_success(mock_post, databricks_api_client):
    """Test successful POST request."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"key": "value"}
    mock_post.return_value = mock_response

    response = databricks_api_client.post("/test-endpoint", {"data": "test"})
    assert response == {"key": "value"}
    mock_post.assert_called_once_with(
        "https://test-workspace.cloud.databricks.com/test-endpoint",
        headers={
            "Authorization": "Bearer fake-token",
            "User-Agent": "amperity",
        },
        json={"data": "test"},
    )


@patch("chuck_data.clients.databricks.requests.post")
def test_post_http_error(mock_post, databricks_api_client):
    """Test POST request with HTTP error."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "HTTP 400"
    )
    mock_response.text = "Bad Request"
    mock_post.return_value = mock_response

    with pytest.raises(ValueError) as exc_info:
        databricks_api_client.post("/test-endpoint", {"data": "test"})

    assert "HTTP error occurred" in str(exc_info.value)
    assert "Bad Request" in str(exc_info.value)


@patch("chuck_data.clients.databricks.requests.post")
def test_post_connection_error(mock_post, databricks_api_client):
    """Test POST request with connection error."""
    mock_post.side_effect = requests.exceptions.ConnectionError("Connection failed")

    with pytest.raises(ConnectionError) as exc_info:
        databricks_api_client.post("/test-endpoint", {"data": "test"})

    assert "Connection error occurred" in str(exc_info.value)


@patch("chuck_data.clients.databricks.requests.post")
def test_fetch_amperity_job_init_http_error(mock_post, databricks_api_client):
    """fetch_amperity_job_init should show helpful message on HTTP errors."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "HTTP 401", response=mock_response
    )
    mock_response.status_code = 401
    mock_response.text = '{"status":401,"message":"Unauthorized"}'
    mock_response.json.return_value = {
        "status": 401,
        "message": "Unauthorized",
    }
    mock_post.return_value = mock_response

    with pytest.raises(ValueError) as exc_info:
        databricks_api_client.fetch_amperity_job_init("fake-token")

    assert "401 Error" in str(exc_info.value)
    assert "Please /logout and /login again" in str(exc_info.value)
