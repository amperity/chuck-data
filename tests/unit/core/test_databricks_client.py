"""Tests for the DatabricksAPIClient class."""

import pytest
from unittest.mock import patch, MagicMock, mock_open
import requests
from chuck_data.clients.databricks import DatabricksAPIClient


@pytest.fixture
def client():
    """Create a DatabricksAPIClient for testing."""
    workspace_url = "test-workspace"
    token = "fake-token"
    return DatabricksAPIClient(workspace_url, token)


def test_normalize_workspace_url(client):
    """Test URL normalization."""
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
        result = client._normalize_workspace_url(input_url)
        assert result == expected_url


def test_azure_client_url_construction():
    """Test that Azure client constructs URLs with correct domain."""
    azure_client = DatabricksAPIClient(
        "adb-3856707039489412.12.azuredatabricks.net", "token"
    )

    # Check that cloud provider is detected correctly
    assert azure_client.cloud_provider == "Azure"
    assert azure_client.base_domain == "azuredatabricks.net"
    assert azure_client.workspace_url == "adb-3856707039489412.12"


def test_base_domain_map():
    """Ensure _get_base_domain uses the shared domain map."""
    from chuck_data.databricks.url_utils import DATABRICKS_DOMAIN_MAP

    for provider, domain in DATABRICKS_DOMAIN_MAP.items():
        client = DatabricksAPIClient("workspace", "token")
        client.cloud_provider = provider
        assert client._get_base_domain() == domain


@patch("requests.get")
def test_azure_get_request_url(mock_get):
    """Test that Azure client constructs correct URLs for GET requests."""
    azure_client = DatabricksAPIClient(
        "adb-3856707039489412.12.azuredatabricks.net", "token"
    )
    mock_response = MagicMock()
    mock_response.json.return_value = {"key": "value"}
    mock_get.return_value = mock_response

    azure_client.get("/test-endpoint")

    mock_get.assert_called_once_with(
        "https://adb-3856707039489412.12.azuredatabricks.net/test-endpoint",
        headers={
            "Authorization": "Bearer token",
            "User-Agent": "amperity",
        },
    )


def test_compute_node_types():
    """Test that appropriate compute node types are returned for each cloud provider."""
    test_cases = [
        ("workspace.cloud.databricks.com", "AWS", "r5d.4xlarge"),
        ("workspace.azuredatabricks.net", "Azure", "Standard_E16ds_v4"),
        ("workspace.gcp.databricks.com", "GCP", "n2-standard-16"),
        ("workspace.databricks.com", "Generic", "r5d.4xlarge"),
    ]

    for url, expected_provider, expected_node_type in test_cases:
        client = DatabricksAPIClient(url, "token")
        assert client.cloud_provider == expected_provider
        assert client.get_compute_node_type() == expected_node_type


def test_cloud_attributes():
    """Test that appropriate cloud attributes are returned for each provider."""
    # Test AWS attributes
    aws_client = DatabricksAPIClient("workspace.cloud.databricks.com", "token")
    aws_attrs = aws_client.get_cloud_attributes()
    assert "aws_attributes" in aws_attrs
    assert aws_attrs["aws_attributes"]["availability"] == "SPOT_WITH_FALLBACK"

    # Test Azure attributes
    azure_client = DatabricksAPIClient("workspace.azuredatabricks.net", "token")
    azure_attrs = azure_client.get_cloud_attributes()
    assert "azure_attributes" in azure_attrs
    assert azure_attrs["azure_attributes"]["availability"] == "SPOT_WITH_FALLBACK_AZURE"

    # Test GCP attributes
    gcp_client = DatabricksAPIClient("workspace.gcp.databricks.com", "token")
    gcp_attrs = gcp_client.get_cloud_attributes()
    assert "gcp_attributes" in gcp_attrs
    assert gcp_attrs["gcp_attributes"]["use_preemptible_executors"]


@patch.object(DatabricksAPIClient, "post")
def test_job_submission_uses_correct_node_type(mock_post):
    """Test that job submission uses the correct node type for Azure."""
    mock_post.return_value = {"run_id": "12345"}

    azure_client = DatabricksAPIClient("workspace.azuredatabricks.net", "token")
    azure_client.submit_job_run("/config/path", "/init/script/path")

    # Verify that post was called and get the payload
    mock_post.assert_called_once()
    call_args = mock_post.call_args
    payload = call_args[0][1]  # Second argument is the data payload

    # Check that the cluster config uses Azure node type
    cluster_config = payload["tasks"][0]["new_cluster"]
    assert cluster_config["node_type_id"] == "Standard_E16ds_v4"

    # Check that Azure attributes are present
    assert "azure_attributes" in cluster_config
    assert (
        cluster_config["azure_attributes"]["availability"] == "SPOT_WITH_FALLBACK_AZURE"
    )

    # Base API request tests


@patch("requests.get")
def test_get_success(mock_get, client):
    """Test successful GET request."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"key": "value"}
    mock_get.return_value = mock_response

    response = client.get("/test-endpoint")
    assert response == {"key": "value"}
    mock_get.assert_called_once_with(
        "https://test-workspace.cloud.databricks.com/test-endpoint",
        headers={
            "Authorization": "Bearer fake-token",
            "User-Agent": "amperity",
        },
    )


@patch("requests.get")
def test_get_http_error(mock_get, client):
    """Test GET request with HTTP error."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "HTTP 404"
    )
    mock_response.text = "Not Found"
    mock_get.return_value = mock_response

    with pytest.raises(ValueError) as exc_info:
        client.get("/test-endpoint")

    assert "HTTP error occurred" in str(exc_info.value)
    assert "Not Found" in str(exc_info.value)


@patch("requests.get")
def test_get_connection_error(mock_get, client):
    """Test GET request with connection error."""
    mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")

    with pytest.raises(ConnectionError) as exc_info:
        client.get("/test-endpoint")

    assert "Connection error occurred" in str(exc_info.value)


@patch("requests.post")
def test_post_success(mock_post, client):
    """Test successful POST request."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"key": "value"}
    mock_post.return_value = mock_response

    response = client.post("/test-endpoint", {"data": "test"})
    assert response == {"key": "value"}
    mock_post.assert_called_once_with(
        "https://test-workspace.cloud.databricks.com/test-endpoint",
        headers={
            "Authorization": "Bearer fake-token",
            "User-Agent": "amperity",
        },
        json={"data": "test"},
    )


@patch("requests.post")
def test_post_http_error(mock_post, client):
    """Test POST request with HTTP error."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "HTTP 400"
    )
    mock_response.text = "Bad Request"
    mock_post.return_value = mock_response

    with pytest.raises(ValueError) as exc_info:
        client.post("/test-endpoint", {"data": "test"})

    assert "HTTP error occurred" in str(exc_info.value)
    assert "Bad Request" in str(exc_info.value)


@patch("requests.post")
def test_post_connection_error(mock_post, client):
    """Test POST request with connection error."""
    mock_post.side_effect = requests.exceptions.ConnectionError("Connection failed")

    with pytest.raises(ConnectionError) as exc_info:
        client.post("/test-endpoint", {"data": "test"})

    assert "Connection error occurred" in str(exc_info.value)

    # Authentication method tests


@patch.object(DatabricksAPIClient, "get")
def test_validate_token_success(mock_get, client):
    """Test successful token validation."""
    mock_get.return_value = {"user_name": "test-user"}

    result = client.validate_token()

    assert result
    mock_get.assert_called_once_with("/api/2.0/preview/scim/v2/Me")


@patch.object(DatabricksAPIClient, "get")
def test_validate_token_failure(mock_get, client):
    """Test failed token validation."""
    mock_get.side_effect = Exception("Token validation failed")

    result = client.validate_token()

    assert not result
    mock_get.assert_called_once_with("/api/2.0/preview/scim/v2/Me")

    # Unity Catalog method tests


@patch.object(DatabricksAPIClient, "get")
@patch.object(DatabricksAPIClient, "get_with_params")
def test_list_catalogs(mock_get_with_params, mock_get, client):
    """Test list_catalogs with and without parameters."""
    # Without parameters
    mock_get.return_value = {"catalogs": [{"name": "test_catalog"}]}
    result = client.list_catalogs()
    assert result == {"catalogs": [{"name": "test_catalog"}]}
    mock_get.assert_called_once_with("/api/2.1/unity-catalog/catalogs")

    # With parameters
    mock_get_with_params.return_value = {"catalogs": [{"name": "test_catalog"}]}
    result = client.list_catalogs(include_browse=True, max_results=10)
    assert result == {"catalogs": [{"name": "test_catalog"}]}
    mock_get_with_params.assert_called_once_with(
        "/api/2.1/unity-catalog/catalogs",
        {"include_browse": "true", "max_results": "10"},
    )


@patch.object(DatabricksAPIClient, "get")
def test_get_catalog(mock_get, client):
    """Test get_catalog method."""
    mock_get.return_value = {"name": "test_catalog", "comment": "Test catalog"}

    result = client.get_catalog("test_catalog")

    assert result == {"name": "test_catalog", "comment": "Test catalog"}
    mock_get.assert_called_once_with("/api/2.1/unity-catalog/catalogs/test_catalog")

    # File system method tests


@patch("requests.put")
def test_upload_file_with_content(mock_put, client):
    """Test successful file upload with content."""
    mock_response = MagicMock()
    mock_response.status_code = 204
    mock_put.return_value = mock_response

    result = client.upload_file("/test/path.txt", content="Test content")

    assert result
    mock_put.assert_called_once()
    # Check URL and headers
    call_args = mock_put.call_args
    assert (
        "https://test-workspace.cloud.databricks.com/api/2.0/fs/files/test/path.txt"
        in call_args[0][0]
    )
    assert call_args[1]["headers"]["Content-Type"] == "application/octet-stream"
    # Check that content was encoded to bytes
    assert call_args[1]["data"] == b"Test content"


@patch("builtins.open", new_callable=mock_open, read_data=b"file content")
@patch("requests.put")
def test_upload_file_with_file_path(mock_put, mock_file, client):
    """Test successful file upload with file path."""
    mock_response = MagicMock()
    mock_response.status_code = 204
    mock_put.return_value = mock_response

    result = client.upload_file("/test/path.txt", file_path="/local/file.txt")

    assert result
    mock_file.assert_called_once_with("/local/file.txt", "rb")
    mock_put.assert_called_once()
    # Check that file content was read
    call_args = mock_put.call_args
    assert call_args[1]["data"] == b"file content"


def test_upload_file_invalid_args(client):
    """Test upload_file with invalid arguments."""
    # Test when both file_path and content are provided
    with pytest.raises(ValueError) as exc_info:
        client.upload_file("/test/path.txt", file_path="/local.txt", content="content")
    assert "Exactly one of file_path or content must be provided" in str(exc_info.value)

    # Test when neither file_path nor content is provided
    with pytest.raises(ValueError) as exc_info:
        client.upload_file("/test/path.txt")
    assert "Exactly one of file_path or content must be provided" in str(exc_info.value)

    # Model serving tests


@patch.object(DatabricksAPIClient, "get")
def test_list_models(mock_get, client):
    """Test list_models method."""
    mock_response = {"endpoints": [{"name": "model1"}, {"name": "model2"}]}
    mock_get.return_value = mock_response

    result = client.list_models()

    assert result == [{"name": "model1"}, {"name": "model2"}]
    mock_get.assert_called_once_with("/api/2.0/serving-endpoints")


@patch.object(DatabricksAPIClient, "get")
def test_get_model(mock_get, client):
    """Test get_model method."""
    mock_response = {"name": "model1", "status": "ready"}
    mock_get.return_value = mock_response

    result = client.get_model("model1")

    assert result == {"name": "model1", "status": "ready"}
    mock_get.assert_called_once_with("/api/2.0/serving-endpoints/model1")


@patch.object(DatabricksAPIClient, "get")
def test_get_model_not_found(mock_get, client):
    """Test get_model with 404 error."""
    mock_get.side_effect = ValueError("HTTP error occurred: 404 Not Found")

    result = client.get_model("nonexistent-model")

    assert result is None
    mock_get.assert_called_once_with("/api/2.0/serving-endpoints/nonexistent-model")

    # SQL warehouse tests


@patch.object(DatabricksAPIClient, "get")
def test_list_warehouses(mock_get, client):
    """Test list_warehouses method."""
    mock_response = {"warehouses": [{"id": "123"}, {"id": "456"}]}
    mock_get.return_value = mock_response

    result = client.list_warehouses()

    assert result == [{"id": "123"}, {"id": "456"}]
    mock_get.assert_called_once_with("/api/2.0/sql/warehouses")


@patch.object(DatabricksAPIClient, "get")
def test_get_warehouse(mock_get, client):
    """Test get_warehouse method."""
    mock_response = {"id": "123", "name": "Test Warehouse"}
    mock_get.return_value = mock_response

    result = client.get_warehouse("123")

    assert result == {"id": "123", "name": "Test Warehouse"}
    mock_get.assert_called_once_with("/api/2.0/sql/warehouses/123")
