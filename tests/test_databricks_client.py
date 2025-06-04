"""Tests for the DatabricksAPIClient class."""

import unittest
from unittest.mock import patch, MagicMock, mock_open
import requests
from src.clients.databricks import DatabricksAPIClient


class TestDatabricksAPIClient(unittest.TestCase):
    """Unit tests for the DatabricksAPIClient class."""

    def setUp(self):
        """Set up the test environment."""
        self.workspace_url = "test-workspace"
        self.token = "fake-token"
        self.client = DatabricksAPIClient(self.workspace_url, self.token)

    def test_normalize_workspace_url(self):
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
            result = self.client._normalize_workspace_url(input_url)
            self.assertEqual(result, expected_url)

    def test_azure_client_url_construction(self):
        """Test that Azure client constructs URLs with correct domain."""
        azure_client = DatabricksAPIClient(
            "adb-3856707039489412.12.azuredatabricks.net", "token"
        )

        # Check that cloud provider is detected correctly
        self.assertEqual(azure_client.cloud_provider, "Azure")
        self.assertEqual(azure_client.base_domain, "azuredatabricks.net")
        self.assertEqual(azure_client.workspace_url, "adb-3856707039489412.12")

    def test_base_domain_map(self):
        """Ensure _get_base_domain uses the shared domain map."""
        from src.databricks.url_utils import DATABRICKS_DOMAIN_MAP

        for provider, domain in DATABRICKS_DOMAIN_MAP.items():
            with self.subTest(provider=provider):
                client = DatabricksAPIClient("workspace", "token")
                client.cloud_provider = provider
                self.assertEqual(client._get_base_domain(), domain)

    @patch("requests.get")
    def test_azure_get_request_url(self, mock_get):
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

    def test_compute_node_types(self):
        """Test that appropriate compute node types are returned for each cloud provider."""
        test_cases = [
            ("workspace.cloud.databricks.com", "AWS", "r5d.4xlarge"),
            ("workspace.azuredatabricks.net", "Azure", "Standard_E16ds_v4"),
            ("workspace.gcp.databricks.com", "GCP", "n2-standard-16"),
            ("workspace.databricks.com", "Generic", "r5d.4xlarge"),
        ]

        for url, expected_provider, expected_node_type in test_cases:
            with self.subTest(url=url):
                client = DatabricksAPIClient(url, "token")
                self.assertEqual(client.cloud_provider, expected_provider)
                self.assertEqual(client.get_compute_node_type(), expected_node_type)

    def test_cloud_attributes(self):
        """Test that appropriate cloud attributes are returned for each provider."""
        # Test AWS attributes
        aws_client = DatabricksAPIClient("workspace.cloud.databricks.com", "token")
        aws_attrs = aws_client.get_cloud_attributes()
        self.assertIn("aws_attributes", aws_attrs)
        self.assertEqual(
            aws_attrs["aws_attributes"]["availability"], "SPOT_WITH_FALLBACK"
        )

        # Test Azure attributes
        azure_client = DatabricksAPIClient("workspace.azuredatabricks.net", "token")
        azure_attrs = azure_client.get_cloud_attributes()
        self.assertIn("azure_attributes", azure_attrs)
        self.assertEqual(
            azure_attrs["azure_attributes"]["availability"], "SPOT_WITH_FALLBACK_AZURE"
        )

        # Test GCP attributes
        gcp_client = DatabricksAPIClient("workspace.gcp.databricks.com", "token")
        gcp_attrs = gcp_client.get_cloud_attributes()
        self.assertIn("gcp_attributes", gcp_attrs)
        self.assertEqual(gcp_attrs["gcp_attributes"]["use_preemptible_executors"], True)

    @patch.object(DatabricksAPIClient, "post")
    def test_job_submission_uses_correct_node_type(self, mock_post):
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
        self.assertEqual(cluster_config["node_type_id"], "Standard_E16ds_v4")

        # Check that Azure attributes are present
        self.assertIn("azure_attributes", cluster_config)
        self.assertEqual(
            cluster_config["azure_attributes"]["availability"],
            "SPOT_WITH_FALLBACK_AZURE",
        )

    # Base API request tests

    @patch("requests.get")
    def test_get_success(self, mock_get):
        """Test successful GET request."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"key": "value"}
        mock_get.return_value = mock_response

        response = self.client.get("/test-endpoint")
        self.assertEqual(response, {"key": "value"})
        mock_get.assert_called_once_with(
            "https://test-workspace.cloud.databricks.com/test-endpoint",
            headers={
                "Authorization": "Bearer fake-token",
                "User-Agent": "amperity",
            },
        )

    @patch("requests.get")
    def test_get_http_error(self, mock_get):
        """Test GET request with HTTP error."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "HTTP 404"
        )
        mock_response.text = "Not Found"
        mock_get.return_value = mock_response

        with self.assertRaises(ValueError) as context:
            self.client.get("/test-endpoint")

        self.assertIn("HTTP error occurred", str(context.exception))
        self.assertIn("Not Found", str(context.exception))

    @patch("requests.get")
    def test_get_connection_error(self, mock_get):
        """Test GET request with connection error."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")

        with self.assertRaises(ConnectionError) as context:
            self.client.get("/test-endpoint")

        self.assertIn("Connection error occurred", str(context.exception))

    @patch("requests.post")
    def test_post_success(self, mock_post):
        """Test successful POST request."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"key": "value"}
        mock_post.return_value = mock_response

        response = self.client.post("/test-endpoint", {"data": "test"})
        self.assertEqual(response, {"key": "value"})
        mock_post.assert_called_once_with(
            "https://test-workspace.cloud.databricks.com/test-endpoint",
            headers={
                "Authorization": "Bearer fake-token",
                "User-Agent": "amperity",
            },
            json={"data": "test"},
        )

    @patch("requests.post")
    def test_post_http_error(self, mock_post):
        """Test POST request with HTTP error."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "HTTP 400"
        )
        mock_response.text = "Bad Request"
        mock_post.return_value = mock_response

        with self.assertRaises(ValueError) as context:
            self.client.post("/test-endpoint", {"data": "test"})

        self.assertIn("HTTP error occurred", str(context.exception))
        self.assertIn("Bad Request", str(context.exception))

    @patch("requests.post")
    def test_post_connection_error(self, mock_post):
        """Test POST request with connection error."""
        mock_post.side_effect = requests.exceptions.ConnectionError("Connection failed")

        with self.assertRaises(ConnectionError) as context:
            self.client.post("/test-endpoint", {"data": "test"})

        self.assertIn("Connection error occurred", str(context.exception))

    # Authentication method tests

    @patch.object(DatabricksAPIClient, "get")
    def test_validate_token_success(self, mock_get):
        """Test successful token validation."""
        mock_get.return_value = {"user_name": "test-user"}

        result = self.client.validate_token()

        self.assertTrue(result)
        mock_get.assert_called_once_with("/api/2.0/preview/scim/v2/Me")

    @patch.object(DatabricksAPIClient, "get")
    def test_validate_token_failure(self, mock_get):
        """Test failed token validation."""
        mock_get.side_effect = Exception("Token validation failed")

        result = self.client.validate_token()

        self.assertFalse(result)
        mock_get.assert_called_once_with("/api/2.0/preview/scim/v2/Me")

    # Unity Catalog method tests

    @patch.object(DatabricksAPIClient, "get")
    @patch.object(DatabricksAPIClient, "get_with_params")
    def test_list_catalogs(self, mock_get_with_params, mock_get):
        """Test list_catalogs with and without parameters."""
        # Without parameters
        mock_get.return_value = {"catalogs": [{"name": "test_catalog"}]}
        result = self.client.list_catalogs()
        self.assertEqual(result, {"catalogs": [{"name": "test_catalog"}]})
        mock_get.assert_called_once_with("/api/2.1/unity-catalog/catalogs")

        # With parameters
        mock_get_with_params.return_value = {"catalogs": [{"name": "test_catalog"}]}
        result = self.client.list_catalogs(include_browse=True, max_results=10)
        self.assertEqual(result, {"catalogs": [{"name": "test_catalog"}]})
        mock_get_with_params.assert_called_once_with(
            "/api/2.1/unity-catalog/catalogs",
            {"include_browse": "true", "max_results": "10"},
        )

    @patch.object(DatabricksAPIClient, "get")
    def test_get_catalog(self, mock_get):
        """Test get_catalog method."""
        mock_get.return_value = {"name": "test_catalog", "comment": "Test catalog"}

        result = self.client.get_catalog("test_catalog")

        self.assertEqual(result, {"name": "test_catalog", "comment": "Test catalog"})
        mock_get.assert_called_once_with("/api/2.1/unity-catalog/catalogs/test_catalog")

    # File system method tests

    @patch("requests.put")
    def test_upload_file_with_content(self, mock_put):
        """Test successful file upload with content."""
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_put.return_value = mock_response

        result = self.client.upload_file("/test/path.txt", content="Test content")

        self.assertTrue(result)
        mock_put.assert_called_once()
        # Check URL and headers
        call_args = mock_put.call_args
        self.assertIn(
            "https://test-workspace.cloud.databricks.com/api/2.0/fs/files/test/path.txt",
            call_args[0][0],
        )
        self.assertEqual(
            call_args[1]["headers"]["Content-Type"], "application/octet-stream"
        )
        # Check that content was encoded to bytes
        self.assertEqual(call_args[1]["data"], b"Test content")

    @patch("builtins.open", new_callable=mock_open, read_data=b"file content")
    @patch("requests.put")
    def test_upload_file_with_file_path(self, mock_put, mock_file):
        """Test successful file upload with file path."""
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_put.return_value = mock_response

        result = self.client.upload_file("/test/path.txt", file_path="/local/file.txt")

        self.assertTrue(result)
        mock_file.assert_called_once_with("/local/file.txt", "rb")
        mock_put.assert_called_once()
        # Check that file content was read
        call_args = mock_put.call_args
        self.assertEqual(call_args[1]["data"], b"file content")

    def test_upload_file_invalid_args(self):
        """Test upload_file with invalid arguments."""
        # Test when both file_path and content are provided
        with self.assertRaises(ValueError) as context:
            self.client.upload_file(
                "/test/path.txt", file_path="/local.txt", content="content"
            )
        self.assertIn(
            "Exactly one of file_path or content must be provided",
            str(context.exception),
        )

        # Test when neither file_path nor content is provided
        with self.assertRaises(ValueError) as context:
            self.client.upload_file("/test/path.txt")
        self.assertIn(
            "Exactly one of file_path or content must be provided",
            str(context.exception),
        )

    # Model serving tests

    @patch.object(DatabricksAPIClient, "get")
    def test_list_models(self, mock_get):
        """Test list_models method."""
        mock_response = {"endpoints": [{"name": "model1"}, {"name": "model2"}]}
        mock_get.return_value = mock_response

        result = self.client.list_models()

        self.assertEqual(result, [{"name": "model1"}, {"name": "model2"}])
        mock_get.assert_called_once_with("/api/2.0/serving-endpoints")

    @patch.object(DatabricksAPIClient, "get")
    def test_get_model(self, mock_get):
        """Test get_model method."""
        mock_response = {"name": "model1", "status": "ready"}
        mock_get.return_value = mock_response

        result = self.client.get_model("model1")

        self.assertEqual(result, {"name": "model1", "status": "ready"})
        mock_get.assert_called_once_with("/api/2.0/serving-endpoints/model1")

    @patch.object(DatabricksAPIClient, "get")
    def test_get_model_not_found(self, mock_get):
        """Test get_model with 404 error."""
        mock_get.side_effect = ValueError("HTTP error occurred: 404 Not Found")

        result = self.client.get_model("nonexistent-model")

        self.assertIsNone(result)
        mock_get.assert_called_once_with("/api/2.0/serving-endpoints/nonexistent-model")

    # SQL warehouse tests

    @patch.object(DatabricksAPIClient, "get")
    def test_list_warehouses(self, mock_get):
        """Test list_warehouses method."""
        mock_response = {"warehouses": [{"id": "123"}, {"id": "456"}]}
        mock_get.return_value = mock_response

        result = self.client.list_warehouses()

        self.assertEqual(result, [{"id": "123"}, {"id": "456"}])
        mock_get.assert_called_once_with("/api/2.0/sql/warehouses")

    @patch.object(DatabricksAPIClient, "get")
    def test_get_warehouse(self, mock_get):
        """Test get_warehouse method."""
        mock_response = {"id": "123", "name": "Test Warehouse"}
        mock_get.return_value = mock_response

        result = self.client.get_warehouse("123")

        self.assertEqual(result, {"id": "123", "name": "Test Warehouse"})
        mock_get.assert_called_once_with("/api/2.0/sql/warehouses/123")
