"""Tests for the DatabricksAPIClient class."""

import unittest
from unittest.mock import patch, MagicMock
import requests
from chuck_data.clients.databricks import DatabricksAPIClient


class TestDatabricksClient(unittest.TestCase):
    """Unit tests for the DatabricksAPIClient class."""

    def setUp(self):
        """Set up the test environment."""
        self.workspace_url = "test-workspace"
        self.token = "fake-token"
        self.client = DatabricksAPIClient(self.workspace_url, self.token)

    def test_workspace_url_normalization(self):
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
            self.assertEqual(
                client.workspace_url,
                expected_url,
                f"URL should be normalized: {input_url} -> {expected_url}",
            )

    def test_azure_domain_detection_and_url_construction(self):
        """Test that Azure domains are detected correctly and URLs are constructed properly."""
        azure_client = DatabricksAPIClient(
            "adb-3856707039489412.12.azuredatabricks.net", "token"
        )

        # Check that cloud provider is detected correctly
        self.assertEqual(azure_client.cloud_provider, "Azure")
        self.assertEqual(azure_client.base_domain, "azuredatabricks.net")
        self.assertEqual(azure_client.workspace_url, "adb-3856707039489412.12")

    def test_gcp_domain_detection_and_url_construction(self):
        """Test that GCP domains are detected correctly and URLs are constructed properly."""
        gcp_client = DatabricksAPIClient("workspace.gcp.databricks.com", "token")

        # Check that cloud provider is detected correctly
        self.assertEqual(gcp_client.cloud_provider, "GCP")
        self.assertEqual(gcp_client.base_domain, "gcp.databricks.com")
        self.assertEqual(gcp_client.workspace_url, "workspace")

    @patch("chuck_data.clients.databricks.requests.get")
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

    @patch("chuck_data.clients.databricks.requests.get")
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

    @patch("chuck_data.clients.databricks.requests.get")
    def test_get_connection_error(self, mock_get):
        """Test GET request with connection error."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")

        with self.assertRaises(ConnectionError) as context:
            self.client.get("/test-endpoint")

        self.assertIn("Connection error occurred", str(context.exception))

    @patch("chuck_data.clients.databricks.requests.post")
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

    @patch("chuck_data.clients.databricks.requests.post")
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

    @patch("chuck_data.clients.databricks.requests.post")
    def test_post_connection_error(self, mock_post):
        """Test POST request with connection error."""
        mock_post.side_effect = requests.exceptions.ConnectionError("Connection failed")

        with self.assertRaises(ConnectionError) as context:
            self.client.post("/test-endpoint", {"data": "test"})

        self.assertIn("Connection error occurred", str(context.exception))

    @patch("chuck_data.clients.databricks.requests.post")
    def test_fetch_amperity_job_init_http_error(self, mock_post):
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

        with self.assertRaises(ValueError) as context:
            self.client.fetch_amperity_job_init("fake-token")

        self.assertIn("401 Error", str(context.exception))
        self.assertIn("Please /logout and /login again", str(context.exception))
