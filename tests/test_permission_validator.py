"""Tests for the permission validator module."""

import unittest
from unittest.mock import patch, MagicMock, call

from src.databricks.permission_validator import (
    validate_all_permissions,
    check_basic_connectivity,
    check_unity_catalog,
    check_sql_warehouse,
    check_jobs,
    check_models,
    check_volumes,
)


class TestPermissionValidator(unittest.TestCase):
    """Test cases for permission validator module."""

    def setUp(self):
        """Set up common test fixtures."""
        self.client = MagicMock()

    def test_validate_all_permissions(self):
        """Test that validate_all_permissions calls all check functions."""
        with (
            patch(
                "src.databricks.permission_validator.check_basic_connectivity"
            ) as mock_basic,
            patch(
                "src.databricks.permission_validator.check_unity_catalog"
            ) as mock_catalog,
            patch(
                "src.databricks.permission_validator.check_sql_warehouse"
            ) as mock_warehouse,
            patch("src.databricks.permission_validator.check_jobs") as mock_jobs,
            patch("src.databricks.permission_validator.check_models") as mock_models,
            patch("src.databricks.permission_validator.check_volumes") as mock_volumes,
        ):

            # Set return values for mock functions
            mock_basic.return_value = {"authorized": True}
            mock_catalog.return_value = {"authorized": True}
            mock_warehouse.return_value = {"authorized": True}
            mock_jobs.return_value = {"authorized": True}
            mock_models.return_value = {"authorized": True}
            mock_volumes.return_value = {"authorized": True}

            # Call the function
            result = validate_all_permissions(self.client)

            # Verify all check functions were called
            mock_basic.assert_called_once_with(self.client)
            mock_catalog.assert_called_once_with(self.client)
            mock_warehouse.assert_called_once_with(self.client)
            mock_jobs.assert_called_once_with(self.client)
            mock_models.assert_called_once_with(self.client)
            mock_volumes.assert_called_once_with(self.client)

            # Verify result contains all categories
            self.assertIn("basic_connectivity", result)
            self.assertIn("unity_catalog", result)
            self.assertIn("sql_warehouse", result)
            self.assertIn("jobs", result)
            self.assertIn("models", result)
            self.assertIn("volumes", result)

    @patch("logging.debug")
    def test_check_basic_connectivity_success(self, mock_debug):
        """Test basic connectivity check with successful response."""
        # Set up mock response
        self.client.get.return_value = {"userName": "test_user"}

        # Call the function
        result = check_basic_connectivity(self.client)

        # Verify the API was called correctly
        self.client.get.assert_called_once_with("/api/2.0/preview/scim/v2/Me")

        # Verify the result
        self.assertTrue(result["authorized"])
        self.assertEqual(result["details"], "Connected as test_user")
        self.assertEqual(result["api_path"], "/api/2.0/preview/scim/v2/Me")

        # Verify logging occurred
        mock_debug.assert_not_called()  # No errors, so no debug logging

    @patch("logging.debug")
    def test_check_basic_connectivity_error(self, mock_debug):
        """Test basic connectivity check with error."""
        # Set up mock response
        self.client.get.side_effect = Exception("Connection failed")

        # Call the function
        result = check_basic_connectivity(self.client)

        # Verify the API was called correctly
        self.client.get.assert_called_once_with("/api/2.0/preview/scim/v2/Me")

        # Verify the result
        self.assertFalse(result["authorized"])
        self.assertEqual(result["error"], "Connection failed")
        self.assertEqual(result["api_path"], "/api/2.0/preview/scim/v2/Me")

        # Verify logging occurred
        mock_debug.assert_called_once()

    @patch("logging.debug")
    def test_check_unity_catalog_success(self, mock_debug):
        """Test Unity Catalog check with successful response."""
        # Set up mock response
        self.client.get.return_value = {"catalogs": [{"name": "test_catalog"}]}

        # Call the function
        result = check_unity_catalog(self.client)

        # Verify the API was called correctly
        self.client.get.assert_called_once_with(
            "/api/2.1/unity-catalog/catalogs?max_results=1"
        )

        # Verify the result
        self.assertTrue(result["authorized"])
        self.assertEqual(
            result["details"], "Unity Catalog access granted (1 catalogs visible)"
        )
        self.assertEqual(result["api_path"], "/api/2.1/unity-catalog/catalogs")

        # Verify logging occurred
        mock_debug.assert_not_called()

    @patch("logging.debug")
    def test_check_unity_catalog_empty(self, mock_debug):
        """Test Unity Catalog check with empty response."""
        # Set up mock response
        self.client.get.return_value = {"catalogs": []}

        # Call the function
        result = check_unity_catalog(self.client)

        # Verify the API was called correctly
        self.client.get.assert_called_once_with(
            "/api/2.1/unity-catalog/catalogs?max_results=1"
        )

        # Verify the result
        self.assertTrue(result["authorized"])
        self.assertEqual(
            result["details"], "Unity Catalog access granted (0 catalogs visible)"
        )
        self.assertEqual(result["api_path"], "/api/2.1/unity-catalog/catalogs")

        # Verify logging occurred
        mock_debug.assert_not_called()

    @patch("logging.debug")
    def test_check_unity_catalog_error(self, mock_debug):
        """Test Unity Catalog check with error."""
        # Set up mock response
        self.client.get.side_effect = Exception("Access denied")

        # Call the function
        result = check_unity_catalog(self.client)

        # Verify the API was called correctly
        self.client.get.assert_called_once_with(
            "/api/2.1/unity-catalog/catalogs?max_results=1"
        )

        # Verify the result
        self.assertFalse(result["authorized"])
        self.assertEqual(result["error"], "Access denied")
        self.assertEqual(result["api_path"], "/api/2.1/unity-catalog/catalogs")

        # Verify logging occurred
        mock_debug.assert_called_once()

    @patch("logging.debug")
    def test_check_sql_warehouse_success(self, mock_debug):
        """Test SQL warehouse check with successful response."""
        # Set up mock response
        self.client.get.return_value = {"warehouses": [{"id": "warehouse1"}]}

        # Call the function
        result = check_sql_warehouse(self.client)

        # Verify the API was called correctly
        self.client.get.assert_called_once_with("/api/2.0/sql/warehouses?page_size=1")

        # Verify the result
        self.assertTrue(result["authorized"])
        self.assertEqual(
            result["details"], "SQL Warehouse access granted (1 warehouses visible)"
        )
        self.assertEqual(result["api_path"], "/api/2.0/sql/warehouses")

        # Verify logging occurred
        mock_debug.assert_not_called()

    @patch("logging.debug")
    def test_check_sql_warehouse_error(self, mock_debug):
        """Test SQL warehouse check with error."""
        # Set up mock response
        self.client.get.side_effect = Exception("Access denied")

        # Call the function
        result = check_sql_warehouse(self.client)

        # Verify the API was called correctly
        self.client.get.assert_called_once_with("/api/2.0/sql/warehouses?page_size=1")

        # Verify the result
        self.assertFalse(result["authorized"])
        self.assertEqual(result["error"], "Access denied")
        self.assertEqual(result["api_path"], "/api/2.0/sql/warehouses")

        # Verify logging occurred
        mock_debug.assert_called_once()

    @patch("logging.debug")
    def test_check_jobs_success(self, mock_debug):
        """Test jobs check with successful response."""
        # Set up mock response
        self.client.get.return_value = {"jobs": [{"job_id": "job1"}]}

        # Call the function
        result = check_jobs(self.client)

        # Verify the API was called correctly
        self.client.get.assert_called_once_with("/api/2.1/jobs/list?limit=1")

        # Verify the result
        self.assertTrue(result["authorized"])
        self.assertEqual(result["details"], "Jobs access granted (1 jobs visible)")
        self.assertEqual(result["api_path"], "/api/2.1/jobs/list")

        # Verify logging occurred
        mock_debug.assert_not_called()

    @patch("logging.debug")
    def test_check_jobs_error(self, mock_debug):
        """Test jobs check with error."""
        # Set up mock response
        self.client.get.side_effect = Exception("Access denied")

        # Call the function
        result = check_jobs(self.client)

        # Verify the API was called correctly
        self.client.get.assert_called_once_with("/api/2.1/jobs/list?limit=1")

        # Verify the result
        self.assertFalse(result["authorized"])
        self.assertEqual(result["error"], "Access denied")
        self.assertEqual(result["api_path"], "/api/2.1/jobs/list")

        # Verify logging occurred
        mock_debug.assert_called_once()

    @patch("logging.debug")
    def test_check_models_success(self, mock_debug):
        """Test models check with successful response."""
        # Set up mock response
        self.client.get.return_value = {"registered_models": [{"name": "model1"}]}

        # Call the function
        result = check_models(self.client)

        # Verify the API was called correctly
        self.client.get.assert_called_once_with(
            "/api/2.0/mlflow/registered-models/list?max_results=1"
        )

        # Verify the result
        self.assertTrue(result["authorized"])
        self.assertEqual(
            result["details"], "ML Models access granted (1 models visible)"
        )
        self.assertEqual(result["api_path"], "/api/2.0/mlflow/registered-models/list")

        # Verify logging occurred
        mock_debug.assert_not_called()

    @patch("logging.debug")
    def test_check_models_error(self, mock_debug):
        """Test models check with error."""
        # Set up mock response
        self.client.get.side_effect = Exception("Access denied")

        # Call the function
        result = check_models(self.client)

        # Verify the API was called correctly
        self.client.get.assert_called_once_with(
            "/api/2.0/mlflow/registered-models/list?max_results=1"
        )

        # Verify the result
        self.assertFalse(result["authorized"])
        self.assertEqual(result["error"], "Access denied")
        self.assertEqual(result["api_path"], "/api/2.0/mlflow/registered-models/list")

        # Verify logging occurred
        mock_debug.assert_called_once()

    @patch("logging.debug")
    def test_check_volumes_success_full_path(self, mock_debug):
        """Test volumes check with successful response through the full path."""
        # Set up mock responses for the multi-step process
        catalog_response = {"catalogs": [{"name": "test_catalog"}]}
        schema_response = {"schemas": [{"name": "test_schema"}]}
        volume_response = {"volumes": [{"name": "test_volume"}]}

        # Configure the client mock to return different responses for different calls
        self.client.get.side_effect = [
            catalog_response,
            schema_response,
            volume_response,
        ]

        # Call the function
        result = check_volumes(self.client)

        # Verify the API calls were made correctly
        expected_calls = [
            call("/api/2.1/unity-catalog/catalogs?max_results=1"),
            call(
                "/api/2.1/unity-catalog/schemas?catalog_name=test_catalog&max_results=1"
            ),
            call(
                "/api/2.1/unity-catalog/volumes?catalog_name=test_catalog&schema_name=test_schema"
            ),
        ]
        self.assertEqual(self.client.get.call_args_list, expected_calls)

        # Verify the result
        self.assertTrue(result["authorized"])
        self.assertEqual(
            result["details"],
            "Volumes access granted in test_catalog.test_schema (1 volumes visible)",
        )
        self.assertEqual(result["api_path"], "/api/2.1/unity-catalog/volumes")

        # Verify logging occurred
        mock_debug.assert_not_called()

    @patch("logging.debug")
    def test_check_volumes_no_catalogs(self, mock_debug):
        """Test volumes check when no catalogs are available."""
        # Set up empty catalog response
        self.client.get.return_value = {"catalogs": []}

        # Call the function
        result = check_volumes(self.client)

        # Verify only the catalogs API was called
        self.client.get.assert_called_once_with(
            "/api/2.1/unity-catalog/catalogs?max_results=1"
        )

        # Verify the result
        self.assertFalse(result["authorized"])
        self.assertEqual(
            result["error"], "No catalogs available to check volumes access"
        )
        self.assertEqual(result["api_path"], "/api/2.1/unity-catalog/volumes")

        # Verify logging occurred
        mock_debug.assert_not_called()

    @patch("logging.debug")
    def test_check_volumes_no_schemas(self, mock_debug):
        """Test volumes check when no schemas are available."""
        # Set up mock responses
        catalog_response = {"catalogs": [{"name": "test_catalog"}]}
        schema_response = {"schemas": []}

        # Configure the client mock
        self.client.get.side_effect = [catalog_response, schema_response]

        # Call the function
        result = check_volumes(self.client)

        # Verify the APIs were called
        expected_calls = [
            call("/api/2.1/unity-catalog/catalogs?max_results=1"),
            call(
                "/api/2.1/unity-catalog/schemas?catalog_name=test_catalog&max_results=1"
            ),
        ]
        self.assertEqual(self.client.get.call_args_list, expected_calls)

        # Verify the result
        self.assertFalse(result["authorized"])
        self.assertEqual(
            result["error"],
            "No schemas available in catalog 'test_catalog' to check volumes access",
        )
        self.assertEqual(result["api_path"], "/api/2.1/unity-catalog/volumes")

        # Verify logging occurred
        mock_debug.assert_not_called()

    @patch("logging.debug")
    def test_check_volumes_error(self, mock_debug):
        """Test volumes check with an API error."""
        # Set up mock response to raise exception
        self.client.get.side_effect = Exception("Access denied")

        # Call the function
        result = check_volumes(self.client)

        # Verify the API was called
        self.client.get.assert_called_once_with(
            "/api/2.1/unity-catalog/catalogs?max_results=1"
        )

        # Verify the result
        self.assertFalse(result["authorized"])
        self.assertEqual(result["error"], "Access denied")
        self.assertEqual(result["api_path"], "/api/2.1/unity-catalog/volumes")

        # Verify logging occurred
        mock_debug.assert_called_once()
