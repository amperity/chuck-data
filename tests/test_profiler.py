"""
Tests for the profiler module.
"""

import unittest
from unittest.mock import patch, MagicMock
from src.profiler import (
    list_tables,
    query_llm,
    generate_manifest,
    store_manifest,
    profile_table,
)


class TestProfiler(unittest.TestCase):
    """Test cases for the profiler module."""

    def setUp(self):
        """Set up common test fixtures."""
        self.client = MagicMock()
        self.warehouse_id = "warehouse-123"

    @patch("src.profiler.time.sleep")
    def test_list_tables(self, mock_sleep):
        """Test listing tables."""
        # Set up mock responses
        self.client.post.return_value = {"statement_id": "stmt-123"}

        # Mock the get call to return a completed query status
        self.client.get.return_value = {
            "status": {"state": "SUCCEEDED"},
            "result": {
                "data": [
                    ["table1", "catalog1", "schema1"],
                    ["table2", "catalog1", "schema2"],
                ]
            },
        }

        # Call the function
        result = list_tables(self.client, self.warehouse_id)

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
        self.assertEqual(result, expected_tables)

        # Verify API calls
        self.client.post.assert_called_once()
        self.client.get.assert_called_once()

    @patch("src.profiler.time.sleep")
    def test_list_tables_polling(self, mock_sleep):
        """Test polling behavior when listing tables."""
        # Set up mock responses
        self.client.post.return_value = {"statement_id": "stmt-123"}

        # Set up get to return PENDING then RUNNING then SUCCEEDED
        self.client.get.side_effect = [
            {"status": {"state": "PENDING"}},
            {"status": {"state": "RUNNING"}},
            {
                "status": {"state": "SUCCEEDED"},
                "result": {"data": [["table1", "catalog1", "schema1"]]},
            },
        ]

        # Call the function
        result = list_tables(self.client, self.warehouse_id)

        # Verify polling behavior
        self.assertEqual(len(self.client.get.call_args_list), 3)
        self.assertEqual(mock_sleep.call_count, 2)

        # Check result
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["table_name"], "table1")

    @patch("src.profiler.time.sleep")
    def test_list_tables_failed_query(self, mock_sleep):
        """Test list tables with failed SQL query."""
        # Set up mock responses
        self.client.post.return_value = {"statement_id": "stmt-123"}
        self.client.get.return_value = {"status": {"state": "FAILED"}}

        # Call the function
        result = list_tables(self.client, self.warehouse_id)

        # Verify it returns empty list on failure
        self.assertEqual(result, [])

    def test_generate_manifest(self):
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
        self.assertEqual(result["table"], table_info)
        self.assertEqual(result["schema"], schema)
        self.assertEqual(result["pii_tags"], pii_tags)
        self.assertTrue("profiling_timestamp" in result)

    @patch("src.profiler.time.sleep")
    @patch("src.profiler.base64.b64encode")
    def test_store_manifest(self, mock_b64encode, mock_sleep):
        """Test storing a manifest."""
        # Set up mock responses
        mock_b64encode.return_value = b"base64_encoded_data"
        self.client.post.return_value = {"success": True}

        # Test data
        manifest = {"table": {"name": "table1"}, "pii_tags": ["id"]}
        manifest_path = "/chuck/manifests/table1_manifest.json"

        # Call the function
        result = store_manifest(self.client, manifest_path, manifest)

        # Check the result
        self.assertTrue(result)

        # Verify API call
        self.client.post.assert_called_once()
        self.assertEqual(self.client.post.call_args[0][0], "/api/2.0/dbfs/put")
        # Verify the manifest path was passed correctly
        self.assertEqual(self.client.post.call_args[0][1]["path"], manifest_path)

    @patch("src.profiler.store_manifest")
    @patch("src.profiler.generate_manifest")
    @patch("src.profiler.query_llm")
    @patch("src.profiler.get_sample_data")
    @patch("src.profiler.get_table_schema")
    @patch("src.profiler.list_tables")
    def test_profile_table_success(
        self,
        mock_list_tables,
        mock_get_schema,
        mock_get_sample,
        mock_query_llm,
        mock_generate_manifest,
        mock_store_manifest,
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
        result = profile_table(self.client, self.warehouse_id, "test-model")

        # Check the result
        self.assertEqual(result, manifest_path)

        # Verify the correct functions were called
        mock_list_tables.assert_called_once_with(self.client, self.warehouse_id)
        mock_get_schema.assert_called_once()
        mock_get_sample.assert_called_once()
        mock_query_llm.assert_called_once()
        mock_generate_manifest.assert_called_once()
        mock_store_manifest.assert_called_once()

    def test_query_llm(self):
        """Test querying the LLM."""
        # Set up mock response
        self.client.post.return_value = {"predictions": [{"pii_tags": ["id"]}]}

        # Test data
        endpoint_name = "test-model"
        input_data = {
            "schema": [{"col_name": "id", "data_type": "integer"}],
            "sample_data": {"column_names": ["id"], "rows": [{"id": 1}]},
        }

        # Call the function
        result = query_llm(self.client, endpoint_name, input_data)

        # Check the result
        self.assertEqual(result, {"predictions": [{"pii_tags": ["id"]}]})

        # Verify API call
        self.client.post.assert_called_once()
        self.assertEqual(
            self.client.post.call_args[0][0],
            "/api/2.0/serving-endpoints/test-model/invocations",
        )
