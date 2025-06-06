"""
Tests for the PII tools helper module.
"""

import unittest
import os
import tempfile
from unittest.mock import patch, MagicMock

from chuck_data.commands.pii_tools import (
    _helper_tag_pii_columns_logic,
    _helper_scan_schema_for_pii_logic,
)
from chuck_data.config import ConfigManager
from tests.fixtures import DatabricksClientStub, LLMClientStub


class TestPIITools(unittest.TestCase):
    """Test cases for the PII tools helper functions."""

    def setUp(self):
        """Set up common test fixtures."""
        self.client_stub = DatabricksClientStub()
        self.llm_client = LLMClientStub()

        # Set up config management
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.temp_dir.name, "test_config.json")
        self.config_manager = ConfigManager(self.config_path)
        self.patcher = patch("chuck_data.config._config_manager", self.config_manager)
        self.patcher.start()

        # Mock columns from database
        self.mock_columns = [
            {"name": "first_name", "type_name": "string"},
            {"name": "email", "type_name": "string"},
            {"name": "signup_date", "type_name": "date"},
        ]

        # Configure LLM client stub for PII detection response
        pii_response_content = '[{"name":"first_name","semantic":"given-name"},{"name":"email","semantic":"email"},{"name":"signup_date","semantic":null}]'
        self.llm_client.set_response_content(pii_response_content)

    def tearDown(self):
        self.patcher.stop()
        self.temp_dir.cleanup()

    @patch("chuck_data.commands.pii_tools.json.loads")
    def test_tag_pii_columns_logic_success(self, mock_json_loads):
        """Test successful tagging of PII columns."""
        # Set up test data using stub
        self.client_stub.add_catalog("mycat")
        self.client_stub.add_schema("mycat", "myschema")
        self.client_stub.add_table(
            "mycat", "myschema", "users", columns=self.mock_columns
        )

        # Mock the JSON parsing instead of relying on actual JSON parsing
        mock_json_loads.return_value = [
            {"name": "first_name", "semantic": "given-name"},
            {"name": "email", "semantic": "email"},
            {"name": "signup_date", "semantic": None},
        ]

        # Call the function
        result = _helper_tag_pii_columns_logic(
            self.client_stub,
            self.llm_client,
            "users",
            catalog_name_context="mycat",
            schema_name_context="myschema",
        )

        # Verify the result
        self.assertEqual(result["full_name"], "mycat.myschema.users")
        self.assertEqual(result["table_name"], "users")
        self.assertEqual(result["column_count"], 3)
        self.assertEqual(result["pii_column_count"], 2)
        self.assertTrue(result["has_pii"])
        self.assertFalse(result["skipped"])
        self.assertEqual(result["columns"][0]["semantic"], "given-name")
        self.assertEqual(result["columns"][1]["semantic"], "email")
        self.assertIsNone(result["columns"][2]["semantic"])

    @patch("concurrent.futures.ThreadPoolExecutor")
    def test_scan_schema_for_pii_logic(self, mock_executor):
        """Test scanning a schema for PII."""
        # Set up test data using stub
        self.client_stub.add_catalog("test_cat")
        self.client_stub.add_schema("test_cat", "test_schema")
        self.client_stub.add_table("test_cat", "test_schema", "users")
        self.client_stub.add_table("test_cat", "test_schema", "orders")
        self.client_stub.add_table("test_cat", "test_schema", "_stitch_temp")

        # Mock the ThreadPoolExecutor
        mock_future = MagicMock()
        mock_future.result.return_value = {
            "table_name": "users",
            "full_name": "test_cat.test_schema.users",
            "pii_column_count": 2,
            "has_pii": True,
            "skipped": False,
        }

        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_context
        mock_context.submit.return_value = mock_future
        mock_executor.return_value = mock_context

        # Mock concurrent.futures.as_completed to return mock_future
        with patch("concurrent.futures.as_completed", return_value=[mock_future]):
            # Call the function
            result = _helper_scan_schema_for_pii_logic(
                self.client_stub, self.llm_client, "test_cat", "test_schema"
            )

            # Verify the result
            self.assertEqual(result["catalog"], "test_cat")
            self.assertEqual(result["schema"], "test_schema")
            self.assertEqual(
                result["tables_scanned_attempted"], 2
            )  # Excluding _stitch_temp
            self.assertEqual(result["tables_successfully_processed"], 1)
            self.assertEqual(result["tables_with_pii"], 1)
            self.assertEqual(result["total_pii_columns"], 2)
