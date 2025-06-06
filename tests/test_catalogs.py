"""
Tests for the catalogs module.
"""

import unittest
from chuck_data.catalogs import (
    list_catalogs,
    get_catalog,
    list_schemas,
    get_schema,
    list_tables,
    get_table,
)
from tests.fixtures import DatabricksClientStub


class TestCatalogs(unittest.TestCase):
    """Test cases for the catalog-related functions."""

    def setUp(self):
        """Set up common test fixtures."""
        self.client = DatabricksClientStub()

    def test_list_catalogs_no_params(self):
        """Test listing catalogs with no parameters."""
        # Set up stub data
        self.client.add_catalog("catalog1", type="MANAGED")
        self.client.add_catalog("catalog2", type="EXTERNAL")
        expected_response = {
            "catalogs": [
                {"name": "catalog1", "type": "MANAGED"},
                {"name": "catalog2", "type": "EXTERNAL"},
            ]
        }

        # Call the function
        result = list_catalogs(self.client)

        # Verify the result
        self.assertEqual(result, expected_response)
        # Verify the call was made with correct parameters
        self.assertEqual(len(self.client.list_catalogs_calls), 1)
        self.assertEqual(self.client.list_catalogs_calls[0], (False, None, None))

    def test_list_catalogs_with_params(self):
        """Test listing catalogs with all parameters."""
        # Set up stub data (empty list)
        expected_response = {"catalogs": []}

        # Call the function with all parameters
        result = list_catalogs(
            self.client, include_browse=True, max_results=10, page_token="abc123"
        )

        # Verify the result
        self.assertEqual(result, expected_response)
        # Verify the call was made with correct parameters
        self.assertEqual(len(self.client.list_catalogs_calls), 1)
        self.assertEqual(self.client.list_catalogs_calls[0], (True, 10, "abc123"))

    def test_get_catalog(self):
        """Test getting a specific catalog."""
        # Set up stub data
        self.client.add_catalog("test-catalog", type="MANAGED")
        catalog_detail = {"name": "test-catalog", "type": "MANAGED"}

        # Call the function
        result = get_catalog(self.client, "test-catalog")

        # Verify the result
        self.assertEqual(result, catalog_detail)
        # Verify the call was made with correct parameters
        self.assertEqual(len(self.client.get_catalog_calls), 1)
        self.assertEqual(self.client.get_catalog_calls[0], ("test-catalog",))

    def test_list_schemas_basic(self):
        """Test listing schemas with only required parameters."""
        # Set up stub data
        self.client.add_catalog("catalog1")
        self.client.add_schema("catalog1", "schema1", full_name="catalog1.schema1")
        self.client.add_schema("catalog1", "schema2", full_name="catalog1.schema2")
        expected_response = {
            "schemas": [
                {
                    "name": "schema1",
                    "catalog_name": "catalog1",
                    "full_name": "catalog1.schema1",
                },
                {
                    "name": "schema2",
                    "catalog_name": "catalog1",
                    "full_name": "catalog1.schema2",
                },
            ]
        }

        # Call the function with just the catalog name
        result = list_schemas(self.client, "catalog1")

        # Verify the result
        self.assertEqual(result, expected_response)
        # Verify the call was made with correct parameters
        self.assertEqual(len(self.client.list_schemas_calls), 1)
        self.assertEqual(
            self.client.list_schemas_calls[0], ("catalog1", False, None, None)
        )

    def test_list_schemas_all_params(self):
        """Test listing schemas with all parameters."""
        # Set up stub data (empty catalog)
        self.client.add_catalog("catalog1")
        expected_response = {"schemas": []}

        # Call the function with all parameters
        result = list_schemas(
            self.client,
            catalog_name="catalog1",
            include_browse=True,
            max_results=20,
            page_token="xyz789",
        )

        # Verify the result
        self.assertEqual(result, expected_response)
        # Verify the call was made with correct parameters
        self.assertEqual(len(self.client.list_schemas_calls), 1)
        self.assertEqual(
            self.client.list_schemas_calls[0], ("catalog1", True, 20, "xyz789")
        )

    def test_get_schema(self):
        """Test getting a specific schema."""
        # Set up stub data
        self.client.add_catalog("test-catalog")
        self.client.add_schema(
            "test-catalog", "test-schema", full_name="test-catalog.test-schema"
        )
        schema_detail = {
            "name": "test-schema",
            "catalog_name": "test-catalog",
            "full_name": "test-catalog.test-schema",
        }

        # Call the function
        result = get_schema(self.client, "test-catalog.test-schema")

        # Verify the result
        self.assertEqual(result, schema_detail)
        # Verify the call was made with correct parameters
        self.assertEqual(len(self.client.get_schema_calls), 1)
        self.assertEqual(self.client.get_schema_calls[0], ("test-catalog.test-schema",))

    def test_list_tables_basic(self):
        """Test listing tables with only required parameters."""
        # Set up stub data
        self.client.add_catalog("test-catalog")
        self.client.add_schema("test-catalog", "test-schema")
        self.client.add_table(
            "test-catalog", "test-schema", "table1", table_type="MANAGED"
        )
        expected_response = {
            "tables": [
                {
                    "name": "table1",
                    "table_type": "MANAGED",
                    "full_name": "test-catalog.test-schema.table1",
                    "catalog_name": "test-catalog",
                    "schema_name": "test-schema",
                    "comment": "",
                    "created_at": "2023-01-01T00:00:00Z",
                    "created_by": "test.user@example.com",
                    "owner": "test.user@example.com",
                    "columns": [],
                    "properties": {},
                }
            ],
            "next_page_token": None,
        }

        # Call the function with just the required parameters
        result = list_tables(self.client, "test-catalog", "test-schema")

        # Verify the result
        self.assertEqual(result, expected_response)
        # Verify the call was made with correct parameters
        self.assertEqual(len(self.client.list_tables_calls), 1)
        self.assertEqual(
            self.client.list_tables_calls[0],
            (
                "test-catalog",
                "test-schema",
                None,
                None,
                False,
                False,
                False,
                False,
                False,
                False,
            ),
        )

    def test_list_tables_all_params(self):
        """Test listing tables with all parameters."""
        # Set up stub data (empty schema)
        self.client.add_catalog("test-catalog")
        self.client.add_schema("test-catalog", "test-schema")
        expected_response = {"tables": [], "next_page_token": None}

        # Call the function with all parameters
        result = list_tables(
            self.client,
            catalog_name="test-catalog",
            schema_name="test-schema",
            max_results=30,
            page_token="page123",
            include_delta_metadata=True,
            omit_columns=True,
            omit_properties=True,
            omit_username=True,
            include_browse=True,
            include_manifest_capabilities=True,
        )

        # Verify the result
        self.assertEqual(result, expected_response)
        # Verify the call was made with correct parameters
        self.assertEqual(len(self.client.list_tables_calls), 1)
        self.assertEqual(
            self.client.list_tables_calls[0],
            (
                "test-catalog",
                "test-schema",
                30,
                "page123",
                True,
                True,
                True,
                True,
                True,
                True,
            ),
        )

    def test_get_table_basic(self):
        """Test getting a specific table with no parameters."""
        # Set up stub data
        self.client.add_catalog("test-catalog")
        self.client.add_schema("test-catalog", "test-schema")
        self.client.add_table(
            "test-catalog", "test-schema", "test-table", table_type="MANAGED"
        )
        table_detail = {
            "name": "test-table",
            "full_name": "test-catalog.test-schema.test-table",
            "table_type": "MANAGED",
            "catalog_name": "test-catalog",
            "schema_name": "test-schema",
            "comment": "",
            "created_at": "2023-01-01T00:00:00Z",
            "created_by": "test.user@example.com",
            "owner": "test.user@example.com",
            "columns": [],
            "properties": {},
        }

        # Call the function with just the table name
        result = get_table(self.client, "test-catalog.test-schema.test-table")

        # Verify the result
        self.assertEqual(result, table_detail)
        # Verify the call was made with correct parameters
        self.assertEqual(len(self.client.get_table_calls), 1)
        self.assertEqual(
            self.client.get_table_calls[0],
            ("test-catalog.test-schema.test-table", False, False, False),
        )

    def test_get_table_all_params(self):
        """Test getting a specific table with all parameters."""
        # Set up stub data
        self.client.add_catalog("test-catalog")
        self.client.add_schema("test-catalog", "test-schema")
        self.client.add_table(
            "test-catalog", "test-schema", "test-table", table_type="MANAGED"
        )
        table_detail = {
            "name": "test-table",
            "table_type": "MANAGED",
            "full_name": "test-catalog.test-schema.test-table",
            "catalog_name": "test-catalog",
            "schema_name": "test-schema",
            "comment": "",
            "created_at": "2023-01-01T00:00:00Z",
            "created_by": "test.user@example.com",
            "owner": "test.user@example.com",
            "columns": [],
            "properties": {},
        }

        # Call the function with all parameters
        result = get_table(
            self.client,
            "test-catalog.test-schema.test-table",
            include_delta_metadata=True,
            include_browse=True,
            include_manifest_capabilities=True,
        )

        # Verify the result
        self.assertEqual(result, table_detail)
        # Verify the call was made with correct parameters
        self.assertEqual(len(self.client.get_table_calls), 1)
        self.assertEqual(
            self.client.get_table_calls[0],
            ("test-catalog.test-schema.test-table", True, True, True),
        )
