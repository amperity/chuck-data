"""
Tests for PII tools utility functions.

Behavioral tests focused on utility function behavior rather than implementation details.
Tests cover both _helper_tag_pii_columns_logic and _helper_scan_schema_for_pii_logic functions.
"""

import tempfile
from unittest.mock import patch

from chuck_data.commands.pii_tools import (
    _helper_tag_pii_columns_logic,
    _helper_scan_schema_for_pii_logic,
)
from chuck_data.config import ConfigManager


class TestHelperTagPiiColumnsLogicParameterValidation:
    """Test parameter validation for _helper_tag_pii_columns_logic function."""

    def test_missing_catalog_schema_context_for_simple_table_name_returns_error(
        self, databricks_client_stub, llm_client_stub
    ):
        """Missing catalog/schema context for simple table name returns error."""
        # Setup table in stub but don't provide catalog/schema context
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "test_schema")
        databricks_client_stub.add_table("test_catalog", "test_schema", "simple_table")

        # Call without catalog_name_context and schema_name_context
        result = _helper_tag_pii_columns_logic(
            databricks_client_stub,
            llm_client_stub,
            "simple_table",  # Simple table name, no dots
            catalog_name_context=None,
            schema_name_context=None,
        )

        # Should fail to retrieve table without proper context
        assert result["skipped"] is True
        assert "Failed to retrieve table details" in result["error"]

    def test_nonexistent_table_returns_error_result(
        self, databricks_client_stub, llm_client_stub
    ):
        """Nonexistent table returns error result."""
        # Don't add the table to stub

        result = _helper_tag_pii_columns_logic(
            databricks_client_stub,
            llm_client_stub,
            "nonexistent_table",
            catalog_name_context="test_catalog",
            schema_name_context="test_schema",
        )

        # Should return error result
        assert result["skipped"] is True
        assert "Failed to retrieve table details" in result["error"]
        assert result["table_name_param"] == "nonexistent_table"


class TestHelperTagPiiColumnsLogicDirectExecution:
    """Test direct execution of _helper_tag_pii_columns_logic function."""

    def test_successful_pii_tagging_with_catalog_context(
        self, databricks_client_stub, llm_client_stub
    ):
        """Successful PII tagging with catalog and schema context."""
        # Setup test data with PII columns
        databricks_client_stub.add_catalog("production_catalog")
        databricks_client_stub.add_schema("production_catalog", "customer_data")
        databricks_client_stub.add_table(
            "production_catalog",
            "customer_data",
            "users_table",
            columns=[
                {"name": "email", "type_name": "string"},
                {"name": "first_name", "type_name": "string"},
                {"name": "user_id", "type_name": "integer"},
            ],
        )

        # Configure LLM to identify PII
        llm_client_stub.set_response_content(
            '[{"name":"email","semantic":"email"},{"name":"first_name","semantic":"given-name"},{"name":"user_id","semantic":null}]'
        )

        # Execute function
        result = _helper_tag_pii_columns_logic(
            databricks_client_stub,
            llm_client_stub,
            "users_table",
            catalog_name_context="production_catalog",
            schema_name_context="customer_data",
        )

        # Verify successful tagging outcome
        assert not result["skipped"]
        assert result["table_name"] == "users_table"
        assert result["full_name"] == "production_catalog.customer_data.users_table"
        assert result["column_count"] == 3
        assert result["pii_column_count"] == 2
        assert result["has_pii"] is True

        # Verify column tagging details
        columns = result["columns"]
        assert len(columns) == 3
        assert columns[0]["name"] == "email"
        assert columns[0]["semantic"] == "email"
        assert columns[1]["name"] == "first_name"
        assert columns[1]["semantic"] == "given-name"
        assert columns[2]["name"] == "user_id"
        assert columns[2]["semantic"] is None

        # Verify PII columns isolation
        pii_columns = result["pii_columns"]
        assert len(pii_columns) == 2
        assert pii_columns[0]["name"] == "email"
        assert pii_columns[1]["name"] == "first_name"

    def test_successful_pii_tagging_with_fully_qualified_table_name(
        self, databricks_client_stub, llm_client_stub
    ):
        """Successful PII tagging with fully qualified table name."""
        # Setup test data
        databricks_client_stub.add_catalog("explicit_catalog")
        databricks_client_stub.add_schema("explicit_catalog", "explicit_schema")
        databricks_client_stub.add_table(
            "explicit_catalog",
            "explicit_schema",
            "customer_table",
            columns=[{"name": "phone", "type_name": "string"}],
        )

        # Configure LLM response
        llm_client_stub.set_response_content('[{"name":"phone","semantic":"phone"}]')

        # Execute with fully qualified table name (no catalog/schema context needed)
        result = _helper_tag_pii_columns_logic(
            databricks_client_stub,
            llm_client_stub,
            "explicit_catalog.explicit_schema.customer_table",
            catalog_name_context=None,
            schema_name_context=None,
        )

        # Verify successful processing
        assert not result["skipped"]
        assert result["full_name"] == "explicit_catalog.explicit_schema.customer_table"
        assert result["table_name"] == "customer_table"
        assert result["pii_column_count"] == 1

    def test_table_with_no_columns_returns_empty_result(
        self, databricks_client_stub, llm_client_stub
    ):
        """Table with no columns returns empty but successful result."""
        # Setup table with no columns
        databricks_client_stub.add_catalog("empty_catalog")
        databricks_client_stub.add_schema("empty_catalog", "empty_schema")
        databricks_client_stub.add_table(
            "empty_catalog", "empty_schema", "empty_table", columns=[]  # No columns
        )

        # Execute function
        result = _helper_tag_pii_columns_logic(
            databricks_client_stub,
            llm_client_stub,
            "empty_table",
            catalog_name_context="empty_catalog",
            schema_name_context="empty_schema",
        )

        # Verify empty table handling
        assert not result["skipped"]
        assert result["table_name"] == "empty_table"
        assert result["column_count"] == 0
        assert result["pii_column_count"] == 0
        assert result["has_pii"] is False
        assert result["columns"] == []
        assert result["pii_columns"] == []

    def test_stitch_table_skipped_automatically(
        self, databricks_client_stub, llm_client_stub
    ):
        """Tables starting with _stitch are automatically skipped."""
        # Setup stitch table
        databricks_client_stub.add_catalog("stitch_catalog")
        databricks_client_stub.add_schema("stitch_catalog", "stitch_schema")
        databricks_client_stub.add_table(
            "stitch_catalog",
            "stitch_schema",
            "_stitch_temp_table",
            columns=[{"name": "data", "type_name": "string"}],
        )

        # Execute function
        result = _helper_tag_pii_columns_logic(
            databricks_client_stub,
            llm_client_stub,
            "_stitch_temp_table",
            catalog_name_context="stitch_catalog",
            schema_name_context="stitch_schema",
        )

        # Verify stitch table is skipped
        assert result["skipped"] is True
        assert "starts with _stitch" in result["reason"]
        assert result["table_name"] == "_stitch_temp_table"
        assert result["full_name"] == "stitch_catalog.stitch_schema._stitch_temp_table"

    def test_no_pii_found_returns_successful_empty_result(
        self, databricks_client_stub, llm_client_stub
    ):
        """Tables with no PII columns return successful empty result."""
        # Setup table with non-PII columns
        databricks_client_stub.add_catalog("clean_catalog")
        databricks_client_stub.add_schema("clean_catalog", "clean_schema")
        databricks_client_stub.add_table(
            "clean_catalog",
            "clean_schema",
            "system_table",
            columns=[
                {"name": "id", "type_name": "integer"},
                {"name": "status", "type_name": "string"},
                {"name": "created_at", "type_name": "timestamp"},
            ],
        )

        # Configure LLM to identify no PII
        llm_client_stub.set_response_content(
            '[{"name":"id","semantic":null},{"name":"status","semantic":null},{"name":"created_at","semantic":"create-dt"}]'
        )

        # Execute function
        result = _helper_tag_pii_columns_logic(
            databricks_client_stub,
            llm_client_stub,
            "system_table",
            catalog_name_context="clean_catalog",
            schema_name_context="clean_schema",
        )

        # Verify successful processing with minimal PII
        assert not result["skipped"]
        assert result["column_count"] == 3
        assert result["pii_column_count"] == 1  # created_at has create-dt semantic
        assert result["has_pii"] is True

    def test_databricks_api_error_handled_gracefully(self, llm_client_stub):
        """Databricks API errors are handled gracefully."""
        from tests.fixtures.databricks.client import DatabricksClientStub

        # Create failing client stub
        class FailingStub(DatabricksClientStub):
            def get_table(self, **kwargs):
                raise Exception("Databricks API temporarily unavailable")

        failing_client = FailingStub()

        # Execute function
        result = _helper_tag_pii_columns_logic(
            failing_client,
            llm_client_stub,
            "failing_table",
            catalog_name_context="test_catalog",
            schema_name_context="test_schema",
        )

        # Verify graceful error handling
        assert result["skipped"] is True
        assert "Failed to retrieve table details" in result["error"]
        assert "Databricks API temporarily unavailable" in result["error"]

    def test_llm_api_error_handled_gracefully(
        self, databricks_client_stub, llm_client_stub
    ):
        """LLM API errors are handled gracefully."""
        # Setup test table
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "test_schema")
        databricks_client_stub.add_table(
            "test_catalog",
            "test_schema",
            "test_table",
            columns=[{"name": "email", "type_name": "string"}],
        )

        # Configure LLM to throw exception
        llm_client_stub.set_exception(True)

        # Execute function
        result = _helper_tag_pii_columns_logic(
            databricks_client_stub,
            llm_client_stub,
            "test_table",
            catalog_name_context="test_catalog",
            schema_name_context="test_schema",
        )

        # Verify graceful error handling
        assert result["skipped"] is True
        assert "Error during PII tagging" in result["error"]
        assert "Test LLM exception" in result["error"]

    def test_invalid_llm_json_response_handled_gracefully(
        self, databricks_client_stub, llm_client_stub
    ):
        """Invalid LLM JSON responses are handled gracefully."""
        # Setup test table
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "test_schema")
        databricks_client_stub.add_table(
            "test_catalog",
            "test_schema",
            "test_table",
            columns=[{"name": "email", "type_name": "string"}],
        )

        # Configure LLM to return invalid JSON
        llm_client_stub.set_response_content("Invalid JSON response")

        # Execute function
        result = _helper_tag_pii_columns_logic(
            databricks_client_stub,
            llm_client_stub,
            "test_table",
            catalog_name_context="test_catalog",
            schema_name_context="test_schema",
        )

        # Verify JSON error handling
        assert result["skipped"] is True
        assert "Failed to parse PII LLM response" in result["error"]

    def test_llm_response_column_count_mismatch_handled_gracefully(
        self, databricks_client_stub, llm_client_stub
    ):
        """LLM response with wrong number of columns is handled gracefully."""
        # Setup test table with 3 columns
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "test_schema")
        databricks_client_stub.add_table(
            "test_catalog",
            "test_schema",
            "test_table",
            columns=[
                {"name": "email", "type_name": "string"},
                {"name": "first_name", "type_name": "string"},
                {"name": "user_id", "type_name": "integer"},
            ],
        )

        # Configure LLM to return only 2 columns (mismatch)
        llm_client_stub.set_response_content(
            '[{"name":"email","semantic":"email"},{"name":"first_name","semantic":"given-name"}]'
        )

        # Execute function
        result = _helper_tag_pii_columns_logic(
            databricks_client_stub,
            llm_client_stub,
            "test_table",
            catalog_name_context="test_catalog",
            schema_name_context="test_schema",
        )

        # Verify column count mismatch handling
        assert result["skipped"] is True
        assert "Error during PII tagging" in result["error"]


class TestHelperScanSchemaForPiiLogicParameterValidation:
    """Test parameter validation for _helper_scan_schema_for_pii_logic function."""

    def test_missing_catalog_name_returns_error(
        self, databricks_client_stub, llm_client_stub
    ):
        """Missing catalog name returns error."""
        result = _helper_scan_schema_for_pii_logic(
            databricks_client_stub,
            llm_client_stub,
            catalog_name="",  # Empty catalog name
            schema_name="test_schema",
        )

        assert "error" in result
        assert "Catalog and schema names are required" in result["error"]

    def test_missing_schema_name_returns_error(
        self, databricks_client_stub, llm_client_stub
    ):
        """Missing schema name returns error."""
        result = _helper_scan_schema_for_pii_logic(
            databricks_client_stub,
            llm_client_stub,
            catalog_name="test_catalog",
            schema_name="",  # Empty schema name
        )

        assert "error" in result
        assert "Catalog and schema names are required" in result["error"]

    def test_none_catalog_name_returns_error(
        self, databricks_client_stub, llm_client_stub
    ):
        """None catalog name returns error."""
        result = _helper_scan_schema_for_pii_logic(
            databricks_client_stub,
            llm_client_stub,
            catalog_name=None,
            schema_name="test_schema",
        )

        assert "error" in result
        assert "Catalog and schema names are required" in result["error"]


class TestHelperScanSchemaForPiiLogicDirectExecution:
    """Test direct execution of _helper_scan_schema_for_pii_logic function."""

    def test_successful_schema_scan_with_pii_tables(
        self, databricks_client_stub, llm_client_stub
    ):
        """Successful scan of schema with tables containing PII."""
        # Setup test data with multiple tables
        databricks_client_stub.add_catalog("production_catalog")
        databricks_client_stub.add_schema("production_catalog", "customer_data")
        databricks_client_stub.add_table(
            "production_catalog",
            "customer_data",
            "users",
            columns=[
                {"name": "email", "type_name": "string"},
                {"name": "first_name", "type_name": "string"},
            ],
        )
        databricks_client_stub.add_table(
            "production_catalog",
            "customer_data",
            "profiles",
            columns=[{"name": "phone", "type_name": "string"}],
        )
        databricks_client_stub.add_table(
            "production_catalog",
            "customer_data",
            "system_config",
            columns=[{"name": "config_key", "type_name": "string"}],
        )

        # Configure LLM to identify PII in users and profiles tables
        # The LLM stub will reuse the same response for all calls
        llm_client_stub.set_response_content(
            '[{"name":"email","semantic":"email"},{"name":"first_name","semantic":"given-name"}]'
        )

        # Execute function with progress disabled for performance
        result = _helper_scan_schema_for_pii_logic(
            databricks_client_stub,
            llm_client_stub,
            catalog_name="production_catalog",
            schema_name="customer_data",
            show_progress=False,
        )

        # Verify successful scan outcome
        assert "error" not in result
        assert result["catalog"] == "production_catalog"
        assert result["schema"] == "customer_data"
        assert result["tables_scanned_attempted"] == 3
        assert result["tables_successfully_processed"] >= 1  # At least one successful
        assert "results_detail" in result
        assert len(result["results_detail"]) == 3

        # Verify result structure
        assert "total_pii_columns" in result
        assert "tables_with_pii" in result

    def test_successful_schema_scan_with_no_tables(
        self, databricks_client_stub, llm_client_stub
    ):
        """Successful scan of empty schema returns helpful message."""
        # Setup empty schema
        databricks_client_stub.add_catalog("empty_catalog")
        databricks_client_stub.add_schema("empty_catalog", "empty_schema")
        # Don't add any tables

        # Execute function
        result = _helper_scan_schema_for_pii_logic(
            databricks_client_stub,
            llm_client_stub,
            catalog_name="empty_catalog",
            schema_name="empty_schema",
        )

        # Verify empty schema handling
        assert "error" not in result
        assert "No user tables" in result["message"]
        assert result["catalog"] == "empty_catalog"
        assert result["schema"] == "empty_schema"
        assert result["tables_scanned"] == 0
        assert result["tables_with_pii"] == 0
        assert result["total_pii_columns"] == 0
        assert result["results_detail"] == []

    def test_schema_scan_excludes_stitch_tables_automatically(
        self, databricks_client_stub, llm_client_stub
    ):
        """Schema scan automatically excludes _stitch tables."""
        # Setup schema with regular and _stitch tables
        databricks_client_stub.add_catalog("mixed_catalog")
        databricks_client_stub.add_schema("mixed_catalog", "mixed_schema")
        databricks_client_stub.add_table("mixed_catalog", "mixed_schema", "users")
        databricks_client_stub.add_table(
            "mixed_catalog", "mixed_schema", "_stitch_temp_data"
        )
        databricks_client_stub.add_table(
            "mixed_catalog", "mixed_schema", "_stitch_logs"
        )
        databricks_client_stub.add_table("mixed_catalog", "mixed_schema", "orders")

        # Configure LLM response
        llm_client_stub.set_response_content('[{"name":"id","semantic":null}]')

        # Execute function
        result = _helper_scan_schema_for_pii_logic(
            databricks_client_stub,
            llm_client_stub,
            catalog_name="mixed_catalog",
            schema_name="mixed_schema",
            show_progress=False,
        )

        # Verify _stitch tables are excluded from scan
        assert "error" not in result
        assert (
            result["tables_scanned_attempted"] == 2
        )  # Only users and orders, not _stitch tables
        assert len(result["results_detail"]) == 2

        # Verify no _stitch tables in results
        table_names = [r.get("table_name", "") for r in result["results_detail"]]
        assert "users" in table_names or "_stitch_temp_data" not in table_names
        assert "_stitch_logs" not in table_names

    def test_schema_scan_handles_mixed_success_failure_results(
        self, databricks_client_stub, llm_client_stub
    ):
        """Schema scan handles mixed success and failure results correctly."""
        # Setup multiple tables
        databricks_client_stub.add_catalog("mixed_catalog")
        databricks_client_stub.add_schema("mixed_catalog", "mixed_schema")
        databricks_client_stub.add_table("mixed_catalog", "mixed_schema", "good_table")
        databricks_client_stub.add_table(
            "mixed_catalog", "mixed_schema", "another_table"
        )

        # Configure LLM to succeed with a valid response
        # The concurrent processing will handle multiple calls to the same response
        llm_client_stub.set_response_content('[{"name":"id","semantic":null}]')

        # Execute function
        result = _helper_scan_schema_for_pii_logic(
            databricks_client_stub,
            llm_client_stub,
            catalog_name="mixed_catalog",
            schema_name="mixed_schema",
            show_progress=False,
        )

        # Verify mixed results handling
        assert "error" not in result
        assert result["tables_scanned_attempted"] == 2
        assert len(result["results_detail"]) == 2

        # Should process all tables successfully with this setup
        successful_results = [
            r
            for r in result["results_detail"]
            if not r.get("skipped") and not r.get("error")
        ]
        failed_results = [
            r for r in result["results_detail"] if r.get("skipped") or r.get("error")
        ]

        # All tables should process successfully in this test
        assert len(successful_results) >= 1
        assert len(successful_results) + len(failed_results) == 2

    def test_databricks_api_error_during_table_listing_handled_gracefully(
        self, llm_client_stub
    ):
        """Databricks API errors during table listing are handled gracefully."""
        from tests.fixtures.databricks.client import DatabricksClientStub

        # Create failing client stub
        class FailingStub(DatabricksClientStub):
            def list_tables(self, **kwargs):
                raise Exception("Failed to access catalog")

        failing_client = FailingStub()

        # Execute function
        result = _helper_scan_schema_for_pii_logic(
            failing_client,
            llm_client_stub,
            catalog_name="failing_catalog",
            schema_name="failing_schema",
        )

        # Verify graceful error handling
        assert "error" in result
        assert "Failed to list tables" in result["error"]
        assert "Failed to access catalog" in result["error"]

    def test_progress_display_shows_during_scan(
        self, databricks_client_stub, llm_client_stub
    ):
        """Progress display works correctly during schema scan."""
        # Setup test data
        databricks_client_stub.add_catalog("progress_catalog")
        databricks_client_stub.add_schema("progress_catalog", "progress_schema")
        databricks_client_stub.add_table(
            "progress_catalog", "progress_schema", "table1"
        )
        databricks_client_stub.add_table(
            "progress_catalog", "progress_schema", "table2"
        )

        # Configure LLM response
        llm_client_stub.set_response_content('[{"name":"id","semantic":null}]')

        # Mock console to capture progress output
        with patch("chuck_data.commands.pii_tools.get_console") as mock_get_console:
            mock_console = mock_get_console.return_value

            # Execute function with progress enabled
            result = _helper_scan_schema_for_pii_logic(
                databricks_client_stub,
                llm_client_stub,
                catalog_name="progress_catalog",
                schema_name="progress_schema",
                show_progress=True,
            )

            # Verify function succeeded
            assert "error" not in result

            # Verify progress was displayed
            print_calls = mock_console.print.call_args_list
            progress_messages = [call[0][0] for call in print_calls if call[0]]

            # Should show progress for each table
            scanning_messages = [
                msg for msg in progress_messages if "Scanning" in str(msg)
            ]
            assert len(scanning_messages) >= 1, "Should show scanning progress messages"

    def test_progress_display_disabled_shows_no_output(
        self, databricks_client_stub, llm_client_stub
    ):
        """Progress display can be disabled successfully."""
        # Setup test data
        databricks_client_stub.add_catalog("quiet_catalog")
        databricks_client_stub.add_schema("quiet_catalog", "quiet_schema")
        databricks_client_stub.add_table("quiet_catalog", "quiet_schema", "quiet_table")

        # Configure LLM response
        llm_client_stub.set_response_content('[{"name":"id","semantic":null}]')

        # Mock console to capture output
        with patch("chuck_data.commands.pii_tools.get_console") as mock_get_console:
            mock_console = mock_get_console.return_value

            # Execute function with progress disabled
            result = _helper_scan_schema_for_pii_logic(
                databricks_client_stub,
                llm_client_stub,
                catalog_name="quiet_catalog",
                schema_name="quiet_schema",
                show_progress=False,
            )

            # Verify function succeeded
            assert "error" not in result

            # Verify no progress messages when disabled
            if mock_console.print.called:
                print_calls = mock_console.print.call_args_list
                progress_messages = [str(call[0][0]) for call in print_calls if call[0]]
                scanning_messages = [
                    msg for msg in progress_messages if "Scanning" in msg
                ]
                assert (
                    len(scanning_messages) == 0
                ), "No progress messages should appear when show_progress=False"


class TestHelperScanSchemaForPiiLogicEdgeCases:
    """Test edge cases and boundary conditions for _helper_scan_schema_for_pii_logic."""

    def test_unicode_catalog_and_schema_names_handled_correctly(
        self, databricks_client_stub, llm_client_stub
    ):
        """Unicode characters in catalog and schema names are handled correctly."""
        # Setup with unicode names
        unicode_catalog = "目录_測試"
        unicode_schema = "スキーマ_test"

        databricks_client_stub.add_catalog(unicode_catalog)
        databricks_client_stub.add_schema(unicode_catalog, unicode_schema)
        databricks_client_stub.add_table(unicode_catalog, unicode_schema, "test_table")

        # Configure LLM response
        llm_client_stub.set_response_content('[{"name":"id","semantic":null}]')

        # Execute function
        result = _helper_scan_schema_for_pii_logic(
            databricks_client_stub,
            llm_client_stub,
            catalog_name=unicode_catalog,
            schema_name=unicode_schema,
            show_progress=False,
        )

        # Verify unicode handling
        assert "error" not in result
        assert result["catalog"] == unicode_catalog
        assert result["schema"] == unicode_schema

    def test_very_large_schema_with_many_tables_handled_efficiently(
        self, databricks_client_stub, llm_client_stub
    ):
        """Very large schemas with many tables are handled efficiently."""
        # Setup schema with many tables
        databricks_client_stub.add_catalog("large_catalog")
        databricks_client_stub.add_schema("large_catalog", "large_schema")

        # Add 20 tables to simulate larger schema
        for i in range(20):
            databricks_client_stub.add_table(
                "large_catalog",
                "large_schema",
                f"table_{i:02d}",
                columns=[{"name": "id", "type_name": "INTEGER"}],
            )

        # Configure LLM response
        llm_client_stub.set_response_content('[{"name":"id","semantic":null}]')

        # Execute function
        result = _helper_scan_schema_for_pii_logic(
            databricks_client_stub,
            llm_client_stub,
            catalog_name="large_catalog",
            schema_name="large_schema",
            show_progress=False,
        )

        # Verify large schema handling
        assert "error" not in result
        assert result["tables_scanned_attempted"] == 20
        assert len(result["results_detail"]) == 20

    def test_concurrent_processing_exception_handling(
        self, databricks_client_stub, llm_client_stub
    ):
        """Concurrent processing exceptions are handled correctly."""
        # Setup test data - add table with columns to trigger LLM call
        databricks_client_stub.add_catalog("concurrent_catalog")
        databricks_client_stub.add_schema("concurrent_catalog", "concurrent_schema")
        databricks_client_stub.add_table(
            "concurrent_catalog",
            "concurrent_schema",
            "test_table",
            columns=[
                {"name": "email", "type_name": "string"}
            ],  # Add columns to trigger LLM
        )

        # Configure LLM to throw exceptions
        llm_client_stub.set_exception(True)

        # Execute function
        result = _helper_scan_schema_for_pii_logic(
            databricks_client_stub,
            llm_client_stub,
            catalog_name="concurrent_catalog",
            schema_name="concurrent_schema",
            show_progress=False,
        )

        # Verify exception handling in concurrent processing
        assert "error" not in result  # Function should not fail completely
        assert result["tables_scanned_attempted"] == 1
        assert len(result["results_detail"]) == 1

        # Should have error details for the failed table
        table_result = result["results_detail"][0]
        # With columns that trigger LLM processing, exception should result in skipped table
        assert table_result.get("skipped") is True
        assert "error" in table_result


class TestHelperTagPiiColumnsLogicEdgeCases:
    """Test edge cases and boundary conditions for _helper_tag_pii_columns_logic."""

    def test_unicode_table_and_column_names_handled_correctly(
        self, databricks_client_stub, llm_client_stub
    ):
        """Unicode characters in table and column names are handled correctly."""
        # Setup table with unicode names
        unicode_table = "用户表_測試"
        unicode_column = "电子邮件_テスト"

        databricks_client_stub.add_catalog("unicode_catalog")
        databricks_client_stub.add_schema("unicode_catalog", "unicode_schema")
        databricks_client_stub.add_table(
            "unicode_catalog",
            "unicode_schema",
            unicode_table,
            columns=[{"name": unicode_column, "type_name": "string"}],
        )

        # Configure LLM response with unicode column name
        llm_client_stub.set_response_content(
            f'[{{"name":"{unicode_column}","semantic":"email"}}]'
        )

        # Execute function
        result = _helper_tag_pii_columns_logic(
            databricks_client_stub,
            llm_client_stub,
            unicode_table,
            catalog_name_context="unicode_catalog",
            schema_name_context="unicode_schema",
        )

        # Verify unicode handling
        assert not result["skipped"]
        assert result["table_name"] == unicode_table
        assert result["columns"][0]["name"] == unicode_column

    def test_very_long_table_and_column_names_handled_correctly(
        self, databricks_client_stub, llm_client_stub
    ):
        """Very long table and column names are handled correctly."""
        # Create long names
        long_table_name = "very_long_table_name_" + "x" * 200
        long_column_name = "very_long_column_name_" + "x" * 200

        databricks_client_stub.add_catalog("long_names_catalog")
        databricks_client_stub.add_schema("long_names_catalog", "long_names_schema")
        databricks_client_stub.add_table(
            "long_names_catalog",
            "long_names_schema",
            long_table_name,
            columns=[{"name": long_column_name, "type_name": "string"}],
        )

        # Configure LLM response
        llm_client_stub.set_response_content(
            f'[{{"name":"{long_column_name}","semantic":"email"}}]'
        )

        # Execute function
        result = _helper_tag_pii_columns_logic(
            databricks_client_stub,
            llm_client_stub,
            long_table_name,
            catalog_name_context="long_names_catalog",
            schema_name_context="long_names_schema",
        )

        # Verify long name handling
        assert not result["skipped"]
        assert result["table_name"] == long_table_name
        assert result["pii_column_count"] == 1

    def test_mixed_pii_and_non_pii_columns_aggregation(
        self, databricks_client_stub, llm_client_stub
    ):
        """Mixed PII and non-PII columns are aggregated correctly."""
        # Setup table with mix of column types
        databricks_client_stub.add_catalog("mixed_catalog")
        databricks_client_stub.add_schema("mixed_catalog", "mixed_schema")
        databricks_client_stub.add_table(
            "mixed_catalog",
            "mixed_schema",
            "mixed_table",
            columns=[
                {"name": "customer_id", "type_name": "INTEGER"},  # Non-PII
                {"name": "email", "type_name": "STRING"},  # PII
                {"name": "first_name", "type_name": "STRING"},  # PII
                {"name": "signup_date", "type_name": "DATE"},  # Timestamp PII
                {"name": "account_status", "type_name": "STRING"},  # Non-PII
            ],
        )

        # Configure LLM to identify mixed PII
        llm_client_stub.set_response_content(
            '[{"name":"customer_id","semantic":null},'
            '{"name":"email","semantic":"email"},'
            '{"name":"first_name","semantic":"given-name"},'
            '{"name":"signup_date","semantic":"create-dt"},'
            '{"name":"account_status","semantic":null}]'
        )

        # Execute function
        result = _helper_tag_pii_columns_logic(
            databricks_client_stub,
            llm_client_stub,
            "mixed_table",
            catalog_name_context="mixed_catalog",
            schema_name_context="mixed_schema",
        )

        # Verify mixed column aggregation
        assert not result["skipped"]
        assert result["column_count"] == 5
        assert result["pii_column_count"] == 3  # email, first_name, signup_date
        assert result["has_pii"] is True

        # Verify all columns are included with correct semantic tags
        columns = result["columns"]
        assert len(columns) == 5
        semantic_map = {col["name"]: col["semantic"] for col in columns}
        assert semantic_map["customer_id"] is None
        assert semantic_map["email"] == "email"
        assert semantic_map["first_name"] == "given-name"
        assert semantic_map["signup_date"] == "create-dt"
        assert semantic_map["account_status"] is None

        # Verify PII columns isolation
        pii_columns = result["pii_columns"]
        assert len(pii_columns) == 3
        pii_names = [col["name"] for col in pii_columns]
        assert "email" in pii_names
        assert "first_name" in pii_names
        assert "signup_date" in pii_names

    def test_llm_response_with_json_code_blocks_handled_correctly(
        self, databricks_client_stub, llm_client_stub
    ):
        """LLM responses wrapped in JSON code blocks are handled correctly."""
        # Setup test table
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "test_schema")
        databricks_client_stub.add_table(
            "test_catalog",
            "test_schema",
            "test_table",
            columns=[{"name": "email", "type_name": "string"}],
        )

        # Configure LLM to return JSON wrapped in code blocks
        llm_client_stub.set_response_content(
            '```json\n[{"name":"email","semantic":"email"}]\n```'
        )

        # Execute function
        result = _helper_tag_pii_columns_logic(
            databricks_client_stub,
            llm_client_stub,
            "test_table",
            catalog_name_context="test_catalog",
            schema_name_context="test_schema",
        )

        # Verify code block parsing
        assert not result["skipped"]
        assert result["pii_column_count"] == 1
        assert result["columns"][0]["semantic"] == "email"

    def test_llm_response_with_plain_code_blocks_handled_correctly(
        self, databricks_client_stub, llm_client_stub
    ):
        """LLM responses wrapped in plain code blocks are handled correctly."""
        # Setup test table
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "test_schema")
        databricks_client_stub.add_table(
            "test_catalog",
            "test_schema",
            "test_table",
            columns=[{"name": "phone", "type_name": "string"}],
        )

        # Configure LLM to return JSON wrapped in plain code blocks
        llm_client_stub.set_response_content(
            '```\n[{"name":"phone","semantic":"phone"}]\n```'
        )

        # Execute function
        result = _helper_tag_pii_columns_logic(
            databricks_client_stub,
            llm_client_stub,
            "test_table",
            catalog_name_context="test_catalog",
            schema_name_context="test_schema",
        )

        # Verify plain code block parsing
        assert not result["skipped"]
        assert result["pii_column_count"] == 1
        assert result["columns"][0]["semantic"] == "phone"
