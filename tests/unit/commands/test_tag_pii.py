"""
Tests for tag_pii command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
Tests cover both direct command execution and agent interaction with tool_output_callback.
"""

import tempfile
from unittest.mock import MagicMock, patch

from chuck_data.commands.tag_pii import handle_command, apply_semantic_tags, DEFINITION
from chuck_data.config import (
    ConfigManager,
    set_warehouse_id,
    set_active_catalog,
    set_active_schema,
)
from chuck_data.agent.tool_executor import execute_tool


class TestTagPiiParameterValidation:
    """Test parameter validation for tag_pii command."""

    def test_missing_table_name_parameter_returns_error(self):
        """Missing table_name parameter returns error."""
        result = handle_command(
            None, pii_columns=[{"name": "test", "semantic": "email"}]
        )

        assert not result.success
        assert "table_name parameter is required" in result.message

    def test_missing_pii_columns_parameter_returns_error(self):
        """Missing pii_columns parameter returns error."""
        result = handle_command(None, table_name="test_table")

        assert not result.success
        assert "pii_columns parameter is required" in result.message

    def test_empty_pii_columns_parameter_returns_error(self):
        """Empty pii_columns list returns error."""
        result = handle_command(None, table_name="test_table", pii_columns=[])

        assert not result.success
        assert "pii_columns parameter is required" in result.message

    def test_none_client_returns_error(self):
        """None client returns error."""
        result = handle_command(
            None,
            table_name="test_table",
            pii_columns=[{"name": "test", "semantic": "email"}],
        )

        assert not result.success
        assert "Client is required for PII tagging" in result.message


class TestDirectTagPiiCommand:
    """Test direct tag_pii command execution (no tool_output_callback)."""

    def test_missing_warehouse_id_returns_error(
        self, databricks_client_stub, temp_config
    ):
        """Missing warehouse ID configuration returns error."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Don't set warehouse ID in config
            result = handle_command(
                databricks_client_stub,
                table_name="test_table",
                pii_columns=[{"name": "test", "semantic": "email"}],
            )

            assert not result.success
            assert "No warehouse ID configured" in result.message
            assert "Use /warehouse command" in result.message

    def test_missing_catalog_schema_for_simple_table_name_returns_error(
        self, databricks_client_stub, temp_config
    ):
        """Missing catalog/schema context for simple table name returns error."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_warehouse_id("warehouse123")
            # Don't set active catalog/schema

            result = handle_command(
                databricks_client_stub,
                table_name="simple_table",  # No dots, so needs catalog/schema
                pii_columns=[{"name": "test", "semantic": "email"}],
            )

            assert not result.success
            assert "No active catalog and schema selected" in result.message

    def test_nonexistent_table_returns_error(self, databricks_client_stub, temp_config):
        """Nonexistent table returns helpful error."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_warehouse_id("warehouse123")
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")

            # Don't add the table to stub - will cause table not found
            result = handle_command(
                databricks_client_stub,
                table_name="nonexistent_table",
                pii_columns=[{"name": "test", "semantic": "email"}],
            )

            assert not result.success
            assert (
                "Table test_catalog.test_schema.nonexistent_table not found"
                in result.message
            )

    def test_direct_command_tags_columns_successfully(
        self, databricks_client_stub, temp_config
    ):
        """Direct command successfully applies semantic tags to columns."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_warehouse_id("warehouse123")
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")

            # Setup test table
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "test_schema")
            databricks_client_stub.add_table(
                "test_catalog",
                "test_schema",
                "users_table",
                columns=[
                    {"name": "email", "type_name": "string"},
                    {"name": "first_name", "type_name": "string"},
                    {"name": "user_id", "type_name": "integer"},
                ],
            )

            # Configure PII columns to tag
            pii_columns = [
                {"name": "email", "semantic": "email"},
                {"name": "first_name", "semantic": "given-name"},
            ]

            # Execute command
            result = handle_command(
                databricks_client_stub,
                table_name="users_table",
                pii_columns=pii_columns,
            )

            # Verify success
            assert result.success
            assert (
                "Applied semantic tags to 2 of 2 columns in users_table"
                in result.message
            )

            # Verify result data structure
            assert result.data["table_name"] == "users_table"
            assert result.data["full_name"] == "test_catalog.test_schema.users_table"
            assert result.data["column_count"] == 3
            assert result.data["pii_column_count"] == 2
            assert result.data["pii_columns"] == pii_columns
            assert len(result.data["tagging_results"]) == 2

            # Verify tagging results
            tagging_results = result.data["tagging_results"]
            assert all(r["success"] for r in tagging_results)
            assert tagging_results[0]["column"] == "email"
            assert tagging_results[0]["semantic_type"] == "email"
            assert tagging_results[1]["column"] == "first_name"
            assert tagging_results[1]["semantic_type"] == "given-name"

    def test_direct_command_with_fully_qualified_table_name(
        self, databricks_client_stub, temp_config
    ):
        """Direct command works with fully qualified table name."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_warehouse_id("warehouse123")
            # Don't set active catalog/schema since we use fully qualified name

            # Setup test table
            databricks_client_stub.add_catalog("explicit_catalog")
            databricks_client_stub.add_schema("explicit_catalog", "explicit_schema")
            databricks_client_stub.add_table(
                "explicit_catalog",
                "explicit_schema",
                "test_table",
                columns=[{"name": "phone", "type_name": "string"}],
            )

            # Execute command with fully qualified table name
            result = handle_command(
                databricks_client_stub,
                table_name="explicit_catalog.explicit_schema.test_table",
                pii_columns=[{"name": "phone", "semantic": "phone"}],
            )

            # Verify success
            assert result.success
            assert (
                result.data["full_name"]
                == "explicit_catalog.explicit_schema.test_table"
            )
            assert result.data["table_name"] == "test_table"

    def test_databricks_api_error_handled_gracefully(
        self, databricks_client_stub, temp_config
    ):
        """Databricks API errors are handled gracefully with helpful messages."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_warehouse_id("warehouse123")
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")

            # Force table lookup error
            def failing_get_table(**kwargs):
                raise Exception("Databricks API temporarily unavailable")

            databricks_client_stub.get_table = failing_get_table

            result = handle_command(
                databricks_client_stub,
                table_name="failing_table",
                pii_columns=[{"name": "email", "semantic": "email"}],
            )

            assert not result.success
            assert "Failed to retrieve table details" in result.message
            assert "Databricks API temporarily unavailable" in result.message

    def test_sql_execution_failures_handled_gracefully(
        self, databricks_client_stub, temp_config
    ):
        """SQL execution failures are handled gracefully."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_warehouse_id("warehouse123")
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")

            # Setup test table
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "test_schema")
            databricks_client_stub.add_table(
                "test_catalog",
                "test_schema",
                "test_table",
                columns=[{"name": "email", "type_name": "string"}],
            )

            # Configure stub to return SQL failure
            def failing_sql_submit(sql_text=None, sql=None, **kwargs):
                return {
                    "status": {
                        "state": "FAILED",
                        "error": {"message": "Permission denied"},
                    }
                }

            databricks_client_stub.submit_sql_statement = failing_sql_submit

            result = handle_command(
                databricks_client_stub,
                table_name="test_table",
                pii_columns=[{"name": "email", "semantic": "email"}],
            )

            # Command succeeds but individual column tagging fails
            assert result.success
            assert "Applied semantic tags to 0 of 1 columns" in result.message

            # Verify tagging failure details
            tagging_results = result.data["tagging_results"]
            assert len(tagging_results) == 1
            assert not tagging_results[0]["success"]
            assert "Permission denied" in tagging_results[0]["error"]


class TestTagPiiCommandConfiguration:
    """Test tag_pii command configuration and registry integration."""

    def test_command_definition_structure(self):
        """Command definition has correct structure."""
        assert DEFINITION.name == "tag-pii-columns"
        assert "Apply semantic tags to columns" in DEFINITION.description
        assert "scan_pii command" in DEFINITION.description
        assert DEFINITION.handler == handle_command
        assert "table_name" in DEFINITION.parameters
        assert "pii_columns" in DEFINITION.parameters
        assert DEFINITION.required_params == ["table_name", "pii_columns"]
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True

    def test_command_parameter_specification(self):
        """Command parameters are correctly specified."""
        table_param = DEFINITION.parameters["table_name"]
        assert table_param["type"] == "string"
        assert "table to tag" in table_param["description"]
        assert "fully qualified" in table_param["description"]

        pii_param = DEFINITION.parameters["pii_columns"]
        assert pii_param["type"] == "array"
        assert "PII information" in pii_param["description"]
        assert "name" in pii_param["description"]
        assert "semantic" in pii_param["description"]

    def test_command_display_configuration(self):
        """Command display configuration is properly set."""
        assert DEFINITION.tui_aliases == ["/tag-pii-columns"]
        assert "Example:" in DEFINITION.usage_hint
        assert "email" in DEFINITION.usage_hint


class TestTagPiiAgentBehavior:
    """Test tag_pii command behavior with agent tool_output_callback."""

    def test_agent_tool_executor_end_to_end_integration(
        self, databricks_client_stub, temp_config
    ):
        """Agent tool_executor integration works end-to-end."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_warehouse_id("warehouse123")
            set_active_catalog("integration_catalog")
            set_active_schema("integration_schema")

            # Setup test table
            databricks_client_stub.add_catalog("integration_catalog")
            databricks_client_stub.add_schema(
                "integration_catalog", "integration_schema"
            )
            databricks_client_stub.add_table(
                "integration_catalog",
                "integration_schema",
                "customer_data",
                columns=[
                    {"name": "email", "type_name": "string"},
                    {"name": "phone", "type_name": "string"},
                ],
            )

            # Execute through agent tool executor
            result = execute_tool(
                api_client=databricks_client_stub,
                tool_name="tag-pii-columns",
                tool_args={
                    "table_name": "customer_data",
                    "pii_columns": [
                        {"name": "email", "semantic": "email"},
                        {"name": "phone", "semantic": "phone"},
                    ],
                },
            )

            # Verify agent gets proper result format
            assert "table_name" in result
            assert result["table_name"] == "customer_data"
            assert "full_name" in result
            assert (
                result["full_name"]
                == "integration_catalog.integration_schema.customer_data"
            )
            assert "pii_column_count" in result
            assert result["pii_column_count"] == 2
            assert "tagging_results" in result
            assert len(result["tagging_results"]) == 2

    def test_agent_callback_errors_handled_gracefully(
        self, databricks_client_stub, temp_config
    ):
        """Agent callback failures are handled gracefully (current behavior)."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_warehouse_id("warehouse123")
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")

            # Setup test table
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "test_schema")
            databricks_client_stub.add_table(
                "test_catalog",
                "test_schema",
                "test_table",
                columns=[{"name": "email", "type_name": "string"}],
            )

            def failing_callback(tool_name, data):
                raise Exception("Display system failure")

            # Note: tag-pii doesn't use tool_output_callback for progress reporting
            # It should complete successfully regardless of callback issues
            result = handle_command(
                databricks_client_stub,
                table_name="test_table",
                pii_columns=[{"name": "email", "semantic": "email"}],
                tool_output_callback=failing_callback,
            )

            # Should complete successfully since tag-pii doesn't depend on callback
            assert result.success
            assert "Applied semantic tags to 1 of 1 columns" in result.message


class TestApplySemanticTagsFunction:
    """Test the apply_semantic_tags helper function behavior."""

    def test_successful_tag_application(self, databricks_client_stub):
        """Successful application of semantic tags to multiple columns."""
        pii_columns = [
            {"name": "email_col", "semantic": "email"},
            {"name": "name_col", "semantic": "given-name"},
        ]

        results = apply_semantic_tags(
            databricks_client_stub, "catalog.schema.table", pii_columns, "warehouse123"
        )

        assert len(results) == 2
        assert all(r["success"] for r in results)
        assert results[0]["column"] == "email_col"
        assert results[0]["semantic_type"] == "email"
        assert results[1]["column"] == "name_col"
        assert results[1]["semantic_type"] == "given-name"

    def test_missing_column_data_handling(self, databricks_client_stub):
        """Missing column data is handled correctly."""
        pii_columns = [
            {"name": "email_col"},  # Missing semantic type
            {"semantic": "email"},  # Missing column name
            {"name": "good_col", "semantic": "phone"},  # Good data
        ]

        results = apply_semantic_tags(
            databricks_client_stub, "catalog.schema.table", pii_columns, "warehouse123"
        )

        assert len(results) == 3
        assert not results[0]["success"]  # Missing semantic type
        assert not results[1]["success"]  # Missing column name
        assert results[2]["success"]  # Good data

        assert "Missing column name or semantic type" in results[0]["error"]
        assert "Missing column name or semantic type" in results[1]["error"]

    def test_sql_execution_failure_handling(self, databricks_client_stub):
        """SQL execution failures are handled correctly."""

        # Configure stub to return SQL failure
        def failing_sql_submit(sql_text=None, sql=None, **kwargs):
            return {
                "status": {
                    "state": "FAILED",
                    "error": {"message": "SQL execution failed"},
                }
            }

        databricks_client_stub.submit_sql_statement = failing_sql_submit

        pii_columns = [{"name": "email_col", "semantic": "email"}]

        results = apply_semantic_tags(
            databricks_client_stub, "catalog.schema.table", pii_columns, "warehouse123"
        )

        assert len(results) == 1
        assert not results[0]["success"]
        assert "SQL execution failed" in results[0]["error"]

    def test_sql_execution_exception_handling(self):
        """Exceptions during SQL execution are handled correctly."""
        mock_client = MagicMock()
        mock_client.submit_sql_statement.side_effect = Exception("Connection error")

        pii_columns = [{"name": "email_col", "semantic": "email"}]

        results = apply_semantic_tags(
            mock_client, "catalog.schema.table", pii_columns, "warehouse123"
        )

        assert len(results) == 1
        assert not results[0]["success"]
        assert "Connection error" in results[0]["error"]


class TestTagPiiEdgeCases:
    """Test edge cases and boundary conditions for tag_pii."""

    def test_unicode_table_and_column_names_handled_correctly(
        self, databricks_client_stub, temp_config
    ):
        """Unicode characters in table and column names are handled correctly."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_warehouse_id("warehouse123")
            set_active_catalog("unicode_catalog")
            set_active_schema("unicode_schema")

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

            result = handle_command(
                databricks_client_stub,
                table_name=unicode_table,
                pii_columns=[{"name": unicode_column, "semantic": "email"}],
            )

            # Verify unicode handling
            assert result.success
            assert result.data["table_name"] == unicode_table
            assert result.data["tagging_results"][0]["column"] == unicode_column

    def test_very_long_table_and_column_names(
        self, databricks_client_stub, temp_config
    ):
        """Very long table and column names are handled correctly."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_warehouse_id("warehouse123")
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")

            # Create long names
            long_table_name = "very_long_table_name_" + "x" * 200
            long_column_name = "very_long_column_name_" + "x" * 200

            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "test_schema")
            databricks_client_stub.add_table(
                "test_catalog",
                "test_schema",
                long_table_name,
                columns=[{"name": long_column_name, "type_name": "string"}],
            )

            result = handle_command(
                databricks_client_stub,
                table_name=long_table_name,
                pii_columns=[{"name": long_column_name, "semantic": "email"}],
            )

            # Verify long name handling
            assert result.success
            assert result.data["table_name"] == long_table_name

    def test_multiple_pii_columns_mixed_success_failure(
        self, databricks_client_stub, temp_config
    ):
        """Mixed success and failure results are handled correctly."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_warehouse_id("warehouse123")
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")

            # Setup test table
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "test_schema")
            databricks_client_stub.add_table(
                "test_catalog",
                "test_schema",
                "mixed_table",
                columns=[
                    {"name": "email", "type_name": "string"},
                    {"name": "phone", "type_name": "string"},
                    {"name": "ssn", "type_name": "string"},
                ],
            )

            # Configure selective SQL failures
            call_count = 0

            def selective_sql_submit(sql_text=None, sql=None, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 2:  # Fail the second call (phone column)
                    return {
                        "status": {
                            "state": "FAILED",
                            "error": {"message": "Column phone tagging failed"},
                        }
                    }
                return {"status": {"state": "SUCCEEDED"}}

            databricks_client_stub.submit_sql_statement = selective_sql_submit

            pii_columns = [
                {"name": "email", "semantic": "email"},
                {"name": "phone", "semantic": "phone"},
                {"name": "ssn", "semantic": "social-security-number"},
            ]

            result = handle_command(
                databricks_client_stub,
                table_name="mixed_table",
                pii_columns=pii_columns,
            )

            # Verify mixed results
            assert result.success
            assert "Applied semantic tags to 2 of 3 columns" in result.message

            tagging_results = result.data["tagging_results"]
            assert len(tagging_results) == 3
            assert tagging_results[0]["success"]  # email succeeded
            assert not tagging_results[1]["success"]  # phone failed
            assert tagging_results[2]["success"]  # ssn succeeded
            assert "Column phone tagging failed" in tagging_results[1]["error"]

    def test_special_characters_in_semantic_types(
        self, databricks_client_stub, temp_config
    ):
        """Special characters in semantic types are handled correctly."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_warehouse_id("warehouse123")
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")

            # Setup test table
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "test_schema")
            databricks_client_stub.add_table(
                "test_catalog",
                "test_schema",
                "special_table",
                columns=[{"name": "custom_field", "type_name": "string"}],
            )

            # Use semantic type with special characters
            special_semantic = "custom-pii-type_v2.1"

            result = handle_command(
                databricks_client_stub,
                table_name="special_table",
                pii_columns=[{"name": "custom_field", "semantic": special_semantic}],
            )

            # Verify special character handling
            assert result.success
            assert (
                result.data["tagging_results"][0]["semantic_type"] == special_semantic
            )
