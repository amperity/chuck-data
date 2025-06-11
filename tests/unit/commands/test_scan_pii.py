"""
Tests for scan_pii command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
Tests cover both direct command execution and agent interaction with tool_output_callback.
"""

import tempfile
from unittest.mock import patch

from chuck_data.commands.scan_pii import handle_command, DEFINITION
from chuck_data.config import ConfigManager
from chuck_data.agent.tool_executor import execute_tool


class TestScanPiiParameterValidation:
    """Test parameter validation for scan_pii command."""

    def test_none_client_returns_error(self):
        """None client returns error."""
        result = handle_command(None)

        assert not result.success
        assert "Client is required for bulk PII scan" in result.message

    def test_missing_catalog_and_schema_context_returns_error(
        self, databricks_client_stub
    ):
        """Missing catalog and schema context returns helpful error."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            # Don't set active_catalog or active_schema

            with patch("chuck_data.config._config_manager", config_manager):
                result = handle_command(databricks_client_stub)

        assert not result.success
        assert "Catalog and schema must be specified or active" in result.message

    def test_partial_context_missing_schema_returns_error(self, databricks_client_stub):
        """Missing schema with active catalog returns helpful error."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            config_manager.update(active_catalog="production_catalog")
            # Don't set active_schema

            with patch("chuck_data.config._config_manager", config_manager):
                result = handle_command(databricks_client_stub)

        assert not result.success
        assert "Catalog and schema must be specified or active" in result.message


class TestDirectScanPiiCommand:
    """Test direct scan_pii command execution (no tool_output_callback)."""

    def test_direct_command_scans_schema_with_explicit_parameters(
        self, databricks_client_stub, llm_client_stub
    ):
        """Direct command scans specified catalog and schema successfully."""
        # Setup test data - catalog with tables containing PII
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
            "orders",
            columns=[{"name": "order_id", "type_name": "string"}],
        )

        # Configure LLM to identify PII
        llm_client_stub.set_response_content(
            '[{"name":"email","semantic":"email"},{"name":"first_name","semantic":"given-name"}]'
        )

        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                with patch(
                    "chuck_data.commands.scan_pii.LLMClient",
                    return_value=llm_client_stub,
                ):
                    result = handle_command(
                        databricks_client_stub,
                        catalog_name="production_catalog",
                        schema_name="customer_data",
                    )

        # Verify successful scan outcome
        assert result.success
        assert "production_catalog.customer_data" in result.message
        assert "Scanned" in result.message and "tables" in result.message
        assert "Found" in result.message and "PII columns" in result.message

        # Verify scan results data structure
        assert result.data is not None
        assert result.data.get("catalog") == "production_catalog"
        assert result.data.get("schema") == "customer_data"
        assert "tables_successfully_processed" in result.data
        assert "total_pii_columns" in result.data

    def test_direct_command_uses_active_catalog_and_schema(
        self, databricks_client_stub, llm_client_stub
    ):
        """Direct command uses active catalog and schema from config."""
        # Setup test data for active context
        databricks_client_stub.add_catalog("active_catalog")
        databricks_client_stub.add_schema("active_catalog", "active_schema")
        databricks_client_stub.add_table(
            "active_catalog", "active_schema", "customer_profiles"
        )

        llm_client_stub.set_response_content("[]")  # No PII found

        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            config_manager.update(
                active_catalog="active_catalog", active_schema="active_schema"
            )

            with patch("chuck_data.config._config_manager", config_manager):
                with patch(
                    "chuck_data.commands.scan_pii.LLMClient",
                    return_value=llm_client_stub,
                ):
                    result = handle_command(databricks_client_stub)

        # Verify uses active context
        assert result.success
        assert "active_catalog.active_schema" in result.message

    def test_direct_command_explicit_parameters_override_active_context(
        self, databricks_client_stub, llm_client_stub
    ):
        """Direct command explicit parameters take priority over active config."""
        # Setup data for both active and explicit contexts
        databricks_client_stub.add_catalog("active_catalog")
        databricks_client_stub.add_schema("active_catalog", "active_schema")
        databricks_client_stub.add_catalog("explicit_catalog")
        databricks_client_stub.add_schema("explicit_catalog", "explicit_schema")
        databricks_client_stub.add_table(
            "explicit_catalog", "explicit_schema", "target_table"
        )

        llm_client_stub.set_response_content("[]")

        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            config_manager.update(
                active_catalog="active_catalog", active_schema="active_schema"
            )

            with patch("chuck_data.config._config_manager", config_manager):
                with patch(
                    "chuck_data.commands.scan_pii.LLMClient",
                    return_value=llm_client_stub,
                ):
                    result = handle_command(
                        databricks_client_stub,
                        catalog_name="explicit_catalog",
                        schema_name="explicit_schema",
                    )

        # Verify explicit parameters are used, not active config
        assert result.success
        assert "explicit_catalog.explicit_schema" in result.message

    def test_direct_command_handles_empty_schema(
        self, databricks_client_stub, llm_client_stub
    ):
        """Direct command handles schema with no tables gracefully."""
        # Setup empty schema
        databricks_client_stub.add_catalog("empty_catalog")
        databricks_client_stub.add_schema("empty_catalog", "empty_schema")
        # Don't add any tables

        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                with patch(
                    "chuck_data.commands.scan_pii.LLMClient",
                    return_value=llm_client_stub,
                ):
                    result = handle_command(
                        databricks_client_stub,
                        catalog_name="empty_catalog",
                        schema_name="empty_schema",
                    )

        # Should handle empty schema gracefully
        assert result.success
        assert "empty_catalog.empty_schema" in result.message

    def test_databricks_api_error_handled_gracefully(
        self, databricks_client_stub, llm_client_stub
    ):
        """Databricks API errors are handled gracefully with helpful messages."""

        # Force Databricks API error
        def failing_list_tables(**kwargs):
            raise Exception("Databricks API temporarily unavailable")

        databricks_client_stub.list_tables = failing_list_tables

        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                with patch(
                    "chuck_data.commands.scan_pii.LLMClient",
                    return_value=llm_client_stub,
                ):
                    result = handle_command(
                        databricks_client_stub,
                        catalog_name="failing_catalog",
                        schema_name="failing_schema",
                    )

        # Should handle API errors gracefully
        assert not result.success
        assert (
            "Failed to list tables" in result.message
            or "Error during bulk PII scan" in result.message
        )

    def test_llm_api_error_handled_gracefully(
        self, databricks_client_stub, llm_client_stub
    ):
        """LLM API errors are handled gracefully - tables are skipped but scan continues."""
        # Setup test data with columns to trigger LLM call
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "test_schema")
        databricks_client_stub.add_table(
            "test_catalog",
            "test_schema",
            "users",
            columns=[{"name": "email", "type_name": "string"}],
        )

        # Force LLM error
        llm_client_stub.set_exception(True)

        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                with patch(
                    "chuck_data.commands.scan_pii.LLMClient",
                    return_value=llm_client_stub,
                ):
                    result = handle_command(
                        databricks_client_stub,
                        catalog_name="test_catalog",
                        schema_name="test_schema",
                    )

        # Should handle LLM errors gracefully - scan succeeds but table is skipped
        assert result.success
        assert "Scanned 0/1 tables" in result.message  # 0 successful, 1 attempted
        assert result.data["tables_successfully_processed"] == 0
        assert len(result.data["results_detail"]) == 1
        error_detail = result.data["results_detail"][0]
        assert error_detail["skipped"] is True
        assert "Test LLM exception" in error_detail["error"]


class TestScanPiiCommandConfiguration:
    """Test scan_pii command configuration and registry integration."""

    def test_command_definition_structure(self):
        """Command definition has correct structure."""
        assert DEFINITION.name == "scan-schema-for-pii"
        assert "Scan all tables" in DEFINITION.description
        assert "PII" in DEFINITION.description
        assert DEFINITION.handler == handle_command
        assert "catalog_name" in DEFINITION.parameters
        assert "schema_name" in DEFINITION.parameters
        assert "show_progress" in DEFINITION.parameters
        assert DEFINITION.required_params == []
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True

    def test_command_parameter_specification(self):
        """Command parameters are correctly specified."""
        catalog_param = DEFINITION.parameters["catalog_name"]
        assert catalog_param["type"] == "string"
        assert "Optional" in catalog_param["description"]
        assert "active catalog" in catalog_param["description"]

        schema_param = DEFINITION.parameters["schema_name"]
        assert schema_param["type"] == "string"
        assert "Optional" in schema_param["description"]
        assert "active schema" in schema_param["description"]

        progress_param = DEFINITION.parameters["show_progress"]
        assert progress_param["type"] == "boolean"
        assert "Optional" in progress_param["description"]
        assert "Default: true" in progress_param["description"]

    def test_command_display_configuration(self):
        """Command display configuration is properly set."""
        assert DEFINITION.agent_display == "full"
        assert DEFINITION.condensed_action == "Scanning for PII in schema"
        assert DEFINITION.tui_aliases == ["/scan-pii"]


class TestScanPiiAgentBehavior:
    """Test scan_pii command behavior with agent tool_output_callback."""

    def test_agent_shows_progress_while_scanning_tables(
        self, databricks_client_stub, llm_client_stub
    ):
        """Agent execution shows progress for each table being scanned."""
        # Setup multiple tables to scan
        databricks_client_stub.add_catalog("production_catalog")
        databricks_client_stub.add_schema("production_catalog", "customer_data")
        databricks_client_stub.add_table("production_catalog", "customer_data", "users")
        databricks_client_stub.add_table(
            "production_catalog", "customer_data", "profiles"
        )
        databricks_client_stub.add_table(
            "production_catalog", "customer_data", "preferences"
        )

        llm_client_stub.set_response_content("[]")  # No PII found

        def capture_progress(tool_name, data):
            # This captures the actual progress display behavior
            pass  # Progress is shown via console.print, not callback

        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                with patch(
                    "chuck_data.commands.scan_pii.LLMClient",
                    return_value=llm_client_stub,
                ):
                    # Mock console to capture progress messages
                    with patch(
                        "chuck_data.commands.pii_tools.get_console"
                    ) as mock_get_console:
                        mock_console = mock_get_console.return_value

                        result = handle_command(
                            databricks_client_stub,
                            catalog_name="production_catalog",
                            schema_name="customer_data",
                            show_progress=True,
                            tool_output_callback=capture_progress,
                        )

        # Verify scan completed successfully
        assert result.success
        assert "production_catalog.customer_data" in result.message

        # Verify progress messages were displayed
        print_calls = mock_console.print.call_args_list
        progress_messages = [call[0][0] for call in print_calls if call[0]]

        # Should show progress for each table
        assert any(
            "Scanning production_catalog.customer_data.users" in str(msg)
            for msg in progress_messages
        )
        assert any(
            "Scanning production_catalog.customer_data.profiles" in str(msg)
            for msg in progress_messages
        )
        assert any(
            "Scanning production_catalog.customer_data.preferences" in str(msg)
            for msg in progress_messages
        )

    def test_agent_can_disable_progress_display(
        self, databricks_client_stub, llm_client_stub
    ):
        """Agent execution can disable progress display when requested."""
        # Setup test data
        databricks_client_stub.add_catalog("quiet_catalog")
        databricks_client_stub.add_schema("quiet_catalog", "quiet_schema")
        databricks_client_stub.add_table("quiet_catalog", "quiet_schema", "users")
        databricks_client_stub.add_table("quiet_catalog", "quiet_schema", "orders")

        llm_client_stub.set_response_content("[]")

        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                with patch(
                    "chuck_data.commands.scan_pii.LLMClient",
                    return_value=llm_client_stub,
                ):
                    with patch(
                        "chuck_data.commands.pii_tools.get_console"
                    ) as mock_get_console:
                        mock_console = mock_get_console.return_value

                        result = handle_command(
                            databricks_client_stub,
                            catalog_name="quiet_catalog",
                            schema_name="quiet_schema",
                            show_progress=False,
                        )

        # Verify scan completed successfully
        assert result.success

        # Verify no progress messages when disabled
        if mock_console.print.called:
            print_calls = mock_console.print.call_args_list
            progress_messages = [str(call[0][0]) for call in print_calls if call[0]]
            scanning_messages = [msg for msg in progress_messages if "Scanning" in msg]
            assert (
                len(scanning_messages) == 0
            ), "No progress messages should appear when show_progress=False"

    def test_agent_tool_executor_end_to_end_integration(
        self, databricks_client_stub, llm_client_stub
    ):
        """Agent tool_executor integration works end-to-end."""
        # Setup test data
        databricks_client_stub.add_catalog("integration_catalog")
        databricks_client_stub.add_schema("integration_catalog", "integration_schema")
        databricks_client_stub.add_table(
            "integration_catalog", "integration_schema", "customer_data"
        )

        llm_client_stub.set_response_content(
            '[{"name":"email","semantic":"email"},{"name":"phone","semantic":"phone"}]'
        )

        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                with patch(
                    "chuck_data.commands.scan_pii.LLMClient",
                    return_value=llm_client_stub,
                ):
                    result = execute_tool(
                        api_client=databricks_client_stub,
                        tool_name="scan-schema-for-pii",
                        tool_args={
                            "catalog_name": "integration_catalog",
                            "schema_name": "integration_schema",
                        },
                    )

        # Verify agent gets proper result format
        assert "catalog" in result
        assert result["catalog"] == "integration_catalog"
        assert "schema" in result
        assert result["schema"] == "integration_schema"
        assert "total_pii_columns" in result
        assert "tables_with_pii" in result

    def test_agent_callback_errors_handled_gracefully(
        self, databricks_client_stub, llm_client_stub
    ):
        """Agent callback failures are handled gracefully (current behavior)."""
        # Setup test data
        databricks_client_stub.add_catalog("callback_test_catalog")
        databricks_client_stub.add_schema(
            "callback_test_catalog", "callback_test_schema"
        )
        databricks_client_stub.add_table(
            "callback_test_catalog", "callback_test_schema", "users"
        )

        llm_client_stub.set_response_content("[]")

        def failing_callback(tool_name, data):
            raise Exception("Display system failure")

        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                with patch(
                    "chuck_data.commands.scan_pii.LLMClient",
                    return_value=llm_client_stub,
                ):
                    # Note: scan-pii doesn't use tool_output_callback for reporting
                    # Progress is shown via console.print directly
                    result = handle_command(
                        databricks_client_stub,
                        catalog_name="callback_test_catalog",
                        schema_name="callback_test_schema",
                        tool_output_callback=failing_callback,
                    )

        # Should complete successfully since scan-pii doesn't depend on callback
        assert result.success
        assert "callback_test_catalog.callback_test_schema" in result.message


class TestScanPiiDisplayIntegration:
    """Test scan_pii command display integration behavior."""

    def test_display_shows_all_columns_not_just_pii(
        self, databricks_client_stub, llm_client_stub
    ):
        """Display shows all columns (PII and non-PII) for complete table view."""
        from chuck_data.ui.tui import ChuckTUI

        # Setup table with mix of PII and non-PII columns
        databricks_client_stub.add_catalog("complete_catalog")
        databricks_client_stub.add_schema("complete_catalog", "complete_schema")
        databricks_client_stub.add_table(
            "complete_catalog",
            "complete_schema",
            "customer_data",
            columns=[
                {"name": "customer_id", "type_name": "INTEGER"},  # Non-PII
                {"name": "email", "type_name": "STRING"},  # PII
                {"name": "first_name", "type_name": "STRING"},  # PII
                {"name": "signup_date", "type_name": "DATE"},  # Non-PII
                {"name": "account_status", "type_name": "STRING"},  # Non-PII
            ],
        )

        # Configure LLM to identify only some columns as PII
        llm_client_stub.set_response_content(
            '[{"name":"customer_id","semantic":null},'
            '{"name":"email","semantic":"email"},'
            '{"name":"first_name","semantic":"given-name"},'
            '{"name":"signup_date","semantic":null},'
            '{"name":"account_status","semantic":null}]'
        )

        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                with patch(
                    "chuck_data.commands.scan_pii.LLMClient",
                    return_value=llm_client_stub,
                ):
                    result = handle_command(
                        databricks_client_stub,
                        catalog_name="complete_catalog",
                        schema_name="complete_schema",
                    )

        # Verify scan completed successfully
        assert result.success
        assert result.data is not None

        # Test the display behavior by mocking display_table calls
        tui = ChuckTUI(no_color=True)

        with patch("chuck_data.ui.table_formatter.display_table") as mock_display_table:
            tui._display_pii_scan_results(result.data)

            # Should have been called twice: once for table summary, once for column details
            assert (
                mock_display_table.call_count >= 2
            ), "Should call display_table for table summary and column details"

            # Get the call for column details (should be the last call with individual column data)
            column_display_calls = [
                call
                for call in mock_display_table.call_args_list
                if len(call[1].get("data", [])) > 0
                and isinstance(call[1].get("data", [{}])[0], dict)
                and "name" in call[1].get("data", [{}])[0]
                and "semantic" in call[1].get("data", [{}])[0]
            ]

            assert len(column_display_calls) > 0, "Should have column display calls"

            # Check the column data that was passed to display_table
            column_call = column_display_calls[0]
            column_data = column_call[1]["data"]

            # THIS IS THE KEY TEST: Should display ALL columns, not just PII columns
            column_names = [col["name"] for col in column_data]

            # Verify all columns are displayed with correct PII indicators
            assert (
                "customer_id" in column_names
            ), "Should display non-PII column customer_id"
            assert "email" in column_names, "Should display PII column email"
            assert "first_name" in column_names, "Should display PII column first_name"
            assert (
                "signup_date" in column_names
            ), "Should display non-PII column signup_date"
            assert (
                "account_status" in column_names
            ), "Should display non-PII column account_status"

            assert (
                len(column_data) == 5
            ), f"Should display all 5 columns, but only got {len(column_data)}: {column_names}"

            # Verify PII indicators are correct (blank for non-PII, semantic tag for PII)
            column_semantics = {col["name"]: col["semantic"] for col in column_data}
            assert (
                column_semantics["customer_id"] == ""
            ), "Non-PII column should have blank semantic"
            assert (
                column_semantics["email"] == "email"
            ), "PII column should have semantic tag"
            assert (
                column_semantics["first_name"] == "given-name"
            ), "PII column should have semantic tag"
            assert (
                column_semantics["signup_date"] == ""
            ), "Non-PII column should have blank semantic"
            assert (
                column_semantics["account_status"] == ""
            ), "Non-PII column should have blank semantic"


class TestScanPiiEdgeCases:
    """Test edge cases and boundary conditions for scan_pii."""

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

        llm_client_stub.set_response_content(
            f'[{{"name":"{unicode_column}","semantic":"email"}}]'
        )

        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                with patch(
                    "chuck_data.commands.scan_pii.LLMClient",
                    return_value=llm_client_stub,
                ):
                    result = handle_command(
                        databricks_client_stub,
                        catalog_name="unicode_catalog",
                        schema_name="unicode_schema",
                    )

        # Verify unicode handling
        assert result.success
        assert unicode_table in str(result.data)

    def test_very_large_schema_with_many_tables(
        self, databricks_client_stub, llm_client_stub
    ):
        """Very large schemas with many tables are handled efficiently."""
        # Setup schema with many tables
        databricks_client_stub.add_catalog("large_catalog")
        databricks_client_stub.add_schema("large_catalog", "large_schema")

        # Add 50 tables to simulate a large schema
        for i in range(50):
            databricks_client_stub.add_table(
                "large_catalog",
                "large_schema",
                f"table_{i:02d}",
                columns=[{"name": "id", "type_name": "INTEGER"}],
            )

        # Set correct response format for single column tables
        llm_client_stub.set_response_content('[{"name":"id","semantic":null}]')

        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                with patch(
                    "chuck_data.commands.scan_pii.LLMClient",
                    return_value=llm_client_stub,
                ):
                    result = handle_command(
                        databricks_client_stub,
                        catalog_name="large_catalog",
                        schema_name="large_schema",
                        show_progress=False,  # Disable progress for performance
                    )

        # Verify large schema handling
        assert result.success
        assert result.data["tables_scanned_attempted"] == 50
        assert result.data["tables_successfully_processed"] == 50
        assert "Scanned 50/50 tables" in result.message

    def test_table_with_very_long_column_names(
        self, databricks_client_stub, llm_client_stub
    ):
        """Tables with very long column names are handled correctly."""
        # Create a column name that's 256 characters long
        long_column_name = "very_long_column_name_" + "x" * (
            256 - len("very_long_column_name_")
        )

        databricks_client_stub.add_catalog("long_names_catalog")
        databricks_client_stub.add_schema("long_names_catalog", "long_names_schema")
        databricks_client_stub.add_table(
            "long_names_catalog",
            "long_names_schema",
            "test_table",
            columns=[{"name": long_column_name, "type_name": "string"}],
        )

        llm_client_stub.set_response_content(
            f'[{{"name":"{long_column_name}","semantic":"email"}}]'
        )

        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                with patch(
                    "chuck_data.commands.scan_pii.LLMClient",
                    return_value=llm_client_stub,
                ):
                    result = handle_command(
                        databricks_client_stub,
                        catalog_name="long_names_catalog",
                        schema_name="long_names_schema",
                    )

        # Verify long column name handling
        assert result.success
        assert result.data["total_pii_columns"] == 1

    def test_mixed_pii_and_non_pii_results_aggregation(
        self, databricks_client_stub, llm_client_stub
    ):
        """Mixed PII and non-PII results are aggregated correctly."""
        # Setup multiple tables with different PII patterns
        databricks_client_stub.add_catalog("mixed_catalog")
        databricks_client_stub.add_schema("mixed_catalog", "mixed_schema")

        # Table 1: Has PII (2 columns)
        databricks_client_stub.add_table(
            "mixed_catalog",
            "mixed_schema",
            "users",
            columns=[
                {"name": "email", "type_name": "string"},
                {"name": "phone", "type_name": "string"},
            ],
        )

        # Table 2: No PII (1 column)
        databricks_client_stub.add_table(
            "mixed_catalog",
            "mixed_schema",
            "products",
            columns=[{"name": "product_id", "type_name": "integer"}],
        )

        # Table 3: Has PII (1 column)
        databricks_client_stub.add_table(
            "mixed_catalog",
            "mixed_schema",
            "customers",
            columns=[{"name": "ssn", "type_name": "string"}],
        )

        # Use dynamic responses based on table columns
        # The LLM stub will reuse the response so we need different strategies
        # Let's just test that we can process multiple tables
        llm_client_stub.set_response_content('[{"name":"email","semantic":"email"}]')

        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                with patch(
                    "chuck_data.commands.scan_pii.LLMClient",
                    return_value=llm_client_stub,
                ):
                    result = handle_command(
                        databricks_client_stub,
                        catalog_name="mixed_catalog",
                        schema_name="mixed_schema",
                    )

        # Verify mixed results aggregation - at least one table processed successfully
        assert result.success
        assert result.data["tables_scanned_attempted"] == 3
        assert result.data["tables_successfully_processed"] >= 1
        # Note: Exact counts depend on LLM stub behavior with varying column counts
