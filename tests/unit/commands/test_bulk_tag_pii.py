"""
Tests for bulk_tag_pii handler.

Behavioral tests focused on command execution patterns rather than implementation details.
"""

from unittest.mock import patch
from chuck_data.commands.bulk_tag_pii import handle_bulk_tag_pii


# ===== PARAMETER VALIDATION TESTS =====


def test_missing_catalog_uses_active_config(
    databricks_client_stub, llm_client_stub, temp_config
):
    """Missing catalog parameter uses active catalog from config."""
    with (
        patch("chuck_data.config._config_manager", temp_config),
        patch(
            "chuck_data.commands.bulk_tag_pii.LLMClient", return_value=llm_client_stub
        ),
    ):
        # Setup real config with test values
        temp_config.update(
            active_catalog="active_catalog",
            active_schema="active_schema",
            warehouse_id="warehouse123",
        )

        # Setup test data with PII for a proper validation test
        databricks_client_stub.add_catalog("active_catalog")
        databricks_client_stub.add_schema("active_catalog", "active_schema")
        databricks_client_stub.add_table(
            "active_catalog",
            "active_schema",
            "users",
            columns=[{"name": "email", "type": "string"}],
        )
        llm_client_stub.set_pii_detection_result(
            [{"column": "email", "semantic": "email"}]
        )

        result = handle_bulk_tag_pii(databricks_client_stub, auto_confirm=True)

        assert result.success
        assert "active_catalog.active_schema" in result.message


def test_missing_warehouse_returns_error(databricks_client_stub, temp_config):
    """Missing warehouse configuration returns helpful error."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Setup real config with missing warehouse
        temp_config.update(
            active_catalog="test_catalog",
            active_schema="test_schema",
            # warehouse_id is None by default
        )

        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
            auto_confirm=True,
        )

        assert not result.success
        assert "warehouse" in result.message.lower()
        assert "configure" in result.message.lower()


def test_nonexistent_schema_returns_helpful_error(databricks_client_stub, temp_config):
    """Nonexistent schema returns error with available options."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Setup real config
        temp_config.update(warehouse_id="warehouse123")

        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "existing_schema")

        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="nonexistent_schema",
            auto_confirm=True,
        )

        assert not result.success
        assert "Schema 'nonexistent_schema' not found" in result.message
        assert "Available schemas: existing_schema" in result.message


def test_nonexistent_catalog_returns_helpful_error(databricks_client_stub, temp_config):
    """Nonexistent catalog returns error with available options."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Setup real config
        temp_config.update(warehouse_id="warehouse123")

        databricks_client_stub.add_catalog("existing_catalog")

        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="nonexistent_catalog",
            schema_name="test_schema",
            auto_confirm=True,
        )

        assert not result.success
        assert "Catalog 'nonexistent_catalog' not found" in result.message
        assert "Available catalogs: existing_catalog" in result.message


def test_missing_schema_parameter_uses_active_config(
    databricks_client_stub, llm_client_stub, temp_config
):
    """Missing schema parameter uses active schema from config."""
    with (
        patch("chuck_data.config._config_manager", temp_config),
        patch(
            "chuck_data.commands.bulk_tag_pii.LLMClient", return_value=llm_client_stub
        ),
    ):
        # Setup real config with test values
        temp_config.update(
            active_catalog="test_catalog",
            active_schema="active_schema",
            warehouse_id="warehouse123",
        )

        # Setup test data with PII for a proper validation test
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "active_schema")
        databricks_client_stub.add_table(
            "test_catalog",
            "active_schema",
            "users",
            columns=[{"name": "email", "type": "string"}],
        )
        llm_client_stub.set_pii_detection_result(
            [{"column": "email", "semantic": "email"}]
        )

        result = handle_bulk_tag_pii(
            databricks_client_stub, catalog_name="test_catalog", auto_confirm=True
        )

        assert result.success
        assert "test_catalog.active_schema" in result.message


def test_both_catalog_and_schema_missing_uses_active_config(
    databricks_client_stub, llm_client_stub, temp_config
):
    """Both catalog and schema missing uses active config."""
    with (
        patch("chuck_data.config._config_manager", temp_config),
        patch(
            "chuck_data.commands.bulk_tag_pii.LLMClient", return_value=llm_client_stub
        ),
    ):
        # Setup real config with test values
        temp_config.update(
            active_catalog="active_catalog",
            active_schema="active_schema",
            warehouse_id="warehouse123",
        )

        # Setup test data with PII for a proper validation test
        databricks_client_stub.add_catalog("active_catalog")
        databricks_client_stub.add_schema("active_catalog", "active_schema")
        databricks_client_stub.add_table(
            "active_catalog",
            "active_schema",
            "users",
            columns=[{"name": "email", "type": "string"}],
        )
        llm_client_stub.set_pii_detection_result(
            [{"column": "email", "semantic": "email"}]
        )

        result = handle_bulk_tag_pii(databricks_client_stub, auto_confirm=True)

        assert result.success
        assert "active_catalog.active_schema" in result.message


# ===== DIRECT COMMAND TESTS =====


def test_direct_command_successful_bulk_tagging(
    databricks_client_stub, llm_client_stub, temp_config
):
    """Direct command with auto_confirm successfully scans and tags PII."""
    with (
        patch("chuck_data.config._config_manager", temp_config),
        patch(
            "chuck_data.commands.bulk_tag_pii.LLMClient", return_value=llm_client_stub
        ),
    ):
        # Setup real config with test values
        temp_config.update(warehouse_id="warehouse123")

        setup_successful_bulk_pii_test_data(databricks_client_stub, llm_client_stub)

        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
            auto_confirm=True,
        )

        assert result.success
        assert "Bulk PII tagging completed" in result.message
        assert "tables_processed" in result.data
        assert "columns_tagged" in result.data
        assert result.data["tables_processed"] >= 0


def test_direct_command_no_pii_found_returns_informative_message(
    databricks_client_stub, llm_client_stub, temp_config
):
    """Direct command with no PII found returns informative message."""
    with (
        patch("chuck_data.config._config_manager", temp_config),
        patch(
            "chuck_data.commands.bulk_tag_pii.LLMClient", return_value=llm_client_stub
        ),
    ):
        # Setup real config with test values
        temp_config.update(warehouse_id="warehouse123")
        setup_no_pii_test_data(databricks_client_stub, llm_client_stub)

        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
            auto_confirm=True,
        )

        assert result.success
        assert "No PII columns found" in result.message
        assert (
            result.data["tables_processed"] >= 0
        )  # Tables were scanned but no PII found
        assert result.data["columns_tagged"] == 0


def test_direct_command_partial_failures_handled_gracefully(
    databricks_client_stub, llm_client_stub, temp_config
):
    """Direct command handles partial tagging failures gracefully."""
    with (
        patch("chuck_data.config._config_manager", temp_config),
        patch(
            "chuck_data.commands.bulk_tag_pii.LLMClient", return_value=llm_client_stub
        ),
    ):
        # Setup real config with test values
        temp_config.update(warehouse_id="warehouse123")
        setup_successful_bulk_pii_test_data(databricks_client_stub, llm_client_stub)

        # Mock SQL execution to return failures for some operations
        def mock_sql_submit(sql_text=None, **kwargs):
            if "users" in sql_text:
                # Fail operations on users table
                return {
                    "status": {
                        "state": "FAILED",
                        "error": {"message": "Permission denied for table users"},
                    }
                }
            else:
                # Succeed for other tables
                return {"status": {"state": "SUCCEEDED"}}

        databricks_client_stub.submit_sql_statement = mock_sql_submit

        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
            auto_confirm=True,
        )

        # Should still be considered a success (partial success)
        assert not result.success  # Changed to failure with enhanced error handling
        assert (
            "partially completed" in result.message.lower()
            or "failed" in result.message.lower()
        )
        assert "columns_failed" in result.data
        assert result.data["columns_failed"] > 0


# ===== AGENT INTEGRATION TESTS =====


def test_agent_shows_progress_during_bulk_operations(
    databricks_client_stub, llm_client_stub, temp_config
):
    """Agent execution shows detailed progress during bulk tagging."""
    with (
        patch("chuck_data.config._config_manager", temp_config),
        patch(
            "chuck_data.commands.bulk_tag_pii.LLMClient", return_value=llm_client_stub
        ),
    ):
        # Setup real config with test values
        temp_config.update(warehouse_id="warehouse123")
        setup_successful_bulk_pii_test_data(databricks_client_stub, llm_client_stub)

        progress_steps = []

        def capture_progress(tool_name, data):
            if "step" in data:
                progress_steps.append(f"→ Bulk Tag PII: ({data['step']})")

        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
            auto_confirm=True,
            tool_output_callback=capture_progress,
        )

        assert result.success
        assert (
            len(progress_steps) >= 4
        )  # Scanning + scan completion + tagging start + per-table progress + completion
        assert any("scanning schema" in step.lower() for step in progress_steps)
        assert any("scan completed" in step.lower() for step in progress_steps)
        assert any("starting bulk tagging" in step.lower() for step in progress_steps)
        assert any(
            "tagging" in step.lower() and "pii columns in" in step.lower()
            for step in progress_steps
        )
        assert any("bulk tagging completed" in step.lower() for step in progress_steps)


def test_agent_tool_executor_integration(
    databricks_client_stub, llm_client_stub, temp_config
):
    """Agent tool_executor integration works end-to-end."""
    from chuck_data.agent.tool_executor import execute_tool

    with (
        patch("chuck_data.config._config_manager", temp_config),
        patch(
            "chuck_data.commands.bulk_tag_pii.LLMClient", return_value=llm_client_stub
        ),
    ):
        # Setup real config with test values
        temp_config.update(warehouse_id="warehouse123")
        setup_successful_bulk_pii_test_data(databricks_client_stub, llm_client_stub)

        result = execute_tool(
            api_client=databricks_client_stub,
            tool_name="bulk-tag-pii",
            tool_args={
                "catalog_name": "test_catalog",
                "schema_name": "test_schema",
                "auto_confirm": True,
            },
        )

        assert "tables_processed" in result
        assert "columns_tagged" in result
        assert result["catalog_name"] == "test_catalog"
        assert result["schema_name"] == "test_schema"


def test_bulk_tagging_execution_includes_detailed_results(
    databricks_client_stub, llm_client_stub, temp_config
):
    """Bulk tagging execution includes detailed tagging results."""
    with (
        patch("chuck_data.config._config_manager", temp_config),
        patch(
            "chuck_data.commands.bulk_tag_pii.LLMClient", return_value=llm_client_stub
        ),
    ):
        # Setup real config with test values
        temp_config.update(warehouse_id="warehouse123")
        setup_successful_bulk_pii_test_data(databricks_client_stub, llm_client_stub)

        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
            auto_confirm=True,
        )

        assert result.success
        assert "tagging_results" in result.data
        assert "scan_summary" in result.data

        # Verify the results contain information about tagging operations
        tagging_results = result.data["tagging_results"]
        assert isinstance(tagging_results, list)

        # Verify that we have some tagging results (SQLStubMixin returns success by default)
        if tagging_results:  # May be empty if no PII was found
            # Check structure of tagging results
            for result_item in tagging_results:
                assert "table" in result_item
                assert "column" in result_item
                assert "success" in result_item


# ===== INTERACTIVE WORKFLOW TESTS =====


def test_interactive_mode_shows_individual_table_progress_like_scan_pii(
    databricks_client_stub, llm_client_stub, temp_config
):
    """Interactive mode shows individual table progress like scan-pii command."""
    with (
        patch("chuck_data.config._config_manager", temp_config),
        patch(
            "chuck_data.commands.bulk_tag_pii.LLMClient", return_value=llm_client_stub
        ),
        patch("chuck_data.commands.pii_tools.get_console") as mock_pii_console,
        patch("chuck_data.commands.bulk_tag_pii.get_console") as mock_bulk_console,
    ):
        # Setup real config with test values
        temp_config.update(warehouse_id="warehouse123")
        setup_successful_bulk_pii_test_data(databricks_client_stub, llm_client_stub)

        # Setup mock console instances
        mock_pii_console_instance = mock_pii_console.return_value
        mock_bulk_console_instance = mock_bulk_console.return_value

        # Call without auto_confirm and without tool_output_callback (direct TUI usage)
        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
        )

        if not result.success:
            print(f"Test failed with message: '{result.message}'")
            print(f"Test failed with error: '{getattr(result, 'error', 'No error')}'")
        assert result.success

        # Should show individual table scanning messages from pii_tools (like scan-pii)
        pii_console_calls = [
            str(call) for call in mock_pii_console_instance.print.call_args_list
        ]
        individual_table_progress = any(
            "Scanning test_catalog.test_schema." in str(call)
            for call in pii_console_calls
        )

        assert (
            individual_table_progress
        ), f"Expected individual table scanning in pii console calls, got: {pii_console_calls}"

        # Should show scan summary and formatted PII preview via bulk console
        bulk_console_calls = [
            str(call) for call in mock_bulk_console_instance.print.call_args_list
        ]
        scan_summary_shown = any(
            "Scanned" in str(call) and "tables in test_catalog.test_schema" in str(call)
            for call in bulk_console_calls
        )
        pii_preview_shown = any(
            "PII Tagging Preview" in str(call) for call in bulk_console_calls
        )

        assert (
            scan_summary_shown
        ), f"Expected scan summary in bulk console calls, got: {bulk_console_calls}"
        assert (
            pii_preview_shown
        ), f"Expected PII preview in bulk console calls, got: {bulk_console_calls}"

        # Message should be empty like stitch setup (console output is the display)
        assert result.message == ""  # Empty message like stitch setup
        assert result.data["interactive"] is True
        assert result.data["awaiting_input"] is True


def test_interactive_mode_phase_1_scanning(
    databricks_client_stub, llm_client_stub, temp_config
):
    """Interactive mode Phase 1 scans schema and shows PII preview."""
    with (
        patch("chuck_data.config._config_manager", temp_config),
        patch(
            "chuck_data.commands.bulk_tag_pii.LLMClient", return_value=llm_client_stub
        ),
    ):
        # Setup real config with test values
        temp_config.update(warehouse_id="warehouse123")
        setup_successful_bulk_pii_test_data(databricks_client_stub, llm_client_stub)

        # Call without auto_confirm to trigger interactive mode
        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
        )

        assert result.success
        # Message is now empty (like stitch setup) - display happens via console
        assert result.message == ""
        assert result.data["interactive"] is True
        assert result.data["awaiting_input"] is True


def test_interactive_confirmation_proceeds_to_tagging(
    databricks_client_stub, llm_client_stub, temp_config
):
    """Interactive confirmation 'proceed' executes bulk tagging."""
    with (
        patch("chuck_data.config._config_manager", temp_config),
        patch(
            "chuck_data.commands.bulk_tag_pii.LLMClient", return_value=llm_client_stub
        ),
    ):
        # Setup real config with test values
        temp_config.update(warehouse_id="warehouse123")
        setup_successful_bulk_pii_test_data(databricks_client_stub, llm_client_stub)

        # First, start interactive mode
        result1 = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
        )
        assert result1.success
        assert result1.data["interactive"] is True

        # Then, send "proceed" input
        result2 = handle_bulk_tag_pii(
            databricks_client_stub, interactive_input="proceed"
        )

        assert result2.success
        assert "Bulk PII tagging completed" in result2.message
        assert "columns_tagged" in result2.data
        assert result2.data["columns_tagged"] > 0


def test_interactive_modification_excludes_tables(
    databricks_client_stub, llm_client_stub, temp_config
):
    """Interactive modification 'exclude table X' removes table from processing."""
    with (
        patch("chuck_data.config._config_manager", temp_config),
        patch(
            "chuck_data.commands.bulk_tag_pii.LLMClient", return_value=llm_client_stub
        ),
    ):
        # Setup real config with test values
        temp_config.update(warehouse_id="warehouse123")
        setup_successful_bulk_pii_test_data(databricks_client_stub, llm_client_stub)

        # First, start interactive mode
        result1 = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
        )
        assert result1.success
        original_columns = result1.data["total_pii_columns"]

        # Then, exclude one table
        result2 = handle_bulk_tag_pii(
            databricks_client_stub, interactive_input="exclude users"
        )

        assert result2.success
        # Message is now empty - display happens via console
        assert result2.message == ""
        assert result2.data["total_pii_columns"] < original_columns
        assert result2.data["interactive"] is True
        assert result2.data["awaiting_input"] is True

        # Then proceed to see excluded table count in final result
        result3 = handle_bulk_tag_pii(
            databricks_client_stub, interactive_input="proceed"
        )

        assert result3.success
        assert result3.data["excluded_tables_count"] == 1
        assert "(1 tables excluded)" in result3.message


def test_interactive_cancellation_cleans_up_context(
    databricks_client_stub, llm_client_stub, temp_config
):
    """Interactive cancellation cleans up context and exits gracefully."""
    from chuck_data.interactive_context import InteractiveContext

    with (
        patch("chuck_data.config._config_manager", temp_config),
        patch(
            "chuck_data.commands.bulk_tag_pii.LLMClient", return_value=llm_client_stub
        ),
    ):
        # Setup real config with test values
        temp_config.update(warehouse_id="warehouse123")
        setup_successful_bulk_pii_test_data(databricks_client_stub, llm_client_stub)

        # First, start interactive mode
        result1 = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
        )
        assert result1.success

        # Verify context is set
        context = InteractiveContext()
        context_data = context.get_context_data("bulk-tag-pii")
        assert context_data  # Should have data

        # Then cancel
        result2 = handle_bulk_tag_pii(
            databricks_client_stub, interactive_input="cancel"
        )

        assert result2.success
        assert "cancelled" in result2.message.lower()

        # Verify context is cleaned up
        context_data_after = context.get_context_data("bulk-tag-pii")
        assert not context_data_after  # Should be empty


# ===== ERROR HANDLING TESTS =====


def test_scan_phase_failure_returns_helpful_error(
    databricks_client_stub, llm_client_stub, temp_config
):
    """Scan phase failure returns helpful error without entering interactive mode."""
    with (
        patch("chuck_data.config._config_manager", temp_config),
        patch(
            "chuck_data.commands.bulk_tag_pii.LLMClient", return_value=llm_client_stub
        ),
    ):
        # Setup real config with test values
        temp_config.update(warehouse_id="warehouse123")

        # Set up basic structure but make LLM scanning fail
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "test_schema")
        databricks_client_stub.add_table("test_catalog", "test_schema", "users")

        # Mock the scanning helper to return an error
        with patch(
            "chuck_data.commands.bulk_tag_pii._helper_scan_schema_for_pii_logic"
        ) as mock_scan:
            mock_scan.return_value = {"error": "LLM service unavailable"}

            result = handle_bulk_tag_pii(
                databricks_client_stub,
                catalog_name="test_catalog",
                schema_name="test_schema",
                auto_confirm=True,
            )

            assert not result.success
            assert "Error during PII scanning" in result.message
            assert "LLM service unavailable" in result.message


def test_lost_interactive_context_shows_helpful_error(databricks_client_stub):
    """Lost interactive context shows helpful error message."""
    # Try to send interactive input without starting interactive mode first
    result = handle_bulk_tag_pii(databricks_client_stub, interactive_input="proceed")

    assert not result.success
    assert "No active bulk-tag-pii session found" in result.message
    assert "restart" in result.message.lower()


def test_tagging_phase_errors_aggregated_properly(
    databricks_client_stub, llm_client_stub, temp_config
):
    """Tagging phase errors are aggregated and reported clearly."""
    with (
        patch("chuck_data.config._config_manager", temp_config),
        patch(
            "chuck_data.commands.bulk_tag_pii.LLMClient", return_value=llm_client_stub
        ),
    ):
        # Setup real config with test values
        temp_config.update(warehouse_id="warehouse123")
        setup_successful_bulk_pii_test_data(databricks_client_stub, llm_client_stub)

        # Mock SQL execution to return various types of failures
        def mock_sql_submit(sql_text=None, **kwargs):
            if "users" in sql_text and "email" in sql_text:
                # Permission error for email column
                return {
                    "status": {
                        "state": "FAILED",
                        "error": {
                            "message": "Permission denied for column users.email"
                        },
                    }
                }
            elif "users" in sql_text and "phone" in sql_text:
                # Same permission error for phone column (should be grouped)
                return {
                    "status": {
                        "state": "FAILED",
                        "error": {
                            "message": "Permission denied for column users.phone"
                        },
                    }
                }
            elif "customer_profiles" in sql_text and "address" in sql_text:
                # Different error type for address column
                return {
                    "status": {
                        "state": "FAILED",
                        "error": {"message": "Timeout exceeded for operation"},
                    }
                }
            else:
                # Success for other operations
                return {"status": {"state": "SUCCEEDED"}}

        databricks_client_stub.submit_sql_statement = mock_sql_submit

        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
            auto_confirm=True,
        )

        # Should report partial failure with aggregated error summary
        assert not result.success  # Has failures
        assert "partially completed" in result.message.lower()
        assert "columns_failed" in result.data
        assert result.data["columns_failed"] > 0

        # Check that error summary aggregates similar errors
        assert "Failures:" in result.message
        # Should group permission errors and show timeout error separately
        assert (
            "Permission denied" in result.message
            or "Timeout exceeded" in result.message
        )


# ===== TEST DATA SETUP HELPERS =====


def setup_successful_bulk_pii_test_data(databricks_client_stub, llm_client_stub):
    """Setup test data for successful bulk PII operations."""
    # Add catalog and schema
    databricks_client_stub.add_catalog("test_catalog")
    databricks_client_stub.add_schema("test_catalog", "test_schema")

    # Add tables with PII columns
    databricks_client_stub.add_table(
        "test_catalog",
        "test_schema",
        "users",
        columns=[
            {"name": "id", "type": "bigint"},
            {"name": "email", "type": "string"},
            {"name": "full_name", "type": "string"},
            {"name": "phone", "type": "string"},
        ],
    )

    databricks_client_stub.add_table(
        "test_catalog",
        "test_schema",
        "customer_profiles",
        columns=[
            {"name": "customer_id", "type": "bigint"},
            {"name": "address", "type": "string"},
            {"name": "city", "type": "string"},
            {"name": "postal", "type": "string"},
        ],
    )

    # Mock LLM PII detection responses using the stub's method
    if llm_client_stub:
        # Set up PII detection results for LLM scanning
        llm_client_stub.set_pii_detection_result(
            [
                # Results for 'users' table
                {"column": "id", "semantic": None},
                {"column": "email", "semantic": "email"},
                {"column": "full_name", "semantic": "full-name"},
                {"column": "phone", "semantic": "phone"},
                # Results for 'customer_profiles' table
                {"column": "customer_id", "semantic": None},
                {"column": "address", "semantic": "address"},
                {"column": "city", "semantic": "city"},
                {"column": "postal", "semantic": "postal"},
            ]
        )


def setup_no_pii_test_data(databricks_client_stub, llm_client_stub):
    """Setup test data with no PII columns found."""
    databricks_client_stub.add_catalog("test_catalog")
    databricks_client_stub.add_schema("test_catalog", "test_schema")

    databricks_client_stub.add_table(
        "test_catalog",
        "test_schema",
        "system_logs",
        columns=[
            {"name": "id", "type": "bigint"},
            {"name": "timestamp", "type": "timestamp"},
            {"name": "log_level", "type": "string"},
            {"name": "message", "type": "string"},
        ],
    )

    # Mock LLM to return no PII using the stub's method
    if llm_client_stub:
        llm_client_stub.set_pii_detection_result(
            [
                {"column": "id", "semantic": None},
                {"column": "timestamp", "semantic": None},
                {"column": "log_level", "semantic": None},
                {"column": "message", "semantic": None},
            ]
        )


def mock_scan_results():
    """Mock scan results for interactive testing."""
    return {
        "catalog": "test_catalog",
        "schema": "test_schema",
        "tables_with_pii": 2,
        "total_pii_columns": 7,
        "results_detail": [
            {
                "table_name": "users",
                "full_name": "test_catalog.test_schema.users",
                "has_pii": True,
                "pii_columns": [
                    {"name": "email", "semantic": "email"},
                    {"name": "full_name", "semantic": "full-name"},
                ],
            },
            {
                "table_name": "sensitive_users",
                "full_name": "test_catalog.test_schema.sensitive_users",
                "has_pii": True,
                "pii_columns": [
                    {"name": "ssn", "semantic": "ssn"},
                    {"name": "address", "semantic": "address"},
                ],
            },
        ],
    }
