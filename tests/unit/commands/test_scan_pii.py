"""
Tests for scan-pii command handler.

Behavioral tests focused on command execution patterns and user-visible behavior.
Tests both direct command execution and agent interaction via tool_output_callback.
"""

import tempfile
from unittest.mock import patch

from chuck_data.commands.scan_pii import handle_command
from chuck_data.config import ConfigManager


# ===== Parameter Validation Tests =====


def test_missing_client_returns_error():
    """Missing client parameter returns helpful error."""
    result = handle_command(None)

    assert not result.success
    assert "Client is required for bulk PII scan" in result.message


def test_missing_catalog_and_schema_returns_error():
    """Missing catalog and schema context returns helpful error."""
    from tests.fixtures.databricks import DatabricksClientStub

    client_stub = DatabricksClientStub()

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        # Don't set active_catalog or active_schema

        with patch("chuck_data.config._config_manager", config_manager):
            result = handle_command(client_stub)

    assert not result.success
    assert "Catalog and schema must be specified or active" in result.message


def test_partial_context_missing_schema_returns_error():
    """Missing schema with active catalog returns helpful error."""
    from tests.fixtures.databricks import DatabricksClientStub

    client_stub = DatabricksClientStub()

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(active_catalog="production_catalog")
        # Don't set active_schema

        with patch("chuck_data.config._config_manager", config_manager):
            result = handle_command(client_stub)

    assert not result.success
    assert "Catalog and schema must be specified or active" in result.message


# ===== Direct Command Tests =====


def test_direct_command_scans_schema_with_explicit_parameters():
    """Direct command scans specified catalog and schema successfully."""
    from tests.fixtures.databricks import DatabricksClientStub
    from tests.fixtures.llm import LLMClientStub

    client_stub = DatabricksClientStub()
    llm_stub = LLMClientStub()

    # Setup test data - catalog with tables containing PII
    client_stub.add_catalog("production_catalog")
    client_stub.add_schema("production_catalog", "customer_data")
    client_stub.add_table(
        "production_catalog",
        "customer_data",
        "users",
        columns=[
            {"name": "email", "type_name": "string"},
            {"name": "first_name", "type_name": "string"},
        ],
    )
    client_stub.add_table(
        "production_catalog",
        "customer_data",
        "orders",
        columns=[{"name": "order_id", "type_name": "string"}],
    )

    # Configure LLM to identify PII
    llm_stub.set_response_content(
        '[{"name":"email","semantic":"email"},{"name":"first_name","semantic":"given-name"}]'
    )

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            with patch("chuck_data.commands.scan_pii.LLMClient", return_value=llm_stub):
                result = handle_command(
                    client_stub,
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


def test_direct_command_uses_active_catalog_and_schema():
    """Direct command uses active catalog and schema from config."""
    from tests.fixtures.databricks import DatabricksClientStub
    from tests.fixtures.llm import LLMClientStub

    client_stub = DatabricksClientStub()
    llm_stub = LLMClientStub()

    # Setup test data for active context
    client_stub.add_catalog("active_catalog")
    client_stub.add_schema("active_catalog", "active_schema")
    client_stub.add_table("active_catalog", "active_schema", "customer_profiles")

    llm_stub.set_response_content("[]")  # No PII found

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(
            active_catalog="active_catalog", active_schema="active_schema"
        )

        with patch("chuck_data.config._config_manager", config_manager):
            with patch("chuck_data.commands.scan_pii.LLMClient", return_value=llm_stub):
                result = handle_command(client_stub)

    # Verify uses active context
    assert result.success
    assert "active_catalog.active_schema" in result.message


def test_direct_command_explicit_parameters_override_active_context():
    """Direct command explicit parameters take priority over active config."""
    from tests.fixtures.databricks import DatabricksClientStub
    from tests.fixtures.llm import LLMClientStub

    client_stub = DatabricksClientStub()
    llm_stub = LLMClientStub()

    # Setup data for both active and explicit contexts
    client_stub.add_catalog("active_catalog")
    client_stub.add_schema("active_catalog", "active_schema")
    client_stub.add_catalog("explicit_catalog")
    client_stub.add_schema("explicit_catalog", "explicit_schema")
    client_stub.add_table("explicit_catalog", "explicit_schema", "target_table")

    llm_stub.set_response_content("[]")

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(
            active_catalog="active_catalog", active_schema="active_schema"
        )

        with patch("chuck_data.config._config_manager", config_manager):
            with patch("chuck_data.commands.scan_pii.LLMClient", return_value=llm_stub):
                result = handle_command(
                    client_stub,
                    catalog_name="explicit_catalog",
                    schema_name="explicit_schema",
                )

    # Verify explicit parameters are used, not active config
    assert result.success
    assert "explicit_catalog.explicit_schema" in result.message


def test_direct_command_handles_empty_schema():
    """Direct command handles schema with no tables gracefully."""
    from tests.fixtures.databricks import DatabricksClientStub
    from tests.fixtures.llm import LLMClientStub

    client_stub = DatabricksClientStub()
    llm_stub = LLMClientStub()

    # Setup empty schema
    client_stub.add_catalog("empty_catalog")
    client_stub.add_schema("empty_catalog", "empty_schema")
    # Don't add any tables

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            with patch("chuck_data.commands.scan_pii.LLMClient", return_value=llm_stub):
                result = handle_command(
                    client_stub,
                    catalog_name="empty_catalog",
                    schema_name="empty_schema",
                )

    # Should handle empty schema gracefully
    assert result.success
    assert "empty_catalog.empty_schema" in result.message


def test_direct_command_handles_databricks_api_errors():
    """Direct command handles Databricks API errors gracefully."""
    from tests.fixtures.databricks import DatabricksClientStub
    from tests.fixtures.llm import LLMClientStub

    client_stub = DatabricksClientStub()
    llm_stub = LLMClientStub()

    # Force Databricks API error
    def failing_list_tables(**kwargs):
        raise Exception("Databricks API temporarily unavailable")

    client_stub.list_tables = failing_list_tables

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            with patch("chuck_data.commands.scan_pii.LLMClient", return_value=llm_stub):
                result = handle_command(
                    client_stub,
                    catalog_name="failing_catalog",
                    schema_name="failing_schema",
                )

    # Should handle API errors gracefully
    assert not result.success
    assert (
        "Failed to list tables" in result.message
        or "Error during bulk PII scan" in result.message
    )


def test_direct_command_handles_llm_errors():
    """Direct command handles LLM API errors gracefully."""
    from tests.fixtures.databricks import DatabricksClientStub
    from tests.fixtures.llm import LLMClientStub

    client_stub = DatabricksClientStub()
    llm_stub = LLMClientStub()

    # Setup test data with columns to trigger LLM call
    client_stub.add_catalog("test_catalog")
    client_stub.add_schema("test_catalog", "test_schema")
    client_stub.add_table(
        "test_catalog",
        "test_schema",
        "users",
        columns=[{"name": "email", "type_name": "string"}],
    )

    # Force LLM error
    llm_stub.set_exception(True)

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            with patch("chuck_data.commands.scan_pii.LLMClient", return_value=llm_stub):
                result = handle_command(
                    client_stub, catalog_name="test_catalog", schema_name="test_schema"
                )

    # Should handle LLM errors gracefully - scan succeeds but table is skipped
    assert result.success
    assert "Scanned 0/1 tables" in result.message  # 0 successful, 1 attempted
    assert result.data["tables_successfully_processed"] == 0
    assert len(result.data["results_detail"]) == 1
    error_detail = result.data["results_detail"][0]
    assert error_detail["skipped"] is True
    assert "Test LLM exception" in error_detail["error"]


# ===== Agent Progress Tests =====


def test_agent_shows_progress_while_scanning_tables():
    """Agent execution shows progress for each table being scanned."""
    from tests.fixtures.databricks import DatabricksClientStub
    from tests.fixtures.llm import LLMClientStub

    client_stub = DatabricksClientStub()
    llm_stub = LLMClientStub()

    # Setup multiple tables to scan
    client_stub.add_catalog("production_catalog")
    client_stub.add_schema("production_catalog", "customer_data")
    client_stub.add_table("production_catalog", "customer_data", "users")
    client_stub.add_table("production_catalog", "customer_data", "profiles")
    client_stub.add_table("production_catalog", "customer_data", "preferences")

    llm_stub.set_response_content("[]")  # No PII found

    def capture_progress(tool_name, data):
        # This captures the actual progress display behavior
        pass  # Progress is shown via console.print, not callback

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            with patch("chuck_data.commands.scan_pii.LLMClient", return_value=llm_stub):
                # Mock console to capture progress messages
                with patch(
                    "chuck_data.commands.pii_tools.get_console"
                ) as mock_get_console:
                    mock_console = mock_get_console.return_value

                    result = handle_command(
                        client_stub,
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
    progress_messages = [call[0][0] for call in print_calls]

    # Should show progress for each table
    assert any(
        "Scanning production_catalog.customer_data.users" in msg
        for msg in progress_messages
    )
    assert any(
        "Scanning production_catalog.customer_data.profiles" in msg
        for msg in progress_messages
    )
    assert any(
        "Scanning production_catalog.customer_data.preferences" in msg
        for msg in progress_messages
    )


def test_agent_can_disable_progress_display():
    """Agent execution can disable progress display when requested."""
    from tests.fixtures.databricks import DatabricksClientStub
    from tests.fixtures.llm import LLMClientStub

    client_stub = DatabricksClientStub()
    llm_stub = LLMClientStub()

    # Setup test data
    client_stub.add_catalog("quiet_catalog")
    client_stub.add_schema("quiet_catalog", "quiet_schema")
    client_stub.add_table("quiet_catalog", "quiet_schema", "users")
    client_stub.add_table("quiet_catalog", "quiet_schema", "orders")

    llm_stub.set_response_content("[]")

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            with patch("chuck_data.commands.scan_pii.LLMClient", return_value=llm_stub):
                with patch(
                    "chuck_data.commands.pii_tools.get_console"
                ) as mock_get_console:
                    mock_console = mock_get_console.return_value

                    result = handle_command(
                        client_stub,
                        catalog_name="quiet_catalog",
                        schema_name="quiet_schema",
                        show_progress=False,
                    )

    # Verify scan completed successfully
    assert result.success

    # Verify no progress messages when disabled
    if mock_console.print.called:
        print_calls = mock_console.print.call_args_list
        progress_messages = [call[0][0] for call in print_calls]
        scanning_messages = [msg for msg in progress_messages if "Scanning" in msg]
        assert (
            len(scanning_messages) == 0
        ), "No progress messages should appear when show_progress=False"


def test_agent_tool_executor_integration():
    """Agent tool_executor integration works end-to-end."""
    from tests.fixtures.databricks import DatabricksClientStub
    from tests.fixtures.llm import LLMClientStub
    from chuck_data.agent.tool_executor import execute_tool

    client_stub = DatabricksClientStub()
    llm_stub = LLMClientStub()

    # Setup test data
    client_stub.add_catalog("integration_catalog")
    client_stub.add_schema("integration_catalog", "integration_schema")
    client_stub.add_table("integration_catalog", "integration_schema", "customer_data")

    llm_stub.set_response_content(
        '[{"name":"email","semantic":"email"},{"name":"phone","semantic":"phone"}]'
    )

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            with patch("chuck_data.commands.scan_pii.LLMClient", return_value=llm_stub):
                result = execute_tool(
                    api_client=client_stub,
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


def test_agent_handles_tool_callback_errors_gracefully():
    """Agent callback failures are handled gracefully (current behavior)."""
    from tests.fixtures.databricks import DatabricksClientStub
    from tests.fixtures.llm import LLMClientStub

    client_stub = DatabricksClientStub()
    llm_stub = LLMClientStub()

    # Setup test data
    client_stub.add_catalog("callback_test_catalog")
    client_stub.add_schema("callback_test_catalog", "callback_test_schema")
    client_stub.add_table("callback_test_catalog", "callback_test_schema", "users")

    llm_stub.set_response_content("[]")

    def failing_callback(tool_name, data):
        raise Exception("Display system failure")

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            with patch("chuck_data.commands.scan_pii.LLMClient", return_value=llm_stub):
                # Note: scan-pii doesn't use tool_output_callback for reporting
                # Progress is shown via console.print directly
                result = handle_command(
                    client_stub,
                    catalog_name="callback_test_catalog",
                    schema_name="callback_test_schema",
                    tool_output_callback=failing_callback,
                )

    # Should complete successfully since scan-pii doesn't depend on callback
    assert result.success
    assert "callback_test_catalog.callback_test_schema" in result.message


def test_direct_command_display_shows_all_columns_not_just_pii():
    """Direct command display shows all columns (PII and non-PII) for complete table view."""
    from tests.fixtures.databricks import DatabricksClientStub
    from tests.fixtures.llm import LLMClientStub
    from chuck_data.ui.tui import ChuckTUI

    client_stub = DatabricksClientStub()
    llm_stub = LLMClientStub()

    # Setup table with mix of PII and non-PII columns
    client_stub.add_catalog("complete_catalog")
    client_stub.add_schema("complete_catalog", "complete_schema")
    client_stub.add_table(
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
    llm_stub.set_response_content(
        '[{"name":"customer_id","semantic":null},'
        '{"name":"email","semantic":"email"},'
        '{"name":"first_name","semantic":"given-name"},'
        '{"name":"signup_date","semantic":null},'
        '{"name":"account_status","semantic":null}]'
    )

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            with patch("chuck_data.commands.scan_pii.LLMClient", return_value=llm_stub):
                result = handle_command(
                    client_stub,
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
