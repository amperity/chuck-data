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
    assert "Error during bulk PII scan" in result.message


def test_direct_command_handles_llm_errors():
    """Direct command handles LLM API errors gracefully."""
    from tests.fixtures.databricks import DatabricksClientStub
    from tests.fixtures.llm import LLMClientStub

    client_stub = DatabricksClientStub()
    llm_stub = LLMClientStub()

    # Setup test data
    client_stub.add_catalog("test_catalog")
    client_stub.add_schema("test_catalog", "test_schema")
    client_stub.add_table("test_catalog", "test_schema", "users")

    # Force LLM error
    llm_stub.set_exception(True)

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            with patch("chuck_data.commands.scan_pii.LLMClient", return_value=llm_stub):
                result = handle_command(
                    client_stub, catalog_name="test_catalog", schema_name="test_schema"
                )

    # Should handle LLM errors gracefully
    assert not result.success
    assert "Error during bulk PII scan" in result.message


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
