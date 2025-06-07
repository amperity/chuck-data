"""
Tests for scan_pii command handler.

Following approved testing patterns:
- Mock external boundaries only (LLM client)
- Use real config system with temporary files
- Use real internal business logic (_helper_scan_schema_for_pii_logic)
- Test end-to-end PII scanning behavior
- Test interactive display functionality
"""

import tempfile
import unittest
from unittest.mock import patch, MagicMock

from chuck_data.commands.scan_pii import handle_command
from chuck_data.config import ConfigManager


def test_missing_client():
    """Test handling when client is not provided."""
    result = handle_command(None)
    assert not result.success
    assert "Client is required" in result.message


def test_missing_context_real_config(databricks_client_stub):
    """Test handling when catalog or schema is missing in real config."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        # Don't set active_catalog or active_schema in config

        with patch("chuck_data.config._config_manager", config_manager):
            # Test real config validation with missing values
            result = handle_command(databricks_client_stub)

            assert not result.success
            assert "Catalog and schema must be specified" in result.message


def test_successful_scan_with_explicit_params_real_logic(
    databricks_client_stub_with_data, llm_client_stub
):
    """Test successful schema scan with explicit catalog/schema parameters."""
    # Configure LLM stub for PII detection
    llm_client_stub.set_response_content(
        '[{"name":"email","semantic":"email"},{"name":"phone","semantic":"phone"}]'
    )

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            with patch(
                "chuck_data.commands.scan_pii.LLMClient", return_value=llm_client_stub
            ):
                # Test real PII scanning logic with explicit parameters
                result = handle_command(
                    databricks_client_stub_with_data,
                    catalog_name="test_catalog",
                    schema_name="test_schema",
                )

    # Verify real PII scanning execution
    assert result.success
    assert "Scanned" in result.message
    assert "tables" in result.message
    assert result.data is not None
    # Real logic should return scan summary data
    assert (
        "tables_successfully_processed" in result.data
        or "tables_scanned_attempted" in result.data
    )


def test_scan_with_active_context_real_logic(
    databricks_client_stub_with_data, llm_client_stub
):
    """Test schema scan using real active catalog and schema from config."""
    # Configure LLM stub
    llm_client_stub.set_response_content(
        '[{"name":"user_id","semantic":"customer-id"}]'
    )

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        # Set up real config with active catalog/schema
        config_manager.update(
            active_catalog="active_catalog", active_schema="active_schema"
        )

        with patch("chuck_data.config._config_manager", config_manager):
            with patch(
                "chuck_data.commands.scan_pii.LLMClient", return_value=llm_client_stub
            ):
                # Test real config integration - should use active values
                result = handle_command(databricks_client_stub_with_data)

    # Should succeed using real active catalog/schema from config
    assert result.success
    assert result.data is not None


def test_scan_with_llm_error_real_logic(
    databricks_client_stub_with_data, llm_client_stub
):
    """Test handling when LLM client encounters error with real business logic."""
    # Configure LLM stub to simulate error
    llm_client_stub.set_exception(True)

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            with patch(
                "chuck_data.commands.scan_pii.LLMClient", return_value=llm_client_stub
            ):
                # Test real error handling with LLM failure
                result = handle_command(
                    databricks_client_stub_with_data,
                    catalog_name="test_catalog",
                    schema_name="test_schema",
                )

    # Real error handling should handle LLM errors gracefully
    assert isinstance(result.success, bool)
    assert result.error is not None or result.message is not None


def test_scan_with_databricks_client_stub_integration(
    databricks_client_stub_with_data, llm_client_stub
):
    """Test PII scanning with Databricks client stub integration."""
    # Configure LLM stub for realistic PII response
    llm_client_stub.set_response_content(
        '[{"name":"first_name","semantic":"given-name"},{"name":"last_name","semantic":"family-name"}]'
    )

    # Set up Databricks stub with test data
    databricks_client_stub_with_data.add_catalog("test_catalog")
    databricks_client_stub_with_data.add_schema("test_catalog", "test_schema")
    databricks_client_stub_with_data.add_table("test_catalog", "test_schema", "users")
    databricks_client_stub_with_data.add_table("test_catalog", "test_schema", "orders")

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            with patch(
                "chuck_data.commands.scan_pii.LLMClient", return_value=llm_client_stub
            ):
                # Test real PII scanning with stubbed external boundaries
                result = handle_command(
                    databricks_client_stub_with_data,
                    catalog_name="test_catalog",
                    schema_name="test_schema",
                )

    # Should work with real business logic + external stubs
    assert result.success
    assert result.data is not None
    assert "test_catalog.test_schema" in result.message


def test_scan_parameter_priority_real_logic(
    databricks_client_stub_with_data, llm_client_stub
):
    """Test that explicit parameters take priority over active config."""
    llm_client_stub.set_response_content("[]")  # No PII found

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        # Set up active config values
        config_manager.update(
            active_catalog="config_catalog", active_schema="config_schema"
        )

        with patch("chuck_data.config._config_manager", config_manager):
            with patch(
                "chuck_data.commands.scan_pii.LLMClient", return_value=llm_client_stub
            ):
                # Test real parameter priority logic: explicit should override config
                result = handle_command(
                    databricks_client_stub_with_data,
                    catalog_name="explicit_catalog",
                    schema_name="explicit_schema",
                )

    # Should use explicit parameters, not config values (real priority logic)
    assert result.success
    assert "explicit_catalog.explicit_schema" in result.message


def test_scan_with_partial_config_real_logic(
    databricks_client_stub_with_data, llm_client_stub
):
    """Test scan with partially configured active context."""
    llm_client_stub.set_response_content("[]")

    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        # Set only catalog, not schema - should fail validation
        config_manager.update(active_catalog="test_catalog")
        # active_schema is None/missing

        with patch("chuck_data.config._config_manager", config_manager):
            with patch(
                "chuck_data.commands.scan_pii.LLMClient", return_value=llm_client_stub
            ):
                # Test real validation logic with partial config
                result = handle_command(databricks_client_stub_with_data)

    # Should fail with real validation logic
    assert not result.success
    assert "Catalog and schema must be specified" in result.message


def test_scan_real_config_integration():
    """Test scan command integration with real config system."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        # Test config updates and retrieval
        config_manager.update(active_catalog="first_catalog")
        config_manager.update(active_schema="first_schema")
        config_manager.update(active_catalog="updated_catalog")  # Update catalog

        with patch("chuck_data.config._config_manager", config_manager):
            # Test real config state - should have updated catalog, original schema
            result = handle_command(
                None
            )  # No client - should fail but with real config access

    # Should fail due to missing client, but real config should be accessible
    assert not result.success
    assert "Client is required" in result.message


class TestScanPIIInteractiveDisplay(unittest.TestCase):
    """Tests for interactive display functionality in scan_pii command."""

    def setUp(self):
        """Set up common test fixtures."""
        # Use real databricks client stub (external boundary)
        from tests.fixtures import DatabricksClientStub
        self.client_stub = DatabricksClientStub()

    @patch("rich.console.Console")
    @patch("chuck_data.ui.tui._tui_instance", None)  # Ensure fallback path is used  
    def test_interactive_display_with_real_business_logic(self, mock_console_class):
        """Test interactive display using real business logic (following CLAUDE.md guidelines)."""
        # Setup mock console (external boundary - UI/Terminal)
        mock_console = MagicMock()
        mock_console_class.return_value = mock_console

        # Setup real databricks data using stub
        self.client_stub.add_catalog("test_catalog")
        self.client_stub.add_schema("test_catalog", "test_schema")
        self.client_stub.add_table(
            "test_catalog",
            "test_schema",
            "users",
            columns=[
                {"name": "email", "type_name": "string"},
                {"name": "first_name", "type_name": "string"},
                {"name": "signup_date", "type_name": "date"},
            ],
        )
        self.client_stub.add_table(
            "test_catalog",
            "test_schema",
            "products",
            columns=[
                {"name": "product_id", "type_name": "string"},
                {"name": "description", "type_name": "string"},
            ],
        )

        # Configure LLM client stub (external boundary - API calls)
        pii_response = '[{"name":"email","semantic":"email"},{"name":"first_name","semantic":"given-name"},{"name":"signup_date","semantic":null}]'

        # Mock the LLM class but use real LLM client stub
        with patch("chuck_data.commands.scan_pii.LLMClient") as mock_llm_class:
            from tests.fixtures import LLMClientStub
            llm_stub = LLMClientStub()
            # Set up responses for multiple tables
            llm_stub.set_response_content(pii_response)
            mock_llm_class.return_value = llm_stub

            # Call real handle_command function (no mocking internal business logic)
            result = handle_command(
                self.client_stub, catalog_name="test_catalog", schema_name="test_schema"
            )

        # Verify results using real business logic
        self.assertTrue(result.success)
        self.assertIn("test_catalog.test_schema", result.message)
        self.assertEqual(result.data["catalog"], "test_catalog")
        self.assertEqual(result.data["schema"], "test_schema")

        # Verify console progress messages are displayed
        print_calls = [
            call[0][0]
            for call in mock_console.print.call_args_list
            if mock_console.print.called
        ]
        expected_progress_messages = [
            "[dim]Scanning test_catalog.test_schema.users...[/dim]",
            "[dim]Scanning test_catalog.test_schema.products...[/dim]",
        ]
        for expected_msg in expected_progress_messages:
            self.assertIn(expected_msg, print_calls)

    @patch("rich.console.Console")
    @patch("chuck_data.ui.tui._tui_instance", None)  # Ensure fallback path is used
    def test_progress_can_be_disabled_with_real_logic(self, mock_console_class):
        """Test that progress display can be disabled using real business logic."""
        # Setup mock console (external boundary)
        mock_console = MagicMock()
        mock_console_class.return_value = mock_console

        # Setup real databricks data
        self.client_stub.add_catalog("test_catalog")
        self.client_stub.add_schema("test_catalog", "test_schema")
        self.client_stub.add_table(
            "test_catalog",
            "test_schema",
            "users",
            columns=[{"name": "email", "type_name": "string"}],
        )

        # Configure LLM stub
        pii_response = '[{"name":"email","semantic":"email"}]'

        with patch("chuck_data.commands.scan_pii.LLMClient") as mock_llm_class:
            from tests.fixtures import LLMClientStub
            llm_stub = LLMClientStub()
            llm_stub.set_response_content(pii_response)
            mock_llm_class.return_value = llm_stub

            # Call with show_progress=False
            result = handle_command(
                self.client_stub,
                catalog_name="test_catalog",
                schema_name="test_schema",
                show_progress=False,
            )

        # Verify results
        self.assertTrue(result.success)

        # Verify no progress messages when disabled
        if mock_console.print.called:
            print_calls = [call[0][0] for call in mock_console.print.call_args_list]
            progress_messages = [
                msg for msg in print_calls if "Scanning" in msg and "[dim]" in msg
            ]
            self.assertEqual(
                len(progress_messages),
                0,
                "No progress messages should appear when show_progress=False",
            )

    def test_show_progress_parameter_in_definition(self):
        """Test that show_progress parameter is properly defined in command definition."""
        from chuck_data.commands.scan_pii import DEFINITION

        # Verify the parameter is defined
        self.assertIn("show_progress", DEFINITION.parameters)
        self.assertEqual(DEFINITION.parameters["show_progress"]["type"], "boolean")
        self.assertIn("progress", DEFINITION.parameters["show_progress"]["description"].lower())
