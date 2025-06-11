"""
Tests for table command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
These tests verify what users see when they interact with the table command,
both directly and when an agent uses the table tool.
"""

import pytest
import tempfile
from unittest.mock import patch

from chuck_data.commands.table import handle_command
from chuck_data.config import ConfigManager


class TestTableParameterValidation:
    """Test parameter validation for table command."""

    def test_missing_name_parameter_returns_error(self, databricks_client_stub):
        """Missing table name parameter returns helpful error."""
        result = handle_command(databricks_client_stub)

        assert not result.success
        assert "name" in result.message.lower() or "table" in result.message.lower()

    def test_none_client_returns_setup_error(self):
        """None client returns workspace setup error."""
        result = handle_command(None, name="test_table")

        assert not result.success
        assert "workspace" in result.message.lower()
        assert "set up" in result.message.lower()

    def test_missing_schema_without_active_schema_returns_error(
        self, databricks_client_stub
    ):
        """Missing schema parameter without active schema returns helpful error."""
        with patch("chuck_data.commands.table.get_active_schema", return_value=None):
            with patch(
                "chuck_data.commands.table.get_active_catalog",
                return_value="test_catalog",
            ):
                result = handle_command(databricks_client_stub, name="test_table")

                assert not result.success
                assert "schema" in result.message.lower()
                assert "active schema" in result.message.lower()

    def test_missing_catalog_without_active_catalog_returns_error(
        self, databricks_client_stub
    ):
        """Missing catalog parameter without active catalog returns helpful error."""
        with patch(
            "chuck_data.commands.table.get_active_schema", return_value="test_schema"
        ):
            with patch(
                "chuck_data.commands.table.get_active_catalog", return_value=None
            ):
                result = handle_command(databricks_client_stub, name="test_table")

                assert not result.success
                assert "catalog" in result.message.lower()
                assert "active catalog" in result.message.lower()


class TestDirectTableCommand:
    """Test direct table command execution (no tool_output_callback)."""

    def test_direct_command_shows_table_details_for_existing_table(
        self, databricks_client_stub, temp_config
    ):
        """Direct table command shows detailed information for existing table."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup test table with columns
            test_columns = [
                {
                    "name": "id",
                    "type_text": "bigint",
                    "nullable": False,
                    "comment": "Primary key",
                },
                {
                    "name": "name",
                    "type_text": "string",
                    "nullable": True,
                    "comment": "User name",
                },
                {
                    "name": "created_at",
                    "type_text": "timestamp",
                    "nullable": False,
                    "comment": "Creation time",
                },
            ]

            databricks_client_stub.add_table(
                catalog_name="production",
                schema_name="analytics",
                table_name="user_events",
                table_type="MANAGED",
                comment="User event tracking table",
                owner="data-team@company.com",
                created_at="2023-01-01T00:00:00Z",
                columns=test_columns,
                properties={"delta.autoOptimize.optimizeWrite": "true"},
            )

            result = handle_command(
                databricks_client_stub,
                name="user_events",
                schema_name="analytics",
                catalog_name="production",
            )

            # Verify successful execution
            assert result.success
            assert "production.analytics.user_events" in result.message
            assert "3 columns" in result.message

            # Verify table data is returned with proper structure
            assert result.data is not None
            assert "table" in result.data
            table_info = result.data["table"]

            assert table_info["name"] == "user_events"
            assert table_info["full_name"] == "production.analytics.user_events"
            assert table_info["table_type"] == "MANAGED"
            assert table_info["column_count"] == 3
            assert len(table_info["columns"]) == 3

            # Verify column formatting
            id_column = next(
                col for col in table_info["columns"] if col["name"] == "id"
            )
            assert id_column["type"] == "bigint"
            assert id_column["nullable"] is False
            assert id_column["comment"] == "Primary key"

    def test_direct_command_uses_active_catalog_and_schema(
        self, databricks_client_stub, temp_config
    ):
        """Direct table command uses active catalog and schema when not specified."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Set active catalog and schema
            temp_config.update(
                active_catalog="default_catalog", active_schema="default_schema"
            )

            # Setup test table in the active catalog/schema
            databricks_client_stub.add_table(
                catalog_name="default_catalog",
                schema_name="default_schema",
                table_name="test_table",
                comment="Test table in active catalog/schema",
            )

            result = handle_command(databricks_client_stub, name="test_table")

            # Verify successful execution using active catalog/schema
            assert result.success
            assert "default_catalog.default_schema.test_table" in result.message
            assert (
                result.data["table"]["full_name"]
                == "default_catalog.default_schema.test_table"
            )

    def test_direct_command_handles_nonexistent_table(self, databricks_client_stub):
        """Direct table command shows helpful error for nonexistent table."""
        # Don't add any tables to databricks_client_stub

        result = handle_command(
            databricks_client_stub,
            name="nonexistent_table",
            schema_name="test_schema",
            catalog_name="test_catalog",
        )

        assert not result.success
        assert "nonexistent_table" in result.message
        assert "test_catalog.test_schema" in result.message
        assert "not found" in result.message.lower()

    def test_direct_command_handles_api_errors_gracefully(self, databricks_client_stub):
        """Direct table command handles API errors with user-friendly messages."""
        # Make get_table raise an exception
        original_get_table = databricks_client_stub.get_table

        def failing_get_table(*args, **kwargs):
            raise Exception("Table access denied - insufficient permissions")

        databricks_client_stub.get_table = failing_get_table

        result = handle_command(
            databricks_client_stub,
            name="restricted_table",
            schema_name="secure_schema",
            catalog_name="secure_catalog",
        )

        assert not result.success
        assert "failed" in result.message.lower()
        assert "Table access denied" in result.message

    def test_direct_command_with_delta_metadata(self, databricks_client_stub):
        """Direct table command includes delta metadata when requested."""
        # Add delta table with metadata
        databricks_client_stub.add_table(
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_name="delta_table",
            table_type="MANAGED",
            delta={
                "format": "DELTA",
                "id": "12345-67890",
                "num_files": 150,
                "size_in_bytes": 1024000,
            },
        )

        result = handle_command(
            databricks_client_stub,
            name="delta_table",
            schema_name="test_schema",
            catalog_name="test_catalog",
            include_delta_metadata=True,
        )

        assert result.success
        assert "delta" in result.data["table"]
        delta_info = result.data["table"]["delta"]
        assert delta_info["format"] == "DELTA"
        assert delta_info["num_files"] == 150

    def test_direct_command_with_minimal_table_data(self, databricks_client_stub):
        """Direct table command works with minimal table information."""
        # Add table with minimal required fields only
        databricks_client_stub.add_table("test_catalog", "test_schema", "minimal_table")

        result = handle_command(
            databricks_client_stub,
            name="minimal_table",
            schema_name="test_schema",
            catalog_name="test_catalog",
        )

        assert result.success
        assert result.data["table"]["name"] == "minimal_table"
        assert result.data["table"]["column_count"] == 0  # No columns defined


class TestAgentTableBehavior:
    """Test table command behavior when used by agent with tool_output_callback."""

    def test_agent_table_lookup_shows_full_display(self, databricks_client_stub):
        """Agent table lookup shows full table details display."""
        # Setup comprehensive table data
        comprehensive_columns = [
            {
                "name": "user_id",
                "type_text": "bigint",
                "nullable": False,
                "comment": "User identifier",
            },
            {
                "name": "event_type",
                "type_text": "string",
                "nullable": False,
                "comment": "Type of event",
            },
            {
                "name": "timestamp",
                "type_text": "timestamp",
                "nullable": False,
                "comment": "Event timestamp",
            },
            {
                "name": "properties",
                "type_text": "map<string,string>",
                "nullable": True,
                "comment": "Event properties",
            },
        ]

        databricks_client_stub.add_table(
            catalog_name="analytics_catalog",
            schema_name="events_schema",
            table_name="user_events",
            table_type="MANAGED",
            comment="Comprehensive user event tracking",
            owner="analytics-team@company.com",
            created_at="2023-06-15T10:30:00Z",
            columns=comprehensive_columns,
            properties={
                "delta.autoOptimize.optimizeWrite": "true",
                "retention.days": "365",
            },
        )

        # Capture progress during agent execution
        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append((tool_name, data))

        # Execute with tool_output_callback (agent mode)
        result = handle_command(
            databricks_client_stub,
            name="user_events",
            schema_name="events_schema",
            catalog_name="analytics_catalog",
            tool_output_callback=capture_progress,
        )

        # Verify command success
        assert result.success
        assert result.data["table"]["name"] == "user_events"
        assert result.data["table"]["column_count"] == 4

        # Since agent_display="full", there should be no progress steps
        # (full display happens in TUI layer, not in command handler)
        assert len(progress_steps) == 0

    def test_agent_nonexistent_table_shows_clear_error(self, databricks_client_stub):
        """Agent looking up nonexistent table gets clear error message."""
        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append((tool_name, data))

        result = handle_command(
            databricks_client_stub,
            name="missing_table",
            schema_name="test_schema",
            catalog_name="test_catalog",
            tool_output_callback=capture_progress,
        )

        # Verify error handling
        assert not result.success
        assert "missing_table" in result.message
        assert "test_catalog.test_schema" in result.message
        assert "not found" in result.message.lower()

        # No progress steps for direct lookup failure
        assert len(progress_steps) == 0

    def test_agent_uses_active_catalog_and_schema_successfully(
        self, databricks_client_stub, temp_config
    ):
        """Agent table lookup can use active catalog and schema when not specified."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Set active catalog and schema
            temp_config.update(
                active_catalog="agent_catalog", active_schema="agent_schema"
            )

            # Setup table in active catalog/schema
            databricks_client_stub.add_table(
                catalog_name="agent_catalog",
                schema_name="agent_schema",
                table_name="agent_table",
                comment="Table for agent testing",
            )

            progress_steps = []

            def capture_progress(tool_name, data):
                progress_steps.append((tool_name, data))

            # Agent calls without specifying catalog/schema (should use active)
            result = handle_command(
                databricks_client_stub,
                name="agent_table",
                tool_output_callback=capture_progress,
            )

            # Verify successful execution using active catalog/schema
            assert result.success
            assert "agent_catalog.agent_schema.agent_table" in result.message
            assert (
                result.data["table"]["full_name"]
                == "agent_catalog.agent_schema.agent_table"
            )

    def test_agent_api_error_handling(self, databricks_client_stub):
        """Agent table lookup handles API errors appropriately."""

        # Simulate API failure
        def failing_get_table(*args, **kwargs):
            raise Exception("Permission denied: insufficient table privileges")

        databricks_client_stub.get_table = failing_get_table

        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append((tool_name, data))

        result = handle_command(
            databricks_client_stub,
            name="restricted_table",
            schema_name="secure_schema",
            catalog_name="secure_catalog",
            tool_output_callback=capture_progress,
        )

        # Verify error is properly communicated
        assert not result.success
        assert "Permission denied" in result.message
        assert result.error is not None

    def test_agent_tool_executor_integration(self, databricks_client_stub):
        """Agent tool executor integration works end-to-end."""
        from chuck_data.agent.tool_executor import execute_tool

        # Setup test table
        databricks_client_stub.add_table(
            catalog_name="integration_catalog",
            schema_name="integration_schema",
            table_name="integration_table",
            comment="Integration test table",
            columns=[
                {"name": "id", "type_text": "bigint", "nullable": False},
                {"name": "data", "type_text": "string", "nullable": True},
            ],
        )

        result = execute_tool(
            api_client=databricks_client_stub,
            tool_name="table",
            tool_args={
                "name": "integration_table",
                "schema_name": "integration_schema",
                "catalog_name": "integration_catalog",
            },
        )

        # Verify agent gets proper result format
        assert "table" in result
        table_info = result["table"]
        assert table_info["name"] == "integration_table"
        assert (
            table_info["full_name"]
            == "integration_catalog.integration_schema.integration_table"
        )
        assert table_info["column_count"] == 2


class TestTableCommandConfiguration:
    """Test table command configuration and registry integration."""

    def test_table_command_definition_properties(self):
        """Table command definition has correct configuration."""
        from chuck_data.commands.table import DEFINITION

        assert DEFINITION.name == "table"
        assert DEFINITION.handler == handle_command
        assert DEFINITION.needs_api_client is True
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True
        assert DEFINITION.agent_display == "full"
        assert "name" in DEFINITION.required_params
        assert "name" in DEFINITION.parameters
        assert "schema_name" in DEFINITION.parameters
        assert "catalog_name" in DEFINITION.parameters
        assert "include_delta_metadata" in DEFINITION.parameters

    def test_table_command_parameter_requirements(self):
        """Table command has properly configured parameter requirements."""
        from chuck_data.commands.table import DEFINITION

        # Verify required parameter
        assert "name" in DEFINITION.required_params
        assert "schema_name" not in DEFINITION.required_params  # Optional
        assert "catalog_name" not in DEFINITION.required_params  # Optional

        # Verify parameter definitions
        name_param = DEFINITION.parameters["name"]
        assert name_param["type"] == "string"
        assert "table" in name_param["description"].lower()

        delta_param = DEFINITION.parameters["include_delta_metadata"]
        assert delta_param["type"] == "boolean"
        assert delta_param["default"] is False


class TestTableDisplayIntegration:
    """Test table command integration with display system."""

    def test_table_result_contains_display_ready_data(self, databricks_client_stub):
        """Table command result contains data ready for display formatting."""
        test_columns = [
            {"name": "id", "type_text": "bigint", "nullable": False, "comment": "ID"},
            {
                "name": "name",
                "type_text": "string",
                "nullable": True,
                "comment": "Name",
            },
        ]

        databricks_client_stub.add_table(
            catalog_name="display_catalog",
            schema_name="display_schema",
            table_name="display_table",
            comment="Test table for display",
            owner="test@company.com",
            columns=test_columns,
            properties={"env": "test"},
        )

        result = handle_command(
            databricks_client_stub,
            name="display_table",
            schema_name="display_schema",
            catalog_name="display_catalog",
        )

        assert result.success

        # Verify data structure matches what display layer expects
        assert "table" in result.data
        table_data = result.data["table"]
        assert isinstance(table_data, dict)
        assert "name" in table_data
        assert "full_name" in table_data
        assert "columns" in table_data
        assert "column_count" in table_data

        # Verify columns are properly formatted
        assert len(table_data["columns"]) == 2
        assert all("name" in col and "type" in col for col in table_data["columns"])

    def test_table_empty_fields_handled_gracefully(self, databricks_client_stub):
        """Table command handles empty or missing fields gracefully."""
        # Add table with some empty fields
        databricks_client_stub.add_table(
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_name="sparse_table",
            comment="",  # Empty comment
            owner=None,  # None owner
            columns=[],  # No columns
        )

        result = handle_command(
            databricks_client_stub,
            name="sparse_table",
            schema_name="test_schema",
            catalog_name="test_catalog",
        )

        assert result.success
        assert result.data["table"]["name"] == "sparse_table"
        assert result.data["table"]["column_count"] == 0
        # Should not fail even with empty/None fields
        assert isinstance(result.data["table"], dict)

    def test_table_full_name_construction(self, databricks_client_stub):
        """Table command correctly constructs full table names."""
        databricks_client_stub.add_table("prod_catalog", "user_schema", "user_events")

        result = handle_command(
            databricks_client_stub,
            name="user_events",
            schema_name="user_schema",
            catalog_name="prod_catalog",
        )

        assert result.success
        assert "prod_catalog.user_schema.user_events" in result.message
        assert (
            result.data["table"]["full_name"] == "prod_catalog.user_schema.user_events"
        )

    def test_table_column_formatting_preserves_important_info(
        self, databricks_client_stub
    ):
        """Table command formats columns while preserving important information."""
        complex_columns = [
            {
                "name": "complex_col",
                "type_text": "struct<id:bigint,name:string>",
                "nullable": True,
                "comment": "Complex nested structure",
                "position": 1,
            },
            {
                "name": "simple_col",
                "type": {"name": "string"},  # Different type format
                "nullable": False,
                "comment": "",
                "position": 2,
            },
        ]

        databricks_client_stub.add_table(
            catalog_name="test_catalog",
            schema_name="test_schema",
            table_name="complex_table",
            columns=complex_columns,
        )

        result = handle_command(
            databricks_client_stub,
            name="complex_table",
            schema_name="test_schema",
            catalog_name="test_catalog",
        )

        assert result.success
        formatted_columns = result.data["table"]["columns"]

        # Verify complex column formatting
        complex_col = next(
            col for col in formatted_columns if col["name"] == "complex_col"
        )
        assert complex_col["type"] == "struct<id:bigint,name:string>"
        assert complex_col["nullable"] is True
        assert complex_col["position"] == 1

        # Verify fallback type handling
        simple_col = next(
            col for col in formatted_columns if col["name"] == "simple_col"
        )
        assert simple_col["type"] == "string"  # Should extract from nested type
        assert simple_col["nullable"] is False
