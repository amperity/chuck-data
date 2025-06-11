"""
Tests for list_tables command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
These tests verify what users see when they interact with the list_tables command,
both directly and when an agent uses the list-tables tool.
"""

import pytest
from unittest.mock import patch

from chuck_data.commands.list_tables import handle_command
from chuck_data.config import set_active_catalog, set_active_schema
from chuck_data.agent.tool_executor import execute_tool


class TestListTablesParameterValidation:
    """Test parameter validation for list_tables command."""

    def test_none_client_returns_error(self):
        """None client returns error."""
        result = handle_command(None)

        assert not result.success
        assert "No Databricks client available" in result.message
        assert "workspace" in result.message.lower()

    def test_no_active_catalog_and_no_catalog_name_returns_error(
        self, databricks_client_stub, temp_config
    ):
        """No active catalog and no catalog_name parameter returns clear error."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Ensure no active catalog
            set_active_catalog(None)

            result = handle_command(databricks_client_stub)

            assert not result.success
            assert (
                "No catalog specified and no active catalog selected" in result.message
            )
            assert "select a catalog" in result.message.lower()

    def test_no_active_schema_and_no_schema_name_returns_error(
        self, databricks_client_stub, temp_config
    ):
        """No active schema and no schema_name parameter returns clear error."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Set catalog but no schema
            set_active_catalog("test_catalog")
            set_active_schema(None)

            result = handle_command(databricks_client_stub)

            assert not result.success
            assert "No schema specified and no active schema selected" in result.message
            assert "select a schema" in result.message.lower()


class TestDirectListTablesCommand:
    """Test direct list_tables command execution."""

    def test_direct_command_lists_tables_with_display_true(
        self, databricks_client_stub, temp_config
    ):
        """Direct command with display=true returns tables with display flag set."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "test_schema")
            databricks_client_stub.add_table("test_catalog", "test_schema", "table1")
            databricks_client_stub.add_table("test_catalog", "test_schema", "table2")

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            assert result.data.get("display") is True
            assert len(result.data.get("tables", [])) == 2
            assert result.data["catalog_name"] == "test_catalog"
            assert result.data["schema_name"] == "test_schema"
            assert "Found 2 table(s) in 'test_catalog.test_schema'" in result.message

    def test_direct_command_lists_tables_with_display_false(
        self, databricks_client_stub, temp_config
    ):
        """Direct command with display=false returns data without display flag."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "test_schema")
            databricks_client_stub.add_table(
                "test_catalog", "test_schema", "test_table"
            )

            result = handle_command(databricks_client_stub, display=False)

            assert result.success
            assert result.data.get("display") is False
            assert len(result.data.get("tables", [])) == 1
            assert result.data["tables"][0]["name"] == "test_table"

    def test_direct_command_uses_active_catalog_and_schema_when_not_specified(
        self, databricks_client_stub, temp_config
    ):
        """Direct command uses active catalog and schema when not provided."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("my_active_catalog")
            set_active_schema("my_active_schema")
            databricks_client_stub.add_catalog("my_active_catalog")
            databricks_client_stub.add_schema("my_active_catalog", "my_active_schema")
            databricks_client_stub.add_table(
                "my_active_catalog", "my_active_schema", "table_in_active"
            )

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            assert result.data["catalog_name"] == "my_active_catalog"
            assert result.data["schema_name"] == "my_active_schema"
            assert result.data["tables"][0]["name"] == "table_in_active"

    def test_direct_command_explicit_catalog_and_schema_override_active(
        self, databricks_client_stub, temp_config
    ):
        """Direct command with explicit catalog_name and schema_name overrides active settings."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("active_catalog")
            set_active_schema("active_schema")
            databricks_client_stub.add_catalog("active_catalog")
            databricks_client_stub.add_schema("active_catalog", "active_schema")
            databricks_client_stub.add_catalog("explicit_catalog")
            databricks_client_stub.add_schema("explicit_catalog", "explicit_schema")
            databricks_client_stub.add_table(
                "explicit_catalog", "explicit_schema", "explicit_table"
            )

            result = handle_command(
                databricks_client_stub,
                catalog_name="explicit_catalog",
                schema_name="explicit_schema",
                display=True,
            )

            assert result.success
            assert result.data["catalog_name"] == "explicit_catalog"
            assert result.data["schema_name"] == "explicit_schema"
            assert result.data["tables"][0]["name"] == "explicit_table"

    def test_direct_command_handles_empty_schema_gracefully(
        self, databricks_client_stub, temp_config
    ):
        """Direct command handles schema with no tables gracefully."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("empty_catalog")
            set_active_schema("empty_schema")
            databricks_client_stub.add_catalog("empty_catalog")
            databricks_client_stub.add_schema("empty_catalog", "empty_schema")
            # Don't add any tables

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            assert len(result.data.get("tables", [])) == 0
            assert result.data["total_count"] == 0
            assert (
                "No tables found in schema 'empty_catalog.empty_schema'"
                in result.message
            )

    def test_direct_command_includes_table_details(
        self, databricks_client_stub, temp_config
    ):
        """Direct command includes detailed table information when available."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "test_schema")

            # Add table with detailed properties
            columns = [
                {"name": "id", "type_text": "INT", "nullable": False},
                {"name": "name", "type_text": "STRING", "nullable": True},
            ]
            properties = {"spark.sql.statistics.numRows": "1000", "size_bytes": "10240"}

            databricks_client_stub.add_table(
                "test_catalog",
                "test_schema",
                "detailed_table",
                table_type="MANAGED",
                comment="Test table with details",
                created_at="2023-01-01T12:34:56Z",
                updated_at="2023-02-01T12:34:56Z",
                created_by="creator@example.com",
                owner="owner@example.com",
                data_source_format="DELTA",
                columns=columns,
                properties=properties,
            )

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            table = result.data["tables"][0]
            assert table["name"] == "detailed_table"
            assert table["table_type"] == "MANAGED"
            assert table["comment"] == "Test table with details"
            assert table["created_at"] == "2023-01-01T12:34:56Z"
            assert table["updated_at"] == "2023-02-01T12:34:56Z"
            assert table["created_by"] == "creator@example.com"
            assert table["owner"] == "owner@example.com"
            assert table["data_source_format"] == "DELTA"
            assert table["row_count"] == "1000"
            assert table["size_bytes"] == "10240"
            assert "columns" in table
            assert len(table["columns"]) == 2
            assert table["column_count"] == 2

    def test_direct_command_omit_columns_parameter(
        self, databricks_client_stub, temp_config
    ):
        """Direct command respects omit_columns parameter."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "test_schema")

            # Add table with columns
            columns = [
                {"name": "id", "type_text": "INT"},
                {"name": "name", "type_text": "STRING"},
            ]
            databricks_client_stub.add_table(
                "test_catalog", "test_schema", "test_table", columns=columns
            )

            # Test with omit_columns=True
            result = handle_command(
                databricks_client_stub, display=True, omit_columns=True
            )

            assert result.success
            assert "columns" not in result.data["tables"][0]
            assert "column_count" not in result.data["tables"][0]

    def test_direct_command_include_delta_metadata_parameter(
        self, databricks_client_stub, temp_config
    ):
        """Direct command passes include_delta_metadata parameter to API."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "test_schema")
            databricks_client_stub.add_table(
                "test_catalog", "test_schema", "test_table"
            )

            # Call with include_delta_metadata parameter
            result = handle_command(databricks_client_stub, include_delta_metadata=True)

            assert result.success
            # Verify the parameter was passed to the API
            assert (
                databricks_client_stub.list_tables_calls[-1][4] is True
            )  # include_delta_metadata flag

    def test_databricks_api_error_handled_gracefully(
        self, databricks_client_stub, temp_config
    ):
        """Databricks API errors are handled gracefully with helpful messages."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")

            # Store the original method and replace it with one that raises an exception
            databricks_client_stub.set_list_tables_error(
                Exception("Databricks API connection failed")
            )

            result = handle_command(databricks_client_stub, display=True)

            # Reset the error for other tests
            databricks_client_stub.set_list_tables_error(None)

            assert not result.success
            assert "Failed to list tables" in result.message
            assert "Databricks API connection failed" in result.message


class TestListTablesCommandConfiguration:
    """Test list_tables command configuration and registry integration."""

    def test_command_definition_properties(self):
        """List_tables command definition has correct configuration."""
        from chuck_data.commands.list_tables import DEFINITION

        assert DEFINITION.name == "list-tables"
        assert "tables" in DEFINITION.description.lower()
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True
        assert DEFINITION.agent_display == "conditional"

    def test_command_parameter_definitions(self):
        """List_tables command has correct parameter definitions."""
        from chuck_data.commands.list_tables import DEFINITION

        parameters = DEFINITION.parameters
        assert "display" in parameters
        assert parameters["display"]["type"] == "boolean"
        assert "catalog_name" in parameters
        assert parameters["catalog_name"]["type"] == "string"
        assert "schema_name" in parameters
        assert parameters["schema_name"]["type"] == "string"
        assert "include_delta_metadata" in parameters
        assert parameters["include_delta_metadata"]["type"] == "boolean"
        assert "omit_columns" in parameters
        assert parameters["omit_columns"]["type"] == "boolean"
        assert "include_browse" in parameters
        assert parameters["include_browse"]["type"] == "boolean"

    def test_command_aliases(self):
        """List_tables command has expected aliases."""
        from chuck_data.commands.list_tables import DEFINITION

        assert "/list-tables" in DEFINITION.tui_aliases
        assert "/tables" in DEFINITION.tui_aliases

    def test_command_display_condition(self):
        """List_tables command has correct display condition logic."""
        from chuck_data.commands.list_tables import DEFINITION

        # Should display when display=True
        assert DEFINITION.display_condition({"display": True})
        # Should not display when display=False
        assert not DEFINITION.display_condition({"display": False})
        # Should not display when display is not specified
        assert not DEFINITION.display_condition({})


class TestListTablesDisplayIntegration:
    """Test list_tables command integration with display system."""

    def test_command_result_contains_display_ready_data(
        self, databricks_client_stub, temp_config
    ):
        """List_tables command returns display-ready data structure."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("display_catalog")
            set_active_schema("display_schema")
            databricks_client_stub.add_catalog("display_catalog")
            databricks_client_stub.add_schema("display_catalog", "display_schema")
            databricks_client_stub.add_table(
                "display_catalog",
                "display_schema",
                "test_table",
                comment="Test table",
                created_at="2023-01-01T00:00:00Z",
                created_by="test.user@example.com",
                owner="table_owner",
                table_type="MANAGED",
                data_source_format="DELTA",
            )

            result = handle_command(databricks_client_stub, display=True)

            # Verify data structure is display-ready
            assert result.success
            assert isinstance(result.data, dict)
            assert "tables" in result.data
            assert isinstance(result.data["tables"], list)
            assert result.data["total_count"] == 1
            assert result.data["catalog_name"] == "display_catalog"
            assert result.data["schema_name"] == "display_schema"

            # Ensure table data has expected display fields
            table = result.data["tables"][0]
            assert "name" in table
            assert "full_name" in table
            assert "table_type" in table
            assert "comment" in table
            assert "created_at" in table
            assert "created_by" in table
            assert "owner" in table
            assert "data_source_format" in table

    def test_command_message_formatting(self, databricks_client_stub, temp_config):
        """List_tables command formats success message correctly."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("msg_catalog")
            set_active_schema("msg_schema")
            databricks_client_stub.add_catalog("msg_catalog")
            databricks_client_stub.add_schema("msg_catalog", "msg_schema")

            # Test with no tables
            result1 = handle_command(databricks_client_stub)
            assert (
                "No tables found in schema 'msg_catalog.msg_schema'" in result1.message
            )

            # Add tables and test count message
            databricks_client_stub.add_table("msg_catalog", "msg_schema", "table1")
            databricks_client_stub.add_table("msg_catalog", "msg_schema", "table2")

            result2 = handle_command(databricks_client_stub)
            assert "Found 2 table(s) in 'msg_catalog.msg_schema'" in result2.message


class TestListTablesAgentBehavior:
    """Test list_tables command agent-specific behavior."""

    def test_agent_default_behavior_without_display_parameter(
        self, databricks_client_stub, temp_config
    ):
        """Agent execution without display parameter uses default behavior (no display)."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "test_schema")
            databricks_client_stub.add_table(
                "test_catalog", "test_schema", "agent_table"
            )

            result = handle_command(databricks_client_stub)

            assert result.success
            assert result.data.get("display") is False
            assert len(result.data.get("tables", [])) == 1

    def test_agent_conditional_display_with_display_true(
        self, databricks_client_stub, temp_config
    ):
        """Agent execution with display=true triggers conditional display."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "test_schema")
            databricks_client_stub.add_table(
                "test_catalog", "test_schema", "display_table"
            )

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            assert result.data.get("display") is True
            assert len(result.data.get("tables", [])) == 1

    def test_agent_tool_executor_end_to_end_integration(
        self, databricks_client_stub, temp_config
    ):
        """Agent tool_executor integration works end-to-end."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("integration_catalog")
            set_active_schema("integration_schema")
            databricks_client_stub.add_catalog("integration_catalog")
            databricks_client_stub.add_schema(
                "integration_catalog", "integration_schema"
            )
            databricks_client_stub.add_table(
                "integration_catalog", "integration_schema", "integration_table"
            )

            result = execute_tool(
                api_client=databricks_client_stub,
                tool_name="list-tables",
                tool_args={"display": True},
            )

            # Verify agent gets proper result format
            assert "tables" in result
            assert "catalog_name" in result
            assert "schema_name" in result
            assert result["catalog_name"] == "integration_catalog"
            assert result["schema_name"] == "integration_schema"
            assert len(result["tables"]) == 1
            assert result["tables"][0]["name"] == "integration_table"

    def test_agent_with_catalog_and_schema_name_parameters(
        self, databricks_client_stub, temp_config
    ):
        """Agent execution with explicit catalog_name and schema_name parameters."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add test catalog different from active catalog
            set_active_catalog("active_catalog")
            set_active_schema("active_schema")
            databricks_client_stub.add_catalog("active_catalog")
            databricks_client_stub.add_schema("active_catalog", "active_schema")
            databricks_client_stub.add_catalog("agent_catalog")
            databricks_client_stub.add_schema("agent_catalog", "agent_schema")
            databricks_client_stub.add_table(
                "agent_catalog", "agent_schema", "agent_table"
            )

            result = execute_tool(
                api_client=databricks_client_stub,
                tool_name="list-tables",
                tool_args={
                    "catalog_name": "agent_catalog",
                    "schema_name": "agent_schema",
                },
            )

            # Verify agent respects catalog_name and schema_name parameters
            assert result["catalog_name"] == "agent_catalog"
            assert result["schema_name"] == "agent_schema"
            assert len(result["tables"]) == 1
            assert result["tables"][0]["name"] == "agent_table"

    def test_agent_callback_errors_bubble_up_as_command_errors(
        self, databricks_client_stub, temp_config
    ):
        """Agent callback failures bubble up as command errors (list-tables doesn't use callbacks)."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "test_schema")
            databricks_client_stub.add_table(
                "test_catalog", "test_schema", "callback_table"
            )

            def failing_callback(tool_name, data):
                raise Exception("Display system crashed")

            # list-tables doesn't use tool_output_callback, so this should work normally
            result = handle_command(
                databricks_client_stub,
                display=True,
                tool_output_callback=failing_callback,
            )

            # Should succeed since list-tables doesn't use callbacks
            assert result.success
            assert len(result.data.get("tables", [])) == 1


class TestListTablesEdgeCases:
    """Test edge cases and boundary conditions for list_tables command."""

    def test_command_handles_unicode_in_table_names(
        self, databricks_client_stub, temp_config
    ):
        """List_tables command handles Unicode characters in table names."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("unicode_catalog")
            set_active_schema("unicode_schema")
            databricks_client_stub.add_catalog("unicode_catalog")
            databricks_client_stub.add_schema("unicode_catalog", "unicode_schema")
            # Add tables with Unicode in name
            databricks_client_stub.add_table(
                "unicode_catalog", "unicode_schema", "数据表"
            )
            databricks_client_stub.add_table(
                "unicode_catalog", "unicode_schema", "üñîçødé_table"
            )

            result = handle_command(
                databricks_client_stub,
                catalog_name="unicode_catalog",
                schema_name="unicode_schema",
            )

            # Verify Unicode handling
            assert result.success
            assert len(result.data["tables"]) == 2
            table_names = [t["name"] for t in result.data["tables"]]
            assert "数据表" in table_names
            assert "üñîçødé_table" in table_names

    def test_command_with_tables_having_complex_metadata(
        self, databricks_client_stub, temp_config
    ):
        """List_tables command handles tables with complex metadata."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("metadata_catalog")
            set_active_schema("metadata_schema")
            databricks_client_stub.add_catalog("metadata_catalog")
            databricks_client_stub.add_schema("metadata_catalog", "metadata_schema")

            # Add table with comprehensive metadata
            columns = [
                {"name": "id", "type_text": "INT", "nullable": False},
                {"name": "name", "type_text": "STRING", "nullable": True},
                {
                    "name": "nested",
                    "type": {
                        "name": "STRUCT",
                        "fields": [
                            {"name": "field1", "type": {"name": "STRING"}},
                            {"name": "field2", "type": {"name": "INT"}},
                        ],
                    },
                },
            ]

            properties = {
                "spark.sql.statistics.numRows": "10000",
                "size_bytes": "1048576",
                "delta.minReaderVersion": "1",
                "delta.minWriterVersion": "2",
                "spark.sql.sources.provider": "delta",
                "created_time": "1640995200000",
            }

            databricks_client_stub.add_table(
                "metadata_catalog",
                "metadata_schema",
                "metadata_table",
                comment="Test table with nested properties",
                created_at="2023-01-01T00:00:00Z",
                updated_at="2023-02-01T00:00:00Z",
                created_by="test.user@example.com",
                owner="table_owner",
                table_type="MANAGED",
                data_source_format="DELTA",
                full_name="metadata_catalog.metadata_schema.metadata_table",
                securable_type="TABLE",
                storage_location="dbfs:/path/to/table",
                columns=columns,
                properties=properties,
            )

            result = handle_command(
                databricks_client_stub,
                catalog_name="metadata_catalog",
                schema_name="metadata_schema",
            )

            # Verify complex metadata handling
            assert result.success
            table = result.data["tables"][0]
            assert table["name"] == "metadata_table"
            assert (
                table["full_name"] == "metadata_catalog.metadata_schema.metadata_table"
            )
            assert table["comment"] == "Test table with nested properties"
            assert table["created_by"] == "test.user@example.com"
            assert table["owner"] == "table_owner"
            assert table["data_source_format"] == "DELTA"
            assert table["table_type"] == "MANAGED"
            assert table["row_count"] == "10000"
            assert table["size_bytes"] == "1048576"
            assert "columns" in table
            assert len(table["columns"]) == 3
            assert table["columns"][2]["name"] == "nested"

    def test_command_handles_many_tables_efficiently(
        self, databricks_client_stub, temp_config
    ):
        """List_tables command handles large numbers of tables efficiently."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("large_catalog")
            set_active_schema("large_schema")
            databricks_client_stub.add_catalog("large_catalog")
            databricks_client_stub.add_schema("large_catalog", "large_schema")

            # Add many tables
            for i in range(100):
                databricks_client_stub.add_table(
                    "large_catalog", "large_schema", f"table_{i}"
                )

            result = handle_command(
                databricks_client_stub,
                catalog_name="large_catalog",
                schema_name="large_schema",
            )

            # Verify efficient handling
            assert result.success
            assert len(result.data["tables"]) == 100
            assert result.data["total_count"] == 100
