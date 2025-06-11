"""
Tests for list_schemas command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
These tests verify what users see when they interact with the list_schemas command,
both directly and when an agent uses the list-schemas tool.
"""

import pytest
from unittest.mock import patch

from chuck_data.commands.list_schemas import handle_command
from chuck_data.config import set_active_catalog, set_active_schema
from chuck_data.agent.tool_executor import execute_tool


class TestListSchemasParameterValidation:
    """Test parameter validation for list_schemas command."""

    def test_missing_client_returns_error(self, temp_config):
        """Missing Databricks client returns clear error."""
        with patch("chuck_data.config._config_manager", temp_config):
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


class TestDirectListSchemasCommand:
    """Test direct list_schemas command execution."""

    def test_direct_command_lists_schemas_with_display_true(
        self, databricks_client_stub, temp_config
    ):
        """Direct command with display=true returns schemas with display flag set."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "production_schema")
            databricks_client_stub.add_schema("test_catalog", "development_schema")

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            assert result.data.get("display") is True
            assert len(result.data.get("schemas", [])) == 2
            assert result.data["catalog_name"] == "test_catalog"
            assert "Found 2 schema(s) in catalog 'test_catalog'" in result.message

    def test_direct_command_lists_schemas_with_display_false(
        self, databricks_client_stub, temp_config
    ):
        """Direct command with display=false returns data without display flag."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "test_schema")

            result = handle_command(databricks_client_stub, display=False)

            assert result.success
            assert result.data.get("display") is False
            assert len(result.data.get("schemas", [])) == 1
            assert result.data["schemas"][0]["name"] == "test_schema"

    def test_direct_command_uses_active_catalog_when_not_specified(
        self, databricks_client_stub, temp_config
    ):
        """Direct command uses active catalog when catalog_name not provided."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("my_active_catalog")
            databricks_client_stub.add_catalog("my_active_catalog")
            databricks_client_stub.add_schema("my_active_catalog", "schema_in_active")

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            assert result.data["catalog_name"] == "my_active_catalog"
            assert result.data["schemas"][0]["name"] == "schema_in_active"

    def test_direct_command_explicit_catalog_overrides_active(
        self, databricks_client_stub, temp_config
    ):
        """Direct command with explicit catalog_name overrides active catalog."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("active_catalog")
            databricks_client_stub.add_catalog("active_catalog")
            databricks_client_stub.add_catalog("explicit_catalog")
            databricks_client_stub.add_schema("explicit_catalog", "explicit_schema")

            result = handle_command(
                databricks_client_stub, catalog_name="explicit_catalog", display=True
            )

            assert result.success
            assert result.data["catalog_name"] == "explicit_catalog"
            assert result.data["schemas"][0]["name"] == "explicit_schema"

    def test_direct_command_handles_empty_catalog_gracefully(
        self, databricks_client_stub, temp_config
    ):
        """Direct command handles catalog with no schemas gracefully."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("empty_catalog")
            databricks_client_stub.add_catalog("empty_catalog")

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            assert len(result.data.get("schemas", [])) == 0
            assert result.data["total_count"] == 0
            assert "No schemas found in catalog 'empty_catalog'" in result.message

    def test_direct_command_includes_current_schema_highlighting(
        self, databricks_client_stub, temp_config
    ):
        """Direct command includes current schema for highlighting purposes."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            set_active_schema("current_schema")
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "current_schema")
            databricks_client_stub.add_schema("test_catalog", "other_schema")

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            assert result.data["current_schema"] == "current_schema"
            assert len(result.data["schemas"]) == 2

    def test_direct_command_supports_pagination_parameters(
        self, databricks_client_stub, temp_config
    ):
        """Direct command supports pagination with max_results and page_token."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "schema_1")
            databricks_client_stub.add_schema("test_catalog", "schema_2")

            result = handle_command(
                databricks_client_stub,
                display=True,
                max_results=1,
                page_token="test_token",
            )

            assert result.success
            # The stub doesn't actually implement pagination, but command should accept the parameters
            assert result.data.get("schemas") is not None

    def test_direct_command_passes_include_browse_parameter(
        self, databricks_client_stub, temp_config
    ):
        """Direct command passes include_browse parameter to API."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            databricks_client_stub.add_catalog("test_catalog")

            # Call with include_browse parameter
            result = handle_command(
                databricks_client_stub, catalog_name="test_catalog", include_browse=True
            )

            assert result.success
            # Verify the parameter was passed to the API
            assert (
                databricks_client_stub.list_schemas_calls[-1][1] is True
            )  # Include browse flag

    def test_direct_command_handles_nonexistent_catalog(
        self, databricks_client_stub, temp_config
    ):
        """Direct command handles nonexistent catalog gracefully."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Use a stub that raises exception for list_schemas on nonexistent catalog
            original_list_schemas = databricks_client_stub.list_schemas

            def list_schemas_failing(catalog_name, *args, **kwargs):
                if catalog_name == "nonexistent_catalog":
                    raise Exception(f"Catalog {catalog_name} does not exist")
                return original_list_schemas(catalog_name, *args, **kwargs)

            databricks_client_stub.list_schemas = list_schemas_failing

            # Attempt to list schemas in nonexistent catalog
            result = handle_command(
                databricks_client_stub, catalog_name="nonexistent_catalog"
            )

            # Reset the method
            databricks_client_stub.list_schemas = original_list_schemas

            # Should return an error
            assert not result.success
            assert (
                "nonexistent_catalog" in result.message.lower()
                or "catalog" in result.message.lower()
            )

    def test_databricks_api_error_handled_gracefully(
        self, databricks_client_stub, temp_config
    ):
        """Databricks API errors are handled gracefully with helpful messages."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")

            # Store the original method and replace it with one that raises an exception
            original_method = databricks_client_stub.list_schemas

            def failing_list_schemas(*args, **kwargs):
                raise Exception("Databricks API connection failed")

            databricks_client_stub.list_schemas = failing_list_schemas

            try:
                result = handle_command(databricks_client_stub, display=True)
            finally:
                # Restore the original method
                databricks_client_stub.list_schemas = original_method

            assert not result.success
            assert "Failed to list schemas" in result.message
            assert "Databricks API connection failed" in result.message


class TestListSchemasCommandConfiguration:
    """Test list_schemas command configuration and registry integration."""

    def test_command_definition_properties(self):
        """List_schemas command definition has correct configuration."""
        from chuck_data.commands.list_schemas import DEFINITION

        assert DEFINITION.name == "list-schemas"
        assert "schemas" in DEFINITION.description.lower()
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True
        assert DEFINITION.agent_display == "conditional"

    def test_command_parameter_definitions(self):
        """List_schemas command has correct parameter definitions."""
        from chuck_data.commands.list_schemas import DEFINITION

        parameters = DEFINITION.parameters
        assert "display" in parameters
        assert parameters["display"]["type"] == "boolean"
        assert "catalog_name" in parameters
        assert parameters["catalog_name"]["type"] == "string"
        assert "include_browse" in parameters
        assert parameters["include_browse"]["type"] == "boolean"
        assert "max_results" in parameters
        assert "page_token" in parameters

    def test_command_aliases(self):
        """List_schemas command has expected aliases."""
        from chuck_data.commands.list_schemas import DEFINITION

        assert "/list-schemas" in DEFINITION.tui_aliases
        assert "/schemas" in DEFINITION.tui_aliases

    def test_command_display_condition(self):
        """List_schemas command has correct display condition logic."""
        from chuck_data.commands.list_schemas import DEFINITION

        # Should display when display=True
        assert DEFINITION.display_condition({"display": True})
        # Should not display when display=False
        assert not DEFINITION.display_condition({"display": False})
        # Should not display when display is not specified
        assert not DEFINITION.display_condition({})


class TestListSchemasDisplayIntegration:
    """Test list_schemas command integration with display system."""

    def test_command_result_contains_display_ready_data(
        self, databricks_client_stub, temp_config
    ):
        """List_schemas command returns display-ready data structure."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("display_catalog")
            databricks_client_stub.add_catalog("display_catalog")
            databricks_client_stub.add_schema(
                "display_catalog",
                "test_schema",
                comment="Test schema",
                created_at=1640995200000,
                created_by="test.user@example.com",
                owner="schema_owner",
            )

            result = handle_command(databricks_client_stub, display=True)

            # Verify data structure is display-ready
            assert result.success
            assert isinstance(result.data, dict)
            assert "schemas" in result.data
            assert isinstance(result.data["schemas"], list)
            assert result.data["total_count"] == 1
            assert result.data["catalog_name"] == "display_catalog"

            # Ensure schema data has expected display fields
            schema = result.data["schemas"][0]
            assert "name" in schema
            assert "full_name" in schema
            assert "comment" in schema
            assert "created_at" in schema
            assert "created_by" in schema
            assert "owner" in schema

    def test_command_message_formatting(self, databricks_client_stub, temp_config):
        """List_schemas command formats success message correctly."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("msg_catalog")
            databricks_client_stub.add_catalog("msg_catalog")

            # Test with no schemas
            result1 = handle_command(databricks_client_stub)
            assert "No schemas found in catalog 'msg_catalog'" in result1.message

            # Add schemas and test count message
            databricks_client_stub.add_schema("msg_catalog", "schema1")
            databricks_client_stub.add_schema("msg_catalog", "schema2")

            result2 = handle_command(databricks_client_stub)
            assert "Found 2 schema(s) in catalog 'msg_catalog'" in result2.message

    def test_command_includes_pagination_token_in_message(
        self, databricks_client_stub, temp_config
    ):
        """List_schemas command includes pagination token in message when available."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("token_catalog")
            databricks_client_stub.add_catalog("token_catalog")
            databricks_client_stub.add_schema("token_catalog", "schema1")

            # Mock pagination token in response
            original_list_schemas = databricks_client_stub.list_schemas

            def mock_list_schemas(*args, **kwargs):
                result = original_list_schemas(*args, **kwargs)
                result["next_page_token"] = "next_page_123"
                return result

            databricks_client_stub.list_schemas = mock_list_schemas

            result = handle_command(databricks_client_stub)

            assert result.success
            assert "next_page_token" in result.data
            assert result.data["next_page_token"] == "next_page_123"
            assert (
                "More schemas available with page token: next_page_123"
                in result.message
            )


class TestListSchemasAgentBehavior:
    """Test list_schemas command agent-specific behavior."""

    def test_agent_default_behavior_without_display_parameter(
        self, databricks_client_stub, temp_config
    ):
        """Agent execution without display parameter uses default behavior (no display)."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "agent_schema")

            result = handle_command(databricks_client_stub)

            assert result.success
            assert result.data.get("display") is False
            assert len(result.data.get("schemas", [])) == 1

    def test_agent_conditional_display_with_display_true(
        self, databricks_client_stub, temp_config
    ):
        """Agent execution with display=true triggers conditional display."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "display_schema")

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            assert result.data.get("display") is True
            assert len(result.data.get("schemas", [])) == 1

    def test_agent_tool_executor_end_to_end_integration(
        self, databricks_client_stub, temp_config
    ):
        """Agent tool_executor integration works end-to-end."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("integration_catalog")
            databricks_client_stub.add_catalog("integration_catalog")
            databricks_client_stub.add_schema(
                "integration_catalog", "integration_schema"
            )

            result = execute_tool(
                api_client=databricks_client_stub,
                tool_name="list-schemas",
                tool_args={"display": True},
            )

            # Verify agent gets proper result format
            assert "schemas" in result
            assert "catalog_name" in result
            assert result["catalog_name"] == "integration_catalog"
            assert len(result["schemas"]) == 1
            assert result["schemas"][0]["name"] == "integration_schema"

    def test_agent_with_catalog_name_parameter(
        self, databricks_client_stub, temp_config
    ):
        """Agent execution with explicit catalog_name parameter."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add test catalog different from active catalog
            set_active_catalog("active_catalog")
            databricks_client_stub.add_catalog("active_catalog")
            databricks_client_stub.add_catalog("agent_catalog")
            databricks_client_stub.add_schema("agent_catalog", "agent_schema")

            result = execute_tool(
                api_client=databricks_client_stub,
                tool_name="list-schemas",
                tool_args={"catalog_name": "agent_catalog"},
            )

            # Verify agent respects catalog_name parameter
            assert result["catalog_name"] == "agent_catalog"
            assert len(result["schemas"]) == 1
            assert result["schemas"][0]["name"] == "agent_schema"

    def test_agent_callback_errors_bubble_up_as_command_errors(
        self, databricks_client_stub, temp_config
    ):
        """Agent callback failures bubble up as command errors (list-schemas doesn't use callbacks)."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "callback_schema")

            def failing_callback(tool_name, data):
                raise Exception("Display system crashed")

            # list-schemas doesn't use tool_output_callback, so this should work normally
            result = handle_command(
                databricks_client_stub,
                display=True,
                tool_output_callback=failing_callback,
            )

            # Should succeed since list-schemas doesn't use callbacks
            assert result.success
            assert len(result.data.get("schemas", [])) == 1


class TestListSchemasEdgeCases:
    """Test edge cases and boundary conditions for list_schemas command."""

    def test_command_handles_unicode_in_schema_names(
        self, databricks_client_stub, temp_config
    ):
        """List_schemas command handles Unicode characters in schema names."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("unicode_catalog")
            databricks_client_stub.add_catalog("unicode_catalog")
            # Add schemas with Unicode in name
            databricks_client_stub.add_schema("unicode_catalog", "数据架构")
            databricks_client_stub.add_schema("unicode_catalog", "üñîçødé_schema")

            result = handle_command(
                databricks_client_stub, catalog_name="unicode_catalog"
            )

            # Verify Unicode handling
            assert result.success
            assert len(result.data["schemas"]) == 2
            schema_names = [s["name"] for s in result.data["schemas"]]
            assert "数据架构" in schema_names
            assert "üñîçødé_schema" in schema_names

    def test_command_with_schemas_having_complex_metadata(
        self, databricks_client_stub, temp_config
    ):
        """List_schemas command handles schemas with complex metadata."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("metadata_catalog")
            databricks_client_stub.add_catalog("metadata_catalog")

            # Add schema with comprehensive metadata
            databricks_client_stub.add_schema(
                "metadata_catalog",
                "metadata_schema",
                comment="Test schema with nested properties",
                created_at=1640995200000,
                created_by="test.user@example.com",
                owner="schema_owner",
                full_name="metadata_catalog.metadata_schema",
                securable_type="SCHEMA",
                storage_location="dbfs:/path/to/schema",
                storage_root="dbfs:/path",
                properties={"prop1": "value1", "prop2": "value2"},
            )

            result = handle_command(
                databricks_client_stub, catalog_name="metadata_catalog"
            )

            # Verify complex metadata handling
            assert result.success
            schema = result.data["schemas"][0]
            assert schema["name"] == "metadata_schema"
            assert schema["full_name"] == "metadata_catalog.metadata_schema"
            assert schema["comment"] == "Test schema with nested properties"
            assert schema["created_by"] == "test.user@example.com"
            assert schema["owner"] == "schema_owner"

    def test_command_handles_many_schemas_efficiently(
        self, databricks_client_stub, temp_config
    ):
        """List_schemas command handles large numbers of schemas efficiently."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("large_catalog")
            databricks_client_stub.add_catalog("large_catalog")

            # Add many schemas
            for i in range(100):
                databricks_client_stub.add_schema("large_catalog", f"schema_{i}")

            result = handle_command(
                databricks_client_stub, catalog_name="large_catalog"
            )

            # Verify efficient handling
            assert result.success
            assert len(result.data["schemas"]) == 100
            assert result.data["total_count"] == 100
