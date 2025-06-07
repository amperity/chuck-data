"""
Tests for list_schemas command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
"""

from unittest.mock import patch

from chuck_data.commands.list_schemas import handle_command
from chuck_data.config import set_active_catalog, set_active_schema
from chuck_data.agent.tool_executor import execute_tool


# ===== Parameter Validation Tests =====


def test_missing_client_returns_error(temp_config):
    """Missing Databricks client returns clear error."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_command(None)

        assert not result.success
        assert "No Databricks client available" in result.message


def test_no_active_catalog_and_no_catalog_name_returns_error(
    databricks_client_stub, temp_config
):
    """No active catalog and no catalog_name parameter returns clear error."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_command(databricks_client_stub)

        assert not result.success
        assert "No catalog specified and no active catalog selected" in result.message


# ===== Direct Command Tests =====


def test_direct_command_lists_schemas_with_display_true(
    databricks_client_stub, temp_config
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
    databricks_client_stub, temp_config
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
    databricks_client_stub, temp_config
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
    databricks_client_stub, temp_config
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
    databricks_client_stub, temp_config
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
    databricks_client_stub, temp_config
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
    databricks_client_stub, temp_config
):
    """Direct command supports pagination with max_results and page_token."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_active_catalog("test_catalog")
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "schema_1")
        databricks_client_stub.add_schema("test_catalog", "schema_2")

        result = handle_command(
            databricks_client_stub, display=True, max_results=1, page_token="test_token"
        )

        assert result.success
        # The stub doesn't actually implement pagination, but command should accept the parameters
        assert result.data.get("schemas") is not None


def test_databricks_api_error_handled_gracefully(databricks_client_stub, temp_config):
    """Databricks API errors are handled gracefully with helpful messages."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_active_catalog("test_catalog")
        # Create a stub that raises an exception during list_schemas
        from tests.fixtures.databricks.client import DatabricksClientStub

        class FailingStub(DatabricksClientStub):
            def list_schemas(self, catalog_name, **kwargs):
                raise Exception("Databricks API connection failed")

        failing_stub = FailingStub()

        result = handle_command(failing_stub, display=True)

        assert not result.success
        assert "Failed to list schemas" in result.message


# ===== Agent Behavioral Tests =====


def test_agent_default_behavior_without_display_parameter(
    databricks_client_stub, temp_config
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
    databricks_client_stub, temp_config
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
    databricks_client_stub, temp_config
):
    """Agent tool_executor integration works end-to-end."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_active_catalog("integration_catalog")
        databricks_client_stub.add_catalog("integration_catalog")
        databricks_client_stub.add_schema("integration_catalog", "integration_schema")

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


def test_agent_callback_errors_bubble_up_as_command_errors(
    databricks_client_stub, temp_config
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
            databricks_client_stub, display=True, tool_output_callback=failing_callback
        )

        # Should succeed since list-schemas doesn't use callbacks
        assert result.success
        assert len(result.data.get("schemas", [])) == 1


def test_agent_handles_multiple_schemas_with_metadata(
    databricks_client_stub, temp_config
):
    """Agent execution handles multiple schemas with complete metadata."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_active_catalog("metadata_catalog")
        databricks_client_stub.add_catalog("metadata_catalog")
        databricks_client_stub.add_schema(
            "metadata_catalog",
            "schema_with_metadata",
            comment="Test schema",
            owner="test_user",
        )
        databricks_client_stub.add_schema("metadata_catalog", "simple_schema")

        result = handle_command(databricks_client_stub, display=True)

        assert result.success
        assert len(result.data["schemas"]) == 2

        # Check that metadata is preserved
        schema_with_meta = next(
            s for s in result.data["schemas"] if s["name"] == "schema_with_metadata"
        )
        assert schema_with_meta["comment"] == "Test schema"
        assert schema_with_meta["owner"] == "test_user"

        simple_schema = next(
            s for s in result.data["schemas"] if s["name"] == "simple_schema"
        )
        assert simple_schema["comment"] == ""
        assert simple_schema["owner"] == ""
