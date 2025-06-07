"""
Tests for schema_selection command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
"""

from unittest.mock import patch

from chuck_data.commands.schema_selection import handle_command
from chuck_data.config import get_active_schema, set_active_catalog
from chuck_data.agent.tool_executor import execute_tool


# ===== Parameter Validation Tests =====


def test_missing_schema_parameter_returns_error(databricks_client_stub, temp_config):
    """Missing schema parameter returns clear error."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_command(databricks_client_stub)

        assert not result.success
        assert "schema parameter is required" in result.message


def test_missing_client_returns_error(temp_config):
    """Missing Databricks client returns clear error."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_command(None, schema="test_schema")

        assert not result.success
        assert "No API client available to verify schema" in result.message


def test_no_active_catalog_returns_error(databricks_client_stub, temp_config):
    """No active catalog returns clear error."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_command(databricks_client_stub, schema="test_schema")

        assert not result.success
        assert "No active catalog selected" in result.message


# ===== Direct Command Tests =====


def test_direct_command_selects_existing_schema_by_exact_name(
    databricks_client_stub, temp_config
):
    """Direct command can select existing schema by exact name (no progress steps)."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_active_catalog("test_catalog")
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "production_schema")

        result = handle_command(databricks_client_stub, schema="production_schema")

        # Command succeeds with schema selection
        assert result.success
        assert "Active schema is now set to 'production_schema'" in result.message
        assert "in catalog 'test_catalog'" in result.message

        # Schema becomes active
        assert get_active_schema() == "production_schema"

        # Agent data format is provided
        assert result.data["schema_name"] == "production_schema"
        assert result.data["catalog_name"] == "test_catalog"


def test_direct_command_selects_schema_with_fuzzy_matching(
    databricks_client_stub, temp_config
):
    """Direct command can fuzzy match partial schema names."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_active_catalog("test_catalog")
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema(
            "test_catalog", "development_environment_schema"
        )

        result = handle_command(databricks_client_stub, schema="dev")

        # Command succeeds with fuzzy matched schema
        assert result.success
        assert (
            "Active schema is now set to 'development_environment_schema'"
            in result.message
        )
        assert get_active_schema() == "development_environment_schema"


def test_direct_command_failure_shows_available_schemas(
    databricks_client_stub, temp_config
):
    """Direct command failure shows error with available schemas list."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_active_catalog("test_catalog")
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "xyz_schema")
        databricks_client_stub.add_schema("test_catalog", "abc_schema")

        result = handle_command(
            databricks_client_stub, schema="qwerty_completely_different"
        )

        # Command fails with helpful error
        assert not result.success
        assert (
            "No schema found matching 'qwerty_completely_different'" in result.message
        )
        assert "Available schemas: xyz_schema, abc_schema" in result.message


def test_direct_command_handles_empty_catalog_gracefully(
    databricks_client_stub, temp_config
):
    """Direct command handles catalog with no schemas gracefully."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_active_catalog("empty_catalog")
        databricks_client_stub.add_catalog("empty_catalog")

        result = handle_command(databricks_client_stub, schema="any_schema")

        assert not result.success
        assert "No schemas found in catalog 'empty_catalog'" in result.message


def test_databricks_api_error_handled_gracefully(databricks_client_stub, temp_config):
    """Databricks API errors are handled gracefully with helpful messages."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_active_catalog("test_catalog")
        # Create a stub that raises an exception during list_schemas
        from tests.fixtures.databricks.client import DatabricksClientStub

        class FailingStub(DatabricksClientStub):
            def list_schemas(
                self,
                catalog_name,
                include_browse=False,
                max_results=None,
                page_token=None,
                **kwargs,
            ):
                raise Exception("Failed to list schemas")

        failing_stub = FailingStub()
        failing_stub.add_catalog("test_catalog")

        result = handle_command(failing_stub, schema="test_schema")

        assert not result.success
        assert "Failed to list schemas" in result.message


# ===== Agent Behavioral Tests =====


def test_agent_exact_match_shows_no_progress_steps(databricks_client_stub, temp_config):
    """Agent exact match shows no progress steps (direct lookup succeeds)."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_active_catalog("test_catalog")
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "exact_match_schema")

        # Capture progress during agent execution
        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append(f"→ Setting schema: ({data['step']})")

        result = handle_command(
            databricks_client_stub,
            schema="exact_match_schema",
            tool_output_callback=capture_progress,
        )

        # Command succeeds
        assert result.success
        assert get_active_schema() == "exact_match_schema"

        # No progress steps since direct lookup succeeded
        assert len(progress_steps) == 0


def test_agent_fuzzy_match_shows_multiple_progress_steps(
    databricks_client_stub, temp_config
):
    """Agent fuzzy match shows multiple progress steps (search required)."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_active_catalog("test_catalog")
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "production_data_schema")

        # Force search path by using partial name
        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append(f"→ Setting schema: ({data['step']})")

        result = handle_command(
            databricks_client_stub, schema="prod", tool_output_callback=capture_progress
        )

        # Command succeeds
        assert result.success
        assert get_active_schema() == "production_data_schema"

        # Should have progress steps during search
        assert len(progress_steps) >= 1
        assert any(
            "Looking for schema matching 'prod'" in step for step in progress_steps
        )
        assert any(
            "Selecting 'production_data_schema'" in step for step in progress_steps
        )


def test_agent_shows_progress_before_failure(databricks_client_stub, temp_config):
    """Agent shows progress before failure when no match is found."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_active_catalog("test_catalog")
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "existing_schema")

        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append(f"→ Setting schema: ({data['step']})")

        result = handle_command(
            databricks_client_stub,
            schema="qwerty_no_match",
            tool_output_callback=capture_progress,
        )

        # Command fails
        assert not result.success
        assert "No schema found matching 'qwerty_no_match'" in result.message

        # Should show search attempt before failure
        assert len(progress_steps) == 1
        assert "Looking for schema matching 'qwerty_no_match'" in progress_steps[0]


def test_agent_callback_errors_bubble_up_as_command_errors(
    databricks_client_stub, temp_config
):
    """Agent callback failures bubble up as command errors (current behavior)."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_active_catalog("test_catalog")
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "trigger_search_schema")

        def failing_callback(tool_name, data):
            raise Exception("Display system crashed")

        result = handle_command(
            databricks_client_stub,
            schema="trigger",  # Force search path which will trigger callback
            tool_output_callback=failing_callback,
        )

        # Document current behavior - callback errors bubble up
        assert not result.success
        assert "Display system crashed" in result.message


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
            tool_name="select-schema",
            tool_args={"schema": "integration_schema"},
        )

        # Verify agent gets proper result format
        assert "schema_name" in result
        assert "catalog_name" in result
        assert result["schema_name"] == "integration_schema"
        assert result["catalog_name"] == "integration_catalog"

        # Verify state actually changed
        assert get_active_schema() == "integration_schema"
