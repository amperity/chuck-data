"""
Tests for warehouse_selection command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
Tests cover both direct command execution and agent interaction with tool_output_callback.
"""

from unittest.mock import patch

from chuck_data.commands.warehouse_selection import handle_command
from chuck_data.config import get_warehouse_id


# Parameter validation tests (universal)
def test_missing_warehouse_parameter(databricks_client_stub, temp_config):
    """Test handling when warehouse parameter is not provided."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_command(databricks_client_stub)
        assert not result.success
        assert "warehouse parameter is required" in result.message


# Direct command tests (no tool_output_callback)
def test_direct_command_selects_existing_warehouse_by_id(
    databricks_client_stub, temp_config
):
    """Direct command successfully selects warehouse by ID."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Set up warehouse in stub
        databricks_client_stub.add_warehouse(
            name="Test Warehouse", state="RUNNING", size="2X-Small"
        )
        # The warehouse_id should be "warehouse_0" based on the stub implementation
        warehouse_id = "warehouse_0"

        # Call function with warehouse ID (no tool_output_callback)
        result = handle_command(databricks_client_stub, warehouse=warehouse_id)

        # Verify behavioral outcome
        assert result.success
        assert "Active SQL warehouse is now set to 'Test Warehouse'" in result.message
        assert f"(ID: {warehouse_id}" in result.message
        assert "State: RUNNING" in result.message
        assert result.data["warehouse_id"] == warehouse_id
        assert result.data["warehouse_name"] == "Test Warehouse"
        assert result.data["state"] == "RUNNING"

        # Verify state change
        assert get_warehouse_id() == warehouse_id


def test_direct_command_failure_shows_helpful_error(
    databricks_client_stub, temp_config
):
    """Direct command failure shows error with available warehouses."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Add a warehouse to stub but call with different name
        databricks_client_stub.add_warehouse(
            name="Production Warehouse", state="RUNNING", size="2X-Small"
        )

        # Call function with non-existent warehouse (no tool_output_callback)
        result = handle_command(
            databricks_client_stub, warehouse="xyz-completely-different-name"
        )

        # Verify helpful error behavior
        assert not result.success
        assert (
            "No warehouse found matching 'xyz-completely-different-name'"
            in result.message
        )
        assert "Available warehouses: Production Warehouse" in result.message


def test_direct_command_no_client_returns_error(temp_config):
    """Direct command with no client returns error."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Call function with no client
        result = handle_command(None, warehouse="abc123")

        # Verify error behavior
        assert not result.success
        assert "No API client available to verify warehouse" in result.message


def test_direct_command_api_errors_handled_gracefully(temp_config):
    """Direct command handles API errors gracefully."""
    from tests.fixtures.databricks.client import DatabricksClientStub

    with patch("chuck_data.config._config_manager", temp_config):
        # Create a stub that raises an exception during warehouse verification
        class FailingStub(DatabricksClientStub):
            def get_warehouse(self, warehouse_id):
                raise Exception("Failed to set warehouse")

            def list_warehouses(self, **kwargs):
                raise Exception("Failed to list warehouses")

        failing_stub = FailingStub()

        # Call function (no tool_output_callback)
        result = handle_command(failing_stub, warehouse="abc123")

        # Verify graceful error handling
        assert not result.success
        assert "Failed to list warehouses" in result.message


def test_direct_command_selects_warehouse_by_name(databricks_client_stub, temp_config):
    """Direct command successfully selects warehouse by exact name."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Set up warehouse in stub
        databricks_client_stub.add_warehouse(
            name="Test Warehouse", state="RUNNING", size="2X-Small"
        )

        # Call function with warehouse name (no tool_output_callback)
        result = handle_command(databricks_client_stub, warehouse="Test Warehouse")

        # Verify behavioral outcome
        assert result.success
        assert "Active SQL warehouse is now set to 'Test Warehouse'" in result.message
        assert result.data["warehouse_name"] == "Test Warehouse"


def test_direct_command_fuzzy_matching_succeeds(databricks_client_stub, temp_config):
    """Direct command successfully performs fuzzy name matching."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Set up warehouse in stub
        databricks_client_stub.add_warehouse(
            name="Starter Warehouse", state="RUNNING", size="2X-Small"
        )

        # Call function with partial name match (no tool_output_callback)
        result = handle_command(databricks_client_stub, warehouse="Starter")

        # Verify fuzzy matching behavior
        assert result.success
        assert (
            "Active SQL warehouse is now set to 'Starter Warehouse'" in result.message
        )
        assert result.data["warehouse_name"] == "Starter Warehouse"


# Agent-specific behavioral tests
def test_agent_exact_match_shows_no_progress_steps(databricks_client_stub, temp_config):
    """Agent execution with exact ID match shows no progress steps."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Setup test data
        databricks_client_stub.add_warehouse(
            name="Production Warehouse", state="RUNNING", size="2X-Small"
        )
        warehouse_id = "warehouse_0"

        # Capture progress during agent execution
        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append(f"→ Setting warehouse: ({data['step']})")

        # Execute with tool_output_callback
        result = handle_command(
            databricks_client_stub,
            warehouse=warehouse_id,
            tool_output_callback=capture_progress,
        )

        # Verify command success
        assert result.success
        assert get_warehouse_id() == warehouse_id

        # Verify no progress steps (direct ID lookup succeeds)
        assert len(progress_steps) == 0


def test_agent_exact_name_match_shows_progress_step(
    databricks_client_stub, temp_config
):
    """Agent execution with exact name match shows progress step."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Setup test data
        databricks_client_stub.add_warehouse(
            name="Production Warehouse", state="RUNNING", size="2X-Small"
        )

        # Force name matching by overriding get_warehouse to fail
        original_get_warehouse = databricks_client_stub.get_warehouse
        databricks_client_stub.get_warehouse = lambda name: None

        # Capture progress during agent execution
        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append(f"→ Setting warehouse: ({data['step']})")

        # Execute with tool_output_callback
        result = handle_command(
            databricks_client_stub,
            warehouse="Production Warehouse",
            tool_output_callback=capture_progress,
        )

        # Restore original method
        databricks_client_stub.get_warehouse = original_get_warehouse

        # Verify command success
        assert result.success
        assert result.data["warehouse_name"] == "Production Warehouse"

        # Verify progress behavior
        assert len(progress_steps) >= 1
        assert any(
            "Found warehouse 'Production Warehouse'" in step for step in progress_steps
        )


def test_agent_fuzzy_match_shows_multiple_progress_steps(
    databricks_client_stub, temp_config
):
    """Agent execution with fuzzy matching shows multiple progress steps."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Setup test data
        databricks_client_stub.add_warehouse(
            name="Production Data Warehouse", state="RUNNING", size="2X-Small"
        )

        # Force name matching by overriding get_warehouse to fail
        original_get_warehouse = databricks_client_stub.get_warehouse
        databricks_client_stub.get_warehouse = lambda name: None

        # Capture progress during agent execution
        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append(f"→ Setting warehouse: ({data['step']})")

        # Execute with tool_output_callback (fuzzy match: "prod" -> "Production Data Warehouse")
        result = handle_command(
            databricks_client_stub,
            warehouse="prod",
            tool_output_callback=capture_progress,
        )

        # Restore original method
        databricks_client_stub.get_warehouse = original_get_warehouse

        # Verify command success
        assert result.success
        assert result.data["warehouse_name"] == "Production Data Warehouse"

        # Verify progress behavior (should have 2 progress steps)
        assert len(progress_steps) == 2
        assert any(
            "Looking for warehouse matching 'prod'" in step for step in progress_steps
        )
        assert any(
            "Selecting 'Production Data Warehouse'" in step for step in progress_steps
        )


def test_agent_shows_progress_before_failure(databricks_client_stub, temp_config):
    """Agent execution shows progress steps before failure."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Setup test data
        databricks_client_stub.add_warehouse(
            name="Available Warehouse", state="RUNNING", size="2X-Small"
        )

        # Force name matching by overriding get_warehouse to fail
        original_get_warehouse = databricks_client_stub.get_warehouse
        databricks_client_stub.get_warehouse = lambda name: None

        # Capture progress during agent execution
        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append(f"→ Setting warehouse: ({data['step']})")

        # Execute with tool_output_callback
        result = handle_command(
            databricks_client_stub,
            warehouse="nonexistent",
            tool_output_callback=capture_progress,
        )

        # Restore original method
        databricks_client_stub.get_warehouse = original_get_warehouse

        # Verify command failure
        assert not result.success
        assert "No warehouse found matching 'nonexistent'" in result.message
        assert "Available warehouses: Available Warehouse" in result.message

        # Verify progress shown before failure
        assert len(progress_steps) == 1
        assert "Looking for warehouse matching 'nonexistent'" in progress_steps[0]


def test_agent_callback_errors_bubble_up_as_command_errors(
    databricks_client_stub, temp_config
):
    """Agent callback failures bubble up as command errors (current behavior)."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Setup test data
        databricks_client_stub.add_warehouse(
            name="Test Warehouse", state="RUNNING", size="2X-Small"
        )

        # Force name matching to trigger callback usage
        original_get_warehouse = databricks_client_stub.get_warehouse
        databricks_client_stub.get_warehouse = lambda name: None

        def failing_callback(tool_name, data):
            raise Exception("Display system crashed")

        # Execute with failing callback
        result = handle_command(
            databricks_client_stub,
            warehouse="Test Warehouse",
            tool_output_callback=failing_callback,
        )

        # Restore original method
        databricks_client_stub.get_warehouse = original_get_warehouse

        # Document current behavior - callback errors bubble up
        assert not result.success
        assert "Display system crashed" in result.message


def test_agent_tool_executor_end_to_end_integration(
    databricks_client_stub, temp_config
):
    """Agent tool_executor integration works end-to-end."""
    from chuck_data.agent.tool_executor import execute_tool

    with patch("chuck_data.config._config_manager", temp_config):
        # Setup test data
        databricks_client_stub.add_warehouse(
            name="Integration Test Warehouse", state="RUNNING", size="2X-Small"
        )

        # Execute through agent tool executor
        result = execute_tool(
            api_client=databricks_client_stub,
            tool_name="select-warehouse",
            tool_args={"warehouse": "Integration Test Warehouse"},
        )

        # Verify agent gets proper result format
        assert "warehouse_name" in result
        assert result["warehouse_name"] == "Integration Test Warehouse"
        assert "warehouse_id" in result
        assert result["warehouse_id"] == "warehouse_0"

        # Verify state actually changed
        assert get_warehouse_id() == "warehouse_0"
