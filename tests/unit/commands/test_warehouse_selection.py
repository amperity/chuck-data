"""
Tests for warehouse_selection command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
Tests cover both direct command execution and agent interaction with tool_output_callback.
"""

from unittest.mock import patch

from chuck_data.commands.warehouse_selection import handle_command, DEFINITION
from chuck_data.config import get_warehouse_id
from chuck_data.agent.tool_executor import execute_tool


class TestWarehouseSelectionParameterValidation:
    """Test parameter validation for warehouse_selection command."""

    def test_missing_warehouse_parameter_returns_error(
        self, databricks_client_stub, temp_config
    ):
        """Missing warehouse parameter returns error."""
        with patch("chuck_data.config._config_manager", temp_config):
            result = handle_command(databricks_client_stub)

            assert not result.success
            assert "warehouse parameter is required" in result.message

    def test_none_client_returns_error(self, temp_config):
        """None client returns error."""
        with patch("chuck_data.config._config_manager", temp_config):
            result = handle_command(None, warehouse="test-warehouse")

            assert not result.success
            assert "No API client available to verify warehouse" in result.message


class TestDirectWarehouseSelectionCommand:
    """Test direct warehouse selection command execution (no tool_output_callback)."""

    def test_direct_command_selects_existing_warehouse_by_id(
        self, databricks_client_stub, temp_config
    ):
        """Direct command successfully selects warehouse by ID."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Set up warehouse in stub
            databricks_client_stub.add_warehouse(
                name="Test Warehouse", state="RUNNING", size="2X-Small"
            )
            warehouse_id = "warehouse_0"

            # Execute command without tool_output_callback
            result = handle_command(databricks_client_stub, warehouse=warehouse_id)

            # Verify success and message format
            assert result.success
            assert (
                "Active SQL warehouse is now set to 'Test Warehouse'" in result.message
            )
            assert f"(ID: {warehouse_id}" in result.message
            assert "State: RUNNING" in result.message

            # Verify result data structure
            assert result.data["warehouse_id"] == warehouse_id
            assert result.data["warehouse_name"] == "Test Warehouse"
            assert result.data["state"] == "RUNNING"

            # Verify configuration state change
            assert get_warehouse_id() == warehouse_id

    def test_direct_command_selects_warehouse_by_exact_name(
        self, databricks_client_stub, temp_config
    ):
        """Direct command successfully selects warehouse by exact name."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Set up warehouse in stub
            databricks_client_stub.add_warehouse(
                name="Test Warehouse", state="RUNNING", size="2X-Small"
            )

            # Execute command with exact warehouse name
            result = handle_command(databricks_client_stub, warehouse="Test Warehouse")

            # Verify success
            assert result.success
            assert (
                "Active SQL warehouse is now set to 'Test Warehouse'" in result.message
            )
            assert result.data["warehouse_name"] == "Test Warehouse"

    def test_direct_command_fuzzy_matching_succeeds(
        self, databricks_client_stub, temp_config
    ):
        """Direct command successfully performs fuzzy name matching."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Set up warehouse in stub
            databricks_client_stub.add_warehouse(
                name="Starter Warehouse", state="RUNNING", size="2X-Small"
            )

            # Execute command with partial name match
            result = handle_command(databricks_client_stub, warehouse="Starter")

            # Verify fuzzy matching success
            assert result.success
            assert (
                "Active SQL warehouse is now set to 'Starter Warehouse'"
                in result.message
            )
            assert result.data["warehouse_name"] == "Starter Warehouse"

    def test_direct_command_nonexistent_warehouse_shows_helpful_error(
        self, databricks_client_stub, temp_config
    ):
        """Direct command failure shows error with available warehouses."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add a warehouse to stub but call with different name
            databricks_client_stub.add_warehouse(
                name="Production Warehouse", state="RUNNING", size="2X-Small"
            )

            # Execute command with non-existent warehouse
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

    def test_databricks_api_error_handled_gracefully(self, temp_config):
        """Databricks API errors are handled gracefully with helpful messages."""
        from tests.fixtures.databricks.client import DatabricksClientStub

        with patch("chuck_data.config._config_manager", temp_config):
            # Create a stub that raises an exception during warehouse operations
            class FailingStub(DatabricksClientStub):
                def get_warehouse(self, warehouse_id):
                    raise Exception("Failed to get warehouse")

                def list_warehouses(self, **kwargs):
                    raise Exception("Failed to list warehouses")

            failing_stub = FailingStub()

            # Execute command
            result = handle_command(failing_stub, warehouse="test-warehouse")

            # Verify graceful error handling
            assert not result.success
            assert "Failed to list warehouses" in result.message


class TestWarehouseSelectionCommandConfiguration:
    """Test warehouse selection command configuration and registry integration."""

    def test_command_definition_structure(self):
        """Command definition has correct structure."""
        assert DEFINITION.name == "select-warehouse"
        assert "Set the active SQL warehouse" in DEFINITION.description
        assert DEFINITION.handler == handle_command
        assert "warehouse" in DEFINITION.parameters
        assert DEFINITION.required_params == ["warehouse"]
        assert DEFINITION.needs_api_client is True
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True

    def test_command_parameter_specification(self):
        """Command parameters are correctly specified."""
        warehouse_param = DEFINITION.parameters["warehouse"]
        assert warehouse_param["type"] == "string"
        assert "SQL warehouse ID or name" in warehouse_param["description"]
        assert "fuzzy matching" in warehouse_param["description"]

    def test_command_display_configuration(self):
        """Command display configuration is properly set."""
        assert DEFINITION.agent_display == "condensed"
        assert DEFINITION.condensed_action == "Setting warehouse:"
        assert DEFINITION.tui_aliases == ["/select-warehouse"]
        assert "Usage:" in DEFINITION.usage_hint


class TestWarehouseSelectionAgentBehavior:
    """Test warehouse selection command behavior with agent tool_output_callback."""

    def test_agent_exact_id_match_shows_no_progress_steps(
        self, databricks_client_stub, temp_config
    ):
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

            # Verify no progress steps for direct ID lookup
            assert len(progress_steps) == 0

    def test_agent_exact_name_match_shows_progress_step(
        self, databricks_client_stub, temp_config
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

            # Verify progress behavior shows found warehouse
            assert len(progress_steps) >= 1
            assert any(
                "Found warehouse 'Production Warehouse'" in step
                for step in progress_steps
            )

    def test_agent_fuzzy_match_shows_multiple_progress_steps(
        self, databricks_client_stub, temp_config
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

            # Execute with tool_output_callback (fuzzy match)
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

            # Verify progress behavior shows search and selection
            assert len(progress_steps) == 2
            assert any(
                "Looking for warehouse matching 'prod'" in step
                for step in progress_steps
            )
            assert any(
                "Selecting 'Production Data Warehouse'" in step
                for step in progress_steps
            )

    def test_agent_shows_progress_before_failure(
        self, databricks_client_stub, temp_config
    ):
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

            # Verify command failure with helpful error
            assert not result.success
            assert "No warehouse found matching 'nonexistent'" in result.message
            assert "Available warehouses: Available Warehouse" in result.message

            # Verify progress shown before failure
            assert len(progress_steps) == 1
            assert "Looking for warehouse matching 'nonexistent'" in progress_steps[0]

    def test_agent_callback_errors_bubble_up_as_command_errors(
        self, databricks_client_stub, temp_config
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
        self, databricks_client_stub, temp_config
    ):
        """Agent tool_executor integration works end-to-end."""
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


class TestWarehouseSelectionEdgeCases:
    """Test edge cases and boundary conditions for warehouse selection."""

    def test_unicode_warehouse_name_handled_correctly(
        self, databricks_client_stub, temp_config
    ):
        """Unicode characters in warehouse names are handled correctly."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup warehouse with unicode name
            unicode_name = "测试仓库-データウェアハウス-🏢"
            databricks_client_stub.add_warehouse(
                name=unicode_name, state="RUNNING", size="2X-Small"
            )

            # Execute command
            result = handle_command(databricks_client_stub, warehouse=unicode_name)

            # Verify unicode handling
            assert result.success
            assert unicode_name in result.message
            assert result.data["warehouse_name"] == unicode_name

    def test_very_long_warehouse_name_handled_correctly(
        self, databricks_client_stub, temp_config
    ):
        """Very long warehouse names are handled correctly."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Create a 256-character warehouse name
            long_name = "Very_Long_Warehouse_Name_" + "x" * (
                256 - len("Very_Long_Warehouse_Name_")
            )
            databricks_client_stub.add_warehouse(
                name=long_name, state="RUNNING", size="2X-Small"
            )

            # Execute command
            result = handle_command(databricks_client_stub, warehouse=long_name)

            # Verify long name handling
            assert result.success
            assert result.data["warehouse_name"] == long_name

    def test_warehouse_name_with_special_characters(
        self, databricks_client_stub, temp_config
    ):
        """Warehouse names with special characters are handled correctly."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup warehouse with special characters
            special_name = "Test-Warehouse_2024@Domain.com (Production) [v2.1]"
            databricks_client_stub.add_warehouse(
                name=special_name, state="RUNNING", size="2X-Small"
            )

            # Execute command
            result = handle_command(databricks_client_stub, warehouse=special_name)

            # Verify special character handling
            assert result.success
            assert result.data["warehouse_name"] == special_name

    def test_empty_warehouse_list_returns_helpful_error(
        self, databricks_client_stub, temp_config
    ):
        """Empty warehouse list returns helpful error message."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Don't add any warehouses to the stub

            # Force name matching path by providing non-ID warehouse name
            result = handle_command(
                databricks_client_stub, warehouse="nonexistent-warehouse"
            )

            # Verify helpful error for empty list
            assert not result.success
            assert "No warehouses found in workspace" in result.message

    def test_case_insensitive_exact_matching(self, databricks_client_stub, temp_config):
        """Case insensitive exact matching works correctly."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup warehouse
            databricks_client_stub.add_warehouse(
                name="Production Warehouse", state="RUNNING", size="2X-Small"
            )

            # Execute command with different case
            result = handle_command(
                databricks_client_stub, warehouse="PRODUCTION WAREHOUSE"
            )

            # Verify case insensitive matching
            assert result.success
            assert result.data["warehouse_name"] == "Production Warehouse"

    def test_fuzzy_matching_threshold_behavior(
        self, databricks_client_stub, temp_config
    ):
        """Fuzzy matching respects similarity threshold."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup warehouse
            databricks_client_stub.add_warehouse(
                name="Production Warehouse", state="RUNNING", size="2X-Small"
            )

            # Execute command with very different name (should fail fuzzy matching)
            result = handle_command(databricks_client_stub, warehouse="xyz123abc")

            # Verify fuzzy matching threshold prevents false matches
            assert not result.success
            assert "No warehouse found matching 'xyz123abc'" in result.message

    def test_substring_matching_priority_over_fuzzy(
        self, databricks_client_stub, temp_config
    ):
        """Substring matching takes priority over fuzzy matching."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup multiple warehouses
            databricks_client_stub.add_warehouse(
                name="Test Warehouse", state="RUNNING", size="2X-Small"
            )
            databricks_client_stub.add_warehouse(
                name="Production Test Environment", state="RUNNING", size="2X-Small"
            )

            # Execute command with substring that should match first warehouse
            result = handle_command(databricks_client_stub, warehouse="Test")

            # Verify substring matching priority (should pick "Test Warehouse" not "Production Test Environment")
            assert result.success
            assert result.data["warehouse_name"] == "Test Warehouse"
