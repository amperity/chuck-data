"""
Tests for list_warehouses command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
These tests verify what users see when they interact with the list_warehouses command,
both directly and when an agent uses the list-warehouses tool.
"""

import pytest
from unittest.mock import patch

from chuck_data.commands.list_warehouses import handle_command
from chuck_data.config import set_warehouse_id
from chuck_data.agent.tool_executor import execute_tool


class TestListWarehousesParameterValidation:
    """Test parameter validation for list_warehouses command."""

    def test_none_client_returns_error(self):
        """None client returns error."""
        result = handle_command(None)

        assert not result.success
        assert "No Databricks client available" in result.message
        assert "workspace" in result.message.lower()


class TestDirectListWarehousesCommand:
    """Test direct list_warehouses command execution."""

    def test_direct_command_lists_warehouses_with_display_true(
        self, databricks_client_stub, temp_config
    ):
        """Direct command with display=true returns warehouses with display flag set."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add test warehouses
            databricks_client_stub.add_warehouse(
                name="Production Warehouse", state="RUNNING", size="LARGE"
            )
            databricks_client_stub.add_warehouse(
                name="Development Warehouse", state="STOPPED", size="SMALL"
            )

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            assert result.data.get("display") is True
            assert len(result.data.get("warehouses", [])) == 2
            assert "Found 2 SQL warehouse(s)" in result.message

    def test_direct_command_lists_warehouses_with_display_false(
        self, databricks_client_stub, temp_config
    ):
        """Direct command with display=false returns data without display flag."""
        with patch("chuck_data.config._config_manager", temp_config):
            databricks_client_stub.add_warehouse(
                name="Test Warehouse", state="RUNNING", size="SMALL"
            )

            result = handle_command(databricks_client_stub, display=False)

            assert result.success
            assert result.data.get("display") is False
            assert len(result.data.get("warehouses", [])) == 1
            assert result.data["warehouses"][0]["name"] == "Test Warehouse"

    def test_direct_command_includes_current_warehouse_id(
        self, databricks_client_stub, temp_config
    ):
        """Direct command includes current warehouse ID for highlighting purposes."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Set active warehouse
            set_warehouse_id("current_warehouse")

            # Add warehouses including the active one
            databricks_client_stub.add_warehouse(
                warehouse_id="current_warehouse",
                name="Current Warehouse",
                state="RUNNING",
            )
            databricks_client_stub.add_warehouse(
                warehouse_id="other_warehouse", name="Other Warehouse", state="STOPPED"
            )

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            assert result.data["current_warehouse_id"] == "current_warehouse"
            assert len(result.data["warehouses"]) == 2

    def test_direct_command_handles_empty_warehouse_list(
        self, databricks_client_stub, temp_config
    ):
        """Direct command handles workspace with no warehouses gracefully."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Don't add any warehouses

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            assert len(result.data.get("warehouses", [])) == 0
            assert result.data["total_count"] == 0
            assert "No SQL warehouses found" in result.message

    def test_direct_command_includes_warehouse_details(
        self, databricks_client_stub, temp_config
    ):
        """Direct command includes detailed warehouse information."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add warehouse with detailed properties
            databricks_client_stub.add_warehouse(
                name="Detailed Warehouse",
                state="RUNNING",
                size="LARGE",
                warehouse_type="PRO",
                enable_serverless_compute=True,
                auto_stop_mins=120,
                creator_name="detailed.user@example.com",
            )

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            warehouse = result.data["warehouses"][0]
            assert warehouse["name"] == "Detailed Warehouse"
            assert warehouse["state"] == "RUNNING"
            assert warehouse["size"] == "LARGE"
            assert warehouse["warehouse_type"] == "PRO"
            assert warehouse["enable_serverless_compute"] is True
            assert warehouse["auto_stop_mins"] == 120
            assert warehouse["creator_name"] == "detailed.user@example.com"

    def test_databricks_api_error_handled_gracefully(
        self, databricks_client_stub, temp_config
    ):
        """Databricks API errors are handled gracefully with helpful messages."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Store the original method and replace it with one that raises an exception
            original_method = databricks_client_stub.list_warehouses

            def failing_list_warehouses(*args, **kwargs):
                raise Exception("Databricks API connection failed")

            databricks_client_stub.list_warehouses = failing_list_warehouses

            try:
                result = handle_command(databricks_client_stub, display=True)
            finally:
                # Restore the original method
                databricks_client_stub.list_warehouses = original_method

            assert not result.success
            assert "Failed to fetch warehouses" in result.message
            assert "Databricks API connection failed" in result.message


class TestListWarehousesCommandConfiguration:
    """Test list_warehouses command configuration and registry integration."""

    def test_command_definition_properties(self):
        """List_warehouses command definition has correct configuration."""
        from chuck_data.commands.list_warehouses import DEFINITION

        assert DEFINITION.name == "list-warehouses"
        assert "warehouses" in DEFINITION.description.lower()
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True
        assert DEFINITION.agent_display == "conditional"

    def test_command_parameter_definitions(self):
        """List_warehouses command has correct parameter definitions."""
        from chuck_data.commands.list_warehouses import DEFINITION

        parameters = DEFINITION.parameters
        assert "display" in parameters
        assert parameters["display"]["type"] == "boolean"

    def test_command_aliases(self):
        """List_warehouses command has expected aliases."""
        from chuck_data.commands.list_warehouses import DEFINITION

        assert "/list-warehouses" in DEFINITION.tui_aliases
        assert "/warehouses" in DEFINITION.tui_aliases

    def test_command_display_condition(self):
        """List_warehouses command has correct display condition logic."""
        from chuck_data.commands.list_warehouses import DEFINITION

        # Should display when display=True
        assert DEFINITION.display_condition({"display": True})
        # Should not display when display=False
        assert not DEFINITION.display_condition({"display": False})
        # Should not display when display is not specified
        assert not DEFINITION.display_condition({})


class TestListWarehousesDisplayIntegration:
    """Test list_warehouses command integration with display system."""

    def test_command_result_contains_display_ready_data(
        self, databricks_client_stub, temp_config
    ):
        """List_warehouses command returns display-ready data structure."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add warehouse with display properties
            databricks_client_stub.add_warehouse(
                name="Display Warehouse",
                state="RUNNING",
                size="MEDIUM",
                creator_name="display.user@example.com",
                warehouse_type="PRO",
            )

            result = handle_command(databricks_client_stub, display=True)

            # Verify data structure is display-ready
            assert result.success
            assert isinstance(result.data, dict)
            assert "warehouses" in result.data
            assert isinstance(result.data["warehouses"], list)
            assert result.data["total_count"] == 1

            # Ensure warehouse data has expected display fields
            warehouse = result.data["warehouses"][0]
            assert "id" in warehouse
            assert "name" in warehouse
            assert "size" in warehouse
            assert "state" in warehouse
            assert "creator_name" in warehouse
            assert "warehouse_type" in warehouse
            assert "auto_stop_mins" in warehouse
            assert "enable_serverless_compute" in warehouse

    def test_command_message_formatting(self, databricks_client_stub, temp_config):
        """List_warehouses command formats success message correctly."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Test with no warehouses
            result1 = handle_command(databricks_client_stub)
            assert "No SQL warehouses found" in result1.message

            # Add warehouses and test count message
            databricks_client_stub.add_warehouse(name="Warehouse1")
            databricks_client_stub.add_warehouse(name="Warehouse2")

            result2 = handle_command(databricks_client_stub)
            assert "Found 2 SQL warehouse(s)" in result2.message


class TestListWarehousesAgentBehavior:
    """Test list_warehouses command agent-specific behavior."""

    def test_agent_default_behavior_without_display_parameter(
        self, databricks_client_stub, temp_config
    ):
        """Agent execution without display parameter uses default behavior (no display)."""
        with patch("chuck_data.config._config_manager", temp_config):
            databricks_client_stub.add_warehouse(name="Agent Warehouse")

            result = handle_command(databricks_client_stub)

            assert result.success
            assert result.data.get("display") is False
            assert len(result.data.get("warehouses", [])) == 1

    def test_agent_conditional_display_with_display_true(
        self, databricks_client_stub, temp_config
    ):
        """Agent execution with display=true triggers conditional display."""
        with patch("chuck_data.config._config_manager", temp_config):
            databricks_client_stub.add_warehouse(name="Display Warehouse")

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            assert result.data.get("display") is True
            assert len(result.data.get("warehouses", [])) == 1

    def test_agent_tool_executor_end_to_end_integration(
        self, databricks_client_stub, temp_config
    ):
        """Agent tool_executor integration works end-to-end."""
        with patch("chuck_data.config._config_manager", temp_config):
            databricks_client_stub.add_warehouse(
                name="Integration Warehouse", warehouse_id="integration_id"
            )
            set_warehouse_id("integration_id")

            result = execute_tool(
                api_client=databricks_client_stub,
                tool_name="list-warehouses",
                tool_args={"display": True},
            )

            # Verify agent gets proper result format
            assert "warehouses" in result
            assert "total_count" in result
            assert "current_warehouse_id" in result
            assert result["current_warehouse_id"] == "integration_id"
            assert len(result["warehouses"]) == 1
            assert result["warehouses"][0]["name"] == "Integration Warehouse"

    def test_agent_callback_errors_bubble_up_as_command_errors(
        self, databricks_client_stub, temp_config
    ):
        """Agent callback failures bubble up as command errors (list-warehouses doesn't use callbacks)."""
        with patch("chuck_data.config._config_manager", temp_config):
            databricks_client_stub.add_warehouse(name="Callback Warehouse")

            def failing_callback(tool_name, data):
                raise Exception("Display system crashed")

            # list-warehouses doesn't use tool_output_callback, so this should work normally
            result = handle_command(
                databricks_client_stub,
                display=True,
                tool_output_callback=failing_callback,
            )

            # Should succeed since list-warehouses doesn't use callbacks
            assert result.success
            assert len(result.data.get("warehouses", [])) == 1


class TestListWarehousesEdgeCases:
    """Test edge cases and boundary conditions for list_warehouses command."""

    def test_command_handles_unicode_in_warehouse_names(
        self, databricks_client_stub, temp_config
    ):
        """List_warehouses command handles Unicode characters in warehouse names."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add warehouses with Unicode in name
            databricks_client_stub.add_warehouse(name="仓库")
            databricks_client_stub.add_warehouse(name="üñîçødé_warehouse")

            result = handle_command(databricks_client_stub)

            # Verify Unicode handling
            assert result.success
            assert len(result.data["warehouses"]) == 2
            warehouse_names = [w["name"] for w in result.data["warehouses"]]
            assert "仓库" in warehouse_names
            assert "üñîçødé_warehouse" in warehouse_names

    def test_command_with_warehouses_having_complex_metadata(
        self, databricks_client_stub, temp_config
    ):
        """List_warehouses command handles warehouses with complex metadata."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add warehouse with comprehensive metadata
            complex_metadata = {
                "spot_instance_policy": "COST_OPTIMIZED",
                "spark_conf": {"spark.databricks.delta.optimizeWrite": "true"},
                "tags": {"purpose": "testing", "env": "dev"},
                "warehouse_url": "https://example.databricks.com/sql/warehouses/abc123",
                "health": {"status": "HEALTHY"},
                "endpoint_id": "endpoint-123",
                "channel": {"name": "CHANNEL_NAME_CURRENT"},
            }

            databricks_client_stub.add_warehouse(
                name="Complex Warehouse", **complex_metadata
            )

            result = handle_command(databricks_client_stub)

            # Verify complex metadata handling
            assert result.success
            warehouse = result.data["warehouses"][0]
            assert warehouse["name"] == "Complex Warehouse"

            # Even with complex metadata, the command should format it correctly
            assert isinstance(warehouse, dict)
            assert "id" in warehouse
            assert "name" in warehouse
            assert "state" in warehouse

    def test_command_handles_many_warehouses_efficiently(
        self, databricks_client_stub, temp_config
    ):
        """List_warehouses command handles large numbers of warehouses efficiently."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add many warehouses
            for i in range(25):
                databricks_client_stub.add_warehouse(
                    name=f"Warehouse_{i}",
                    warehouse_id=f"id_{i}",
                    state="RUNNING" if i % 2 == 0 else "STOPPED",
                )

            result = handle_command(databricks_client_stub)

            # Verify efficient handling
            assert result.success
            assert len(result.data["warehouses"]) == 25
            assert result.data["total_count"] == 25
