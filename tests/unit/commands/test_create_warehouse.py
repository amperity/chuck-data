"""
Tests for create_warehouse command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
These tests verify what users see when they interact with the create-warehouse command,
including warehouse creation, parameter validation, and configuration handling.
"""

import pytest
from chuck_data.commands.create_warehouse import handle_command


class TestCreateWarehouseParameterValidation:
    """Test parameter validation for create_warehouse command."""

    def test_missing_name_parameter_returns_error(self, databricks_client_stub):
        """Missing name parameter returns helpful error."""
        result = handle_command(databricks_client_stub, size="Medium")

        assert not result.success
        assert "name" in result.message.lower()
        assert "specify" in result.message.lower()

    def test_none_client_returns_setup_error(self):
        """None client returns workspace setup error."""
        result = handle_command(None, name="test_warehouse")

        assert not result.success
        assert "workspace" in result.message.lower()
        assert "set up" in result.message.lower()


class TestDirectCreateWarehouseCommand:
    """Test direct create_warehouse command execution."""

    def test_direct_command_creates_warehouse_with_all_parameters(
        self, databricks_client_stub
    ):
        """Direct create_warehouse command creates warehouse with all parameters."""
        result = handle_command(
            databricks_client_stub,
            name="analytics_warehouse",
            size="Large",
            auto_stop_mins=60,
            min_num_clusters=1,
            max_num_clusters=10,
        )

        # Verify successful execution
        assert result.success
        assert "Successfully created SQL warehouse" in result.message
        assert "'analytics_warehouse'" in result.message
        assert "ID:" in result.message

        # Verify warehouse data is returned
        assert result.data is not None
        assert result.data["name"] == "analytics_warehouse"
        assert result.data["size"] == "Large"
        assert result.data["auto_stop_mins"] == 60
        assert result.data["min_num_clusters"] == 1
        assert result.data["max_num_clusters"] == 10

        # Verify the API was called correctly
        assert len(databricks_client_stub.create_warehouse_calls) == 1
        call_config = databricks_client_stub.create_warehouse_calls[0]
        assert call_config["name"] == "analytics_warehouse"
        assert call_config["size"] == "Large"
        assert call_config["auto_stop_mins"] == 60
        assert call_config["min_num_clusters"] == 1
        assert call_config["max_num_clusters"] == 10

    def test_direct_command_creates_warehouse_with_minimal_parameters(
        self, databricks_client_stub
    ):
        """Direct create_warehouse command creates warehouse with minimal parameters and defaults."""
        result = handle_command(databricks_client_stub, name="simple_warehouse")

        # Verify successful execution
        assert result.success
        assert "Successfully created SQL warehouse" in result.message
        assert "'simple_warehouse'" in result.message

        # Verify defaults were applied
        assert result.data["name"] == "simple_warehouse"
        assert result.data["size"] == "Small"  # Default
        assert result.data["auto_stop_mins"] == 120  # Default

        # Verify the API was called with defaults
        call_config = databricks_client_stub.create_warehouse_calls[0]
        assert call_config["name"] == "simple_warehouse"
        assert call_config["size"] == "Small"
        assert call_config["auto_stop_mins"] == 120
        assert "min_num_clusters" not in call_config
        assert "max_num_clusters" not in call_config

    def test_direct_command_creates_warehouse_with_custom_size(
        self, databricks_client_stub
    ):
        """Direct create_warehouse command respects custom size parameter."""
        result = handle_command(
            databricks_client_stub, name="medium_warehouse", size="Medium"
        )

        assert result.success
        assert result.data["size"] == "Medium"

        call_config = databricks_client_stub.create_warehouse_calls[0]
        assert call_config["size"] == "Medium"

    def test_direct_command_creates_warehouse_with_custom_auto_stop(
        self, databricks_client_stub
    ):
        """Direct create_warehouse command respects custom auto_stop_mins parameter."""
        result = handle_command(
            databricks_client_stub, name="quick_warehouse", auto_stop_mins=30
        )

        assert result.success
        assert result.data["auto_stop_mins"] == 30

        call_config = databricks_client_stub.create_warehouse_calls[0]
        assert call_config["auto_stop_mins"] == 30

    def test_direct_command_creates_serverless_warehouse_with_cluster_config(
        self, databricks_client_stub
    ):
        """Direct create_warehouse command handles serverless cluster configuration."""
        result = handle_command(
            databricks_client_stub,
            name="serverless_warehouse",
            min_num_clusters=2,
            max_num_clusters=8,
        )

        assert result.success
        assert result.data["min_num_clusters"] == 2
        assert result.data["max_num_clusters"] == 8

        # Verify cluster config was passed to API
        call_config = databricks_client_stub.create_warehouse_calls[0]
        assert call_config["min_num_clusters"] == 2
        assert call_config["max_num_clusters"] == 8

    def test_direct_command_handles_partial_cluster_config(
        self, databricks_client_stub
    ):
        """Direct create_warehouse command handles partial cluster configuration."""
        # Test with only min_num_clusters
        result1 = handle_command(
            databricks_client_stub, name="min_only_warehouse", min_num_clusters=1
        )

        assert result1.success
        assert result1.data["min_num_clusters"] == 1
        assert "max_num_clusters" not in result1.data

        # Clear previous calls
        databricks_client_stub.create_warehouse_calls.clear()

        # Test with only max_num_clusters
        result2 = handle_command(
            databricks_client_stub, name="max_only_warehouse", max_num_clusters=5
        )

        assert result2.success
        assert result2.data["max_num_clusters"] == 5
        assert "min_num_clusters" not in result2.data

    def test_direct_command_handles_api_errors_gracefully(self, databricks_client_stub):
        """Direct create_warehouse command handles API errors with user-friendly messages."""
        # Configure stub to raise exception
        databricks_client_stub.set_create_warehouse_error(
            Exception("Warehouse quota exceeded for workspace")
        )

        result = handle_command(databricks_client_stub, name="failing_warehouse")

        assert not result.success
        assert "Failed to create warehouse" in result.message
        assert "Warehouse quota exceeded for workspace" in result.message
        assert result.error is not None

    def test_direct_command_handles_permission_errors(self, databricks_client_stub):
        """Direct create_warehouse command handles permission errors clearly."""
        databricks_client_stub.set_create_warehouse_error(
            Exception("Access denied: insufficient privileges to create warehouses")
        )

        result = handle_command(databricks_client_stub, name="restricted_warehouse")

        assert not result.success
        assert "Failed to create warehouse" in result.message
        assert "Access denied" in result.message

    def test_direct_command_handles_invalid_configuration_errors(
        self, databricks_client_stub
    ):
        """Direct create_warehouse command handles configuration validation errors."""
        databricks_client_stub.set_create_warehouse_error(
            Exception(
                "Invalid warehouse size: 'HUGE'. Valid sizes are: 2X-Small, X-Small, Small, Medium, Large, X-Large, 2X-Large, 3X-Large, 4X-Large"
            )
        )

        result = handle_command(
            databricks_client_stub, name="invalid_warehouse", size="HUGE"
        )

        assert not result.success
        assert "Failed to create warehouse" in result.message
        assert "Invalid warehouse size" in result.message

    def test_direct_command_returns_warehouse_id_in_message(
        self, databricks_client_stub
    ):
        """Direct create_warehouse command includes warehouse ID in success message."""
        result = handle_command(databricks_client_stub, name="id_test_warehouse")

        assert result.success
        assert "ID:" in result.message
        warehouse_id = result.data["id"]
        assert warehouse_id in result.message


class TestCreateWarehouseCommandConfiguration:
    """Test create_warehouse command configuration and registry integration."""

    def test_create_warehouse_command_definition_properties(self):
        """Create_warehouse command definition has correct configuration."""
        from chuck_data.commands.create_warehouse import DEFINITION

        assert DEFINITION.name == "create-warehouse"
        assert DEFINITION.handler == handle_command
        assert DEFINITION.needs_api_client is True
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True
        assert "name" in DEFINITION.required_params
        assert "name" in DEFINITION.parameters
        assert "size" in DEFINITION.parameters
        assert "auto_stop_mins" in DEFINITION.parameters
        assert "min_num_clusters" in DEFINITION.parameters
        assert "max_num_clusters" in DEFINITION.parameters

    def test_create_warehouse_command_parameter_requirements(self):
        """Create_warehouse command has properly configured parameter requirements."""
        from chuck_data.commands.create_warehouse import DEFINITION

        # Verify required parameter
        assert "name" in DEFINITION.required_params
        assert "size" not in DEFINITION.required_params  # Optional with default
        assert (
            "auto_stop_mins" not in DEFINITION.required_params
        )  # Optional with default

        # Verify parameter definitions
        name_param = DEFINITION.parameters["name"]
        assert name_param["type"] == "string"
        assert "warehouse" in name_param["description"].lower()

        size_param = DEFINITION.parameters["size"]
        assert size_param["type"] == "string"
        assert size_param["default"] == "Small"

        auto_stop_param = DEFINITION.parameters["auto_stop_mins"]
        assert auto_stop_param["type"] == "integer"
        assert auto_stop_param["default"] == 120

        # Verify optional cluster parameters
        min_clusters_param = DEFINITION.parameters["min_num_clusters"]
        assert min_clusters_param["type"] == "integer"
        assert "default" not in min_clusters_param  # Truly optional

        max_clusters_param = DEFINITION.parameters["max_num_clusters"]
        assert max_clusters_param["type"] == "integer"
        assert "default" not in max_clusters_param  # Truly optional

    def test_create_warehouse_command_has_aliases(self):
        """Create_warehouse command has proper TUI aliases configured."""
        from chuck_data.commands.create_warehouse import DEFINITION

        assert "/create-warehouse" in DEFINITION.tui_aliases


class TestCreateWarehouseDisplayIntegration:
    """Test create_warehouse command integration with display system."""

    def test_create_warehouse_result_contains_display_ready_data(
        self, databricks_client_stub
    ):
        """Create_warehouse command result contains data ready for display formatting."""
        result = handle_command(
            databricks_client_stub,
            name="display_warehouse",
            size="Large",
            auto_stop_mins=180,
        )

        assert result.success

        # Verify data structure matches what display layer expects
        warehouse_data = result.data
        assert isinstance(warehouse_data, dict)
        assert "id" in warehouse_data
        assert "name" in warehouse_data
        assert "size" in warehouse_data
        assert "auto_stop_mins" in warehouse_data
        assert "state" in warehouse_data
        assert warehouse_data["name"] == "display_warehouse"

    def test_create_warehouse_success_message_format(self, databricks_client_stub):
        """Create_warehouse command formats success messages consistently."""
        result = handle_command(
            databricks_client_stub, name="message_test_warehouse", size="Medium"
        )

        assert result.success

        # Verify message format
        message = result.message
        assert "Successfully created SQL warehouse" in message
        assert "'message_test_warehouse'" in message
        assert "ID:" in message
        warehouse_id = result.data["id"]
        assert warehouse_id in message

    def test_create_warehouse_error_messages_are_user_friendly(
        self, databricks_client_stub
    ):
        """Create_warehouse command error messages are user-friendly."""
        # Test missing name parameter
        result1 = handle_command(databricks_client_stub, size="Large")
        assert not result1.success
        assert "name" in result1.message.lower()
        assert "specify" in result1.message.lower()

        # Test API error
        databricks_client_stub.set_create_warehouse_error(
            Exception("Network timeout occurred")
        )
        result2 = handle_command(databricks_client_stub, name="test_warehouse")
        assert not result2.success
        assert "Failed to create warehouse" in result2.message
        assert "Network timeout occurred" in result2.message


class TestCreateWarehouseEdgeCases:
    """Test edge cases and boundary conditions for create_warehouse command."""

    def test_create_warehouse_with_special_characters_in_name(
        self, databricks_client_stub
    ):
        """Create_warehouse command handles names with special characters."""
        result = handle_command(databricks_client_stub, name="analytics-warehouse_v2")

        assert result.success
        assert result.data["name"] == "analytics-warehouse_v2"
        assert "analytics-warehouse_v2" in result.message

    def test_create_warehouse_with_various_sizes(self, databricks_client_stub):
        """Create_warehouse command handles various warehouse sizes."""
        sizes = [
            "2X-Small",
            "X-Small",
            "Small",
            "Medium",
            "Large",
            "X-Large",
            "2X-Large",
        ]

        for size in sizes:
            databricks_client_stub.create_warehouse_calls.clear()

            result = handle_command(
                databricks_client_stub, name=f"warehouse_{size.lower()}", size=size
            )

            assert result.success
            assert result.data["size"] == size

            call_config = databricks_client_stub.create_warehouse_calls[0]
            assert call_config["size"] == size

    def test_create_warehouse_with_extreme_auto_stop_values(
        self, databricks_client_stub
    ):
        """Create_warehouse command handles various auto_stop_mins values."""
        # Test very short auto-stop
        result1 = handle_command(
            databricks_client_stub, name="quick_stop_warehouse", auto_stop_mins=1
        )
        assert result1.success
        assert result1.data["auto_stop_mins"] == 1

        # Test very long auto-stop
        result2 = handle_command(
            databricks_client_stub,
            name="long_stop_warehouse",
            auto_stop_mins=10080,  # 1 week
        )
        assert result2.success
        assert result2.data["auto_stop_mins"] == 10080

    def test_create_warehouse_with_cluster_boundary_values(
        self, databricks_client_stub
    ):
        """Create_warehouse command handles boundary values for cluster configuration."""
        # Test minimum cluster values
        result1 = handle_command(
            databricks_client_stub,
            name="min_cluster_warehouse",
            min_num_clusters=1,
            max_num_clusters=1,
        )
        assert result1.success
        assert result1.data["min_num_clusters"] == 1
        assert result1.data["max_num_clusters"] == 1

        # Test larger cluster values
        databricks_client_stub.create_warehouse_calls.clear()
        result2 = handle_command(
            databricks_client_stub,
            name="large_cluster_warehouse",
            min_num_clusters=5,
            max_num_clusters=50,
        )
        assert result2.success
        assert result2.data["min_num_clusters"] == 5
        assert result2.data["max_num_clusters"] == 50

    def test_create_warehouse_parameter_type_handling(self, databricks_client_stub):
        """Create_warehouse command handles parameter types correctly."""
        # Ensure integer parameters are properly handled
        result = handle_command(
            databricks_client_stub,
            name="type_test_warehouse",
            auto_stop_mins=90,  # Integer
            min_num_clusters=2,  # Integer
            max_num_clusters=8,  # Integer
        )

        assert result.success

        # Verify types are preserved in the result
        assert isinstance(result.data["auto_stop_mins"], int)
        assert isinstance(result.data["min_num_clusters"], int)
        assert isinstance(result.data["max_num_clusters"], int)

        # Verify API call received correct types
        call_config = databricks_client_stub.create_warehouse_calls[0]
        assert isinstance(call_config["auto_stop_mins"], int)
        assert isinstance(call_config["min_num_clusters"], int)
        assert isinstance(call_config["max_num_clusters"], int)

    def test_create_warehouse_with_zero_cluster_values(self, databricks_client_stub):
        """Create_warehouse command handles zero cluster values (edge case)."""
        result = handle_command(
            databricks_client_stub,
            name="zero_cluster_warehouse",
            min_num_clusters=0,
            max_num_clusters=0,
        )

        assert result.success
        assert result.data["min_num_clusters"] == 0
        assert result.data["max_num_clusters"] == 0
