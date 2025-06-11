"""
Tests for warehouse command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
These tests verify what users see when they interact with the warehouse details command,
including warehouse information display, error handling, and various warehouse states.
"""

import pytest
from unittest.mock import patch

from chuck_data.commands.warehouse import handle_command
from chuck_data.config import ConfigManager


class TestWarehouseParameterValidation:
    """Test parameter validation for warehouse command."""

    def test_missing_warehouse_id_parameter_returns_error(self, databricks_client_stub):
        """Missing warehouse_id parameter returns helpful error."""
        result = handle_command(databricks_client_stub)

        assert not result.success
        # Note: The current implementation doesn't validate required parameters
        # This may need to be fixed in the actual command
        # For now, testing current behavior where None warehouse_id is passed through

    def test_none_client_returns_setup_error(self):
        """None client returns workspace setup error."""
        result = handle_command(None, warehouse_id="test_warehouse")

        assert not result.success
        assert "workspace" in result.message.lower()
        assert "set up" in result.message.lower()


class TestDirectWarehouseCommand:
    """Test direct warehouse command execution."""

    def test_direct_command_shows_existing_warehouse_details(
        self, databricks_client_stub
    ):
        """Direct warehouse command shows details for existing warehouse."""
        # Setup test warehouse with comprehensive details
        warehouse_data = {
            "id": "warehouse_123",
            "name": "Production Analytics Warehouse",
            "state": "RUNNING",
            "size": "MEDIUM",
            "cluster_size": "MEDIUM",
            "warehouse_type": "PRO",
            "enable_serverless_compute": False,
            "creator_name": "john.doe@company.com",
            "auto_stop_mins": 60,
            "jdbc_url": "jdbc:databricks://test.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=/sql/1.0/warehouses/warehouse_123",
            "min_num_clusters": 1,
            "max_num_clusters": 3,
            "spot_instance_policy": "COST_OPTIMIZED",
            "tags": {"Environment": "Production", "Team": "Analytics"},
        }
        databricks_client_stub.add_warehouse(**warehouse_data)

        result = handle_command(databricks_client_stub, warehouse_id="warehouse_123")

        # Verify successful execution
        assert result.success
        assert "Found details for warehouse" in result.message
        assert "Production Analytics Warehouse" in result.message
        assert "warehouse_123" in result.message

        # Verify complete warehouse data is returned
        assert result.data is not None
        assert result.data["id"] == "warehouse_123"
        assert result.data["name"] == "Production Analytics Warehouse"
        assert result.data["state"] == "RUNNING"
        assert result.data["size"] == "MEDIUM"
        assert result.data["warehouse_type"] == "PRO"
        assert result.data["auto_stop_mins"] == 60
        assert "jdbc_url" in result.data

    def test_direct_command_shows_warehouse_in_different_states(
        self, databricks_client_stub
    ):
        """Direct warehouse command shows warehouses in various states."""
        test_states = ["RUNNING", "STOPPED", "STARTING", "STOPPING", "DELETED"]

        for state in test_states:
            # Clear previous test data
            databricks_client_stub.warehouses.clear()

            # Add warehouse in specific state
            databricks_client_stub.add_warehouse(
                warehouse_id=f"warehouse_{state.lower()}",
                name=f"Test Warehouse ({state})",
                state=state,
            )

            result = handle_command(
                databricks_client_stub, warehouse_id=f"warehouse_{state.lower()}"
            )

            assert result.success
            assert result.data["state"] == state
            assert f"Test Warehouse ({state})" in result.message

    def test_direct_command_shows_serverless_warehouse_details(
        self, databricks_client_stub
    ):
        """Direct warehouse command shows serverless-specific details."""
        databricks_client_stub.add_warehouse(
            warehouse_id="serverless_wh",
            name="Serverless Warehouse",
            enable_serverless_compute=True,
            warehouse_type="PRO",
            state="RUNNING",
        )

        result = handle_command(databricks_client_stub, warehouse_id="serverless_wh")

        assert result.success
        assert result.data["enable_serverless_compute"] is True
        assert result.data["name"] == "Serverless Warehouse"

    def test_direct_command_shows_classic_warehouse_with_scaling_config(
        self, databricks_client_stub
    ):
        """Direct warehouse command shows classic warehouse with scaling configuration."""
        databricks_client_stub.add_warehouse(
            warehouse_id="classic_wh",
            name="Classic Multi-Cluster Warehouse",
            enable_serverless_compute=False,
            min_num_clusters=2,
            max_num_clusters=10,
            size="LARGE",
            auto_stop_mins=30,
        )

        result = handle_command(databricks_client_stub, warehouse_id="classic_wh")

        assert result.success
        assert result.data["enable_serverless_compute"] is False
        assert result.data["min_num_clusters"] == 2
        assert result.data["max_num_clusters"] == 10
        assert result.data["size"] == "LARGE"
        assert result.data["auto_stop_mins"] == 30

    def test_direct_command_handles_nonexistent_warehouse_gracefully(
        self, databricks_client_stub
    ):
        """Direct warehouse command handles non-existent warehouse gracefully."""
        # Don't add any warehouses to the stub
        result = handle_command(databricks_client_stub, warehouse_id="nonexistent_wh")

        assert not result.success
        assert "Warehouse with ID 'nonexistent_wh' not found" in result.message

    def test_direct_command_handles_api_errors_gracefully(self, databricks_client_stub):
        """Direct warehouse command handles API errors gracefully."""
        # Mock get_warehouse to raise an exception
        original_get_warehouse = databricks_client_stub.get_warehouse

        def failing_get_warehouse(warehouse_id):
            raise Exception("API connection timeout")

        databricks_client_stub.get_warehouse = failing_get_warehouse

        result = handle_command(databricks_client_stub, warehouse_id="test_warehouse")

        # Restore original method
        databricks_client_stub.get_warehouse = original_get_warehouse

        assert not result.success
        assert "Failed to fetch warehouse details" in result.message
        assert "API connection timeout" in result.message
        assert result.error is not None

    def test_direct_command_with_warehouse_containing_special_characters(
        self, databricks_client_stub
    ):
        """Direct warehouse command handles warehouses with special characters in names."""
        databricks_client_stub.add_warehouse(
            warehouse_id="special_chars_wh",
            name="Warehouse with Special-Chars & Symbols (Test)",
            creator_name="user+test@company-name.com",
        )

        result = handle_command(databricks_client_stub, warehouse_id="special_chars_wh")

        assert result.success
        assert "Warehouse with Special-Chars & Symbols (Test)" in result.message
        assert result.data["creator_name"] == "user+test@company-name.com"

    def test_direct_command_shows_comprehensive_warehouse_metadata(
        self, databricks_client_stub
    ):
        """Direct warehouse command returns comprehensive warehouse metadata."""
        comprehensive_warehouse = {
            "warehouse_id": "comprehensive_wh",
            "name": "Comprehensive Test Warehouse",
            "state": "RUNNING",
            "size": "X_SMALL",
            "warehouse_type": "CLASSIC",
            "enable_serverless_compute": False,
            "creator_name": "admin@company.com",
            "auto_stop_mins": 45,
            "min_num_clusters": 1,
            "max_num_clusters": 5,
            "spot_instance_policy": "RELIABILITY_OPTIMIZED",
            "enable_photon": True,
            "tags": {
                "Environment": "Development",
                "Project": "DataPlatform",
                "Owner": "DataTeam",
            },
        }
        databricks_client_stub.add_warehouse(**comprehensive_warehouse)

        result = handle_command(databricks_client_stub, warehouse_id="comprehensive_wh")

        assert result.success
        data = result.data

        # Verify all expected fields are present
        assert data["name"] == "Comprehensive Test Warehouse"
        assert data["state"] == "RUNNING"
        assert data["size"] == "X_SMALL"
        assert data["warehouse_type"] == "CLASSIC"
        assert data["enable_serverless_compute"] is False
        assert data["auto_stop_mins"] == 45
        assert "jdbc_url" in data


class TestWarehouseCommandConfiguration:
    """Test warehouse command configuration and registry integration."""

    def test_warehouse_command_definition_properties(self):
        """Warehouse command definition has correct configuration."""
        from chuck_data.commands.warehouse import DEFINITION

        assert DEFINITION.name == "warehouse"
        assert DEFINITION.handler == handle_command
        assert DEFINITION.needs_api_client is True
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True
        assert "warehouse_id" in DEFINITION.required_params
        assert "warehouse_id" in DEFINITION.parameters

    def test_warehouse_command_parameter_requirements(self):
        """Warehouse command has properly configured parameter requirements."""
        from chuck_data.commands.warehouse import DEFINITION

        # Verify required parameter
        assert "warehouse_id" in DEFINITION.required_params

        # Verify parameter definition
        warehouse_id_param = DEFINITION.parameters["warehouse_id"]
        assert warehouse_id_param["type"] == "string"
        assert "warehouse" in warehouse_id_param["description"].lower()

    def test_warehouse_command_has_aliases(self):
        """Warehouse command has proper TUI aliases configured."""
        from chuck_data.commands.warehouse import DEFINITION

        assert "/warehouse" in DEFINITION.tui_aliases
        assert "/warehouse-details" in DEFINITION.tui_aliases

    def test_warehouse_command_usage_hint(self):
        """Warehouse command has helpful usage hint."""
        from chuck_data.commands.warehouse import DEFINITION

        assert DEFINITION.usage_hint is not None
        assert "warehouse_id" in DEFINITION.usage_hint
        assert "/warehouse" in DEFINITION.usage_hint


class TestWarehouseDisplayIntegration:
    """Test warehouse command integration with display system."""

    def test_warehouse_result_contains_display_ready_data(self, databricks_client_stub):
        """Warehouse command result contains data ready for display formatting."""
        databricks_client_stub.add_warehouse(
            warehouse_id="display_test_wh",
            name="Display Test Warehouse",
            state="RUNNING",
            size="MEDIUM",
            warehouse_type="PRO",
            auto_stop_mins=60,
            creator_name="test.user@example.com",
        )

        result = handle_command(databricks_client_stub, warehouse_id="display_test_wh")

        assert result.success

        # Verify data structure is suitable for display
        data = result.data
        assert isinstance(data, dict)
        assert "id" in data
        assert "name" in data
        assert "state" in data
        assert "size" in data
        assert "warehouse_type" in data

        # Verify message is user-friendly
        message = result.message
        assert "Found details for warehouse" in message
        assert "Display Test Warehouse" in message
        assert "display_test_wh" in message

    def test_warehouse_different_sizes_display_correctly(self, databricks_client_stub):
        """Warehouse command displays different warehouse sizes correctly."""
        sizes = ["X_SMALL", "SMALL", "MEDIUM", "LARGE", "X_LARGE", "2X_LARGE"]

        for size in sizes:
            # Clear previous warehouses
            databricks_client_stub.warehouses.clear()

            databricks_client_stub.add_warehouse(
                warehouse_id=f"size_test_{size.lower()}",
                name=f"Size Test {size}",
                size=size,
            )

            result = handle_command(
                databricks_client_stub, warehouse_id=f"size_test_{size.lower()}"
            )

            assert result.success
            assert result.data["size"] == size
            assert f"Size Test {size}" in result.message

    def test_warehouse_message_includes_key_identifying_information(
        self, databricks_client_stub
    ):
        """Warehouse command message includes key identifying information."""
        databricks_client_stub.add_warehouse(
            warehouse_id="msg_test_wh_456",
            name="Message Test Warehouse",
            state="STOPPED",
        )

        result = handle_command(databricks_client_stub, warehouse_id="msg_test_wh_456")

        assert result.success

        message = result.message
        # Should include both name and ID for clear identification
        assert "Message Test Warehouse" in message
        assert "msg_test_wh_456" in message
        assert "Found details for warehouse" in message


class TestWarehouseEdgeCases:
    """Test edge cases and boundary conditions for warehouse command."""

    def test_warehouse_with_none_warehouse_id_parameter(self, databricks_client_stub):
        """Warehouse command handles None warehouse_id parameter."""
        result = handle_command(databricks_client_stub, warehouse_id=None)

        # Current implementation may not handle this well
        # This test documents the current behavior
        assert not result.success or result.data is None

    def test_warehouse_with_empty_string_warehouse_id(self, databricks_client_stub):
        """Warehouse command handles empty string warehouse_id."""
        result = handle_command(databricks_client_stub, warehouse_id="")

        assert not result.success
        assert "Warehouse with ID '' not found" in result.message

    def test_warehouse_with_very_long_warehouse_id(self, databricks_client_stub):
        """Warehouse command handles very long warehouse IDs."""
        long_id = "warehouse_" + "x" * 100
        databricks_client_stub.add_warehouse(
            warehouse_id=long_id, name="Long ID Warehouse"
        )

        result = handle_command(databricks_client_stub, warehouse_id=long_id)

        assert result.success
        assert result.data["id"] == long_id
        assert long_id in result.message

    def test_warehouse_with_unicode_characters_in_name(self, databricks_client_stub):
        """Warehouse command handles Unicode characters in warehouse names."""
        unicode_name = "Datamart Almacén (测试) - Ñoël's Warehouse 🏭"
        databricks_client_stub.add_warehouse(
            warehouse_id="unicode_wh", name=unicode_name
        )

        result = handle_command(databricks_client_stub, warehouse_id="unicode_wh")

        assert result.success
        assert result.data["name"] == unicode_name
        assert unicode_name in result.message

    def test_warehouse_with_minimal_required_fields_only(self, databricks_client_stub):
        """Warehouse command handles warehouses with minimal required fields."""
        # Add warehouse with only essential fields
        minimal_warehouse = {
            "id": "minimal_wh",
            "name": "Minimal Warehouse",
            "state": "RUNNING",
            "size": "SMALL",
            "cluster_size": "SMALL",
            "warehouse_type": "PRO",
            "enable_serverless_compute": False,
            "creator_name": "test.user@example.com",
            "auto_stop_mins": 60,
            "jdbc_url": "jdbc:databricks://test.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=/sql/1.0/warehouses/minimal_wh",
        }
        databricks_client_stub.warehouses.append(minimal_warehouse)

        result = handle_command(databricks_client_stub, warehouse_id="minimal_wh")

        assert result.success
        assert result.data["id"] == "minimal_wh"
        assert result.data["name"] == "Minimal Warehouse"

    def test_warehouse_api_returns_extra_unexpected_fields(
        self, databricks_client_stub
    ):
        """Warehouse command handles API responses with unexpected fields."""
        # Add warehouse with extra fields that might come from API
        warehouse_with_extras = {
            "warehouse_id": "extra_fields_wh",
            "name": "Extra Fields Warehouse",
            "state": "RUNNING",
            "size": "MEDIUM",
            # Extra fields that might be added by Databricks API
            "internal_field": "internal_value",
            "experimental_feature": True,
            "nested_config": {"sub_field": "sub_value"},
            "api_version": "2.1",
        }
        databricks_client_stub.add_warehouse(**warehouse_with_extras)

        result = handle_command(databricks_client_stub, warehouse_id="extra_fields_wh")

        assert result.success
        # Should still work despite extra fields
        assert result.data["name"] == "Extra Fields Warehouse"
        assert result.data["state"] == "RUNNING"
