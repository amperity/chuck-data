"""
Tests for create_volume command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
These tests verify what users see when they interact with the create-volume command,
including volume creation, parameter validation, and active catalog/schema usage.
"""

import pytest
import tempfile
from unittest.mock import patch

from chuck_data.commands.create_volume import handle_command
from chuck_data.config import ConfigManager


class TestCreateVolumeParameterValidation:
    """Test parameter validation for create_volume command."""

    def test_missing_name_parameter_returns_error(self, databricks_client_stub):
        """Missing name parameter returns helpful error."""
        result = handle_command(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
        )

        assert not result.success
        assert "name" in result.message.lower()

    def test_none_client_returns_setup_error(self):
        """None client returns workspace setup error."""
        result = handle_command(
            None,
            name="test_volume",
            catalog_name="test_catalog",
            schema_name="test_schema",
        )

        assert not result.success
        assert "workspace" in result.message.lower()
        assert "set up" in result.message.lower()

    def test_missing_catalog_without_active_catalog_returns_error(
        self, databricks_client_stub
    ):
        """Missing catalog_name without active catalog returns helpful error."""
        with patch(
            "chuck_data.commands.create_volume.get_active_catalog", return_value=None
        ):
            with patch(
                "chuck_data.commands.create_volume.get_active_schema",
                return_value="test_schema",
            ):
                result = handle_command(databricks_client_stub, name="test_volume")

                assert not result.success
                assert "catalog" in result.message.lower()
                assert "active catalog" in result.message.lower()
                assert "select-catalog" in result.message

    def test_missing_schema_without_active_schema_returns_error(
        self, databricks_client_stub
    ):
        """Missing schema_name without active schema returns helpful error."""
        with patch(
            "chuck_data.commands.create_volume.get_active_catalog",
            return_value="test_catalog",
        ):
            with patch(
                "chuck_data.commands.create_volume.get_active_schema", return_value=None
            ):
                result = handle_command(databricks_client_stub, name="test_volume")

                assert not result.success
                assert "schema" in result.message.lower()
                assert "active schema" in result.message.lower()
                assert "select-schema" in result.message


class TestDirectCreateVolumeCommand:
    """Test direct create_volume command execution."""

    def test_direct_command_creates_managed_volume_successfully(
        self, databricks_client_stub, temp_config
    ):
        """Direct create_volume command creates managed volume successfully."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup successful creation result
            created_volume = {
                "name": "analytics_data",
                "full_name": "production.analytics.analytics_data",
                "catalog_name": "production",
                "schema_name": "analytics",
                "volume_type": "MANAGED",
                "volume_id": "volume-12345",
                "created_at": "2023-01-01T00:00:00Z",
                "owner": "admin@company.com",
            }
            databricks_client_stub.create_volume = lambda **kwargs: created_volume

            result = handle_command(
                databricks_client_stub,
                name="analytics_data",
                catalog_name="production",
                schema_name="analytics",
            )

            # Verify successful execution
            assert result.success
            assert "Successfully created volume" in result.message
            assert "production.analytics.analytics_data" in result.message
            assert "MANAGED" in result.message

            # Verify volume data is returned
            assert result.data is not None
            assert result.data["name"] == "analytics_data"
            assert result.data["volume_type"] == "MANAGED"
            assert result.data["full_name"] == "production.analytics.analytics_data"

    def test_direct_command_creates_external_volume_successfully(
        self, databricks_client_stub
    ):
        """Direct create_volume command creates external volume successfully."""
        created_volume = {
            "name": "external_storage",
            "full_name": "data_lake.raw.external_storage",
            "catalog_name": "data_lake",
            "schema_name": "raw",
            "volume_type": "EXTERNAL",
            "volume_id": "volume-67890",
            "storage_location": "s3://data-lake-bucket/external/",
        }
        databricks_client_stub.create_volume = lambda **kwargs: created_volume

        result = handle_command(
            databricks_client_stub,
            name="external_storage",
            catalog_name="data_lake",
            schema_name="raw",
            volume_type="EXTERNAL",
        )

        # Verify successful execution
        assert result.success
        assert "Successfully created volume" in result.message
        assert "data_lake.raw.external_storage" in result.message
        assert "EXTERNAL" in result.message

        # Verify external volume data
        assert result.data["volume_type"] == "EXTERNAL"
        assert "storage_location" in result.data

    def test_direct_command_uses_active_catalog_and_schema(
        self, databricks_client_stub, temp_config
    ):
        """Direct create_volume command uses active catalog and schema when not specified."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Set active catalog and schema
            temp_config.update(
                active_catalog="default_catalog", active_schema="default_schema"
            )

            # Track create_volume calls to verify parameters
            create_calls = []

            def capture_create_volume(**kwargs):
                create_calls.append(kwargs)
                return {
                    "name": kwargs["name"],
                    "full_name": f"{kwargs['catalog_name']}.{kwargs['schema_name']}.{kwargs['name']}",
                    "catalog_name": kwargs["catalog_name"],
                    "schema_name": kwargs["schema_name"],
                    "volume_type": kwargs["volume_type"],
                }

            databricks_client_stub.create_volume = capture_create_volume

            result = handle_command(databricks_client_stub, name="test_volume")

            # Verify successful execution using active catalog/schema
            assert result.success
            assert "default_catalog.default_schema.test_volume" in result.message

            # Verify the API was called with active catalog/schema
            assert len(create_calls) == 1
            call_args = create_calls[0]
            assert call_args["catalog_name"] == "default_catalog"
            assert call_args["schema_name"] == "default_schema"
            assert call_args["name"] == "test_volume"
            assert call_args["volume_type"] == "MANAGED"  # Default

    def test_direct_command_respects_explicit_volume_type(self, databricks_client_stub):
        """Direct create_volume command respects explicitly provided volume_type."""
        create_calls = []

        def capture_create_volume(**kwargs):
            create_calls.append(kwargs)
            return {
                "name": kwargs["name"],
                "volume_type": kwargs["volume_type"],
                "full_name": f"test.test.{kwargs['name']}",
            }

        databricks_client_stub.create_volume = capture_create_volume

        result = handle_command(
            databricks_client_stub,
            name="test_volume",
            catalog_name="test_catalog",
            schema_name="test_schema",
            volume_type="EXTERNAL",
        )

        assert result.success
        assert len(create_calls) == 1
        assert create_calls[0]["volume_type"] == "EXTERNAL"

    def test_direct_command_handles_api_errors_gracefully(self, databricks_client_stub):
        """Direct create_volume command handles API errors with user-friendly messages."""

        # Make create_volume raise an exception
        def failing_create_volume(**kwargs):
            raise Exception("Volume name already exists in schema")

        databricks_client_stub.create_volume = failing_create_volume

        result = handle_command(
            databricks_client_stub,
            name="existing_volume",
            catalog_name="test_catalog",
            schema_name="test_schema",
        )

        assert not result.success
        assert "Failed to create volume" in result.message
        assert "Volume name already exists in schema" in result.message
        assert result.error is not None

    def test_direct_command_handles_permission_errors(self, databricks_client_stub):
        """Direct create_volume command handles permission errors clearly."""

        def permission_error(**kwargs):
            raise Exception(
                "Insufficient privileges to create volume in schema test_schema"
            )

        databricks_client_stub.create_volume = permission_error

        result = handle_command(
            databricks_client_stub,
            name="restricted_volume",
            catalog_name="test_catalog",
            schema_name="test_schema",
        )

        assert not result.success
        assert "Failed to create volume" in result.message
        assert "Insufficient privileges" in result.message

    def test_direct_command_with_minimal_parameters(self, databricks_client_stub):
        """Direct create_volume command works with minimal parameters using defaults."""
        with patch(
            "chuck_data.commands.create_volume.get_active_catalog",
            return_value="active_catalog",
        ):
            with patch(
                "chuck_data.commands.create_volume.get_active_schema",
                return_value="active_schema",
            ):
                create_calls = []

                def capture_create_volume(**kwargs):
                    create_calls.append(kwargs)
                    return {
                        "name": kwargs["name"],
                        "volume_type": kwargs["volume_type"],
                        "full_name": f"{kwargs['catalog_name']}.{kwargs['schema_name']}.{kwargs['name']}",
                    }

                databricks_client_stub.create_volume = capture_create_volume

                result = handle_command(databricks_client_stub, name="simple_volume")

                assert result.success
                assert len(create_calls) == 1
                call_args = create_calls[0]
                assert call_args["name"] == "simple_volume"
                assert call_args["catalog_name"] == "active_catalog"
                assert call_args["schema_name"] == "active_schema"
                assert call_args["volume_type"] == "MANAGED"  # Default

    def test_direct_command_constructs_full_name_correctly(
        self, databricks_client_stub
    ):
        """Direct create_volume command constructs full volume names correctly."""
        databricks_client_stub.create_volume = lambda **kwargs: {
            "name": kwargs["name"],
            "full_name": f"{kwargs['catalog_name']}.{kwargs['schema_name']}.{kwargs['name']}",
            "volume_type": kwargs["volume_type"],
        }

        result = handle_command(
            databricks_client_stub,
            name="my_volume",
            catalog_name="prod_catalog",
            schema_name="analytics_schema",
        )

        assert result.success
        assert "prod_catalog.analytics_schema.my_volume" in result.message


class TestCreateVolumeCommandConfiguration:
    """Test create_volume command configuration and registry integration."""

    def test_create_volume_command_definition_properties(self):
        """Create_volume command definition has correct configuration."""
        from chuck_data.commands.create_volume import DEFINITION

        assert DEFINITION.name == "create-volume"
        assert DEFINITION.handler == handle_command
        assert DEFINITION.needs_api_client is True
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True
        assert "name" in DEFINITION.required_params
        assert "name" in DEFINITION.parameters
        assert "catalog_name" in DEFINITION.parameters
        assert "schema_name" in DEFINITION.parameters
        assert "volume_type" in DEFINITION.parameters

    def test_create_volume_command_parameter_requirements(self):
        """Create_volume command has properly configured parameter requirements."""
        from chuck_data.commands.create_volume import DEFINITION

        # Verify required parameter
        assert "name" in DEFINITION.required_params
        assert (
            "catalog_name" not in DEFINITION.required_params
        )  # Optional (uses active)
        assert "schema_name" not in DEFINITION.required_params  # Optional (uses active)

        # Verify parameter definitions
        name_param = DEFINITION.parameters["name"]
        assert name_param["type"] == "string"
        assert "volume" in name_param["description"].lower()

        volume_type_param = DEFINITION.parameters["volume_type"]
        assert volume_type_param["type"] == "string"
        assert volume_type_param["default"] == "MANAGED"

    def test_create_volume_command_has_aliases(self):
        """Create_volume command has proper TUI aliases configured."""
        from chuck_data.commands.create_volume import DEFINITION

        assert "/create-volume" in DEFINITION.tui_aliases


class TestCreateVolumeDisplayIntegration:
    """Test create_volume command integration with display system."""

    def test_create_volume_result_contains_display_ready_data(
        self, databricks_client_stub
    ):
        """Create_volume command result contains data ready for display formatting."""
        created_volume = {
            "name": "display_volume",
            "full_name": "display_catalog.display_schema.display_volume",
            "catalog_name": "display_catalog",
            "schema_name": "display_schema",
            "volume_type": "MANAGED",
            "volume_id": "volume-display-123",
            "owner": "test@company.com",
            "created_at": "2023-01-01T00:00:00Z",
            "comment": "Test volume for display",
        }

        databricks_client_stub.create_volume = lambda **kwargs: created_volume

        result = handle_command(
            databricks_client_stub,
            name="display_volume",
            catalog_name="display_catalog",
            schema_name="display_schema",
        )

        assert result.success

        # Verify data structure matches what display layer expects
        volume_data = result.data
        assert isinstance(volume_data, dict)
        assert "name" in volume_data
        assert "full_name" in volume_data
        assert "volume_type" in volume_data
        assert "volume_id" in volume_data
        assert volume_data["name"] == "display_volume"

    def test_create_volume_success_message_format(self, databricks_client_stub):
        """Create_volume command formats success messages consistently."""
        databricks_client_stub.create_volume = lambda **kwargs: {
            "name": kwargs["name"],
            "full_name": f"{kwargs['catalog_name']}.{kwargs['schema_name']}.{kwargs['name']}",
            "volume_type": kwargs["volume_type"],
        }

        result = handle_command(
            databricks_client_stub,
            name="test_volume",
            catalog_name="test_catalog",
            schema_name="test_schema",
            volume_type="EXTERNAL",
        )

        assert result.success

        # Verify message format
        message = result.message
        assert "Successfully created volume" in message
        assert "'test_catalog.test_schema.test_volume'" in message
        assert "'EXTERNAL'" in message

    def test_create_volume_error_messages_are_user_friendly(
        self, databricks_client_stub
    ):
        """Create_volume command error messages are user-friendly."""
        # Test missing name parameter
        result1 = handle_command(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
        )
        assert not result1.success
        assert "name" in result1.message.lower()

        # Test missing catalog without active
        with patch(
            "chuck_data.commands.create_volume.get_active_catalog", return_value=None
        ):
            with patch(
                "chuck_data.commands.create_volume.get_active_schema",
                return_value="test_schema",
            ):
                result2 = handle_command(databricks_client_stub, name="test_volume")
                assert not result2.success
                assert "active catalog" in result2.message.lower()

        # Test API error
        databricks_client_stub.create_volume = lambda **kwargs: exec(
            'raise Exception("Storage quota exceeded")'
        )
        result3 = handle_command(
            databricks_client_stub,
            name="test_volume",
            catalog_name="test_catalog",
            schema_name="test_schema",
        )
        assert not result3.success
        assert "Failed to create volume" in result3.message
        assert "Storage quota exceeded" in result3.message


class TestCreateVolumeEdgeCases:
    """Test edge cases and boundary conditions for create_volume command."""

    def test_create_volume_with_special_characters_in_name(
        self, databricks_client_stub
    ):
        """Create_volume command handles names with special characters."""
        databricks_client_stub.create_volume = lambda **kwargs: {
            "name": kwargs["name"],
            "full_name": f"test.test.{kwargs['name']}",
            "volume_type": kwargs["volume_type"],
        }

        # Test with underscores and numbers (typically allowed)
        result = handle_command(
            databricks_client_stub,
            name="data_volume_v1",
            catalog_name="test_catalog",
            schema_name="test_schema",
        )

        assert result.success
        assert "data_volume_v1" in result.message

    def test_create_volume_parameter_case_sensitivity(self, databricks_client_stub):
        """Create_volume command parameter handling is consistent."""
        create_calls = []

        def capture_create_volume(**kwargs):
            create_calls.append(kwargs)
            return {
                "name": kwargs["name"],
                "volume_type": kwargs["volume_type"],
                "full_name": f"test.test.{kwargs['name']}",
            }

        databricks_client_stub.create_volume = capture_create_volume

        result = handle_command(
            databricks_client_stub,
            name="test_volume",
            catalog_name="Test_Catalog",
            schema_name="Test_Schema",
            volume_type="MANAGED",
        )

        assert result.success
        call_args = create_calls[0]
        assert call_args["catalog_name"] == "Test_Catalog"
        assert call_args["schema_name"] == "Test_Schema"
        assert call_args["volume_type"] == "MANAGED"

    def test_create_volume_with_very_long_name(self, databricks_client_stub):
        """Create_volume command handles very long volume names."""
        long_name = "very_long_volume_name_" * 5  # 100+ characters

        databricks_client_stub.create_volume = lambda **kwargs: {
            "name": kwargs["name"],
            "full_name": f"test.test.{kwargs['name']}",
            "volume_type": kwargs["volume_type"],
        }

        result = handle_command(
            databricks_client_stub,
            name=long_name,
            catalog_name="test_catalog",
            schema_name="test_schema",
        )

        assert result.success
        assert long_name in result.message

    def test_create_volume_fallback_behavior_with_partial_config(
        self, databricks_client_stub
    ):
        """Create_volume command handles partial active configuration correctly."""
        # Test with only active catalog set
        with patch(
            "chuck_data.commands.create_volume.get_active_catalog",
            return_value="active_catalog",
        ):
            with patch(
                "chuck_data.commands.create_volume.get_active_schema", return_value=None
            ):
                result = handle_command(databricks_client_stub, name="test_volume")

                assert not result.success
                assert "schema" in result.message.lower()
                assert "active schema" in result.message.lower()

        # Test with only active schema set
        with patch(
            "chuck_data.commands.create_volume.get_active_catalog", return_value=None
        ):
            with patch(
                "chuck_data.commands.create_volume.get_active_schema",
                return_value="active_schema",
            ):
                result = handle_command(databricks_client_stub, name="test_volume")

                assert not result.success
                assert "catalog" in result.message.lower()
                assert "active catalog" in result.message.lower()
