"""
Tests for list_volumes command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
These tests verify what users see when they interact with the list_volumes command,
both directly and when an agent uses the list-volumes tool.
"""

import pytest
import tempfile
from unittest.mock import patch

from chuck_data.commands.list_volumes import handle_command
from chuck_data.config import ConfigManager


class TestListVolumesParameterValidation:
    """Test parameter validation for list_volumes command."""

    def test_none_client_returns_setup_error(self):
        """None client returns workspace setup error."""
        result = handle_command(None)

        assert not result.success
        assert "workspace" in result.message.lower()
        assert "set up" in result.message.lower()

    def test_missing_catalog_without_active_catalog_returns_error(
        self, databricks_client_stub
    ):
        """Missing catalog parameter without active catalog returns helpful error."""
        with patch(
            "chuck_data.commands.list_volumes.get_active_catalog", return_value=None
        ):
            with patch(
                "chuck_data.commands.list_volumes.get_active_schema",
                return_value="test_schema",
            ):
                result = handle_command(databricks_client_stub)

                assert not result.success
                assert "catalog" in result.message.lower()
                assert "active catalog" in result.message.lower()
                assert "select-catalog" in result.message

    def test_missing_schema_without_active_schema_returns_error(
        self, databricks_client_stub
    ):
        """Missing schema parameter without active schema returns helpful error."""
        with patch(
            "chuck_data.commands.list_volumes.get_active_catalog",
            return_value="test_catalog",
        ):
            with patch(
                "chuck_data.commands.list_volumes.get_active_schema", return_value=None
            ):
                result = handle_command(databricks_client_stub)

                assert not result.success
                assert "schema" in result.message.lower()
                assert "active schema" in result.message.lower()
                assert "select-schema" in result.message


class TestDirectListVolumesCommand:
    """Test direct list_volumes command execution (no tool_output_callback)."""

    def test_direct_command_lists_volumes_in_schema(
        self, databricks_client_stub, temp_config
    ):
        """Direct list_volumes command shows volumes in specified schema."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup test volumes
            databricks_client_stub.add_volume(
                catalog_name="production",
                schema_name="analytics",
                volume_name="data_lake",
                volume_type="EXTERNAL",
                comment="External data lake storage",
                owner="data-team@company.com",
                created_at="2023-01-01T00:00:00Z",
            )
            databricks_client_stub.add_volume(
                catalog_name="production",
                schema_name="analytics",
                volume_name="staging_area",
                volume_type="MANAGED",
                comment="Managed staging volume",
                owner="data-team@company.com",
            )

            result = handle_command(
                databricks_client_stub,
                catalog_name="production",
                schema_name="analytics",
            )

            # Verify successful execution
            assert result.success
            assert "2 volume(s)" in result.message
            assert "production.analytics" in result.message

            # Verify volume data is returned with proper structure
            assert result.data is not None
            assert "volumes" in result.data
            assert "total_count" in result.data
            assert result.data["total_count"] == 2
            assert result.data["catalog_name"] == "production"
            assert result.data["schema_name"] == "analytics"

            # Verify volume formatting
            volumes = result.data["volumes"]
            assert len(volumes) == 2

            # Check that volumes have expected fields
            volume_names = [v["name"] for v in volumes]
            assert "data_lake" in volume_names
            assert "staging_area" in volume_names

            # Find and verify specific volume
            data_lake = next(v for v in volumes if v["name"] == "data_lake")
            assert data_lake["volume_type"] == "EXTERNAL"
            assert data_lake["comment"] == "External data lake storage"

    def test_direct_command_uses_active_catalog_and_schema(
        self, databricks_client_stub, temp_config
    ):
        """Direct list_volumes command uses active catalog and schema when not specified."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Set active catalog and schema
            temp_config.update(
                active_catalog="default_catalog", active_schema="default_schema"
            )

            # Setup test volume in the active catalog/schema
            databricks_client_stub.add_volume(
                catalog_name="default_catalog",
                schema_name="default_schema",
                volume_name="default_volume",
                comment="Volume in active catalog/schema",
            )

            result = handle_command(databricks_client_stub)

            # Verify successful execution using active catalog/schema
            assert result.success
            assert "default_catalog.default_schema" in result.message
            assert result.data["catalog_name"] == "default_catalog"
            assert result.data["schema_name"] == "default_schema"

    def test_direct_command_handles_empty_volume_list(self, databricks_client_stub):
        """Direct list_volumes command handles empty volume list gracefully."""
        # Don't add any volumes to databricks_client_stub

        result = handle_command(
            databricks_client_stub,
            catalog_name="empty_catalog",
            schema_name="empty_schema",
        )

        assert result.success  # Empty list is still success
        assert "No volumes found" in result.message
        assert "empty_catalog.empty_schema" in result.message

    def test_direct_command_handles_api_errors_gracefully(self, databricks_client_stub):
        """Direct list_volumes command handles API errors with user-friendly messages."""

        # Make list_volumes raise an exception
        def failing_list_volumes(*args, **kwargs):
            raise Exception("Access denied to volume metadata")

        databricks_client_stub.list_volumes = failing_list_volumes

        result = handle_command(
            databricks_client_stub,
            catalog_name="restricted_catalog",
            schema_name="restricted_schema",
        )

        assert not result.success
        assert "failed" in result.message.lower()
        assert "Access denied to volume metadata" in result.message

    def test_direct_command_with_include_browse_parameter(self, databricks_client_stub):
        """Direct list_volumes command handles include_browse parameter."""
        # Add volumes to test with
        databricks_client_stub.add_volume(
            catalog_name="test_catalog",
            schema_name="test_schema",
            volume_name="test_volume",
        )

        result = handle_command(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
            include_browse=True,
        )

        assert result.success
        assert result.data["volumes"][0]["name"] == "test_volume"

    def test_direct_command_with_minimal_volume_data(self, databricks_client_stub):
        """Direct list_volumes command works with minimal volume information."""
        # Add volume with minimal required fields only
        databricks_client_stub.add_volume(
            "test_catalog", "test_schema", "minimal_volume"
        )

        result = handle_command(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
        )

        assert result.success
        assert result.data["total_count"] == 1
        assert result.data["volumes"][0]["name"] == "minimal_volume"


class TestAgentListVolumesBehavior:
    """Test list_volumes command behavior when used by agent with tool_output_callback."""

    def test_agent_volume_listing_shows_full_display(self, databricks_client_stub):
        """Agent volume listing shows full volume list display."""
        # Setup comprehensive volume data
        databricks_client_stub.add_volume(
            catalog_name="analytics_catalog",
            schema_name="storage_schema",
            volume_name="ml_datasets",
            volume_type="EXTERNAL",
            comment="Machine learning datasets storage",
            owner="ml-team@company.com",
            created_at="2023-06-15T10:30:00Z",
        )
        databricks_client_stub.add_volume(
            catalog_name="analytics_catalog",
            schema_name="storage_schema",
            volume_name="processed_data",
            volume_type="MANAGED",
            comment="Processed analytical data",
            owner="analytics-team@company.com",
        )

        # Capture progress during agent execution
        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append((tool_name, data))

        # Execute with tool_output_callback (agent mode)
        result = handle_command(
            databricks_client_stub,
            catalog_name="analytics_catalog",
            schema_name="storage_schema",
            tool_output_callback=capture_progress,
        )

        # Verify command success
        assert result.success
        assert result.data["total_count"] == 2
        assert len(result.data["volumes"]) == 2

        # Since agent_display="full", there should be no progress steps
        # (full display happens in TUI layer, not in command handler)
        assert len(progress_steps) == 0

    def test_agent_empty_volume_list_shows_clear_message(self, databricks_client_stub):
        """Agent looking up volumes in empty schema gets clear message."""
        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append((tool_name, data))

        result = handle_command(
            databricks_client_stub,
            catalog_name="empty_catalog",
            schema_name="empty_schema",
            tool_output_callback=capture_progress,
        )

        # Verify success with clear message
        assert result.success
        assert "No volumes found" in result.message
        assert "empty_catalog.empty_schema" in result.message

        # No progress steps for empty result
        assert len(progress_steps) == 0

    def test_agent_uses_active_catalog_and_schema_successfully(
        self, databricks_client_stub, temp_config
    ):
        """Agent volume listing can use active catalog and schema when not specified."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Set active catalog and schema
            temp_config.update(
                active_catalog="agent_catalog", active_schema="agent_schema"
            )

            # Setup volume in active catalog/schema
            databricks_client_stub.add_volume(
                catalog_name="agent_catalog",
                schema_name="agent_schema",
                volume_name="agent_volume",
                comment="Volume for agent testing",
            )

            progress_steps = []

            def capture_progress(tool_name, data):
                progress_steps.append((tool_name, data))

            # Agent calls without specifying catalog/schema (should use active)
            result = handle_command(
                databricks_client_stub, tool_output_callback=capture_progress
            )

            # Verify successful execution using active catalog/schema
            assert result.success
            assert "agent_catalog.agent_schema" in result.message
            assert result.data["catalog_name"] == "agent_catalog"
            assert result.data["schema_name"] == "agent_schema"

    def test_agent_api_error_handling(self, databricks_client_stub):
        """Agent volume listing handles API errors appropriately."""

        # Simulate API failure
        def failing_list_volumes(*args, **kwargs):
            raise Exception("Permission denied: insufficient volume access")

        databricks_client_stub.list_volumes = failing_list_volumes

        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append((tool_name, data))

        result = handle_command(
            databricks_client_stub,
            catalog_name="secure_catalog",
            schema_name="secure_schema",
            tool_output_callback=capture_progress,
        )

        # Verify error is properly communicated
        assert not result.success
        assert "Permission denied" in result.message
        assert result.error is not None

    def test_agent_tool_executor_integration(self, databricks_client_stub):
        """Agent tool executor integration works end-to-end."""
        from chuck_data.agent.tool_executor import execute_tool

        # Setup test volumes
        databricks_client_stub.add_volume(
            catalog_name="integration_catalog",
            schema_name="integration_schema",
            volume_name="integration_volume",
            volume_type="MANAGED",
            comment="Integration test volume",
        )

        result = execute_tool(
            api_client=databricks_client_stub,
            tool_name="list-volumes",
            tool_args={
                "catalog_name": "integration_catalog",
                "schema_name": "integration_schema",
            },
        )

        # Verify agent gets proper result format
        assert "volumes" in result
        assert "total_count" in result
        assert result["total_count"] == 1
        assert result["volumes"][0]["name"] == "integration_volume"


class TestListVolumesCommandConfiguration:
    """Test list_volumes command configuration and registry integration."""

    def test_list_volumes_command_definition_properties(self):
        """List_volumes command definition has correct configuration."""
        from chuck_data.commands.list_volumes import DEFINITION

        assert DEFINITION.name == "list-volumes"
        assert DEFINITION.handler == handle_command
        assert DEFINITION.needs_api_client is True
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True
        assert DEFINITION.agent_display == "full"
        assert (
            len(DEFINITION.required_params) == 0
        )  # No required params (uses active config)
        assert "catalog_name" in DEFINITION.parameters
        assert "schema_name" in DEFINITION.parameters
        assert "include_browse" in DEFINITION.parameters

    def test_list_volumes_command_parameter_requirements(self):
        """List_volumes command has properly configured parameter requirements."""
        from chuck_data.commands.list_volumes import DEFINITION

        # Verify no required parameters (uses active config)
        assert len(DEFINITION.required_params) == 0

        # Verify parameter definitions
        catalog_param = DEFINITION.parameters["catalog_name"]
        assert catalog_param["type"] == "string"
        assert "catalog" in catalog_param["description"].lower()

        schema_param = DEFINITION.parameters["schema_name"]
        assert schema_param["type"] == "string"
        assert "schema" in schema_param["description"].lower()

        browse_param = DEFINITION.parameters["include_browse"]
        assert browse_param["type"] == "boolean"
        assert browse_param["default"] is False


class TestListVolumesDisplayIntegration:
    """Test list_volumes command integration with display system."""

    def test_volume_result_contains_display_ready_data(self, databricks_client_stub):
        """List_volumes command result contains data ready for display formatting."""
        databricks_client_stub.add_volume(
            catalog_name="display_catalog",
            schema_name="display_schema",
            volume_name="display_volume",
            volume_type="EXTERNAL",
            comment="Test volume for display",
            owner="test@company.com",
            created_at="2023-01-01T00:00:00Z",
        )

        result = handle_command(
            databricks_client_stub,
            catalog_name="display_catalog",
            schema_name="display_schema",
        )

        assert result.success

        # Verify data structure matches what display layer expects
        assert "volumes" in result.data
        assert "total_count" in result.data
        assert "catalog_name" in result.data
        assert "schema_name" in result.data

        volumes = result.data["volumes"]
        assert len(volumes) == 1

        # Verify volume structure
        volume = volumes[0]
        assert "name" in volume
        assert "full_name" in volume
        assert "volume_type" in volume
        assert volume["name"] == "display_volume"
        assert volume["volume_type"] == "EXTERNAL"

    def test_volume_empty_fields_handled_gracefully(self, databricks_client_stub):
        """List_volumes command handles empty or missing fields gracefully."""
        # Add volume with some empty fields
        databricks_client_stub.add_volume(
            catalog_name="test_catalog",
            schema_name="test_schema",
            volume_name="sparse_volume",
            comment="",  # Empty comment
            owner=None,  # None owner
        )

        result = handle_command(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
        )

        assert result.success
        assert result.data["total_count"] == 1
        # Should not fail even with empty/None fields
        assert isinstance(result.data["volumes"][0], dict)
        assert result.data["volumes"][0]["name"] == "sparse_volume"

    def test_volume_count_and_message_consistency(self, databricks_client_stub):
        """List_volumes command count and message are consistent."""
        # Add multiple volumes
        for i in range(3):
            databricks_client_stub.add_volume(
                "count_catalog", "count_schema", f"volume_{i}"
            )

        result = handle_command(
            databricks_client_stub,
            catalog_name="count_catalog",
            schema_name="count_schema",
        )

        assert result.success
        assert result.data["total_count"] == 3
        assert len(result.data["volumes"]) == 3
        assert "3 volume(s)" in result.message
        assert "count_catalog.count_schema" in result.message

    def test_volume_full_name_construction(self, databricks_client_stub):
        """List_volumes command correctly constructs full volume names."""
        databricks_client_stub.add_volume("prod_catalog", "storage_schema", "ml_data")

        result = handle_command(
            databricks_client_stub,
            catalog_name="prod_catalog",
            schema_name="storage_schema",
        )

        assert result.success
        volume = result.data["volumes"][0]
        assert volume["full_name"] == "prod_catalog.storage_schema.ml_data"
