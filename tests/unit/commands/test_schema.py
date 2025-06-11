"""
Tests for schema command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
These tests verify what users see when they interact with the schema command,
both directly and when an agent uses the schema tool.
"""

import pytest
import tempfile
from unittest.mock import patch

from chuck_data.commands.schema import handle_command
from chuck_data.config import ConfigManager


class TestSchemaParameterValidation:
    """Test parameter validation for schema command."""

    def test_missing_name_parameter_returns_error(self, databricks_client_stub):
        """Missing schema name parameter returns helpful error."""
        result = handle_command(databricks_client_stub)

        assert not result.success
        assert "name" in result.message.lower() or "schema" in result.message.lower()

    def test_none_client_returns_setup_error(self):
        """None client returns workspace setup error."""
        result = handle_command(None, name="test_schema")

        assert not result.success
        assert "workspace" in result.message.lower()
        assert "set up" in result.message.lower()

    def test_missing_catalog_without_active_catalog_returns_error(
        self, databricks_client_stub
    ):
        """Missing catalog parameter without active catalog returns helpful error."""
        with patch("chuck_data.commands.schema.get_active_catalog", return_value=None):
            result = handle_command(databricks_client_stub, name="test_schema")

            assert not result.success
            assert "catalog" in result.message.lower()
            assert "active catalog" in result.message.lower()


class TestDirectSchemaCommand:
    """Test direct schema command execution (no tool_output_callback)."""

    def test_direct_command_shows_schema_details_for_existing_schema(
        self, databricks_client_stub, temp_config
    ):
        """Direct schema command shows detailed information for existing schema."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup test schema
            databricks_client_stub.add_schema(
                catalog_name="production",
                schema_name="analytics",
                comment="Analytics data schema",
                owner="data-team@company.com",
                created_at="2023-01-01T00:00:00Z",
                storage_location="s3://bucket/analytics/",
            )

            result = handle_command(
                databricks_client_stub, name="analytics", catalog_name="production"
            )

            # Verify successful execution
            assert result.success
            assert "production.analytics" in result.message

            # Verify schema data is returned
            assert result.data is not None
            assert result.data["name"] == "analytics"
            assert result.data["catalog_name"] == "production"
            assert result.data["comment"] == "Analytics data schema"

    def test_direct_command_uses_active_catalog_when_not_specified(
        self, databricks_client_stub, temp_config
    ):
        """Direct schema command uses active catalog when catalog_name not provided."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Set active catalog
            temp_config.update(active_catalog="default_catalog")

            # Setup test schema in the active catalog
            databricks_client_stub.add_schema(
                catalog_name="default_catalog",
                schema_name="test_schema",
                comment="Test schema in active catalog",
            )

            result = handle_command(databricks_client_stub, name="test_schema")

            # Verify successful execution using active catalog
            assert result.success
            assert "default_catalog.test_schema" in result.message
            assert result.data["catalog_name"] == "default_catalog"

    def test_direct_command_handles_nonexistent_schema(self, databricks_client_stub):
        """Direct schema command shows helpful error for nonexistent schema."""
        # Don't add any schemas to databricks_client_stub

        result = handle_command(
            databricks_client_stub,
            name="nonexistent_schema",
            catalog_name="test_catalog",
        )

        assert not result.success
        assert "nonexistent_schema" in result.message
        assert "test_catalog" in result.message
        assert "not found" in result.message.lower()

    def test_direct_command_handles_api_errors_gracefully(self, databricks_client_stub):
        """Direct schema command handles API errors with user-friendly messages."""

        # Make get_schema raise an exception
        def failing_get_schema(full_name):
            raise Exception("API timeout occurred")

        databricks_client_stub.get_schema = failing_get_schema

        result = handle_command(
            databricks_client_stub, name="test_schema", catalog_name="test_catalog"
        )

        assert not result.success
        assert "failed" in result.message.lower()
        assert "API timeout occurred" in result.message

    def test_direct_command_with_minimal_schema_data(self, databricks_client_stub):
        """Direct schema command works with minimal schema information."""
        # Add schema with minimal required fields only
        databricks_client_stub.add_schema("test_catalog", "minimal_schema")

        result = handle_command(
            databricks_client_stub, name="minimal_schema", catalog_name="test_catalog"
        )

        assert result.success
        assert result.data["name"] == "minimal_schema"
        assert result.data["catalog_name"] == "test_catalog"


class TestAgentSchemaBehavior:
    """Test schema command behavior when used by agent with tool_output_callback."""

    def test_agent_schema_lookup_shows_full_display(self, databricks_client_stub):
        """Agent schema lookup shows full schema details display."""
        # Setup comprehensive schema data
        databricks_client_stub.add_schema(
            catalog_name="analytics_catalog",
            schema_name="customer_data",
            comment="Customer data warehouse schema",
            owner="data-engineering@company.com",
            created_at="2023-06-15T10:30:00Z",
            storage_location="abfss://container@storage.dfs.core.windows.net/customer",
            properties={"environment": "production", "retention_days": "365"},
        )

        # Capture progress during agent execution
        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append((tool_name, data))

        # Execute with tool_output_callback (agent mode)
        result = handle_command(
            databricks_client_stub,
            name="customer_data",
            catalog_name="analytics_catalog",
            tool_output_callback=capture_progress,
        )

        # Verify command success
        assert result.success
        assert result.data["name"] == "customer_data"
        assert result.data["catalog_name"] == "analytics_catalog"

        # Since agent_display="full", there should be no progress steps
        # (full display happens in TUI layer, not in command handler)
        assert len(progress_steps) == 0

    def test_agent_nonexistent_schema_shows_clear_error(self, databricks_client_stub):
        """Agent looking up nonexistent schema gets clear error message."""
        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append((tool_name, data))

        result = handle_command(
            databricks_client_stub,
            name="missing_schema",
            catalog_name="test_catalog",
            tool_output_callback=capture_progress,
        )

        # Verify error handling
        assert not result.success
        assert "missing_schema" in result.message
        assert "test_catalog" in result.message
        assert "not found" in result.message.lower()

        # No progress steps for direct lookup failure
        assert len(progress_steps) == 0

    def test_agent_uses_active_catalog_successfully(
        self, databricks_client_stub, temp_config
    ):
        """Agent schema lookup can use active catalog when not specified."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Set active catalog
            temp_config.update(active_catalog="agent_catalog")

            # Setup schema in active catalog
            databricks_client_stub.add_schema(
                catalog_name="agent_catalog",
                schema_name="agent_schema",
                comment="Schema for agent testing",
            )

            progress_steps = []

            def capture_progress(tool_name, data):
                progress_steps.append((tool_name, data))

            # Agent calls without specifying catalog (should use active)
            result = handle_command(
                databricks_client_stub,
                name="agent_schema",
                tool_output_callback=capture_progress,
            )

            # Verify successful execution using active catalog
            assert result.success
            assert "agent_catalog.agent_schema" in result.message
            assert result.data["catalog_name"] == "agent_catalog"

    def test_agent_api_error_handling(self, databricks_client_stub):
        """Agent schema lookup handles API errors appropriately."""

        # Simulate API failure
        def failing_get_schema(full_name):
            raise Exception("Permission denied: insufficient schema privileges")

        databricks_client_stub.get_schema = failing_get_schema

        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append((tool_name, data))

        result = handle_command(
            databricks_client_stub,
            name="restricted_schema",
            catalog_name="secure_catalog",
            tool_output_callback=capture_progress,
        )

        # Verify error is properly communicated
        assert not result.success
        assert "Permission denied" in result.message
        assert result.error is not None

    def test_agent_tool_executor_integration(self, databricks_client_stub):
        """Agent tool executor integration works end-to-end."""
        from chuck_data.agent.tool_executor import execute_tool

        # Setup test schema
        databricks_client_stub.add_schema(
            catalog_name="integration_catalog",
            schema_name="integration_schema",
            comment="Integration test schema",
        )

        result = execute_tool(
            api_client=databricks_client_stub,
            tool_name="schema",
            tool_args={
                "name": "integration_schema",
                "catalog_name": "integration_catalog",
            },
        )

        # Verify agent gets proper result format
        assert "name" in result
        assert result["name"] == "integration_schema"
        assert "catalog_name" in result
        assert result["catalog_name"] == "integration_catalog"


class TestSchemaCommandConfiguration:
    """Test schema command configuration and registry integration."""

    def test_schema_command_definition_properties(self):
        """Schema command definition has correct configuration."""
        from chuck_data.commands.schema import DEFINITION

        assert DEFINITION.name == "schema"
        assert DEFINITION.handler == handle_command
        assert DEFINITION.needs_api_client is True
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True
        assert DEFINITION.agent_display == "full"
        assert "name" in DEFINITION.required_params
        assert "name" in DEFINITION.parameters
        assert "catalog_name" in DEFINITION.parameters

    def test_schema_command_parameter_requirements(self):
        """Schema command has properly configured parameter requirements."""
        from chuck_data.commands.schema import DEFINITION

        # Verify required parameter
        assert "name" in DEFINITION.required_params
        assert "catalog_name" not in DEFINITION.required_params  # Optional

        # Verify parameter definitions
        name_param = DEFINITION.parameters["name"]
        assert name_param["type"] == "string"
        assert "schema" in name_param["description"].lower()

        catalog_param = DEFINITION.parameters["catalog_name"]
        assert catalog_param["type"] == "string"
        assert "catalog" in catalog_param["description"].lower()


class TestSchemaDisplayIntegration:
    """Test schema command integration with display system."""

    def test_schema_result_contains_display_ready_data(self, databricks_client_stub):
        """Schema command result contains data ready for display formatting."""
        databricks_client_stub.add_schema(
            catalog_name="display_catalog",
            schema_name="display_schema",
            comment="Test schema for display",
            owner="test@company.com",
            storage_location="s3://test-bucket/schema-data",
            properties={"env": "test"},
        )

        result = handle_command(
            databricks_client_stub,
            name="display_schema",
            catalog_name="display_catalog",
        )

        assert result.success

        # Verify data structure matches what display layer expects
        schema_data = result.data
        assert isinstance(schema_data, dict)
        assert "name" in schema_data
        assert "catalog_name" in schema_data

        # Verify optional fields are present when available
        if "comment" in schema_data:
            assert schema_data["comment"] == "Test schema for display"
        if "owner" in schema_data:
            assert schema_data["owner"] == "test@company.com"

    def test_schema_empty_fields_handled_gracefully(self, databricks_client_stub):
        """Schema command handles empty or missing fields gracefully."""
        # Add schema with some empty fields
        databricks_client_stub.add_schema(
            catalog_name="test_catalog",
            schema_name="sparse_schema",
            comment="",  # Empty comment
            owner=None,  # None owner
        )

        result = handle_command(
            databricks_client_stub, name="sparse_schema", catalog_name="test_catalog"
        )

        assert result.success
        assert result.data["name"] == "sparse_schema"
        assert result.data["catalog_name"] == "test_catalog"
        # Should not fail even with empty/None fields
        assert isinstance(result.data, dict)

    def test_schema_full_name_construction(self, databricks_client_stub):
        """Schema command correctly constructs full schema names."""
        databricks_client_stub.add_schema("prod_catalog", "user_events")

        result = handle_command(
            databricks_client_stub, name="user_events", catalog_name="prod_catalog"
        )

        assert result.success
        assert "prod_catalog.user_events" in result.message
