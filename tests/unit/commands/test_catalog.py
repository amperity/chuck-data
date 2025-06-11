"""
Tests for catalog command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
These tests verify what users see when they interact with the catalog command,
both directly and when an agent uses the catalog tool.
"""

import pytest
import tempfile
from unittest.mock import patch

from chuck_data.commands.catalog import handle_command
from chuck_data.config import ConfigManager


class TestCatalogParameterValidation:
    """Test parameter validation for catalog command."""

    def test_missing_name_parameter_returns_error(self, databricks_client_stub):
        """Missing catalog name parameter returns helpful error."""
        result = handle_command(databricks_client_stub)

        assert not result.success
        assert "name" in result.message.lower() or "catalog" in result.message.lower()

    def test_none_client_returns_setup_error(self):
        """None client returns workspace setup error."""
        result = handle_command(None, name="test_catalog")

        assert not result.success
        assert "workspace" in result.message.lower()
        assert "set up" in result.message.lower()


class TestDirectCatalogCommand:
    """Test direct catalog command execution (no tool_output_callback)."""

    def test_direct_command_shows_catalog_details_for_existing_catalog(
        self, databricks_client_stub
    ):
        """Direct catalog command shows detailed information for existing catalog."""
        # Setup test catalog
        databricks_client_stub.add_catalog(
            name="production_catalog",
            catalog_type="MANAGED",
            comment="Production data catalog",
            owner="admin@company.com",
            created_at="2023-01-01T00:00:00Z",
            storage_root="s3://bucket/path",
        )

        result = handle_command(databricks_client_stub, name="production_catalog")

        # Verify successful execution
        assert result.success
        assert "production_catalog" in result.message

        # Verify catalog data is returned
        assert result.data is not None
        assert result.data["name"] == "production_catalog"
        assert result.data["catalog_type"] == "MANAGED"
        assert result.data["comment"] == "Production data catalog"

    def test_direct_command_handles_nonexistent_catalog(self, databricks_client_stub):
        """Direct catalog command shows helpful error for nonexistent catalog."""
        # Don't add any catalogs to databricks_client_stub

        result = handle_command(databricks_client_stub, name="nonexistent_catalog")

        assert not result.success
        assert "nonexistent_catalog" in result.message
        assert "not found" in result.message.lower()

    def test_direct_command_handles_api_errors_gracefully(self, databricks_client_stub):
        """Direct catalog command handles API errors with user-friendly messages."""

        # Make get_catalog raise an exception
        def failing_get_catalog(name):
            raise Exception("API connection timeout")

        databricks_client_stub.get_catalog = failing_get_catalog

        result = handle_command(databricks_client_stub, name="test_catalog")

        assert not result.success
        assert "failed" in result.message.lower()
        assert "API connection timeout" in result.message

    def test_direct_command_with_minimal_catalog_data(self, databricks_client_stub):
        """Direct catalog command works with minimal catalog information."""
        # Add catalog with minimal required fields only
        databricks_client_stub.add_catalog(name="minimal_catalog")

        result = handle_command(databricks_client_stub, name="minimal_catalog")

        assert result.success
        assert result.data["name"] == "minimal_catalog"
        assert "minimal_catalog" in result.message


class TestAgentCatalogBehavior:
    """Test catalog command behavior when used by agent with tool_output_callback."""

    def test_agent_catalog_lookup_shows_full_display(self, databricks_client_stub):
        """Agent catalog lookup shows full catalog details display."""
        # Setup comprehensive catalog data
        databricks_client_stub.add_catalog(
            name="analytics_catalog",
            catalog_type="EXTERNAL",
            comment="Analytics data warehouse",
            owner="data-team@company.com",
            created_at="2023-06-15T10:30:00Z",
            provider={"name": "external_provider"},
            storage_root="abfss://container@storage.dfs.core.windows.net/analytics",
            options={"isolation_mode": "ISOLATED"},
        )

        # Capture progress during agent execution
        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append((tool_name, data))

        # Execute with tool_output_callback (agent mode)
        result = handle_command(
            databricks_client_stub,
            name="analytics_catalog",
            tool_output_callback=capture_progress,
        )

        # Verify command success
        assert result.success
        assert result.data["name"] == "analytics_catalog"
        assert result.data["catalog_type"] == "EXTERNAL"

        # Since agent_display="full", there should be no progress steps
        # (full display happens in TUI layer, not in command handler)
        assert len(progress_steps) == 0

    def test_agent_nonexistent_catalog_shows_clear_error(self, databricks_client_stub):
        """Agent looking up nonexistent catalog gets clear error message."""
        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append((tool_name, data))

        result = handle_command(
            databricks_client_stub,
            name="missing_catalog",
            tool_output_callback=capture_progress,
        )

        # Verify error handling
        assert not result.success
        assert "missing_catalog" in result.message
        assert "not found" in result.message.lower()

        # No progress steps for direct lookup failure
        assert len(progress_steps) == 0

    def test_agent_api_error_handling(self, databricks_client_stub):
        """Agent catalog lookup handles API errors appropriately."""

        # Simulate API failure
        def failing_get_catalog(name):
            raise Exception("Permission denied: insufficient privileges")

        databricks_client_stub.get_catalog = failing_get_catalog

        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append((tool_name, data))

        result = handle_command(
            databricks_client_stub,
            name="restricted_catalog",
            tool_output_callback=capture_progress,
        )

        # Verify error is properly communicated
        assert not result.success
        assert "Permission denied" in result.message
        assert result.error is not None

    def test_agent_tool_executor_integration(self, databricks_client_stub):
        """Agent tool executor integration works end-to-end."""
        from chuck_data.agent.tool_executor import execute_tool

        # Setup test catalog
        databricks_client_stub.add_catalog(
            name="integration_catalog",
            catalog_type="MANAGED",
            comment="Integration test catalog",
        )

        result = execute_tool(
            api_client=databricks_client_stub,
            tool_name="catalog",
            tool_args={"name": "integration_catalog"},
        )

        # Verify agent gets proper result format
        assert "name" in result
        assert result["name"] == "integration_catalog"
        assert "catalog_type" in result
        assert result["catalog_type"] == "MANAGED"


class TestCatalogCommandConfiguration:
    """Test catalog command configuration and registry integration."""

    def test_catalog_command_definition_properties(self):
        """Catalog command definition has correct configuration."""
        from chuck_data.commands.catalog import DEFINITION

        assert DEFINITION.name == "catalog"
        assert DEFINITION.handler == handle_command
        assert DEFINITION.needs_api_client is True
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True
        assert DEFINITION.agent_display == "full"
        assert "name" in DEFINITION.required_params
        assert "name" in DEFINITION.parameters

    def test_catalog_command_parameter_requirements(self):
        """Catalog command has properly configured parameter requirements."""
        from chuck_data.commands.catalog import DEFINITION

        # Verify required parameter
        assert "name" in DEFINITION.required_params

        # Verify parameter definition
        name_param = DEFINITION.parameters["name"]
        assert name_param["type"] == "string"
        assert "catalog" in name_param["description"].lower()


class TestCatalogDisplayIntegration:
    """Test catalog command integration with display system."""

    def test_catalog_result_contains_display_ready_data(self, databricks_client_stub):
        """Catalog command result contains data ready for display formatting."""
        databricks_client_stub.add_catalog(
            name="display_test_catalog",
            catalog_type="MANAGED",
            comment="Test catalog for display",
            owner="test@company.com",
            storage_root="s3://test-bucket/catalog-data",
        )

        result = handle_command(databricks_client_stub, name="display_test_catalog")

        assert result.success

        # Verify data structure matches what display layer expects
        catalog_data = result.data
        assert isinstance(catalog_data, dict)
        assert "name" in catalog_data
        assert "catalog_type" in catalog_data or "type" in catalog_data

        # Verify optional fields are present when available
        if "comment" in catalog_data:
            assert catalog_data["comment"] == "Test catalog for display"
        if "owner" in catalog_data:
            assert catalog_data["owner"] == "test@company.com"

    def test_catalog_empty_fields_handled_gracefully(self, databricks_client_stub):
        """Catalog command handles empty or missing fields gracefully."""
        # Add catalog with some empty fields
        databricks_client_stub.add_catalog(
            name="sparse_catalog",
            catalog_type="MANAGED",
            comment="",  # Empty comment
            owner=None,  # None owner
        )

        result = handle_command(databricks_client_stub, name="sparse_catalog")

        assert result.success
        assert result.data["name"] == "sparse_catalog"
        # Should not fail even with empty/None fields
        assert isinstance(result.data, dict)
