"""
Tests for list_catalogs command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
These tests verify what users see when they interact with the list_catalogs command,
both directly and when an agent uses the list-catalogs tool.
"""

import pytest
from unittest.mock import patch

from chuck_data.commands.list_catalogs import handle_command
from chuck_data.config import set_active_catalog
from chuck_data.agent.tool_executor import execute_tool


class TestListCatalogsParameterValidation:
    """Test parameter validation for list_catalogs command."""

    def test_none_client_returns_error(self):
        """None client returns error."""
        result = handle_command(None)

        assert not result.success
        assert "No Databricks client available" in result.message
        assert "workspace" in result.message.lower()


class TestDirectListCatalogsCommand:
    """Test direct list_catalogs command execution."""

    def test_direct_command_lists_catalogs_with_display_true(
        self, databricks_client_stub, temp_config
    ):
        """Direct command with display=true returns catalogs with display flag set."""
        with patch("chuck_data.config._config_manager", temp_config):
            databricks_client_stub.add_catalog(
                "production", catalog_type="MANAGED_CATALOG", comment="Production data"
            )
            databricks_client_stub.add_catalog(
                "development",
                catalog_type="DELTASHARING_CATALOG",
                comment="Dev environment",
            )

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            assert result.data.get("display") is True
            assert len(result.data.get("catalogs", [])) == 2
            assert result.data["total_count"] == 2
            assert "Found 2 catalog(s)" in result.message

            # Catalog data is formatted properly
            catalog_names = [c["name"] for c in result.data["catalogs"]]
            assert "production" in catalog_names
            assert "development" in catalog_names

            # Catalog types are properly mapped from catalog_type field
            prod_catalog = next(
                c for c in result.data["catalogs"] if c["name"] == "production"
            )
            dev_catalog = next(
                c for c in result.data["catalogs"] if c["name"] == "development"
            )
            assert prod_catalog["type"] == "MANAGED_CATALOG"
            assert dev_catalog["type"] == "DELTASHARING_CATALOG"

    def test_direct_command_lists_catalogs_with_display_false(
        self, databricks_client_stub, temp_config
    ):
        """Direct command with display=false returns data without display flag."""
        with patch("chuck_data.config._config_manager", temp_config):
            databricks_client_stub.add_catalog("test_catalog", catalog_type="MANAGED")

            result = handle_command(databricks_client_stub, display=False)

            assert result.success
            assert result.data.get("display") is False
            assert len(result.data.get("catalogs", [])) == 1
            assert result.data["catalogs"][0]["name"] == "test_catalog"

    def test_direct_command_includes_current_catalog(
        self, databricks_client_stub, temp_config
    ):
        """Direct command includes current catalog for highlighting purposes."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Set active catalog
            set_active_catalog("current_catalog")

            # Add catalogs including the active one
            databricks_client_stub.add_catalog(
                "current_catalog", catalog_type="MANAGED"
            )
            databricks_client_stub.add_catalog("other_catalog", catalog_type="EXTERNAL")

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            assert result.data["current_catalog"] == "current_catalog"
            assert len(result.data["catalogs"]) == 2

    def test_direct_command_handles_empty_catalog_list(
        self, databricks_client_stub, temp_config
    ):
        """Direct command handles workspace with no catalogs gracefully."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Don't add any catalogs

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            assert len(result.data.get("catalogs", [])) == 0
            assert result.data["total_count"] == 0
            assert "No catalogs found in this workspace" in result.message

    def test_direct_command_includes_catalog_details(
        self, databricks_client_stub, temp_config
    ):
        """Direct command includes detailed catalog information."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add catalog with detailed properties
            databricks_client_stub.add_catalog(
                name="detailed_catalog",
                catalog_type="MANAGED",
                comment="A detailed catalog",
                created_at="2023-01-01T00:00:00Z",
                created_by="creator@example.com",
                owner="owner@example.com",
                provider={"name": "test_provider"},
            )

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            catalog = result.data["catalogs"][0]
            assert catalog["name"] == "detailed_catalog"
            assert catalog["type"] == "MANAGED"
            assert catalog["comment"] == "A detailed catalog"
            assert catalog["created_at"] == "2023-01-01T00:00:00Z"
            assert catalog["created_by"] == "creator@example.com"
            assert catalog["owner"] == "owner@example.com"
            assert catalog["provider"] == "test_provider"

    def test_direct_command_with_pagination_parameters(
        self, databricks_client_stub, temp_config
    ):
        """Direct command supports pagination with max_results and page_token."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add a few catalogs
            for i in range(3):
                databricks_client_stub.add_catalog(f"catalog_{i}")

            # Mock pagination token in response
            original_list_catalogs = databricks_client_stub.list_catalogs

            def mock_list_catalogs(*args, **kwargs):
                result = original_list_catalogs(*args, **kwargs)
                result["next_page_token"] = "next_page_123"
                return result

            databricks_client_stub.list_catalogs = mock_list_catalogs

            # Execute command with pagination parameters
            result = handle_command(
                databricks_client_stub, max_results=2, page_token="current_token"
            )

            # Restore original method
            databricks_client_stub.list_catalogs = original_list_catalogs

            # Verify pagination handling
            assert result.success
            assert result.data["next_page_token"] == "next_page_123"
            assert (
                "More catalogs available with page token: next_page_123"
                in result.message
            )

            # Verify parameters were passed to API
            last_call = databricks_client_stub.list_catalogs_calls[-1]
            assert last_call[1] == 2  # max_results
            assert last_call[2] == "current_token"  # page_token

    def test_direct_command_passes_include_browse_parameter(
        self, databricks_client_stub, temp_config
    ):
        """Direct command passes include_browse parameter to API."""
        with patch("chuck_data.config._config_manager", temp_config):
            databricks_client_stub.add_catalog("test_catalog")

            # Call with include_browse parameter
            result = handle_command(databricks_client_stub, include_browse=True)

            assert result.success
            # Verify the parameter was passed to the API
            assert (
                databricks_client_stub.list_catalogs_calls[-1][0] is True
            )  # include_browse flag

    def test_databricks_api_error_handled_gracefully(
        self, databricks_client_stub, temp_config
    ):
        """Databricks API errors are handled gracefully with helpful messages."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Store the original method and replace it with one that raises an exception
            original_method = databricks_client_stub.list_catalogs

            def failing_list_catalogs(*args, **kwargs):
                raise Exception("Databricks API connection failed")

            databricks_client_stub.list_catalogs = failing_list_catalogs

            try:
                result = handle_command(databricks_client_stub, display=True)
            finally:
                # Restore the original method
                databricks_client_stub.list_catalogs = original_method

            assert not result.success
            assert "Failed to list catalogs" in result.message
            assert "Databricks API connection failed" in result.message


class TestListCatalogsCommandConfiguration:
    """Test list_catalogs command configuration and registry integration."""

    def test_command_definition_properties(self):
        """List_catalogs command definition has correct configuration."""
        from chuck_data.commands.list_catalogs import DEFINITION

        assert DEFINITION.name == "list-catalogs"
        assert "catalogs" in DEFINITION.description.lower()
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True
        assert DEFINITION.agent_display == "conditional"

    def test_command_parameter_definitions(self):
        """List_catalogs command has correct parameter definitions."""
        from chuck_data.commands.list_catalogs import DEFINITION

        parameters = DEFINITION.parameters
        assert "display" in parameters
        assert parameters["display"]["type"] == "boolean"
        assert "include_browse" in parameters
        assert parameters["include_browse"]["type"] == "boolean"
        assert "max_results" in parameters
        assert parameters["max_results"]["type"] == "integer"
        assert "page_token" in parameters
        assert parameters["page_token"]["type"] == "string"

    def test_command_aliases(self):
        """List_catalogs command has expected aliases."""
        from chuck_data.commands.list_catalogs import DEFINITION

        assert "/list-catalogs" in DEFINITION.tui_aliases
        assert "/catalogs" in DEFINITION.tui_aliases

    def test_command_display_condition(self):
        """List_catalogs command has correct display condition logic."""
        from chuck_data.commands.list_catalogs import DEFINITION

        # Should display when display=True
        assert DEFINITION.display_condition({"display": True})
        # Should not display when display=False
        assert not DEFINITION.display_condition({"display": False})
        # Should not display when display is not specified
        assert not DEFINITION.display_condition({})


class TestListCatalogsDisplayIntegration:
    """Test list_catalogs command integration with display system."""

    def test_command_result_contains_display_ready_data(
        self, databricks_client_stub, temp_config
    ):
        """List_catalogs command returns display-ready data structure."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add catalog with display properties
            databricks_client_stub.add_catalog(
                "display_catalog",
                catalog_type="MANAGED",
                comment="Test catalog",
                created_at="2023-01-01T00:00:00Z",
                created_by="test.user@example.com",
                owner="catalog_owner",
                provider={"name": "databricks"},
            )

            result = handle_command(databricks_client_stub, display=True)

            # Verify data structure is display-ready
            assert result.success
            assert isinstance(result.data, dict)
            assert "catalogs" in result.data
            assert isinstance(result.data["catalogs"], list)
            assert result.data["total_count"] == 1

            # Ensure catalog data has expected display fields
            catalog = result.data["catalogs"][0]
            assert "name" in catalog
            assert "type" in catalog
            assert "comment" in catalog
            assert "created_at" in catalog
            assert "created_by" in catalog
            assert "owner" in catalog
            assert "provider" in catalog

    def test_command_message_formatting(self, databricks_client_stub, temp_config):
        """List_catalogs command formats success message correctly."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Test with no catalogs
            result1 = handle_command(databricks_client_stub)
            assert "No catalogs found in this workspace" in result1.message

            # Add catalogs and test count message
            databricks_client_stub.add_catalog("catalog1")
            databricks_client_stub.add_catalog("catalog2")

            result2 = handle_command(databricks_client_stub)
            assert "Found 2 catalog(s)" in result2.message


class TestListCatalogsAgentBehavior:
    """Test list_catalogs command agent-specific behavior."""

    def test_agent_default_behavior_without_display_parameter(
        self, databricks_client_stub, temp_config
    ):
        """Agent execution without display parameter uses default behavior (no display)."""
        with patch("chuck_data.config._config_manager", temp_config):
            databricks_client_stub.add_catalog("agent_catalog")

            result = handle_command(databricks_client_stub)

            assert result.success
            assert result.data.get("display") is False
            assert len(result.data.get("catalogs", [])) == 1

    def test_agent_conditional_display_with_display_true(
        self, databricks_client_stub, temp_config
    ):
        """Agent execution with display=true triggers conditional display."""
        with patch("chuck_data.config._config_manager", temp_config):
            databricks_client_stub.add_catalog("display_catalog")

            result = handle_command(databricks_client_stub, display=True)

            assert result.success
            assert result.data.get("display") is True
            assert len(result.data.get("catalogs", [])) == 1

    def test_agent_tool_executor_end_to_end_integration(
        self, databricks_client_stub, temp_config
    ):
        """Agent tool_executor integration works end-to-end."""
        with patch("chuck_data.config._config_manager", temp_config):
            databricks_client_stub.add_catalog("integration_catalog")
            set_active_catalog("integration_catalog")

            result = execute_tool(
                api_client=databricks_client_stub,
                tool_name="list-catalogs",
                tool_args={"display": True},
            )

            # Verify agent gets proper result format
            assert "catalogs" in result
            assert "total_count" in result
            assert "current_catalog" in result
            assert result["current_catalog"] == "integration_catalog"
            assert len(result["catalogs"]) == 1
            assert result["catalogs"][0]["name"] == "integration_catalog"

    def test_agent_with_pagination_parameters(
        self, databricks_client_stub, temp_config
    ):
        """Agent execution with pagination parameters passes them correctly."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add a few catalogs
            databricks_client_stub.add_catalog("agent_catalog")

            result = execute_tool(
                api_client=databricks_client_stub,
                tool_name="list-catalogs",
                tool_args={
                    "max_results": 10,
                    "page_token": "agent_token",
                    "include_browse": True,
                },
            )

            # Verify parameters were passed to API
            last_call = databricks_client_stub.list_catalogs_calls[-1]
            assert last_call[0] is True  # include_browse
            assert last_call[1] == 10  # max_results
            assert last_call[2] == "agent_token"  # page_token

    def test_agent_callback_errors_bubble_up_as_command_errors(
        self, databricks_client_stub, temp_config
    ):
        """Agent callback failures bubble up as command errors (list-catalogs doesn't use callbacks)."""
        with patch("chuck_data.config._config_manager", temp_config):
            databricks_client_stub.add_catalog("callback_catalog")

            def failing_callback(tool_name, data):
                raise Exception("Display system crashed")

            # list-catalogs doesn't use tool_output_callback, so this should work normally
            result = handle_command(
                databricks_client_stub,
                display=True,
                tool_output_callback=failing_callback,
            )

            # Should succeed since list-catalogs doesn't use callbacks
            assert result.success
            assert len(result.data.get("catalogs", [])) == 1


class TestListCatalogsEdgeCases:
    """Test edge cases and boundary conditions for list_catalogs command."""

    def test_command_handles_unicode_in_catalog_names(
        self, databricks_client_stub, temp_config
    ):
        """List_catalogs command handles Unicode characters in catalog names."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add catalogs with Unicode in name
            databricks_client_stub.add_catalog("目录")
            databricks_client_stub.add_catalog("üñîçødé_catalog")

            result = handle_command(databricks_client_stub)

            # Verify Unicode handling
            assert result.success
            assert len(result.data["catalogs"]) == 2
            catalog_names = [c["name"] for c in result.data["catalogs"]]
            assert "目录" in catalog_names
            assert "üñîçødé_catalog" in catalog_names

    def test_command_with_catalogs_having_complex_metadata(
        self, databricks_client_stub, temp_config
    ):
        """List_catalogs command handles catalogs with complex metadata."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add catalog with comprehensive metadata
            complex_provider = {
                "name": "complex_provider",
                "recipientProfileName": "shared_profile",
                "shareCredentialsVersion": "1.0",
            }

            complex_properties = {
                "property1": "value1",
                "property2": "value2",
                "options": {"opt1": "val1", "opt2": "val2"},
            }

            databricks_client_stub.add_catalog(
                "metadata_catalog",
                catalog_type="DELTASHARING_CATALOG",
                comment="Catalog with nested properties",
                created_at="2023-01-01T00:00:00Z",
                created_by="test.user@example.com",
                owner="catalog_owner",
                provider=complex_provider,
                storage_root="s3://bucket/path",
                securable_type="CATALOG",
                securable_kind="MANAGED_CATALOG",
                properties=complex_properties,
            )

            result = handle_command(databricks_client_stub)

            # Verify complex metadata handling
            assert result.success
            catalog = result.data["catalogs"][0]
            assert catalog["name"] == "metadata_catalog"
            assert catalog["type"] == "DELTASHARING_CATALOG"
            assert catalog["comment"] == "Catalog with nested properties"
            assert catalog["created_by"] == "test.user@example.com"
            assert catalog["owner"] == "catalog_owner"
            assert catalog["provider"] == "complex_provider"

    def test_command_handles_many_catalogs_efficiently(
        self, databricks_client_stub, temp_config
    ):
        """List_catalogs command handles large numbers of catalogs efficiently."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add many catalogs
            for i in range(50):
                databricks_client_stub.add_catalog(
                    f"catalog_{i}", catalog_type="MANAGED" if i % 2 == 0 else "EXTERNAL"
                )

            result = handle_command(databricks_client_stub)

            # Verify efficient handling
            assert result.success
            assert len(result.data["catalogs"]) == 50
            assert result.data["total_count"] == 50

    def test_real_api_format_mapping(self, temp_config):
        """Test handling of real Databricks API format for catalog types."""

        class RealApiFormatStub:
            def list_catalogs(
                self, include_browse=False, max_results=None, page_token=None
            ):
                return {
                    "catalogs": [
                        {
                            "name": "internal_catalog",
                            "catalog_type": "INTERNAL_CATALOG",
                            "owner": "system",
                        },
                        {
                            "name": "managed_catalog",
                            "catalog_type": "MANAGED_CATALOG",
                            "owner": "admin",
                        },
                        {
                            "name": "sharing_catalog",
                            "catalog_type": "DELTASHARING_CATALOG",
                            "owner": "user",
                        },
                    ]
                }

        with patch("chuck_data.config._config_manager", temp_config):
            real_format_stub = RealApiFormatStub()
            result = handle_command(real_format_stub)

            # Verify correct mapping of catalog types
            assert result.success
            assert len(result.data["catalogs"]) == 3

            catalogs_by_name = {c["name"]: c for c in result.data["catalogs"]}
            assert catalogs_by_name["internal_catalog"]["type"] == "INTERNAL_CATALOG"
            assert catalogs_by_name["managed_catalog"]["type"] == "MANAGED_CATALOG"
            assert catalogs_by_name["sharing_catalog"]["type"] == "DELTASHARING_CATALOG"
