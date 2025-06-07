"""
Tests for list_catalogs command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
"""

from unittest.mock import patch

from chuck_data.commands.list_catalogs import handle_command


def test_missing_databricks_client_returns_error():
    """Missing Databricks client returns clear error."""
    result = handle_command(None)

    assert not result.success
    assert "No Databricks client available" in result.message


def test_direct_command_lists_available_catalogs(databricks_client_stub, temp_config):
    """Direct command lists available catalogs successfully."""
    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.add_catalog(
            "production", catalog_type="MANAGED_CATALOG", comment="Production data"
        )
        databricks_client_stub.add_catalog(
            "development",
            catalog_type="DELTASHARING_CATALOG",
            comment="Dev environment",
        )

        result = handle_command(databricks_client_stub)

        # Command succeeds with catalog data
        assert result.success
        assert "Found 2 catalog(s)" in result.message
        assert result.data["total_count"] == 2
        assert len(result.data["catalogs"]) == 2

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

        # Default behavior: no table display
        assert not result.data.get("display", True)


def test_direct_command_with_display_shows_table(databricks_client_stub, temp_config):
    """Direct command with display=true shows catalog table."""
    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.add_catalog("analytics", catalog_type="MANAGED")

        result = handle_command(databricks_client_stub, display=True)

        # Command succeeds and requests table display
        assert result.success
        assert result.data["display"] is True
        assert len(result.data["catalogs"]) == 1
        assert result.data["catalogs"][0]["name"] == "analytics"


def test_direct_command_handles_empty_workspace():
    """Direct command handles workspace with no catalogs gracefully."""
    from tests.fixtures.databricks.client import DatabricksClientStub

    empty_client_stub = DatabricksClientStub()
    empty_client_stub.catalogs.clear()

    result = handle_command(empty_client_stub)

    # Command succeeds with empty result
    assert result.success
    assert "No catalogs found in this workspace" in result.message
    assert result.data["total_count"] == 0
    assert result.data["catalogs"] == []


def test_direct_command_handles_pagination():
    """Direct command handles paginated results with next page token."""
    from tests.fixtures.databricks.client import DatabricksClientStub

    class PaginatingClientStub(DatabricksClientStub):
        def list_catalogs(
            self, include_browse=False, max_results=None, page_token=None
        ):
            result = super().list_catalogs(include_browse, max_results, page_token)
            if page_token:
                result["next_page_token"] = "next_page_token_123"
            return result

    paginating_stub = PaginatingClientStub()
    paginating_stub.add_catalog("catalog_1", catalog_type="MANAGED")
    paginating_stub.add_catalog("catalog_2", catalog_type="EXTERNAL")

    result = handle_command(paginating_stub, page_token="current_page_token")

    # Command succeeds with pagination info
    assert result.success
    assert result.data["next_page_token"] == "next_page_token_123"
    assert (
        "More catalogs available with page token: next_page_token_123" in result.message
    )


def test_catalog_type_mapping_from_databricks_api():
    """Catalog types are correctly mapped from Databricks API catalog_type field."""
    from tests.fixtures.databricks.client import DatabricksClientStub

    # Create a stub that returns data in the same format as real Databricks API
    class RealApiFormatStub(DatabricksClientStub):
        def list_catalogs(
            self, include_browse=False, max_results=None, page_token=None
        ):
            # Return data like real Databricks API (with catalog_type instead of type)
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

    real_format_stub = RealApiFormatStub()
    result = handle_command(real_format_stub)

    # Command succeeds
    assert result.success
    assert len(result.data["catalogs"]) == 3

    # Catalog types are correctly mapped from API response catalog_type field
    catalogs_by_name = {c["name"]: c for c in result.data["catalogs"]}
    assert catalogs_by_name["internal_catalog"]["type"] == "INTERNAL_CATALOG"
    assert catalogs_by_name["managed_catalog"]["type"] == "MANAGED_CATALOG"
    assert catalogs_by_name["sharing_catalog"]["type"] == "DELTASHARING_CATALOG"


def test_databricks_api_errors_handled_gracefully():
    """Databricks API errors are handled gracefully."""
    from tests.fixtures.databricks.client import DatabricksClientStub

    class FailingClientStub(DatabricksClientStub):
        def list_catalogs(
            self, include_browse=False, max_results=None, page_token=None
        ):
            raise Exception("Databricks API connection failed")

    failing_client = FailingClientStub()

    result = handle_command(failing_client)

    # Command fails gracefully
    assert not result.success
    assert "Failed to list catalogs" in result.message
    assert "Databricks API connection failed" in str(result.error)


# Agent-specific behavioral tests


def test_agent_lists_catalogs_without_display_by_default(
    databricks_client_stub, temp_config
):
    """Agent catalog listing returns data without showing table by default."""
    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.add_catalog("prod_catalog", catalog_type="MANAGED")
        databricks_client_stub.add_catalog("dev_catalog", catalog_type="EXTERNAL")

        # Agent call (no display parameter)
        result = handle_command(databricks_client_stub)

        # Command succeeds with data but no table display
        assert result.success
        assert result.data["total_count"] == 2
        assert not result.data.get("display", True)  # Default: no display

        # Agent gets structured data
        catalog_names = [c["name"] for c in result.data["catalogs"]]
        assert "prod_catalog" in catalog_names
        assert "dev_catalog" in catalog_names


def test_agent_requests_display_when_user_asks_to_see_catalogs(
    databricks_client_stub, temp_config
):
    """Agent requests table display when user explicitly asks to see catalogs."""
    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.add_catalog("warehouse_catalog", catalog_type="MANAGED")

        # Agent call with display=true (user asked to see catalogs)
        result = handle_command(databricks_client_stub, display=True)

        # Command succeeds and shows table
        assert result.success
        assert result.data["display"] is True
        assert len(result.data["catalogs"]) == 1


def test_agent_handles_large_catalog_lists_efficiently(
    databricks_client_stub, temp_config
):
    """Agent handles large catalog lists with proper data structure."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Add many catalogs to test data structure handling
        for i in range(15):
            databricks_client_stub.add_catalog(
                f"tenant_catalog_{i:02d}", catalog_type="MANAGED"
            )

        result = handle_command(databricks_client_stub, max_results=10)

        # Command succeeds with proper data structure
        assert result.success
        assert result.data["total_count"] == 15  # Number of catalogs returned
        assert len(result.data["catalogs"]) == 15  # All catalogs in response

        # Data structure is consistent
        for catalog in result.data["catalogs"]:
            assert "name" in catalog
            assert "type" in catalog
            assert catalog["name"].startswith("tenant_catalog_")

        # max_results parameter is passed to underlying API
        # (actual limiting would be handled by real Databricks API)


def test_agent_tool_executor_end_to_end_integration(
    databricks_client_stub, temp_config
):
    """Agent tool_executor integration works end-to-end."""
    from chuck_data.agent.tool_executor import execute_tool

    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.add_catalog("integration_test", catalog_type="MANAGED")

        # Execute through tool_executor like agent does
        result = execute_tool(
            api_client=databricks_client_stub, tool_name="list-catalogs", tool_args={}
        )

        # Agent gets properly formatted result
        assert "catalogs" in result
        assert "total_count" in result
        assert result["total_count"] == 1
        assert result["catalogs"][0]["name"] == "integration_test"


def test_agent_conditional_display_logic():
    """Agent conditional display works based on display parameter."""
    from chuck_data.commands.list_catalogs import DEFINITION

    # Test display condition function
    display_condition = DEFINITION.display_condition

    # Should show display when display=True
    result_with_display = {"display": True, "catalogs": [{"name": "test"}]}
    assert display_condition(result_with_display) is True

    # Should not show display when display=False
    result_without_display = {"display": False, "catalogs": [{"name": "test"}]}
    assert display_condition(result_without_display) is False

    # Should not show display when display not specified (default)
    result_default = {"catalogs": [{"name": "test"}]}
    assert display_condition(result_default) is False
