"""
Tests for catalog_selection command handler.

Behavioral tests focused on user experience rather than implementation details.
"""

from unittest.mock import patch

from chuck_data.commands.catalog_selection import handle_command
from chuck_data.config import get_active_catalog


def test_missing_catalog_parameter_returns_error(databricks_client_stub, temp_config):
    """Missing catalog parameter returns clear error."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_command(databricks_client_stub)

        assert not result.success
        assert "catalog parameter is required" in result.message


def test_direct_command_selects_existing_catalog(databricks_client_stub, temp_config):
    """Direct command can select existing catalog successfully."""
    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.add_catalog("production", catalog_type="MANAGED")

        result = handle_command(databricks_client_stub, catalog="production")

        # Command succeeds with catalog details
        assert result.success
        assert "Active catalog is now set to 'production'" in result.message
        assert "Type: MANAGED" in result.message

        # Catalog becomes active
        assert get_active_catalog() == "production"


def test_direct_command_failure_shows_limited_available_catalogs(
    databricks_client_stub, temp_config
):
    """Direct command failure shows error with limited available catalogs list."""
    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.add_catalog("xyz", catalog_type="MANAGED")

        result = handle_command(databricks_client_stub, catalog="asdfkjasdf")

        # Command fails with helpful but limited catalog list
        assert not result.success
        assert "No catalog found matching 'asdfkjasdf'" in result.message
        assert "Available catalogs: xyz" in result.message


def test_direct_command_failure_truncates_long_catalog_list(
    databricks_client_stub, temp_config
):
    """Direct command failure truncates very long catalog lists with '... and X more'."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Add many catalogs to test truncation
        for i in range(10):
            databricks_client_stub.add_catalog(
                f"catalog_{i:02d}", catalog_type="MANAGED"
            )

        result = handle_command(databricks_client_stub, catalog="nonexistent")

        # Command fails with truncated catalog list
        assert not result.success
        assert "No catalog found matching 'nonexistent'" in result.message
        assert "Available catalogs:" in result.message
        assert "... and 5 more" in result.message


def test_direct_command_failure_shows_all_catalogs_when_five_or_fewer(
    databricks_client_stub, temp_config
):
    """Direct command failure shows all catalogs when 5 or fewer exist."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Add exactly 5 catalogs (boundary case)
        for i in range(5):
            databricks_client_stub.add_catalog(f"catalog_{i}", catalog_type="MANAGED")

        result = handle_command(databricks_client_stub, catalog="nonexistent")

        # Command fails with all catalogs shown (no truncation)
        assert not result.success
        assert "No catalog found matching 'nonexistent'" in result.message
        assert (
            "Available catalogs: catalog_0, catalog_1, catalog_2, catalog_3, catalog_4"
            in result.message
        )
        assert "... and" not in result.message


def test_direct_command_fuzzy_matching_works(databricks_client_stub, temp_config):
    """Direct command can fuzzy match partial catalog names."""
    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.add_catalog(
            "development_environment", catalog_type="MANAGED"
        )

        result = handle_command(databricks_client_stub, catalog="dev")

        # Command succeeds with fuzzy match
        assert result.success
        assert "development_environment" in result.message
        assert get_active_catalog() == "development_environment"


def test_databricks_api_errors_handled_gracefully(databricks_client_stub, temp_config):
    """Databricks API errors are handled gracefully."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Configure stub to fail on catalog operations
        def failing_get_catalog(catalog_name):
            raise Exception("Databricks connection failed")

        databricks_client_stub.get_catalog = failing_get_catalog

        result = handle_command(databricks_client_stub, catalog="any_catalog")

        # Command fails gracefully
        assert not result.success
        assert "No catalogs found in workspace" in result.message


# Agent-specific behavioral tests


def test_agent_exact_match_shows_no_progress_steps(databricks_client_stub, temp_config):
    """Agent exact catalog match shows no progress steps (direct lookup succeeds)."""
    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.add_catalog("production_data", catalog_type="MANAGED")

        # Capture progress during agent execution
        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append(f"→ Setting catalog: ({data['step']})")

        result = handle_command(
            databricks_client_stub,
            catalog="production_data",
            tool_output_callback=capture_progress,
        )

        # Command succeeds
        assert result.success
        assert get_active_catalog() == "production_data"

        # No progress steps for direct match
        assert len(progress_steps) == 0


def test_agent_fuzzy_match_shows_multiple_progress_steps(
    databricks_client_stub, temp_config
):
    """Agent fuzzy matching shows multiple progress steps during search."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Add catalog but make direct lookup fail
        databricks_client_stub.add_catalog(
            "customer_analytics_prod", catalog_type="MANAGED"
        )

        # Force the search path by making get_catalog return None
        original_get_catalog = databricks_client_stub.get_catalog
        databricks_client_stub.get_catalog = lambda name: None

        # Capture progress during agent execution
        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append(f"→ Setting catalog: ({data['step']})")

        result = handle_command(
            databricks_client_stub,
            catalog="customer",  # Partial match requiring search
            tool_output_callback=capture_progress,
        )

        # Restore original method
        databricks_client_stub.get_catalog = original_get_catalog

        # Command succeeds with fuzzy match
        assert result.success
        assert get_active_catalog() == "customer_analytics_prod"

        # Multiple progress steps shown: looking -> selecting
        assert len(progress_steps) >= 2
        assert any(
            "Looking for catalog matching 'customer'" in step for step in progress_steps
        )
        assert any(
            "Selecting 'customer_analytics_prod'" in step for step in progress_steps
        )


def test_agent_callback_errors_bubble_up_as_command_errors(
    databricks_client_stub, temp_config
):
    """Agent callback failures bubble up as command errors (current behavior)."""
    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.add_catalog("test_catalog", catalog_type="MANAGED")

        # Progress callback that fails
        def failing_progress_callback(tool_name, data):
            raise Exception("Display system crashed")

        # Force the search path by using a name that requires searching
        result = handle_command(
            databricks_client_stub,
            catalog="xyz_nonexistent",  # This will trigger search and callback
            tool_output_callback=failing_progress_callback,
        )

        # Current behavior: callback errors bubble up as command errors
        assert not result.success
        assert "Display system crashed" in result.message


def test_agent_shows_search_progress_before_failure(
    databricks_client_stub, temp_config
):
    """Agent execution shows search progress before catalog not found failure."""
    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.add_catalog("xyz", catalog_type="MANAGED")

        # Force search path by making get_catalog return None
        databricks_client_stub.get_catalog = lambda name: None

        # Capture progress during agent execution
        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append(f"→ Setting catalog: ({data['step']})")

        result = handle_command(
            databricks_client_stub,
            catalog="asdkfjasdfjaweef",  # Name that definitely won't fuzzy match
            tool_output_callback=capture_progress,
        )

        # Command fails with helpful but limited catalog list
        assert not result.success
        assert "No catalog found matching 'asdkfjasdfjaweef'" in result.message
        assert "Available catalogs: xyz" in result.message

        # Progress shown before failure
        assert len(progress_steps) == 1
        assert (
            progress_steps[0]
            == "→ Setting catalog: (Looking for catalog matching 'asdkfjasdfjaweef')"
        )


def test_agent_tool_executor_end_to_end_integration(
    databricks_client_stub, temp_config
):
    """Agent tool_executor integration works end-to-end."""
    from chuck_data.agent.tool_executor import execute_tool

    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.add_catalog("end_to_end_test", catalog_type="MANAGED")

        # Execute through tool_executor like agent does
        result = execute_tool(
            api_client=databricks_client_stub,
            tool_name="select-catalog",
            tool_args={"catalog": "end_to_end_test"},
        )

        # Agent gets properly formatted result
        assert "catalog_name" in result
        assert result["catalog_name"] == "end_to_end_test"
        assert result["catalog_type"] == "MANAGED"

        # Catalog is actually updated
        assert get_active_catalog() == "end_to_end_test"
