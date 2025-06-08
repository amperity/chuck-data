"""
Tests for list_warehouses command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
"""

from chuck_data.commands.list_warehouses import handle_command


# Parameter validation tests
def test_no_client_returns_error():
    """Command with no client returns error."""
    result = handle_command(None)
    assert not result.success
    assert "No Databricks client available" in result.message


# Command execution tests
def test_successful_list_warehouses_shows_count(databricks_client_stub):
    """Command successfully lists warehouses and shows count."""
    # Add test warehouses
    databricks_client_stub.add_warehouse(
        name="Production Warehouse", state="RUNNING", size="LARGE"
    )
    databricks_client_stub.add_warehouse(
        name="Development Warehouse", state="STOPPED", size="SMALL"
    )
    databricks_client_stub.add_warehouse(
        name="Staging Warehouse", state="RUNNING", size="MEDIUM"
    )

    # Execute command
    result = handle_command(databricks_client_stub)

    # Verify behavioral outcome
    assert result.success
    assert "Found 3 SQL warehouse(s)" in result.message
    assert len(result.data["warehouses"]) == 3

    # Verify warehouse names are included
    warehouse_names = [w["name"] for w in result.data["warehouses"]]
    assert "Production Warehouse" in warehouse_names
    assert "Development Warehouse" in warehouse_names
    assert "Staging Warehouse" in warehouse_names


def test_empty_warehouse_list_shows_appropriate_message(databricks_client_stub):
    """Command with no warehouses shows appropriate message."""
    # Don't add any warehouses to the stub

    # Execute command
    result = handle_command(databricks_client_stub)

    # Verify behavioral outcome
    assert result.success
    assert "No SQL warehouses found" in result.message
    assert len(result.data["warehouses"]) == 0


def test_api_errors_handled_gracefully(databricks_client_stub):
    """Command handles API errors gracefully."""

    # Configure stub to raise an exception
    def list_warehouses_failing(**kwargs):
        raise Exception("API connection error")

    databricks_client_stub.list_warehouses = list_warehouses_failing

    # Execute command
    result = handle_command(databricks_client_stub)

    # Verify graceful error handling
    assert not result.success
    assert "Failed to fetch warehouses" in result.message


def test_display_parameter_controls_behavior(databricks_client_stub):
    """Display parameter controls command behavior."""
    # Add test warehouse
    databricks_client_stub.add_warehouse(
        name="Test Warehouse", state="RUNNING", size="SMALL"
    )

    # Execute command with display=True
    result_with_display = handle_command(databricks_client_stub, display=True)

    # Execute command with display=False (default)
    result_without_display = handle_command(databricks_client_stub, display=False)

    # Verify both succeed but have different display behavior
    assert result_with_display.success
    assert result_without_display.success
    assert result_with_display.data["display"] is True
    assert result_without_display.data["display"] is False

    # Both should contain warehouse data
    assert len(result_with_display.data["warehouses"]) == 1
    assert len(result_without_display.data["warehouses"]) == 1


def test_includes_current_warehouse_context(databricks_client_stub):
    """Command includes current warehouse context for highlighting."""
    # Add test warehouse
    databricks_client_stub.add_warehouse(
        name="Test Warehouse", state="RUNNING", size="SMALL"
    )

    # Execute command
    result = handle_command(databricks_client_stub)

    # Verify context is included
    assert result.success
    assert "current_warehouse_id" in result.data
