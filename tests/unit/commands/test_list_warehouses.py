"""
Tests for list_warehouses command handler.

This module contains tests for the list_warehouses command handler.
"""

from chuck_data.commands.list_warehouses import handle_command


def test_no_client():
    """Test handling when no client is provided."""
    result = handle_command(None)
    assert not result.success
    assert "No Databricks client available" in result.message


def test_successful_list_warehouses(databricks_client_stub):
    """Test successful warehouse listing with various warehouse types."""
    # Add test warehouses with different configurations
    databricks_client_stub.add_warehouse(
        warehouse_id="warehouse-123",
        name="Test Serverless Warehouse",
        size="XLARGE",
        state="STOPPED",
        enable_serverless_compute=True,
        warehouse_type="PRO",
        creator_name="test.user@example.com",
        auto_stop_mins=10,
    )
    databricks_client_stub.add_warehouse(
        warehouse_id="warehouse-456",
        name="Test Regular Warehouse",
        size="SMALL",
        state="RUNNING",
        enable_serverless_compute=False,
        warehouse_type="CLASSIC",
        creator_name="another.user@example.com",
        auto_stop_mins=60,
    )
    databricks_client_stub.add_warehouse(
        warehouse_id="warehouse-789",
        name="Test XXSMALL Warehouse",
        size="XXSMALL",
        state="STARTING",
        enable_serverless_compute=True,
        warehouse_type="PRO",
        creator_name="third.user@example.com",
        auto_stop_mins=15,
    )

    # Call the function
    result = handle_command(databricks_client_stub)

    # Verify results
    assert result.success
    assert len(result.data["warehouses"]) == 3
    assert result.data["total_count"] == 3
    assert "Found 3 SQL warehouse(s)" in result.message

    # Verify warehouse data structure and content
    warehouses = result.data["warehouses"]
    warehouse_names = [w["name"] for w in warehouses]
    assert "Test Serverless Warehouse" in warehouse_names
    assert "Test Regular Warehouse" in warehouse_names
    assert "Test XXSMALL Warehouse" in warehouse_names

    # Verify specific warehouse details
    serverless_warehouse = next(
        w for w in warehouses if w["name"] == "Test Serverless Warehouse"
    )
    assert serverless_warehouse["id"] == "warehouse-123"
    assert serverless_warehouse["size"] == "XLARGE"
    assert serverless_warehouse["state"] == "STOPPED"
    assert serverless_warehouse["enable_serverless_compute"]
    assert serverless_warehouse["warehouse_type"] == "PRO"
    assert serverless_warehouse["creator_name"] == "test.user@example.com"
    assert serverless_warehouse["auto_stop_mins"] == 10

    regular_warehouse = next(
        w for w in warehouses if w["name"] == "Test Regular Warehouse"
    )
    assert regular_warehouse["id"] == "warehouse-456"
    assert regular_warehouse["size"] == "SMALL"
    assert regular_warehouse["state"] == "RUNNING"
    assert not regular_warehouse["enable_serverless_compute"]
    assert regular_warehouse["warehouse_type"] == "CLASSIC"
    assert regular_warehouse["creator_name"] == "another.user@example.com"
    assert regular_warehouse["auto_stop_mins"] == 60


def test_empty_warehouse_list(databricks_client_stub):
    """Test handling when no warehouses are found."""
    # Don't add any warehouses to the stub

    # Call the function
    result = handle_command(databricks_client_stub)

    # Verify results
    assert result.success
    assert "No SQL warehouses found" in result.message


def test_list_warehouses_exception(databricks_client_stub):
    """Test list_warehouses with unexpected exception."""

    # Configure stub to raise an exception for list_warehouses
    def list_warehouses_failing(**kwargs):
        raise Exception("API connection error")

    databricks_client_stub.list_warehouses = list_warehouses_failing

    # Call the function
    result = handle_command(databricks_client_stub)

    # Verify results
    assert not result.success
    assert "Failed to fetch warehouses" in result.message
    assert str(result.error) == "API connection error"


def test_warehouse_data_integrity(databricks_client_stub):
    """Test that all required warehouse fields are preserved."""
    # Add a warehouse with all possible fields
    databricks_client_stub.add_warehouse(
        warehouse_id="warehouse-complete",
        name="Complete Test Warehouse",
        size="MEDIUM",
        state="STOPPED",
        enable_serverless_compute=True,
        creator_name="complete.user@example.com",
        auto_stop_mins=30,
        # Additional fields that might be present
        cluster_size="Medium",
        min_num_clusters=1,
        max_num_clusters=5,
        warehouse_type="PRO",
        enable_photon=True,
    )

    # Call the function
    result = handle_command(databricks_client_stub)

    # Verify results
    assert result.success
    warehouses = result.data["warehouses"]
    assert len(warehouses) == 1

    warehouse = warehouses[0]
    # Verify all required fields are present
    required_fields = [
        "id",
        "name",
        "size",
        "state",
        "creator_name",
        "auto_stop_mins",
        "enable_serverless_compute",
    ]
    for field in required_fields:
        assert (
            field in warehouse
        ), f"Required field '{field}' missing from warehouse data"

    # Verify field values
    assert warehouse["id"] == "warehouse-complete"
    assert warehouse["name"] == "Complete Test Warehouse"
    assert warehouse["size"] == "MEDIUM"
    assert warehouse["state"] == "STOPPED"
    assert warehouse["enable_serverless_compute"]
    assert warehouse["creator_name"] == "complete.user@example.com"
    assert warehouse["auto_stop_mins"] == 30


def test_various_warehouse_sizes(databricks_client_stub):
    """Test that different warehouse sizes are handled correctly."""
    sizes = [
        "XXSMALL",
        "XSMALL",
        "SMALL",
        "MEDIUM",
        "LARGE",
        "XLARGE",
        "2XLARGE",
        "3XLARGE",
        "4XLARGE",
    ]

    # Add warehouses with different sizes
    for i, size in enumerate(sizes):
        databricks_client_stub.add_warehouse(
            warehouse_id=f"warehouse-{i}",
            name=f"Test {size} Warehouse",
            size=size,
            state="STOPPED",
            enable_serverless_compute=True,
        )

    # Call the function
    result = handle_command(databricks_client_stub)

    # Verify results
    assert result.success
    assert len(result.data["warehouses"]) == len(sizes)

    # Verify all sizes are preserved correctly
    warehouses = result.data["warehouses"]
    returned_sizes = [w["size"] for w in warehouses]
    for size in sizes:
        assert size in returned_sizes, f"Size {size} not found in returned warehouses"


def test_various_warehouse_states(databricks_client_stub):
    """Test that different warehouse states are handled correctly."""
    states = ["RUNNING", "STOPPED", "STARTING", "STOPPING", "DELETING", "DELETED"]

    # Add warehouses with different states
    for i, state in enumerate(states):
        databricks_client_stub.add_warehouse(
            warehouse_id=f"warehouse-{i}",
            name=f"Test {state} Warehouse",
            size="SMALL",
            state=state,
            enable_serverless_compute=False,
        )

    # Call the function
    result = handle_command(databricks_client_stub)

    # Verify results
    assert result.success
    assert len(result.data["warehouses"]) == len(states)

    # Verify all states are preserved correctly
    warehouses = result.data["warehouses"]
    returned_states = [w["state"] for w in warehouses]
    for state in states:
        assert (
            state in returned_states
        ), f"State {state} not found in returned warehouses"


def test_serverless_compute_boolean_handling(databricks_client_stub):
    """Test that serverless compute boolean values are handled correctly."""
    # Add warehouses with different serverless settings
    databricks_client_stub.add_warehouse(
        warehouse_id="warehouse-serverless-true",
        name="Serverless True Warehouse",
        size="SMALL",
        state="STOPPED",
        enable_serverless_compute=True,
    )
    databricks_client_stub.add_warehouse(
        warehouse_id="warehouse-serverless-false",
        name="Serverless False Warehouse",
        size="SMALL",
        state="STOPPED",
        enable_serverless_compute=False,
    )

    # Call the function
    result = handle_command(databricks_client_stub)

    # Verify results
    assert result.success
    warehouses = result.data["warehouses"]
    assert len(warehouses) == 2

    # Find warehouses by name and verify serverless settings
    serverless_true = next(
        w for w in warehouses if w["name"] == "Serverless True Warehouse"
    )
    serverless_false = next(
        w for w in warehouses if w["name"] == "Serverless False Warehouse"
    )

    assert serverless_true["enable_serverless_compute"]
    assert not serverless_false["enable_serverless_compute"]

    # Ensure they're proper boolean values, not strings
    assert isinstance(serverless_true["enable_serverless_compute"], bool)
    assert isinstance(serverless_false["enable_serverless_compute"], bool)


def test_display_parameter_false(databricks_client_stub):
    """Test that display=False parameter works correctly."""
    # Add test warehouse
    databricks_client_stub.add_warehouse(
        warehouse_id="warehouse-test",
        name="Test Warehouse",
        size="SMALL",
        state="RUNNING",
    )

    # Call function with display=False
    result = handle_command(databricks_client_stub, display=False)

    # Verify results
    assert result.success
    assert len(result.data["warehouses"]) == 1
    # Should still include current_warehouse_id for highlighting
    assert "current_warehouse_id" in result.data


def test_display_parameter_false_default(databricks_client_stub):
    """Test that display parameter defaults to False."""
    # Add test warehouse
    databricks_client_stub.add_warehouse(
        warehouse_id="warehouse-test",
        name="Test Warehouse",
        size="SMALL",
        state="RUNNING",
    )

    # Call function without display parameter
    result = handle_command(databricks_client_stub)

    # Verify results
    assert result.success
    assert len(result.data["warehouses"]) == 1
    # Should include current_warehouse_id for highlighting
    assert "current_warehouse_id" in result.data
    # Should default to display=False
    assert not result.data["display"]
