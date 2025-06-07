"""
Tests for the warehouses module.
"""

import pytest
from unittest.mock import MagicMock
from chuck_data.warehouses import list_warehouses, get_warehouse, create_warehouse


@pytest.fixture
def client():
    """Mock client fixture."""
    return MagicMock()


@pytest.fixture
def sample_warehouses():
    """Sample warehouses fixture."""
    return [
        {"id": "warehouse-123", "name": "Test Warehouse 1", "state": "RUNNING"},
        {"id": "warehouse-456", "name": "Test Warehouse 2", "state": "STOPPED"},
    ]


def test_list_warehouses(client, sample_warehouses):
    """Test listing warehouses."""
    # Set up mock response
    client.list_warehouses.return_value = sample_warehouses

    # Call the function
    result = list_warehouses(client)

    # Verify the result
    assert result == sample_warehouses
    client.list_warehouses.assert_called_once()


def test_list_warehouses_empty_response(client):
    """Test listing warehouses with empty response."""
    # Set up mock response
    client.list_warehouses.return_value = []

    # Call the function
    result = list_warehouses(client)

    # Verify the result is an empty list
    assert result == []
    client.list_warehouses.assert_called_once()


def test_get_warehouse(client):
    """Test getting a specific warehouse."""
    # Set up mock response
    warehouse_detail = {
        "id": "warehouse-123",
        "name": "Test Warehouse",
        "state": "RUNNING",
    }
    client.get_warehouse.return_value = warehouse_detail

    # Call the function
    result = get_warehouse(client, "warehouse-123")

    # Verify the result
    assert result == warehouse_detail
    client.get_warehouse.assert_called_once_with("warehouse-123")


def test_create_warehouse(client):
    """Test creating a warehouse."""
    # Set up mock response
    new_warehouse = {
        "id": "warehouse-789",
        "name": "New Warehouse",
        "state": "CREATING",
    }
    client.create_warehouse.return_value = new_warehouse

    # Create options for new warehouse
    warehouse_options = {
        "name": "New Warehouse",
        "cluster_size": "Small",
        "auto_stop_mins": 120,
    }

    # Call the function
    result = create_warehouse(client, warehouse_options)

    # Verify the result
    assert result == new_warehouse
    client.create_warehouse.assert_called_once_with(warehouse_options)
