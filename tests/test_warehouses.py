"""
Tests for the warehouses module.
"""

import unittest
from unittest.mock import MagicMock
from src.warehouses import list_warehouses, get_warehouse, create_warehouse


class TestWarehouses(unittest.TestCase):
    """Test cases for the warehouse-related functions."""

    def setUp(self):
        """Set up common test fixtures."""
        self.client = MagicMock()
        self.sample_warehouses = [
            {"id": "warehouse-123", "name": "Test Warehouse 1", "state": "RUNNING"},
            {"id": "warehouse-456", "name": "Test Warehouse 2", "state": "STOPPED"},
        ]

    def test_list_warehouses(self):
        """Test listing warehouses."""
        # Set up mock response
        self.client.list_warehouses.return_value = self.sample_warehouses

        # Call the function
        result = list_warehouses(self.client)

        # Verify the result
        self.assertEqual(result, self.sample_warehouses)
        self.client.list_warehouses.assert_called_once()

    def test_list_warehouses_empty_response(self):
        """Test listing warehouses with empty response."""
        # Set up mock response
        self.client.list_warehouses.return_value = []

        # Call the function
        result = list_warehouses(self.client)

        # Verify the result is an empty list
        self.assertEqual(result, [])
        self.client.list_warehouses.assert_called_once()

    def test_get_warehouse(self):
        """Test getting a specific warehouse."""
        # Set up mock response
        warehouse_detail = {
            "id": "warehouse-123",
            "name": "Test Warehouse",
            "state": "RUNNING",
        }
        self.client.get_warehouse.return_value = warehouse_detail

        # Call the function
        result = get_warehouse(self.client, "warehouse-123")

        # Verify the result
        self.assertEqual(result, warehouse_detail)
        self.client.get_warehouse.assert_called_once_with("warehouse-123")

    def test_create_warehouse(self):
        """Test creating a warehouse."""
        # Set up mock response
        new_warehouse = {
            "id": "warehouse-789",
            "name": "New Warehouse",
            "state": "CREATING",
        }
        self.client.create_warehouse.return_value = new_warehouse

        # Create options for new warehouse
        warehouse_options = {
            "name": "New Warehouse",
            "cluster_size": "Small",
            "auto_stop_mins": 120,
        }

        # Call the function
        result = create_warehouse(self.client, warehouse_options)

        # Verify the result
        self.assertEqual(result, new_warehouse)
        self.client.create_warehouse.assert_called_once_with(warehouse_options)
