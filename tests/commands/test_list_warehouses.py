"""
Tests for list_warehouses command handler.

This module contains tests for the list_warehouses command handler.
"""

import unittest

from src.commands.list_warehouses import handle_command
from tests.fixtures import DatabricksClientStub


class TestListWarehouses(unittest.TestCase):
    """Tests for list_warehouses command handler."""

    def setUp(self):
        """Set up test fixtures."""
        self.client_stub = DatabricksClientStub()

    def test_no_client(self):
        """Test handling when no client is provided."""
        result = handle_command(None)
        self.assertFalse(result.success)
        self.assertIn("No Databricks client available", result.message)

    def test_successful_list_warehouses(self):
        """Test successful warehouse listing with various warehouse types."""
        # Add test warehouses with different configurations
        self.client_stub.add_warehouse(
            warehouse_id="warehouse-123",
            name="Test Serverless Warehouse",
            size="XLARGE",
            state="STOPPED",
            enable_serverless_compute=True,
            warehouse_type="PRO",
            creator_name="test.user@example.com",
            auto_stop_mins=10,
        )
        self.client_stub.add_warehouse(
            warehouse_id="warehouse-456",
            name="Test Regular Warehouse",
            size="SMALL",
            state="RUNNING",
            enable_serverless_compute=False,
            warehouse_type="CLASSIC",
            creator_name="another.user@example.com",
            auto_stop_mins=60,
        )
        self.client_stub.add_warehouse(
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
        result = handle_command(self.client_stub)

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(len(result.data["warehouses"]), 3)
        self.assertEqual(result.data["total_count"], 3)
        self.assertIn("Found 3 SQL warehouse(s)", result.message)

        # Verify warehouse data structure and content
        warehouses = result.data["warehouses"]
        warehouse_names = [w["name"] for w in warehouses]
        self.assertIn("Test Serverless Warehouse", warehouse_names)
        self.assertIn("Test Regular Warehouse", warehouse_names)
        self.assertIn("Test XXSMALL Warehouse", warehouse_names)

        # Verify specific warehouse details
        serverless_warehouse = next(
            w for w in warehouses if w["name"] == "Test Serverless Warehouse"
        )
        self.assertEqual(serverless_warehouse["id"], "warehouse-123")
        self.assertEqual(serverless_warehouse["size"], "XLARGE")
        self.assertEqual(serverless_warehouse["state"], "STOPPED")
        self.assertEqual(serverless_warehouse["enable_serverless_compute"], True)
        self.assertEqual(serverless_warehouse["warehouse_type"], "PRO")
        self.assertEqual(serverless_warehouse["creator_name"], "test.user@example.com")
        self.assertEqual(serverless_warehouse["auto_stop_mins"], 10)

        regular_warehouse = next(
            w for w in warehouses if w["name"] == "Test Regular Warehouse"
        )
        self.assertEqual(regular_warehouse["id"], "warehouse-456")
        self.assertEqual(regular_warehouse["size"], "SMALL")
        self.assertEqual(regular_warehouse["state"], "RUNNING")
        self.assertEqual(regular_warehouse["enable_serverless_compute"], False)
        self.assertEqual(regular_warehouse["warehouse_type"], "CLASSIC")
        self.assertEqual(regular_warehouse["creator_name"], "another.user@example.com")
        self.assertEqual(regular_warehouse["auto_stop_mins"], 60)

    def test_empty_warehouse_list(self):
        """Test handling when no warehouses are found."""
        # Don't add any warehouses to the stub

        # Call the function
        result = handle_command(self.client_stub)

        # Verify results
        self.assertTrue(result.success)
        self.assertIn("No SQL warehouses found", result.message)

    def test_list_warehouses_exception(self):
        """Test list_warehouses with unexpected exception."""

        # Create a stub that raises an exception for list_warehouses
        class FailingClientStub(DatabricksClientStub):
            def list_warehouses(self, **kwargs):
                raise Exception("API connection error")

        failing_client = FailingClientStub()

        # Call the function
        result = handle_command(failing_client)

        # Verify results
        self.assertFalse(result.success)
        self.assertIn("Failed to fetch warehouses", result.message)
        self.assertEqual(str(result.error), "API connection error")

    def test_warehouse_data_integrity(self):
        """Test that all required warehouse fields are preserved."""
        # Add a warehouse with all possible fields
        self.client_stub.add_warehouse(
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
        result = handle_command(self.client_stub)

        # Verify results
        self.assertTrue(result.success)
        warehouses = result.data["warehouses"]
        self.assertEqual(len(warehouses), 1)

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
            self.assertIn(
                field,
                warehouse,
                f"Required field '{field}' missing from warehouse data",
            )

        # Verify field values
        self.assertEqual(warehouse["id"], "warehouse-complete")
        self.assertEqual(warehouse["name"], "Complete Test Warehouse")
        self.assertEqual(warehouse["size"], "MEDIUM")
        self.assertEqual(warehouse["state"], "STOPPED")
        self.assertEqual(warehouse["enable_serverless_compute"], True)
        self.assertEqual(warehouse["creator_name"], "complete.user@example.com")
        self.assertEqual(warehouse["auto_stop_mins"], 30)

    def test_various_warehouse_sizes(self):
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
            self.client_stub.add_warehouse(
                warehouse_id=f"warehouse-{i}",
                name=f"Test {size} Warehouse",
                size=size,
                state="STOPPED",
                enable_serverless_compute=True,
            )

        # Call the function
        result = handle_command(self.client_stub)

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(len(result.data["warehouses"]), len(sizes))

        # Verify all sizes are preserved correctly
        warehouses = result.data["warehouses"]
        returned_sizes = [w["size"] for w in warehouses]
        for size in sizes:
            self.assertIn(
                size, returned_sizes, f"Size {size} not found in returned warehouses"
            )

    def test_various_warehouse_states(self):
        """Test that different warehouse states are handled correctly."""
        states = ["RUNNING", "STOPPED", "STARTING", "STOPPING", "DELETING", "DELETED"]

        # Add warehouses with different states
        for i, state in enumerate(states):
            self.client_stub.add_warehouse(
                warehouse_id=f"warehouse-{i}",
                name=f"Test {state} Warehouse",
                size="SMALL",
                state=state,
                enable_serverless_compute=False,
            )

        # Call the function
        result = handle_command(self.client_stub)

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(len(result.data["warehouses"]), len(states))

        # Verify all states are preserved correctly
        warehouses = result.data["warehouses"]
        returned_states = [w["state"] for w in warehouses]
        for state in states:
            self.assertIn(
                state,
                returned_states,
                f"State {state} not found in returned warehouses",
            )

    def test_serverless_compute_boolean_handling(self):
        """Test that serverless compute boolean values are handled correctly."""
        # Add warehouses with different serverless settings
        self.client_stub.add_warehouse(
            warehouse_id="warehouse-serverless-true",
            name="Serverless True Warehouse",
            size="SMALL",
            state="STOPPED",
            enable_serverless_compute=True,
        )
        self.client_stub.add_warehouse(
            warehouse_id="warehouse-serverless-false",
            name="Serverless False Warehouse",
            size="SMALL",
            state="STOPPED",
            enable_serverless_compute=False,
        )

        # Call the function
        result = handle_command(self.client_stub)

        # Verify results
        self.assertTrue(result.success)
        warehouses = result.data["warehouses"]
        self.assertEqual(len(warehouses), 2)

        # Find warehouses by name and verify serverless settings
        serverless_true = next(
            w for w in warehouses if w["name"] == "Serverless True Warehouse"
        )
        serverless_false = next(
            w for w in warehouses if w["name"] == "Serverless False Warehouse"
        )

        self.assertTrue(serverless_true["enable_serverless_compute"])
        self.assertFalse(serverless_false["enable_serverless_compute"])

        # Ensure they're proper boolean values, not strings
        self.assertIsInstance(serverless_true["enable_serverless_compute"], bool)
        self.assertIsInstance(serverless_false["enable_serverless_compute"], bool)

    def test_display_parameter_false(self):
        """Test that display=False parameter works correctly."""
        # Add test warehouse
        self.client_stub.add_warehouse(
            warehouse_id="warehouse-test",
            name="Test Warehouse",
            size="SMALL",
            state="RUNNING",
        )

        # Call function with display=False
        result = handle_command(self.client_stub, display=False)

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(len(result.data["warehouses"]), 1)
        # Should still include current_warehouse_id for highlighting
        self.assertIn("current_warehouse_id", result.data)

    def test_display_parameter_false_default(self):
        """Test that display parameter defaults to False."""
        # Add test warehouse
        self.client_stub.add_warehouse(
            warehouse_id="warehouse-test",
            name="Test Warehouse",
            size="SMALL",
            state="RUNNING",
        )

        # Call function without display parameter
        result = handle_command(self.client_stub)

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(len(result.data["warehouses"]), 1)
        # Should include current_warehouse_id for highlighting
        self.assertIn("current_warehouse_id", result.data)
        # Should default to display=False
        self.assertEqual(result.data["display"], False)
