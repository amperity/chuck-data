"""
Tests for warehouse_selection command handler.

This module contains tests for the warehouse selection command handler.
"""

import unittest
import os
import tempfile
from unittest.mock import patch

from src.commands.warehouse_selection import handle_command
from src.config import ConfigManager, get_warehouse_id
from tests.fixtures import DatabricksClientStub


class TestWarehouseSelection(unittest.TestCase):
    """Tests for warehouse selection command handler."""

    def setUp(self):
        """Set up test fixtures."""
        self.client_stub = DatabricksClientStub()

        # Set up config management
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.temp_dir.name, "test_config.json")
        self.config_manager = ConfigManager(self.config_path)
        self.patcher = patch("src.config._config_manager", self.config_manager)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        self.temp_dir.cleanup()

    def test_missing_warehouse_parameter(self):
        """Test handling when warehouse parameter is not provided."""
        result = handle_command(self.client_stub)
        self.assertFalse(result.success)
        self.assertIn(
            "warehouse parameter is required",
            result.message,
        )

    def test_successful_warehouse_selection_by_id(self):
        """Test successful warehouse selection by ID."""
        # Set up warehouse in stub
        self.client_stub.add_warehouse(
            name="Test Warehouse", state="RUNNING", size="2X-Small"
        )
        # The warehouse_id should be "warehouse_0" based on the stub implementation
        warehouse_id = "warehouse_0"

        # Call function with warehouse ID
        result = handle_command(self.client_stub, warehouse=warehouse_id)

        # Verify results
        self.assertTrue(result.success)
        self.assertIn(
            "Active SQL warehouse is now set to 'Test Warehouse'", result.message
        )
        self.assertIn(f"(ID: {warehouse_id}", result.message)
        self.assertIn("State: RUNNING", result.message)
        self.assertEqual(result.data["warehouse_id"], warehouse_id)
        self.assertEqual(result.data["warehouse_name"], "Test Warehouse")
        self.assertEqual(result.data["state"], "RUNNING")

        # Verify config was updated
        self.assertEqual(get_warehouse_id(), warehouse_id)

    def test_warehouse_selection_with_verification_failure(self):
        """Test warehouse selection when verification fails."""
        # Add a warehouse to stub but call with different ID - will cause verification failure
        self.client_stub.add_warehouse(
            name="Production Warehouse", state="RUNNING", size="2X-Small"
        )

        # Call function with non-existent warehouse ID that won't match by name
        result = handle_command(
            self.client_stub, warehouse="xyz-completely-different-name"
        )

        # Verify results - should now fail when warehouse is not found
        self.assertFalse(result.success)
        self.assertIn(
            "No warehouse found matching 'xyz-completely-different-name'",
            result.message,
        )

    def test_warehouse_selection_no_client(self):
        """Test warehouse selection with no client available."""
        # Call function with no client
        result = handle_command(None, warehouse="abc123")

        # Verify results - should now fail when no client is available
        self.assertFalse(result.success)
        self.assertIn(
            "No API client available to verify warehouse",
            result.message,
        )

    def test_warehouse_selection_exception(self):
        """Test warehouse selection with unexpected exception."""

        # Create a stub that raises an exception during warehouse verification
        class FailingStub(DatabricksClientStub):
            def get_warehouse(self, warehouse_id):
                raise Exception("Failed to set warehouse")

            def list_warehouses(self, **kwargs):
                raise Exception("Failed to list warehouses")

        failing_stub = FailingStub()

        # Call function
        result = handle_command(failing_stub, warehouse="abc123")

        # Should fail when both get_warehouse and list_warehouses fail
        self.assertFalse(result.success)
        self.assertIn("Failed to list warehouses", result.message)

    def test_warehouse_selection_by_name(self):
        """Test warehouse selection by name parameter."""
        # Set up warehouse in stub
        self.client_stub.add_warehouse(
            name="Test Warehouse", state="RUNNING", size="2X-Small"
        )

        # Call function with warehouse name
        result = handle_command(self.client_stub, warehouse="Test Warehouse")

        # Verify results
        self.assertTrue(result.success)
        self.assertIn(
            "Active SQL warehouse is now set to 'Test Warehouse'", result.message
        )
        self.assertEqual(result.data["warehouse_name"], "Test Warehouse")

    def test_warehouse_selection_fuzzy_matching(self):
        """Test warehouse selection with fuzzy name matching."""
        # Set up warehouse in stub
        self.client_stub.add_warehouse(
            name="Starter Warehouse", state="RUNNING", size="2X-Small"
        )

        # Call function with partial name match
        result = handle_command(self.client_stub, warehouse="Starter")

        # Verify results
        self.assertTrue(result.success)
        self.assertIn(
            "Active SQL warehouse is now set to 'Starter Warehouse'", result.message
        )
        self.assertEqual(result.data["warehouse_name"], "Starter Warehouse")
