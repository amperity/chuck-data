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

    def test_missing_warehouse_id(self):
        """Test handling when warehouse_id is not provided."""
        result = handle_command(self.client_stub)
        self.assertFalse(result.success)
        self.assertIn("warehouse_id parameter is required", result.message)

    def test_successful_warehouse_selection(self):
        """Test successful warehouse selection."""
        # Set up warehouse in stub
        self.client_stub.add_warehouse(
            "Test Warehouse", state="RUNNING", size="2X-Small"
        )
        # The warehouse_id should be "warehouse_0" based on the stub implementation
        warehouse_id = "warehouse_0"

        # Call function
        result = handle_command(self.client_stub, warehouse_id=warehouse_id)

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
        # Don't add warehouse to stub - will cause verification failure

        # Call function
        result = handle_command(self.client_stub, warehouse_id="nonexistent_warehouse")

        # Verify results - should still succeed but with warning
        self.assertTrue(result.success)
        self.assertIn(
            "Warning: Could not verify warehouse 'nonexistent_warehouse'",
            result.message,
        )
        self.assertEqual(result.data["warehouse_id"], "nonexistent_warehouse")

        # Verify config was still updated
        self.assertEqual(get_warehouse_id(), "nonexistent_warehouse")

    def test_warehouse_selection_no_client(self):
        """Test warehouse selection with no client available."""
        # Call function with no client
        result = handle_command(None, warehouse_id="abc123")

        # Verify results
        self.assertTrue(result.success)
        self.assertIn(
            "Warning: No API client available to verify warehouse 'abc123'",
            result.message,
        )
        self.assertEqual(result.data["warehouse_id"], "abc123")

        # Verify config was updated
        self.assertEqual(get_warehouse_id(), "abc123")

    def test_warehouse_selection_exception(self):
        """Test warehouse selection with unexpected exception."""

        # Create a stub that raises an exception during warehouse verification
        class FailingStub(DatabricksClientStub):
            def get_warehouse(self, warehouse_id):
                raise Exception("Failed to set warehouse")

        failing_stub = FailingStub()

        # Call function
        result = handle_command(failing_stub, warehouse_id="abc123")

        # Should still succeed with warning since it catches the exception
        self.assertTrue(result.success)
        self.assertIn("Warning: Could not verify warehouse", result.message)
