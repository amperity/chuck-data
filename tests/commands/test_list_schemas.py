"""
Tests for schema commands including list-schemas and select-schema.
"""

import unittest
import os
import tempfile
from unittest.mock import patch

from chuck_data.commands.list_schemas import handle_command as list_schemas_handler
from chuck_data.commands.schema_selection import handle_command as select_schema_handler
from chuck_data.config import ConfigManager, get_active_schema, set_active_catalog
from tests.fixtures import DatabricksClientStub


class TestSchemaCommands(unittest.TestCase):
    """Tests for schema-related commands."""

    def setUp(self):
        """Set up test fixtures."""
        self.client_stub = DatabricksClientStub()

        # Set up config management
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.temp_dir.name, "test_config.json")
        self.config_manager = ConfigManager(self.config_path)
        self.patcher = patch("chuck_data.config._config_manager", self.config_manager)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        self.temp_dir.cleanup()

    # Tests for list-schemas command
    def test_list_schemas_with_display_true(self):
        """Test list schemas with display=true shows table."""
        # Set up test data
        set_active_catalog("test_catalog")
        self.client_stub.add_catalog("test_catalog")
        self.client_stub.add_schema("test_catalog", "test_schema")

        result = list_schemas_handler(self.client_stub, display=True)

        self.assertTrue(result.success)
        self.assertTrue(result.data.get("display"))
        self.assertEqual(len(result.data.get("schemas", [])), 1)
        self.assertEqual(result.data["schemas"][0]["name"], "test_schema")

    def test_list_schemas_with_display_false(self):
        """Test list schemas with display=false returns data without display."""
        # Set up test data
        set_active_catalog("test_catalog")
        self.client_stub.add_catalog("test_catalog")
        self.client_stub.add_schema("test_catalog", "test_schema")

        result = list_schemas_handler(self.client_stub, display=False)

        self.assertTrue(result.success)
        self.assertFalse(result.data.get("display"))
        self.assertEqual(len(result.data.get("schemas", [])), 1)

    def test_list_schemas_no_active_catalog(self):
        """Test list schemas when no active catalog is set."""
        result = list_schemas_handler(self.client_stub)

        self.assertFalse(result.success)
        self.assertIn(
            "No catalog specified and no active catalog selected", result.message
        )

    def test_list_schemas_empty_catalog(self):
        """Test list schemas with empty catalog."""
        set_active_catalog("empty_catalog")
        self.client_stub.add_catalog("empty_catalog")

        result = list_schemas_handler(self.client_stub, display=True)

        self.assertTrue(result.success)
        self.assertEqual(len(result.data.get("schemas", [])), 0)
        self.assertTrue(result.data.get("display"))

    # Tests for select-schema command
    def test_select_schema_by_name(self):
        """Test schema selection by name."""
        set_active_catalog("test_catalog")
        self.client_stub.add_catalog("test_catalog")
        self.client_stub.add_schema("test_catalog", "test_schema")

        result = select_schema_handler(self.client_stub, schema="test_schema")

        self.assertTrue(result.success)
        self.assertIn("Active schema is now set to 'test_schema'", result.message)
        self.assertEqual(get_active_schema(), "test_schema")

    def test_select_schema_fuzzy_matching(self):
        """Test schema selection with fuzzy matching."""
        set_active_catalog("test_catalog")
        self.client_stub.add_catalog("test_catalog")
        self.client_stub.add_schema("test_catalog", "test_schema_long_name")

        result = select_schema_handler(self.client_stub, schema="test")

        self.assertTrue(result.success)
        self.assertIn("test_schema_long_name", result.message)
        self.assertEqual(get_active_schema(), "test_schema_long_name")

    def test_select_schema_no_match(self):
        """Test schema selection with no matching schema."""
        set_active_catalog("test_catalog")
        self.client_stub.add_catalog("test_catalog")
        self.client_stub.add_schema("test_catalog", "different_schema")

        result = select_schema_handler(self.client_stub, schema="nonexistent")

        self.assertFalse(result.success)
        self.assertIn("No schema found matching 'nonexistent'", result.message)
        self.assertIn("Available schemas:", result.message)

    def test_select_schema_missing_parameter(self):
        """Test schema selection with missing schema parameter."""
        result = select_schema_handler(self.client_stub)

        self.assertFalse(result.success)
        self.assertIn("schema parameter is required", result.message)

    def test_select_schema_no_active_catalog(self):
        """Test schema selection with no active catalog."""
        result = select_schema_handler(self.client_stub, schema="test_schema")

        self.assertFalse(result.success)
        self.assertIn("No active catalog selected", result.message)

    def test_select_schema_tool_output_callback(self):
        """Test schema selection with tool output callback."""
        set_active_catalog("test_catalog")
        self.client_stub.add_catalog("test_catalog")
        self.client_stub.add_schema("test_catalog", "test_schema_with_callback")

        # Mock callback to capture output
        callback_calls = []

        def mock_callback(tool_name, data):
            callback_calls.append((tool_name, data))

        result = select_schema_handler(
            self.client_stub, schema="callback", tool_output_callback=mock_callback
        )

        self.assertTrue(result.success)
        # Should have called the callback with step information
        self.assertTrue(len(callback_calls) > 0)
        self.assertEqual(callback_calls[0][0], "select-schema")
