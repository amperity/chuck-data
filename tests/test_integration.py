"""Integration tests for the Chuck application."""

import unittest
from unittest.mock import patch
from src.config import (
    set_active_model,
    get_active_model,
    ConfigManager,
)
import os
import json


class TestChuckIntegration(unittest.TestCase):
    """Integration test cases for Chuck application."""

    def setUp(self):
        """Set up the test environment with controlled configuration."""
        # Set up test environment
        self.test_config_path = "/tmp/.test_chuck_integration_config.json"

        # Create a test config manager instance
        self.config_manager = ConfigManager(config_path=self.test_config_path)

        # Replace the global config manager with our test instance
        self.config_manager_patcher = patch(
            "src.config._config_manager", self.config_manager
        )
        self.mock_config_manager = self.config_manager_patcher.start()

        # Mock environment for authentication
        self.env_patcher = patch.dict(
            "os.environ",
            {
                "DATABRICKS_TOKEN": "test_token",
                "DATABRICKS_WORKSPACE_URL": "test-workspace",
            },
        )
        self.env_patcher.start()

        # Initialize the config with workspace_url
        self.config_manager.update(workspace_url="test-workspace")

    def tearDown(self):
        """Clean up the test environment after tests."""
        if os.path.exists(self.test_config_path):
            os.remove(self.test_config_path)
        self.config_manager_patcher.stop()
        self.env_patcher.stop()

    def test_config_operations(self):
        """Test that config operations work properly."""
        # Test writing and reading config
        set_active_model("test-model")

        # Verify the config file was actually created with correct content
        self.assertTrue(os.path.exists(self.test_config_path))
        with open(self.test_config_path, "r") as f:
            saved_config = json.load(f)
        self.assertEqual(saved_config["active_model"], "test-model")

        # Test reading the config
        active_model = get_active_model()
        self.assertEqual(active_model, "test-model")

    def test_catalog_config_operations(self):
        """Test catalog config operations."""
        # Test writing and reading catalog config
        from src.config import set_active_catalog, get_active_catalog

        test_catalog = "test-catalog"
        set_active_catalog(test_catalog)

        # Verify the config file was updated with catalog
        with open(self.test_config_path, "r") as f:
            saved_config = json.load(f)
        self.assertEqual(saved_config["active_catalog"], test_catalog)

        # Test reading the catalog config
        active_catalog = get_active_catalog()
        self.assertEqual(active_catalog, test_catalog)

    def test_schema_config_operations(self):
        """Test schema config operations."""
        # Test writing and reading schema config
        from src.config import set_active_schema, get_active_schema

        test_schema = "test-schema"
        set_active_schema(test_schema)

        # Verify the config file was updated with schema
        with open(self.test_config_path, "r") as f:
            saved_config = json.load(f)
        self.assertEqual(saved_config["active_schema"], test_schema)

        # Test reading the schema config
        active_schema = get_active_schema()
        self.assertEqual(active_schema, test_schema)
