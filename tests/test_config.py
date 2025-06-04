"""Tests for the configuration functionality in Chuck."""

import unittest
import os
import json
import tempfile
from unittest.mock import patch

from src.config import (
    ConfigManager,
    get_workspace_url,
    set_workspace_url,
    get_active_model,
    set_active_model,
    get_warehouse_id,
    set_warehouse_id,
    get_active_catalog,
    set_active_catalog,
    get_active_schema,
    set_active_schema,
    get_databricks_token,
    set_databricks_token,
)


class TestPydanticConfig(unittest.TestCase):
    """Test cases for Pydantic-based configuration."""

    def setUp(self):
        """Set up the test environment."""
        # Create a temporary file for testing
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.temp_dir.name, "test_config.json")

        # Create a test-specific config manager
        self.config_manager = ConfigManager(self.config_path)

        # Mock the global config manager
        self.patcher = patch("src.config._config_manager", self.config_manager)
        self.mock_manager = self.patcher.start()

    def tearDown(self):
        """Clean up after tests."""
        self.patcher.stop()
        self.temp_dir.cleanup()

    def test_default_config(self):
        """Test default configuration values."""
        config = self.config_manager.get_config()
        # No longer expecting a specific default workspace URL since we now preserve full URLs
        # and the default might be None until explicitly set
        self.assertIsNone(config.active_model)
        self.assertIsNone(config.warehouse_id)
        self.assertIsNone(config.active_catalog)
        self.assertIsNone(config.active_schema)

    def test_config_update(self):
        """Test updating configuration values."""
        # Mock out environment variables that could interfere
        with patch.dict(os.environ, {}, clear=True):
            # Update values
            self.config_manager.update(
                workspace_url="test-workspace",
                active_model="test-model",
                warehouse_id="test-warehouse",
                active_catalog="test-catalog",
                active_schema="test-schema",
            )

            # Check values were updated in memory
            config = self.config_manager.get_config()
            self.assertEqual(config.workspace_url, "test-workspace")
            self.assertEqual(config.active_model, "test-model")
            self.assertEqual(config.warehouse_id, "test-warehouse")
            self.assertEqual(config.active_catalog, "test-catalog")
            self.assertEqual(config.active_schema, "test-schema")

            # Check file was created
            self.assertTrue(os.path.exists(self.config_path))

            # Check file contents
            with open(self.config_path, "r") as f:
                saved_config = json.load(f)

            self.assertEqual(saved_config["workspace_url"], "test-workspace")
            self.assertEqual(saved_config["active_model"], "test-model")
            self.assertEqual(saved_config["warehouse_id"], "test-warehouse")
            self.assertEqual(saved_config["active_catalog"], "test-catalog")
            self.assertEqual(saved_config["active_schema"], "test-schema")

    def test_config_load_save_cycle(self):
        """Test loading and saving configuration."""
        # Mock out environment variables that could interfere
        with patch.dict(os.environ, {}, clear=True):
            # Set test values
            test_url = (
                "https://test-workspace.cloud.databricks.com"  # Need valid URL string
            )
            test_model = "test-model"
            test_warehouse = "warehouse-id-123"

            # Update config values using the update method
            self.config_manager.update(
                workspace_url=test_url,
                active_model=test_model,
                warehouse_id=test_warehouse,
            )

            # Create a new manager to load from disk
            another_manager = ConfigManager(self.config_path)
            config = another_manager.get_config()

            # Verify saved values were loaded
            self.assertEqual(config.workspace_url, test_url)
            self.assertEqual(config.active_model, test_model)
            self.assertEqual(config.warehouse_id, test_warehouse)

    def test_api_functions(self):
        """Test compatibility API functions."""
        # Mock out environment variable that could interfere
        with patch.dict(os.environ, {}, clear=True):
            # Set values using API functions
            set_workspace_url("api-workspace")
            set_active_model("api-model")
            set_warehouse_id("api-warehouse")
            set_active_catalog("api-catalog")
            set_active_schema("api-schema")

            # Check values using API functions
            self.assertEqual(get_workspace_url(), "api-workspace")
            self.assertEqual(get_active_model(), "api-model")
            self.assertEqual(get_warehouse_id(), "api-warehouse")
            self.assertEqual(get_active_catalog(), "api-catalog")
            self.assertEqual(get_active_schema(), "api-schema")

    def test_environment_override(self):
        """Test environment variable override for all config values."""
        # Start with clean environment, set config values
        with patch.dict(os.environ, {}, clear=True):
            set_workspace_url("config-workspace")
            set_active_model("config-model")
            set_warehouse_id("config-warehouse")
            set_active_catalog("config-catalog")
            set_active_schema("config-schema")

            # Test CHUCK_ prefix environment variables take precedence
            with patch.dict(
                os.environ,
                {
                    "CHUCK_WORKSPACE_URL": "chuck-workspace",
                    "CHUCK_ACTIVE_MODEL": "chuck-model",
                    "CHUCK_WAREHOUSE_ID": "chuck-warehouse",
                    "CHUCK_ACTIVE_CATALOG": "chuck-catalog",
                    "CHUCK_ACTIVE_SCHEMA": "chuck-schema",
                    "CHUCK_USAGE_TRACKING_CONSENT": "true",
                },
            ):
                config = self.config_manager.get_config()
                self.assertEqual(config.workspace_url, "chuck-workspace")
                self.assertEqual(config.active_model, "chuck-model")
                self.assertEqual(config.warehouse_id, "chuck-warehouse")
                self.assertEqual(config.active_catalog, "chuck-catalog")
                self.assertEqual(config.active_schema, "chuck-schema")
                self.assertTrue(config.usage_tracking_consent)

            # Test without environment variables fall back to config
            config = self.config_manager.get_config()
            self.assertEqual(config.workspace_url, "config-workspace")

    def test_graceful_validation(self):
        """Test configuration validation is graceful."""
        # Mock out environment variables that could interfere
        with patch.dict(os.environ, {}, clear=True):
            # Set a valid URL that we'll use for testing
            test_url = "https://valid-workspace.cloud.databricks.com"

            # First test with a valid configuration
            self.config_manager.update(workspace_url=test_url)

            # Verify the URL was saved correctly
            reloaded_config = self.config_manager.get_config()
            self.assertEqual(reloaded_config.workspace_url, test_url)

            # Now test with an empty URL string
            self.config_manager.update(workspace_url="")

            # With empty string, config validation should handle it - either use default or keep empty
            reloaded_config = self.config_manager.get_config()
            # We don't assert exact value because validation might reject empty strings
            self.assertTrue(
                isinstance(reloaded_config.workspace_url, str),
                "Workspace URL should be a string type",
            )

            # Test other fields
            self.config_manager.update(
                workspace_url=test_url,  # Reset to valid URL
                active_model="",
                warehouse_id=None,
            )

            # Verify the values were saved correctly
            reloaded_config = self.config_manager.get_config()
            self.assertEqual(reloaded_config.active_model, "")
            self.assertIsNone(reloaded_config.warehouse_id)

    def test_singleton_pattern(self):
        """Test that ConfigManager follows singleton pattern."""
        # Using same path should return same instance
        test_path = os.path.join(self.temp_dir.name, "singleton_test.json")
        manager1 = ConfigManager(test_path)
        manager2 = ConfigManager(test_path)

        # Same instance when using same path
        self.assertIs(manager1, manager2)

        # Different paths should be different instances in tests
        other_path = os.path.join(self.temp_dir.name, "other_test.json")
        manager3 = ConfigManager(other_path)
        self.assertIsNot(manager1, manager3)

    def test_databricks_token(self):
        """Test Databricks token getter and setter functions."""
        # Initialize config with a valid workspace URL to avoid validation errors
        test_url = "test-workspace"
        set_workspace_url(test_url)

        # Test with no token set initially (should be None by default)
        initial_token = get_databricks_token()
        self.assertIsNone(initial_token)

        # Set token and verify it's stored correctly
        test_token = "dapi1234567890abcdef"
        set_databricks_token(test_token)

        # Check value was set in memory
        self.assertEqual(get_databricks_token(), test_token)

        # Check file was updated
        with open(self.config_path, "r") as f:
            saved_config = json.load(f)
        self.assertEqual(saved_config["databricks_token"], test_token)

        # Create a new manager to verify it loads from disk
        another_manager = ConfigManager(self.config_path)
        config = another_manager.get_config()
        self.assertEqual(config.databricks_token, test_token)

    def test_needs_setup_method(self):
        """Test the needs_setup method for determining first-time setup requirement."""
        # Test with no config - should need setup
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(self.config_manager.needs_setup())

        # Test with partial config - should still need setup
        with patch.dict(
            os.environ, {"CHUCK_WORKSPACE_URL": "test-workspace"}, clear=True
        ):
            self.assertTrue(self.config_manager.needs_setup())

        # Test with complete config via environment variables - should not need setup
        with patch.dict(
            os.environ,
            {
                "CHUCK_WORKSPACE_URL": "test-workspace",
                "CHUCK_AMPERITY_TOKEN": "test-amperity-token",
                "CHUCK_DATABRICKS_TOKEN": "test-databricks-token",
                "CHUCK_ACTIVE_MODEL": "test-model",
            },
            clear=True,
        ):
            self.assertFalse(self.config_manager.needs_setup())

        # Test with complete config in file - should not need setup
        with patch.dict(os.environ, {}, clear=True):
            self.config_manager.update(
                workspace_url="file-workspace",
                amperity_token="file-amperity-token",
                databricks_token="file-databricks-token",
                active_model="file-model",
            )
            self.assertFalse(self.config_manager.needs_setup())

    @patch("src.config.clear_agent_history")
    def test_set_active_model_clears_history(self, mock_clear_history):
        """Ensure agent history is cleared when switching models."""
        with patch.dict(os.environ, {}, clear=True):
            set_active_model("new-model")
            mock_clear_history.assert_called_once()
