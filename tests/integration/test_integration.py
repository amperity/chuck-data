"""Integration tests for the Chuck application."""

import pytest
from unittest.mock import patch
from chuck_data.config import (
    set_active_model,
    get_active_model,
    ConfigManager,
)
import os
import json


@pytest.fixture
def integration_setup():
    """Set up the test environment with controlled configuration."""
    # Set up test environment
    test_config_path = "/tmp/.test_chuck_integration_config.json"

    # Create a test config manager instance
    config_manager = ConfigManager(config_path=test_config_path)

    # Replace the global config manager with our test instance
    config_manager_patcher = patch("chuck_data.config._config_manager", config_manager)
    config_manager_patcher.start()

    # Mock environment for authentication
    env_patcher = patch.dict(
        "os.environ",
        {
            "DATABRICKS_TOKEN": "test_token",
            "DATABRICKS_WORKSPACE_URL": "test-workspace",
        },
    )
    env_patcher.start()

    # Initialize the config with workspace_url
    config_manager.update(workspace_url="test-workspace")

    yield {
        "test_config_path": test_config_path,
        "config_manager": config_manager,
        "config_manager_patcher": config_manager_patcher,
        "env_patcher": env_patcher,
    }

    # Cleanup
    if os.path.exists(test_config_path):
        os.remove(test_config_path)
    config_manager_patcher.stop()
    env_patcher.stop()


def test_config_operations(integration_setup):
    """Test that config operations work properly."""
    test_config_path = integration_setup["test_config_path"]

    # Test writing and reading config
    set_active_model("test-model")

    # Verify the config file was actually created with correct content
    assert os.path.exists(test_config_path)
    with open(test_config_path, "r") as f:
        saved_config = json.load(f)
    assert saved_config["active_model"] == "test-model"

    # Test reading the config
    active_model = get_active_model()
    assert active_model == "test-model"


def test_catalog_config_operations(integration_setup):
    """Test catalog config operations."""
    test_config_path = integration_setup["test_config_path"]

    # Test writing and reading catalog config
    from chuck_data.config import set_active_catalog, get_active_catalog

    test_catalog = "test-catalog"
    set_active_catalog(test_catalog)

    # Verify the config file was updated with catalog
    with open(test_config_path, "r") as f:
        saved_config = json.load(f)
    assert saved_config["active_catalog"] == test_catalog

    # Test reading the catalog config
    active_catalog = get_active_catalog()
    assert active_catalog == test_catalog


def test_schema_config_operations(integration_setup):
    """Test schema config operations."""
    test_config_path = integration_setup["test_config_path"]

    # Test writing and reading schema config
    from chuck_data.config import set_active_schema, get_active_schema

    test_schema = "test-schema"
    set_active_schema(test_schema)

    # Verify the config file was updated with schema
    with open(test_config_path, "r") as f:
        saved_config = json.load(f)
    assert saved_config["active_schema"] == test_schema

    # Test reading the schema config
    active_schema = get_active_schema()
    assert active_schema == test_schema
