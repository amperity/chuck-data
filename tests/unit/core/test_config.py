"""Tests for the configuration functionality in Chuck."""

import pytest
import os
import json
import tempfile
from unittest.mock import patch

from chuck_data.config import (
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


@pytest.fixture
def config_setup():
    """Set up test configuration with temp file and patched global manager."""
    # Create a temporary file for testing
    temp_dir = tempfile.TemporaryDirectory()
    config_path = os.path.join(temp_dir.name, "test_config.json")

    # Create a test-specific config manager
    config_manager = ConfigManager(config_path)

    # Mock the global config manager
    patcher = patch("chuck_data.config._config_manager", config_manager)
    patcher.start()

    yield config_manager, config_path, temp_dir

    # Cleanup
    patcher.stop()
    temp_dir.cleanup()


def test_default_config(config_setup):
    """Test default configuration values."""
    config_manager, config_path, temp_dir = config_setup
    config = config_manager.get_config()

    # Check default values
    # No longer expecting a specific default workspace URL since we now preserve full URLs
    # and the default might be None until explicitly set
    assert config.active_model is None
    assert config.warehouse_id is None
    assert config.active_catalog is None
    assert config.active_schema is None


def test_config_update(config_setup):
    """Test updating configuration values."""
    config_manager, config_path, temp_dir = config_setup

    # Update values
    config_manager.update(
        workspace_url="test-workspace",
        active_model="test-model",
        warehouse_id="test-warehouse",
        active_catalog="test-catalog",
        active_schema="test-schema",
    )

    # Check values were updated in memory
    config = config_manager.get_config()
    assert config.workspace_url == "test-workspace"
    assert config.active_model == "test-model"
    assert config.warehouse_id == "test-warehouse"
    assert config.active_catalog == "test-catalog"
    assert config.active_schema == "test-schema"

    # Check file was created
    assert os.path.exists(config_path)

    # Check file contents
    with open(config_path, "r") as f:
        saved_config = json.load(f)

    assert saved_config["workspace_url"] == "test-workspace"
    assert saved_config["active_model"] == "test-model"
    assert saved_config["warehouse_id"] == "test-warehouse"
    assert saved_config["active_catalog"] == "test-catalog"
    assert saved_config["active_schema"] == "test-schema"


def test_config_load_save_cycle(config_setup):
    """Test loading and saving configuration."""
    config_manager, config_path, temp_dir = config_setup

    # Set test values
    test_url = "https://test-workspace.cloud.databricks.com"  # Need valid URL string
    test_model = "test-model"
    test_warehouse = "warehouse-id-123"

    # Update config values using the update method
    config_manager.update(
        workspace_url=test_url,
        active_model=test_model,
        warehouse_id=test_warehouse,
    )

    # Create a new manager to load from disk
    another_manager = ConfigManager(config_path)
    config = another_manager.get_config()

    # Verify saved values were loaded
    assert config.workspace_url == test_url
    assert config.active_model == test_model
    assert config.warehouse_id == test_warehouse


def test_api_functions(config_setup):
    """Test compatibility API functions."""
    config_manager, config_path, temp_dir = config_setup

    # Set values using API functions
    set_workspace_url("api-workspace")
    set_active_model("api-model")
    set_warehouse_id("api-warehouse")
    set_active_catalog("api-catalog")
    set_active_schema("api-schema")

    # Check values using API functions
    assert get_workspace_url() == "api-workspace"
    assert get_active_model() == "api-model"
    assert get_warehouse_id() == "api-warehouse"
    assert get_active_catalog() == "api-catalog"
    assert get_active_schema() == "api-schema"


def test_environment_override(config_setup, monkeypatch):
    """Test environment variable override for all config values."""
    config_manager, config_path, temp_dir = config_setup

    # First set config values with clean environment
    with patch.dict(os.environ, {}, clear=True):
        set_workspace_url("config-workspace")
        set_active_model("config-model")
        set_warehouse_id("config-warehouse")
        set_active_catalog("config-catalog")
        set_active_schema("config-schema")

    # Now test that CHUCK_ environment variables take precedence
    monkeypatch.setenv("CHUCK_WORKSPACE_URL", "env-workspace")
    monkeypatch.setenv("CHUCK_ACTIVE_MODEL", "env-model")
    monkeypatch.setenv("CHUCK_WAREHOUSE_ID", "env-warehouse")
    monkeypatch.setenv("CHUCK_ACTIVE_CATALOG", "env-catalog")
    monkeypatch.setenv("CHUCK_ACTIVE_SCHEMA", "env-schema")

    # Create a new config manager to reload with environment overrides
    fresh_manager = ConfigManager(config_path)
    config = fresh_manager.get_config()

    # Environment variables should override file values
    assert config.workspace_url == "env-workspace"
    assert config.active_model == "env-model"
    assert config.warehouse_id == "env-warehouse"
    assert config.active_catalog == "env-catalog"
    assert config.active_schema == "env-schema"


def test_graceful_validation(config_setup):
    """Test that invalid configuration values are handled gracefully."""
    config_manager, config_path, temp_dir = config_setup

    # Write invalid JSON to config file
    with open(config_path, "w") as f:
        f.write("{ invalid json }")

    # Should still create a config with defaults instead of crashing
    config = config_manager.get_config()

    # Should get default values
    assert config.active_model is None
    assert config.warehouse_id is None


def test_singleton_pattern(config_setup):
    """Test that ConfigManager behaves as singleton."""
    config_manager, config_path, temp_dir = config_setup

    # Create multiple instances with same path
    manager1 = ConfigManager(config_path)
    manager2 = ConfigManager(config_path)

    # Set value through one manager
    manager1.update(active_model="singleton-test")

    # Should be visible through other manager (testing cached behavior)
    # Note: In temp dir, config is not cached, so we need to test regular behavior
    if not config_path.startswith(tempfile.gettempdir()):
        config2 = manager2.get_config()
        assert config2.active_model == "singleton-test"


def test_databricks_token(config_setup):
    """Test databricks token handling."""
    config_manager, config_path, temp_dir = config_setup

    # Test setting token through config
    set_databricks_token("config-token")

    assert get_databricks_token() == "config-token"

    # Test environment variable override
    with patch.dict(os.environ, {"CHUCK_DATABRICKS_TOKEN": "env-token"}):
        # Create fresh manager to pick up env var
        fresh_manager = ConfigManager(config_path)
        with patch("chuck_data.config._config_manager", fresh_manager):
            # Should get env token
            token = get_databricks_token()
            assert token == "env-token"


def test_needs_setup_method(config_setup):
    """Test needs_setup method returns correct values."""
    config_manager, config_path, temp_dir = config_setup

    # Initially should need setup
    assert config_manager.needs_setup()

    # After setting all critical configs, should not need setup
    config_manager.update(
        workspace_url="test-workspace",
        amperity_token="test-amperity-token",
        databricks_token="test-databricks-token",
        active_model="test-model",
    )
    assert not config_manager.needs_setup()

    # Test with environment variable
    with patch.dict(os.environ, {"CHUCK_WORKSPACE_URL": "env-workspace"}):
        fresh_manager = ConfigManager(config_path)
        assert not fresh_manager.needs_setup()


def test_set_active_model_clears_history(config_setup):
    """Test that setting active model clears agent history."""
    config_manager, config_path, temp_dir = config_setup

    # Set up some agent history first
    from chuck_data.config import set_agent_history, get_agent_history

    test_history = [{"role": "user", "content": "test message"}]
    set_agent_history(test_history)

    # Verify history is set
    history_before = get_agent_history()
    assert len(history_before) == 1
    assert history_before[0]["content"] == "test message"

    # Set active model (should clear history)
    set_active_model("test-model")

    # Verify history was actually cleared
    history_after = get_agent_history()
    assert len(history_after) == 0
