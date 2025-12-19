"""Pytest fixtures for Chuck tests."""

import pytest
import tempfile
import os
from unittest.mock import MagicMock, patch

# CRITICAL: Set test config path BEFORE any imports that use ConfigManager
# This prevents tests from modifying the user's real ~/.chuck_config.json
_TEST_CONFIG_DIR = tempfile.mkdtemp(prefix="chuck_test_config_")
_TEST_CONFIG_PATH = os.path.join(_TEST_CONFIG_DIR, "test_config.json")
with open(_TEST_CONFIG_PATH, "w") as f:
    f.write('{"usage_tracking_consent": false}')
os.environ["CHUCK_CONFIG_PATH"] = _TEST_CONFIG_PATH

from tests.fixtures.databricks.client import DatabricksClientStub
from tests.fixtures.amperity import AmperityClientStub
from tests.fixtures.llm import LLMClientStub
from tests.fixtures.collectors import MetricsCollectorStub
from chuck_data.config import ConfigManager

# Import environment fixtures to make them available globally


@pytest.fixture(autouse=True, scope="function")
def reset_config_singleton():
    """
    Function-level fixture to reset config singleton between tests.

    This ensures each test gets a fresh ConfigManager instance, preventing
    test pollution while still using the session-level test config path.
    """
    # Clear cached singleton instances before each test
    ConfigManager._instance = None
    ConfigManager._instances_by_path.clear()

    yield

    # Clear again after test
    ConfigManager._instance = None
    ConfigManager._instances_by_path.clear()


@pytest.fixture(autouse=True)
def mock_job_cache():
    """
    Automatically mock job cache for all tests to prevent cache pollution.

    This fixture runs automatically for every test and prevents tests from
    writing to the user's actual job cache file (~/.chuck_job_cache.json).
    Tests that specifically need to test cache behavior should use the
    JobCache class directly with a temporary file.
    """
    with patch("chuck_data.job_cache.cache_job") as mock_cache:
        yield mock_cache


@pytest.fixture
def databricks_client_stub():
    """Create a fresh DatabricksClientStub for each test."""
    return DatabricksClientStub()


@pytest.fixture
def databricks_client_stub_with_data():
    """Create a DatabricksClientStub with default test data."""
    stub = DatabricksClientStub()
    # Add some default test data
    stub.add_catalog("test_catalog", catalog_type="MANAGED")
    stub.add_schema("test_catalog", "test_schema")
    stub.add_table("test_catalog", "test_schema", "test_table")
    stub.add_warehouse(warehouse_id="test-warehouse", name="Test Warehouse")
    return stub


@pytest.fixture
def amperity_client_stub():
    """Create a fresh AmperityClientStub for each test."""
    return AmperityClientStub()


@pytest.fixture
def llm_client_stub():
    """Create a fresh LLMClientStub for each test."""
    return LLMClientStub()


@pytest.fixture
def metrics_collector_stub():
    """Create a fresh MetricsCollectorStub for each test."""
    return MetricsCollectorStub()


@pytest.fixture
def temp_config():
    """Create a temporary config file for testing."""
    temp_dir = tempfile.TemporaryDirectory()
    config_path = os.path.join(temp_dir.name, "test_config.json")
    config_manager = ConfigManager(config_path)
    yield config_manager
    temp_dir.cleanup()


@pytest.fixture
def mock_console():
    """Create a mock console for TUI testing."""
    return MagicMock()
