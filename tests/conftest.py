"""Pytest fixtures for Chuck tests."""

import pytest
import tempfile
import os
from unittest.mock import MagicMock

from tests.fixtures.databricks.client import DatabricksClientStub
from tests.fixtures.amperity import AmperityClientStub
from tests.fixtures.llm import LLMClientStub
from tests.fixtures.collectors import MetricsCollectorStub
from chuck_data.config import ConfigManager

# Import environment fixtures to make them available globally


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
