"""Environment fixtures for Chuck tests.

These fixtures provide clean, isolated environment setups for different test scenarios,
replacing scattered @patch.dict calls throughout the test suite.
"""

import pytest
import os
from unittest.mock import patch


@pytest.fixture
def clean_env():
    """
    Provide completely clean environment for config tests.

    This fixture clears all environment variables to ensure config tests
    get predictable behavior without interference from host environment
    CHUCK_* variables or other system settings.

    Usage:
        def test_config_behavior(clean_env):
            # Test runs with empty environment
            # Config values come only from test setup, not env vars
    """
    with patch.dict(os.environ, {}, clear=True):
        yield


@pytest.fixture
def mock_databricks_env():
    """
    Provide standard Databricks test environment variables.

    Sets up common Databricks environment variables needed for
    authentication and workspace tests.

    Usage:
        def test_databricks_auth(mock_databricks_env):
            # DATABRICKS_TOKEN and DATABRICKS_WORKSPACE_URL are set
    """
    test_env = {
        "DATABRICKS_TOKEN": "test_token",
        "DATABRICKS_WORKSPACE_URL": "test-workspace",
    }
    with patch.dict(os.environ, test_env, clear=True):
        yield


@pytest.fixture
def no_color_env():
    """
    Provide NO_COLOR environment for display tests.

    Sets NO_COLOR environment variable to test color output behavior.

    Usage:
        def test_no_color_output(no_color_env):
            # NO_COLOR is set, color output should be disabled
    """
    with patch.dict(os.environ, {"NO_COLOR": "1"}, clear=True):
        yield


@pytest.fixture
def no_color_true_env():
    """
    Provide NO_COLOR=true environment for display tests.

    Sets NO_COLOR=true to test alternative true value handling.

    Usage:
        def test_no_color_true_output(no_color_true_env):
            # NO_COLOR=true, color output should be disabled
    """
    with patch.dict(os.environ, {"NO_COLOR": "true"}, clear=True):
        yield


@pytest.fixture
def chuck_env_vars():
    """
    Provide specific CHUCK_* environment variables for config override tests.

    Sets up CHUCK_* prefixed environment variables to test the config system's
    environment variable override behavior.

    Usage:
        def test_config_env_override(chuck_env_vars):
            # CHUCK_WORKSPACE_URL and other vars are set
            # Config system should read from these env vars
    """
    test_env = {
        "CHUCK_WORKSPACE_URL": "env-workspace",
        "CHUCK_ACTIVE_MODEL": "env-model",
        "CHUCK_WAREHOUSE_ID": "env-warehouse",
        "CHUCK_ACTIVE_CATALOG": "env-catalog",
        "CHUCK_ACTIVE_SCHEMA": "env-schema",
        "CHUCK_DATABRICKS_TOKEN": "env-token",
    }
    with patch.dict(os.environ, test_env, clear=True):
        yield
