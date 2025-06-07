"""
Tests for the utils module.
"""

import pytest
from unittest.mock import patch, MagicMock
from chuck_data.utils import build_query_params, execute_sql_statement


def test_build_query_params_empty():
    """Test building query params with empty input."""
    result = build_query_params({})
    assert result == ""


def test_build_query_params_none_values():
    """Test building query params with None values."""
    params = {"key1": "value1", "key2": None, "key3": "value3"}
    result = build_query_params(params)
    assert result == "?key1=value1&key3=value3"


def test_build_query_params_bool_values():
    """Test building query params with boolean values."""
    params = {"key1": True, "key2": False, "key3": "value3"}
    result = build_query_params(params)
    assert result == "?key1=true&key2=false&key3=value3"


def test_build_query_params_int_values():
    """Test building query params with integer values."""
    params = {"key1": 123, "key2": "value2"}
    result = build_query_params(params)
    assert result == "?key1=123&key2=value2"


def test_build_query_params_multiple_params():
    """Test building query params with multiple parameters."""
    params = {"param1": "value1", "param2": "value2", "param3": "value3"}
    result = build_query_params(params)
    # Check that all params are included and properly formatted
    assert result.startswith("?")
    assert "param1=value1" in result
    assert "param2=value2" in result
    assert "param3=value3" in result
    assert len(result.split("&")) == 3


@patch("chuck_data.utils.time.sleep")  # Mock sleep to speed up test
def test_execute_sql_statement_success(mock_sleep):
    """Test successful SQL statement execution."""
    # Create mock client
    mock_client = MagicMock()

    # Set up mock responses
    mock_client.post.return_value = {"statement_id": "123"}
    mock_client.get.return_value = {
        "status": {"state": "SUCCEEDED"},
        "result": {"data": [["row1"], ["row2"]]},
    }

    # Execute the function
    result = execute_sql_statement(mock_client, "warehouse-123", "SELECT * FROM table")

    # Verify interactions
    mock_client.post.assert_called_once()
    mock_client.get.assert_called_once_with("/api/2.0/sql/statements/123")

    # Verify result
    assert result == {"data": [["row1"], ["row2"]]}


@patch("chuck_data.utils.time.sleep")  # Mock sleep to speed up test
def test_execute_sql_statement_with_catalog(mock_sleep):
    """Test SQL statement execution with catalog parameter."""
    # Create mock client
    mock_client = MagicMock()

    # Set up mock responses
    mock_client.post.return_value = {"statement_id": "123"}
    mock_client.get.return_value = {
        "status": {"state": "SUCCEEDED"},
        "result": {"data": []},
    }

    # Execute with catalog parameter
    execute_sql_statement(
        mock_client, "warehouse-123", "SELECT * FROM table", catalog="test-catalog"
    )

    # Verify the catalog was included in the request
    post_args = mock_client.post.call_args[0][1]
    assert post_args.get("catalog") == "test-catalog"


@patch("chuck_data.utils.time.sleep")  # Mock sleep to speed up test
def test_execute_sql_statement_with_custom_timeout(mock_sleep):
    """Test SQL statement execution with custom timeout."""
    # Create mock client
    mock_client = MagicMock()

    # Set up mock responses
    mock_client.post.return_value = {"statement_id": "123"}
    mock_client.get.return_value = {
        "status": {"state": "SUCCEEDED"},
        "result": {},
    }

    # Execute with custom timeout
    custom_timeout = "60s"
    execute_sql_statement(
        mock_client,
        "warehouse-123",
        "SELECT * FROM table",
        wait_timeout=custom_timeout,
    )

    # Verify the timeout was included in the request
    post_args = mock_client.post.call_args[0][1]
    assert post_args.get("wait_timeout") == custom_timeout


@patch("chuck_data.utils.time.sleep")  # Mock sleep to speed up test
def test_execute_sql_statement_polling(mock_sleep):
    """Test SQL statement execution with polling."""
    # Create mock client
    mock_client = MagicMock()

    # Set up mock responses for polling
    mock_client.post.return_value = {"statement_id": "123"}

    # Configure get to return "RUNNING" twice then "SUCCEEDED"
    mock_client.get.side_effect = [
        {"status": {"state": "PENDING"}},
        {"status": {"state": "RUNNING"}},
        {"status": {"state": "SUCCEEDED"}, "result": {"data": []}},
    ]

    # Execute the function
    execute_sql_statement(mock_client, "warehouse-123", "SELECT * FROM table")

    # Verify that get was called 3 times (polling behavior)
    assert mock_client.get.call_count == 3

    # Verify sleep was called twice (once for each non-complete state)
    mock_sleep.assert_called_with(1)
    assert mock_sleep.call_count == 2


@patch("chuck_data.utils.time.sleep")  # Mock sleep to speed up test
def test_execute_sql_statement_failed(mock_sleep):
    """Test SQL statement execution that fails."""
    # Create mock client
    mock_client = MagicMock()

    # Set up mock responses
    mock_client.post.return_value = {"statement_id": "123"}
    mock_client.get.return_value = {
        "status": {"state": "FAILED", "error": {"message": "SQL syntax error"}},
    }

    # Execute the function and check for exception
    with pytest.raises(ValueError) as excinfo:
        execute_sql_statement(mock_client, "warehouse-123", "SELECT * INVALID SQL")

    # Verify error message
    assert "SQL statement failed: SQL syntax error" in str(excinfo.value)


@patch("chuck_data.utils.time.sleep")  # Mock sleep to speed up test
def test_execute_sql_statement_error_without_message(mock_sleep):
    """Test SQL statement execution that fails without specific message."""
    # Create mock client
    mock_client = MagicMock()

    # Set up mock responses
    mock_client.post.return_value = {"statement_id": "123"}
    mock_client.get.return_value = {
        "status": {"state": "FAILED", "error": {}},
    }

    # Execute the function and check for exception
    with pytest.raises(ValueError) as excinfo:
        execute_sql_statement(mock_client, "warehouse-123", "SELECT * INVALID SQL")

    # Verify default error message
    assert "SQL statement failed: Unknown error" in str(excinfo.value)
