"""
Tests for workspace_selection command handler.

This module contains tests for the workspace selection command handler.
"""

from unittest.mock import patch

from chuck_data.commands.workspace_selection import handle_command


def test_missing_workspace_url():
    """Test handling when workspace_url is not provided."""
    result = handle_command(None)
    assert not result.success
    assert "workspace_url parameter is required" in result.message


@patch("chuck_data.databricks.url_utils.validate_workspace_url")
def test_invalid_workspace_url(mock_validate_workspace_url):
    """Test handling when workspace_url is invalid."""
    # Setup mocks
    mock_validate_workspace_url.return_value = (False, "Invalid URL format")

    # Call function
    result = handle_command(None, workspace_url="invalid-url")

    # Verify results
    assert not result.success
    assert "Error: Invalid URL format" in result.message
    mock_validate_workspace_url.assert_called_once_with("invalid-url")


@patch("chuck_data.databricks.url_utils.validate_workspace_url")
@patch("chuck_data.databricks.url_utils.normalize_workspace_url")
@patch("chuck_data.databricks.url_utils.detect_cloud_provider")
@patch("chuck_data.databricks.url_utils.format_workspace_url_for_display")
@patch("chuck_data.commands.workspace_selection.set_workspace_url")
def test_successful_workspace_selection(
    mock_set_workspace_url,
    mock_format_url,
    mock_detect_cloud,
    mock_normalize_url,
    mock_validate_url,
):
    """Test successful workspace selection."""
    # Setup mocks
    mock_validate_url.return_value = (True, "")
    mock_normalize_url.return_value = "dbc-example.cloud.databricks.com"
    mock_detect_cloud.return_value = "Azure"
    mock_format_url.return_value = "dbc-example (Azure)"

    # Call function
    result = handle_command(
        None, workspace_url="https://dbc-example.cloud.databricks.com"
    )

    # Verify results
    assert result.success
    assert "Workspace URL is now set to 'dbc-example (Azure)'" in result.message
    assert "Restart may be needed" in result.message
    assert result.data["workspace_url"] == "https://dbc-example.cloud.databricks.com"
    assert result.data["display_url"] == "dbc-example (Azure)"
    assert result.data["cloud_provider"] == "Azure"
    assert result.data["requires_restart"]
    mock_set_workspace_url.assert_called_once_with(
        "https://dbc-example.cloud.databricks.com"
    )


@patch("chuck_data.databricks.url_utils.validate_workspace_url")
def test_workspace_url_exception(mock_validate_workspace_url):
    """Test handling when an exception occurs."""
    # Setup mocks
    mock_validate_workspace_url.side_effect = Exception("Validation error")

    # Call function
    result = handle_command(None, workspace_url="https://dbc-example.databricks.com")

    # Verify results
    assert not result.success
    assert str(result.error) == "Validation error"
