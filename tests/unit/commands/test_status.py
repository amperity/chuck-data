"""
Tests for the status command module.
"""

from unittest.mock import patch, MagicMock

from chuck_data.commands.status import handle_command


@patch("chuck_data.commands.status.get_workspace_url")
@patch("chuck_data.commands.status.get_active_catalog")
@patch("chuck_data.commands.status.get_active_schema")
@patch("chuck_data.commands.status.get_active_model")
@patch("chuck_data.commands.status.validate_all_permissions")
def test_handle_status_with_valid_connection(
    mock_permissions,
    mock_get_model,
    mock_get_schema,
    mock_get_catalog,
    mock_get_url,
):
    """Test status command with valid connection."""
    client = MagicMock()
    
    # Setup mocks
    mock_get_url.return_value = "test-workspace"
    mock_get_catalog.return_value = "test-catalog"
    mock_get_schema.return_value = "test-schema"
    mock_get_model.return_value = "test-model"
    mock_permissions.return_value = {"test_resource": {"authorized": True}}

    # Call function
    result = handle_command(client)

    # Verify result
    assert result.success
    assert result.data["workspace_url"] == "test-workspace"
    assert result.data["active_catalog"] == "test-catalog"
    assert result.data["active_schema"] == "test-schema"
    assert result.data["active_model"] == "test-model"
    assert result.data["connection_status"] == "Connected (client present)."
    assert result.data["permissions"] == mock_permissions.return_value


@patch("chuck_data.commands.status.get_workspace_url")
@patch("chuck_data.commands.status.get_active_catalog")
@patch("chuck_data.commands.status.get_active_schema")
@patch("chuck_data.commands.status.get_active_model")
def test_handle_status_with_no_client(
    mock_get_model, mock_get_schema, mock_get_catalog, mock_get_url
):
    """Test status command with no client provided."""
    # Setup mocks
    mock_get_url.return_value = "test-workspace"
    mock_get_catalog.return_value = "test-catalog"
    mock_get_schema.return_value = "test-schema"
    mock_get_model.return_value = "test-model"

    # Call function with no client
    result = handle_command(None)

    # Verify result
    assert result.success
    assert result.data["workspace_url"] == "test-workspace"
    assert result.data["active_catalog"] == "test-catalog"
    assert result.data["active_schema"] == "test-schema"
    assert result.data["active_model"] == "test-model"
    assert result.data["connection_status"] == "Client not available or not initialized."


@patch("chuck_data.commands.status.get_workspace_url")
@patch("chuck_data.commands.status.get_active_catalog")
@patch("chuck_data.commands.status.get_active_schema")
@patch("chuck_data.commands.status.get_active_model")
@patch("chuck_data.commands.status.validate_all_permissions")
@patch("logging.error")
def test_handle_status_with_exception(
    mock_log,
    mock_permissions,
    mock_get_model,
    mock_get_schema,
    mock_get_catalog,
    mock_get_url,
):
    """Test status command when an exception occurs."""
    client = MagicMock()
    
    # Setup mock to raise exception
    mock_get_url.side_effect = ValueError("Config error")

    # Call function
    result = handle_command(client)

    # Verify result
    assert not result.success
    assert result.error is not None
    mock_log.assert_called_once()
