"""
Tests for add_stitch_report command handler.

This module contains tests for the add_stitch_report command handler.
"""

from unittest.mock import patch

from chuck_data.commands.add_stitch_report import handle_command


def test_missing_client():
    """Test handling when client is not provided."""
    result = handle_command(None, table_path="catalog.schema.table")
    assert not result.success
    assert "Client is required" in result.message


def test_missing_table_path(databricks_client_stub):
    """Test handling when table_path is missing."""
    result = handle_command(databricks_client_stub)
    assert not result.success
    assert "Table path must be provided" in result.message


def test_invalid_table_path_format(databricks_client_stub):
    """Test handling when table_path format is invalid."""
    result = handle_command(databricks_client_stub, table_path="invalid_format")
    assert not result.success
    assert "must be in the format" in result.message


@patch("chuck_data.commands.add_stitch_report.get_metrics_collector")
def test_successful_report_creation(
    mock_get_metrics_collector, databricks_client_stub, metrics_collector_stub
):
    """Test successful stitch report notebook creation."""
    # Setup mocks
    mock_get_metrics_collector.return_value = metrics_collector_stub

    databricks_client_stub.set_create_stitch_notebook_result(
        {
            "path": "/Workspace/Users/user@example.com/Stitch Results",
            "status": "success",
        }
    )

    # Call function
    result = handle_command(databricks_client_stub, table_path="catalog.schema.table")

    # Verify results
    assert result.success
    assert "Successfully created" in result.message
    # Verify the call was made with correct arguments
    assert len(databricks_client_stub.create_stitch_notebook_calls) == 1
    args, kwargs = databricks_client_stub.create_stitch_notebook_calls[0]
    assert args == ("catalog.schema.table", None)

    # Verify metrics collection
    assert len(metrics_collector_stub.track_event_calls) == 1
    call = metrics_collector_stub.track_event_calls[0]
    assert call["prompt"] == "add-stitch-report command"
    assert call["additional_data"]["status"] == "success"


@patch("chuck_data.commands.add_stitch_report.get_metrics_collector")
def test_report_creation_with_custom_name(
    mock_get_metrics_collector, databricks_client_stub, metrics_collector_stub
):
    """Test stitch report creation with custom notebook name."""
    # Setup mocks
    mock_get_metrics_collector.return_value = metrics_collector_stub

    databricks_client_stub.set_create_stitch_notebook_result(
        {
            "path": "/Workspace/Users/user@example.com/My Custom Report",
            "status": "success",
        }
    )

    # Call function
    result = handle_command(
        databricks_client_stub,
        table_path="catalog.schema.table",
        name="My Custom Report",
    )

    # Verify results
    assert result.success
    assert "Successfully created" in result.message
    # Verify the call was made with correct arguments
    assert len(databricks_client_stub.create_stitch_notebook_calls) == 1
    args, kwargs = databricks_client_stub.create_stitch_notebook_calls[0]
    assert args == ("catalog.schema.table", "My Custom Report")


@patch("chuck_data.commands.add_stitch_report.get_metrics_collector")
def test_report_creation_with_rest_args(
    mock_get_metrics_collector, databricks_client_stub, metrics_collector_stub
):
    """Test stitch report creation with rest arguments as notebook name."""
    # Setup mocks
    mock_get_metrics_collector.return_value = metrics_collector_stub

    databricks_client_stub.set_create_stitch_notebook_result(
        {
            "path": "/Workspace/Users/user@example.com/Multi Word Name",
            "status": "success",
        }
    )

    # Call function with rest parameter
    result = handle_command(
        databricks_client_stub,
        table_path="catalog.schema.table",
        rest="Multi Word Name",
    )

    # Verify results
    assert result.success
    assert "Successfully created" in result.message
    # Verify the call was made with correct arguments
    assert len(databricks_client_stub.create_stitch_notebook_calls) == 1
    args, kwargs = databricks_client_stub.create_stitch_notebook_calls[0]
    assert args == ("catalog.schema.table", "Multi Word Name")


@patch("chuck_data.commands.add_stitch_report.get_metrics_collector")
def test_report_creation_api_error(
    mock_get_metrics_collector, databricks_client_stub, metrics_collector_stub
):
    """Test handling when API call to create notebook fails."""
    # Setup mocks
    mock_get_metrics_collector.return_value = metrics_collector_stub

    databricks_client_stub.set_create_stitch_notebook_error(ValueError("API Error"))

    # Call function
    result = handle_command(databricks_client_stub, table_path="catalog.schema.table")

    # Verify results
    assert not result.success
    assert "Error creating Stitch report" in result.message

    # Verify metrics collection for error
    assert len(metrics_collector_stub.track_event_calls) == 1
    call = metrics_collector_stub.track_event_calls[0]
    assert call["prompt"] == "add-stitch-report command"
    assert call["error"] == "API Error"
