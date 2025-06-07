"""
Tests for setup_stitch command handler.

This module contains tests for the setup_stitch command handler.
"""

import pytest
from unittest.mock import patch, MagicMock

from chuck_data.commands.setup_stitch import handle_command
from tests.fixtures.llm import LLMClientStub


@pytest.fixture
def client():
    """Mock client fixture."""
    return MagicMock()


def test_missing_client():
    """Test handling when client is not provided."""
    result = handle_command(None)
    assert not result.success
    assert "Client is required" in result.message


@patch("chuck_data.commands.setup_stitch.get_active_catalog")
@patch("chuck_data.commands.setup_stitch.get_active_schema")
def test_missing_context(mock_get_active_schema, mock_get_active_catalog, client):
    """Test handling when catalog or schema is missing."""
    # Setup mocks
    mock_get_active_catalog.return_value = None
    mock_get_active_schema.return_value = None

    # Call function
    result = handle_command(client)

    # Verify results
    assert not result.success
    assert "Target catalog and schema must be specified" in result.message


@patch("chuck_data.commands.setup_stitch._helper_launch_stitch_job")
@patch("chuck_data.commands.setup_stitch.LLMClient")
@patch("chuck_data.commands.setup_stitch._helper_setup_stitch_logic")
@patch("chuck_data.commands.setup_stitch.get_metrics_collector")
def test_successful_setup(
    mock_get_metrics_collector,
    mock_helper_setup,
    mock_llm_client,
    mock_launch_job,
    client,
):
    """Test successful Stitch setup."""
    # Setup mocks
    llm_client_stub = LLMClientStub()
    mock_llm_client.return_value = llm_client_stub
    mock_metrics_collector = MagicMock()
    mock_get_metrics_collector.return_value = mock_metrics_collector

    mock_helper_setup.return_value = {
        "stitch_config": {},
        "metadata": {
            "target_catalog": "test_catalog",
            "target_schema": "test_schema",
        },
    }
    mock_launch_job.return_value = {
        "message": "Stitch setup completed successfully.",
        "tables_processed": 5,
        "pii_columns_tagged": 8,
        "config_created": True,
        "config_path": "/Volumes/test_catalog/test_schema/_stitch/config.json",
    }

    # Call function with auto_confirm to use legacy behavior
    result = handle_command(
        client,
        **{
            "catalog_name": "test_catalog",
            "schema_name": "test_schema",
            "auto_confirm": True,
        },
    )

    # Verify results
    assert result.success
    assert result.message == "Stitch setup completed successfully."
    assert result.data["tables_processed"] == 5
    assert result.data["pii_columns_tagged"] == 8
    assert result.data["config_created"]
    mock_helper_setup.assert_called_once_with(
        client, llm_client_stub, "test_catalog", "test_schema"
    )
    mock_launch_job.assert_called_once_with(
        client,
        {},
        {"target_catalog": "test_catalog", "target_schema": "test_schema"},
    )

    # Verify metrics collection
    mock_metrics_collector.track_event.assert_called_once_with(
        prompt="setup-stitch command",
        tools=[
            {
                "name": "setup_stitch",
                "arguments": {"catalog": "test_catalog", "schema": "test_schema"},
            }
        ],
        additional_data={
            "event_context": "direct_stitch_command",
            "status": "success",
            "tables_processed": 5,
            "pii_columns_tagged": 8,
            "config_created": True,
            "config_path": "/Volumes/test_catalog/test_schema/_stitch/config.json",
        },
    )


@patch("chuck_data.commands.setup_stitch._helper_launch_stitch_job")
@patch("chuck_data.commands.setup_stitch.get_active_catalog")
@patch("chuck_data.commands.setup_stitch.get_active_schema")
@patch("chuck_data.commands.setup_stitch.LLMClient")
@patch("chuck_data.commands.setup_stitch._helper_setup_stitch_logic")
def test_setup_with_active_context(
    mock_helper_setup,
    mock_llm_client,
    mock_get_active_schema,
    mock_get_active_catalog,
    mock_launch_job,
    client,
):
    """Test Stitch setup using active catalog and schema."""
    # Setup mocks
    mock_get_active_catalog.return_value = "active_catalog"
    mock_get_active_schema.return_value = "active_schema"

    llm_client_stub = LLMClientStub()
    mock_llm_client.return_value = llm_client_stub

    mock_helper_setup.return_value = {
        "stitch_config": {},
        "metadata": {
            "target_catalog": "active_catalog",
            "target_schema": "active_schema",
        },
    }
    mock_launch_job.return_value = {
        "message": "Stitch setup completed.",
        "tables_processed": 3,
        "config_created": True,
    }

    # Call function without catalog/schema args, with auto_confirm
    result = handle_command(client, **{"auto_confirm": True})

    # Verify results
    assert result.success
    mock_helper_setup.assert_called_once_with(
        client, llm_client_stub, "active_catalog", "active_schema"
    )
    mock_launch_job.assert_called_once_with(
        client,
        {},
        {"target_catalog": "active_catalog", "target_schema": "active_schema"},
    )


@patch("chuck_data.commands.setup_stitch.LLMClient")
@patch("chuck_data.commands.setup_stitch._helper_setup_stitch_logic")
@patch("chuck_data.commands.setup_stitch.get_metrics_collector")
def test_setup_with_helper_error(
    mock_get_metrics_collector, mock_helper_setup, mock_llm_client, client
):
    """Test handling when helper returns an error."""
    # Setup mocks
    llm_client_stub = LLMClientStub()
    mock_llm_client.return_value = llm_client_stub
    mock_metrics_collector = MagicMock()
    mock_get_metrics_collector.return_value = mock_metrics_collector

    mock_helper_setup.return_value = {"error": "Failed to scan tables for PII"}

    # Call function with auto_confirm
    result = handle_command(
        client,
        **{
            "catalog_name": "test_catalog",
            "schema_name": "test_schema",
            "auto_confirm": True,
        },
    )

    # Verify results
    assert not result.success
    assert result.message == "Failed to scan tables for PII"

    # Verify metrics collection for error
    mock_metrics_collector.track_event.assert_called_once_with(
        prompt="setup-stitch command",
        tools=[
            {
                "name": "setup_stitch",
                "arguments": {"catalog": "test_catalog", "schema": "test_schema"},
            }
        ],
        error="Failed to scan tables for PII",
        additional_data={
            "event_context": "direct_stitch_command",
            "status": "error",
        },
    )


@patch("chuck_data.commands.setup_stitch.LLMClient")
def test_setup_with_exception(mock_llm_client, client):
    """Test handling when an exception occurs."""
    # Setup mocks
    mock_llm_client.side_effect = Exception("LLM client error")

    # Call function
    result = handle_command(
        client, catalog_name="test_catalog", schema_name="test_schema"
    )

    # Verify results
    assert not result.success
    assert "Error setting up Stitch" in result.message
    assert str(result.error) == "LLM client error"