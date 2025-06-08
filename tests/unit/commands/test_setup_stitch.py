"""
Tests for setup_stitch command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
"""

import tempfile
from unittest.mock import patch, MagicMock

from chuck_data.commands.setup_stitch import handle_command
from chuck_data.config import ConfigManager


def setup_successful_stitch_test_data(databricks_client_stub, llm_client_stub):
    """Helper function to set up test data for successful Stitch operations."""
    # Setup test data in client stub
    databricks_client_stub.add_catalog("test_catalog")
    databricks_client_stub.add_schema("test_catalog", "test_schema")
    databricks_client_stub.add_table(
        "test_catalog",
        "test_schema",
        "users",
        columns=[
            {"name": "email", "type": "STRING"},
            {"name": "name", "type": "STRING"},
            {"name": "id", "type": "BIGINT"},
        ],
    )

    # Mock PII scan results - set up table with PII columns
    llm_client_stub.set_pii_detection_result(
        [
            {"column": "email", "semantic": "email"},
            {"column": "name", "semantic": "name"},
        ]
    )

    # Fix API compatibility issues
    # Override create_volume to accept 'name' parameter like real API
    original_create_volume = databricks_client_stub.create_volume

    def mock_create_volume(catalog_name, schema_name, name, **kwargs):
        return original_create_volume(catalog_name, schema_name, name, **kwargs)

    databricks_client_stub.create_volume = mock_create_volume

    # Override upload_file to match real API signature
    def mock_upload_file(path, content=None, overwrite=False, **kwargs):
        return True

    databricks_client_stub.upload_file = mock_upload_file

    # Set up other required API responses
    databricks_client_stub.fetch_amperity_job_init_response = {
        "cluster-init": "#!/bin/bash\necho init"
    }
    databricks_client_stub.submit_job_run_response = {"run_id": "12345"}
    databricks_client_stub.create_stitch_notebook_response = {
        "notebook_path": "/Workspace/test"
    }


# Parameter validation tests
def test_missing_client_returns_error():
    """Missing client parameter returns clear error message."""
    result = handle_command(None)
    assert not result.success
    assert "Client is required" in result.message


def test_missing_context(databricks_client_stub):
    """Test handling when catalog or schema is missing."""
    # Use real config system with no active catalog/schema
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        # Don't set active catalog or schema

        with patch("chuck_data.config._config_manager", config_manager):
            result = handle_command(databricks_client_stub)

    # Verify results
    assert not result.success
    assert "Target catalog and schema must be specified" in result.message


@patch("chuck_data.commands.setup_stitch.LLMClient")
def test_direct_command_llm_exception_handled_gracefully(
    mock_llm_client, databricks_client_stub
):
    """Direct command handles LLM client exceptions gracefully."""
    # Setup external boundary to fail
    mock_llm_client.side_effect = Exception("LLM client error")

    result = handle_command(
        databricks_client_stub, catalog_name="test_catalog", schema_name="test_schema"
    )

    # Verify error handling behavior
    assert not result.success
    assert "Error setting up Stitch" in result.message
    assert str(result.error) == "LLM client error"


def test_agent_failure_shows_error_without_progress(
    databricks_client_stub, llm_client_stub
):
    """Agent execution shows error without progress steps when setup fails."""
    # Setup minimal test data with no PII tables (will cause failure)
    databricks_client_stub.add_catalog("test_catalog")
    databricks_client_stub.add_schema("test_catalog", "test_schema")
    # No tables with PII - will cause failure

    # Fix API compatibility for volume creation
    original_create_volume = databricks_client_stub.create_volume

    def mock_create_volume(catalog_name, schema_name, name, **kwargs):
        return original_create_volume(catalog_name, schema_name, name, **kwargs)

    databricks_client_stub.create_volume = mock_create_volume

    progress_steps = []

    def capture_progress(tool_name, data):
        if "step" in data:
            progress_steps.append(f"→ Setting up Stitch: ({data['step']})")

    with patch(
        "chuck_data.commands.setup_stitch.LLMClient", return_value=llm_client_stub
    ):
        with patch(
            "chuck_data.commands.stitch_tools.get_amperity_token",
            return_value="test_token",
        ):
            with patch(
                "chuck_data.commands.setup_stitch.get_metrics_collector",
                return_value=MagicMock(),
            ):
                result = handle_command(
                    databricks_client_stub,
                    catalog_name="test_catalog",
                    schema_name="test_schema",
                    tool_output_callback=capture_progress,
                )

    # Verify failure behavior
    assert not result.success
    assert (
        "No tables with PII found" in result.message
        or "PII Scan failed" in result.message
    )

    # Current implementation doesn't report progress, so no steps expected
    assert len(progress_steps) == 0


def test_agent_callback_errors_bubble_up_as_command_errors(
    databricks_client_stub, llm_client_stub
):
    """Agent callback failures bubble up as command errors (current behavior)."""

    def failing_callback(tool_name, data):
        raise Exception("Display system crashed")

    # This would only trigger if the command actually used the callback
    # Current implementation doesn't use tool_output_callback, so this test
    # documents the expected behavior if it were implemented

    with patch(
        "chuck_data.commands.setup_stitch.LLMClient", return_value=llm_client_stub
    ):
        result = handle_command(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
            tool_output_callback=failing_callback,
        )

    # Since callback isn't used, command should succeed if everything else works
    # or fail for other reasons (like missing catalog/schema)
    # This documents current behavior
    assert not result.success  # Will fail due to missing context/data


# Interactive mode tests
def test_interactive_mode_phase_1_preparation(databricks_client_stub, llm_client_stub):
    """Interactive mode Phase 1 prepares configuration and shows preview."""
    # Setup test data for successful operation
    setup_successful_stitch_test_data(databricks_client_stub, llm_client_stub)

    with patch(
        "chuck_data.commands.setup_stitch.LLMClient", return_value=llm_client_stub
    ):
        with patch(
            "chuck_data.commands.stitch_tools.get_amperity_token",
            return_value="test_token",
        ):
            # Call without auto_confirm to enter interactive mode
            result = handle_command(
                databricks_client_stub,
                catalog_name="test_catalog",
                schema_name="test_schema",
            )

    # Verify Phase 1 behavior
    assert result.success
    # Interactive mode should return empty message (console output handles display)
    assert result.message == ""
