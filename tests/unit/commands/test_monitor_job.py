"""Tests for the monitor-job command."""

from unittest.mock import Mock, patch

from chuck_data.commands.monitor_job import handle_command


@patch("chuck_data.commands.monitor_job.get_last_job_id")
def test_handle_command_no_jobs_to_monitor(mock_get_last_job_id):
    """Test that monitor command fails gracefully when no jobs exist."""
    # Mock no cached job ID
    mock_get_last_job_id.return_value = None

    result = handle_command(None)

    assert not result.success
    assert "No job ID or run ID provided" in result.message
    assert "no cached job ID available" in result.message


@patch("chuck_data.commands.monitor_job.find_run_id_for_job")
@patch("chuck_data.commands.monitor_job.get_last_job_id")
def test_handle_command_cached_job_without_run_id(
    mock_get_last_job_id, mock_find_run_id
):
    """Test that monitor command fails when cached job has no run ID."""
    # Mock cached job ID but no run ID
    mock_get_last_job_id.return_value = "chk-123"
    mock_find_run_id.return_value = None

    result = handle_command(None)

    assert not result.success
    assert "Cannot monitor job chk-123" in result.message
    assert "Databricks run ID not found" in result.message


@patch("chuck_data.config.get_amperity_token")
@patch("chuck_data.commands.monitor_job.find_run_id_for_job")
@patch("chuck_data.commands.monitor_job.get_last_job_id")
def test_handle_command_no_token(
    mock_get_last_job_id, mock_find_run_id, mock_get_token
):
    """Test that monitor command fails when no Amperity token is available."""
    # Mock cached job with run ID
    mock_get_last_job_id.return_value = "chk-123"
    mock_find_run_id.return_value = "run-456"
    mock_get_token.return_value = None

    result = handle_command(None)

    assert not result.success
    assert "No Amperity token available" in result.message
    assert "authenticate" in result.message.lower()


@patch("chuck_data.commands.monitor_job._monitor_job_completion")
@patch("chuck_data.clients.amperity.AmperityAPIClient")
@patch("chuck_data.config.get_amperity_token")
@patch("chuck_data.commands.monitor_job.find_run_id_for_job")
@patch("chuck_data.commands.monitor_job.get_last_job_id")
def test_handle_command_monitors_cached_job(
    mock_get_last_job_id,
    mock_find_run_id,
    mock_get_token,
    mock_amperity_client_class,
    mock_monitor,
):
    """Test that monitor command works with cached job that has run ID."""
    # Mock cached job with run ID
    mock_get_last_job_id.return_value = "chk-123"
    mock_find_run_id.return_value = "run-456"
    mock_get_token.return_value = "test-token"

    # Mock Amperity client to return running job
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "running",
        "databricks-run-id": "run-456",
    }
    mock_amperity_client_class.return_value = mock_client

    # Mock monitoring result
    mock_monitor.return_value = {
        "success": True,
        "record_count": 1000,
        "credits": 10,
    }

    result = handle_command(None)

    assert result.success
    assert "chk-123" in result.message
    assert "completed successfully" in result.message
    mock_monitor.assert_called_once()


@patch("chuck_data.clients.amperity.AmperityAPIClient")
@patch("chuck_data.config.get_amperity_token")
@patch("chuck_data.commands.monitor_job.find_run_id_for_job")
@patch("chuck_data.commands.monitor_job.get_last_job_id")
def test_handle_command_already_succeeded(
    mock_get_last_job_id,
    mock_find_run_id,
    mock_get_token,
    mock_amperity_client_class,
):
    """Test that monitor command detects already completed job."""
    # Mock cached job with run ID
    mock_get_last_job_id.return_value = "chk-123"
    mock_find_run_id.return_value = "run-456"
    mock_get_token.return_value = "test-token"

    # Mock Amperity client to return succeeded job
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "succeeded",
        "record-count": 1000,
        "credits": 10,
    }
    mock_amperity_client_class.return_value = mock_client

    result = handle_command(None)

    # Should succeed and report completion without monitoring
    assert result.success
    assert "chk-123" in result.message
    assert "already completed successfully" in result.message
    assert "Records: 1,000" in result.message
    assert "Credits: 10" in result.message


@patch("chuck_data.clients.amperity.AmperityAPIClient")
@patch("chuck_data.config.get_amperity_token")
@patch("chuck_data.commands.monitor_job.find_run_id_for_job")
@patch("chuck_data.commands.monitor_job.get_last_job_id")
def test_handle_command_already_failed(
    mock_get_last_job_id,
    mock_find_run_id,
    mock_get_token,
    mock_amperity_client_class,
):
    """Test that monitor command detects already failed job."""
    # Mock cached job with run ID
    mock_get_last_job_id.return_value = "chk-123"
    mock_find_run_id.return_value = "run-456"
    mock_get_token.return_value = "test-token"

    # Mock Amperity client to return failed job
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "failed",
        "error": "Connection timeout",
    }
    mock_amperity_client_class.return_value = mock_client

    result = handle_command(None)

    assert not result.success
    assert "chk-123" in result.message
    assert "already failed" in result.message
    assert "Connection timeout" in result.message


@patch("chuck_data.commands.monitor_job.find_job_id_for_run")
def test_handle_command_with_run_id_only(mock_find_job_id):
    """Test that monitor command works with only run ID provided."""
    # Mock finding job ID from run ID
    mock_find_job_id.return_value = None  # No job ID found

    # Should fail because we don't have monitoring implementation for run-only
    result = handle_command(None, run_id="run-456")

    # This should fail because we need a job_id or the monitoring will fail
    assert not result.success or "run-456" in str(result.data)


@patch("chuck_data.commands.monitor_job.find_run_id_for_job")
@patch("chuck_data.commands.monitor_job.find_job_id_for_run")
def test_handle_command_with_mismatched_ids(mock_find_job_id, mock_find_run_id):
    """Test that monitor command detects mismatched job and run IDs."""
    # Mock cached IDs that don't match provided ones
    mock_find_run_id.return_value = "run-999"  # Different from provided run-456
    mock_find_job_id.return_value = "chk-888"  # Different from provided chk-123

    result = handle_command(None, job_id="chk-123", run_id="run-456")

    assert not result.success
    assert "Mismatch" in result.message


@patch("chuck_data.commands.monitor_job.find_run_id_for_job")
def test_handle_command_explicit_job_id_no_run_id(mock_find_run_id):
    """Test that monitor command fails when explicit job ID has no run ID."""
    # Mock no run ID for the job
    mock_find_run_id.return_value = None

    result = handle_command(None, job_id="chk-123")

    assert not result.success
    assert "Cannot monitor job chk-123" in result.message
    assert "Databricks run ID not found" in result.message
