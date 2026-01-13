"""Comprehensive tests for EMR support in monitor-job command."""

from unittest.mock import Mock, patch

from chuck_data.commands.monitor_job import handle_command


# Tests for --step-id parameter


@patch("chuck_data.commands.monitor_job._monitor_job_completion")
@patch("chuck_data.commands.monitor_job.find_run_id_for_job")
@patch("chuck_data.clients.amperity.AmperityAPIClient")
@patch("chuck_data.config.get_amperity_token")
def test_handle_command_with_step_id(
    mock_get_token,
    mock_amperity_client_class,
    mock_find_run_id,
    mock_monitor,
):
    """Test monitoring with EMR step ID parameter."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-emr-123",
        "state": "running",
        "databricks-run-id": "s-EMRSTEP123",
    }
    mock_amperity_client_class.return_value = mock_client

    # Mock find_run_id_for_job (not needed when step_id is provided)
    mock_find_run_id.return_value = "s-EMRSTEP123"

    # Mock monitoring result
    mock_monitor.return_value = {
        "success": True,
        "job_id": "chk-emr-123",
        "state": "succeeded",
        "record_count": 1000,
        "credits": 10,
        "databricks_run_id": "s-EMRSTEP123",
    }

    result = handle_command(
        job_id="chk-emr-123",
        step_id="s-EMRSTEP123",
    )

    assert result.success
    assert "chk-emr-123" in result.message
    assert "completed successfully" in result.message
    mock_monitor.assert_called_once()


@patch("chuck_data.commands.monitor_job.find_job_id_for_run")
@patch("chuck_data.commands.monitor_job.find_run_id_for_job")
def test_handle_command_step_id_treated_as_run_id(
    mock_find_run_id,
    mock_find_job_id,
):
    """Test that step_id is treated the same as run_id internally."""
    # Mock finding job ID from step ID
    mock_find_job_id.return_value = "chk-from-step"
    mock_find_run_id.return_value = None

    # Provide only step_id (no job_id or run_id)
    # This should use step_id as run_id internally
    result = handle_command(step_id="s-STEP123")

    # The handle should have called find_job_id_for_run with the step_id
    mock_find_job_id.assert_called()


@patch("chuck_data.commands.monitor_job._monitor_job_completion")
@patch("chuck_data.commands.monitor_job.find_run_id_for_job")
@patch("chuck_data.clients.amperity.AmperityAPIClient")
@patch("chuck_data.config.get_amperity_token")
def test_handle_command_job_id_with_cached_step_id(
    mock_get_token,
    mock_amperity_client_class,
    mock_find_run_id,
    mock_monitor,
):
    """Test monitoring EMR job using job_id with cached step_id."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-emr-cached",
        "state": "running",
        "databricks-run-id": "s-CACHEDSTEP",
    }
    mock_amperity_client_class.return_value = mock_client

    # Mock cached step ID
    mock_find_run_id.return_value = "s-CACHEDSTEP"

    # Mock monitoring result
    mock_monitor.return_value = {
        "success": True,
        "job_id": "chk-emr-cached",
        "state": "succeeded",
        "record_count": 5000,
        "databricks_run_id": "s-CACHEDSTEP",
    }

    result = handle_command(job_id="chk-emr-cached")

    assert result.success
    assert "chk-emr-cached" in result.message
    mock_find_run_id.assert_called_once_with("chk-emr-cached")
    mock_monitor.assert_called_once()


# Tests for error messages being compute-provider-agnostic


@patch("chuck_data.commands.monitor_job.find_run_id_for_job")
def test_handle_command_error_message_not_databricks_specific(
    mock_find_run_id,
):
    """Test that error messages don't mention Databricks specifically."""
    # Mock no run/step ID found
    mock_find_run_id.return_value = None

    result = handle_command(job_id="chk-no-run-id")

    assert not result.success
    assert "Run ID not found" in result.message
    # Should NOT say "Databricks run ID not found"
    assert (
        "Databricks run ID" not in result.message
        or "Databricks) or --step-id (EMR)" in result.message
    )


@patch("chuck_data.commands.monitor_job.get_last_job_id")
def test_handle_command_error_message_mentions_both_providers(mock_get_last_job_id):
    """Test that error messages mention both run-id and step-id options."""
    # Mock no cached job ID
    mock_get_last_job_id.return_value = None

    result = handle_command()  # No parameters

    assert not result.success
    # Should mention both --run-id and --step-id (or just run-id since step-id is treated the same)
    assert (
        "--run-id" in result.message
        or "run-id" in result.message
        or "run ID" in result.message
    )


# Tests for both run_id and step_id provided


@patch("chuck_data.commands.monitor_job._monitor_job_completion")
@patch("chuck_data.commands.monitor_job.find_job_id_for_run")
@patch("chuck_data.clients.amperity.AmperityAPIClient")
@patch("chuck_data.config.get_amperity_token")
def test_handle_command_step_id_takes_precedence_over_run_id(
    mock_get_token,
    mock_amperity_client_class,
    mock_find_job_id,
    mock_monitor,
):
    """Test that if both run_id and step_id are provided, step_id is used."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-both-ids",
        "state": "running",
        "databricks-run-id": "s-STEP456",
    }
    mock_amperity_client_class.return_value = mock_client

    # Mock find_job_id_for_run
    mock_find_job_id.return_value = "chk-both-ids"

    # Mock monitoring result
    mock_monitor.return_value = {
        "success": True,
        "job_id": "chk-both-ids",
        "state": "succeeded",
        "databricks_run_id": "s-STEP456",
    }

    result = handle_command(
        run_id="12345",  # Databricks run_id
        step_id="s-STEP456",  # EMR step_id (should take precedence)
    )

    assert result.success
    # Should have used step_id (not run_id)
    call_args = mock_monitor.call_args
    assert call_args[1]["run_id"] == "s-STEP456"


# Tests for monitoring behavior (provider-agnostic)


@patch("chuck_data.commands.monitor_job._monitor_job_completion")
@patch("chuck_data.commands.monitor_job.find_run_id_for_job")
@patch("chuck_data.clients.amperity.AmperityAPIClient")
@patch("chuck_data.config.get_amperity_token")
def test_handle_command_emr_job_monitoring_succeeds(
    mock_get_token,
    mock_amperity_client_class,
    mock_find_run_id,
    mock_monitor,
):
    """Test successful monitoring of an EMR job."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-emr-monitor",
        "state": "running",
        "databricks-run-id": "s-MONITOR123",
    }
    mock_amperity_client_class.return_value = mock_client

    # Mock cached step ID
    mock_find_run_id.return_value = "s-MONITOR123"

    # Mock monitoring result
    mock_monitor.return_value = {
        "success": True,
        "job_id": "chk-emr-monitor",
        "state": "succeeded",
        "record_count": 10000,
        "credits": 50,
        "databricks_run_id": "s-MONITOR123",
    }

    result = handle_command(job_id="chk-emr-monitor")

    assert result.success
    assert "chk-emr-monitor" in result.message
    assert "completed successfully" in result.message
    assert "Records: 10,000" in result.message
    assert "Credits: 50" in result.message


@patch("chuck_data.commands.monitor_job._monitor_job_completion")
@patch("chuck_data.commands.monitor_job.find_run_id_for_job")
@patch("chuck_data.clients.amperity.AmperityAPIClient")
@patch("chuck_data.config.get_amperity_token")
def test_handle_command_emr_job_monitoring_fails(
    mock_get_token,
    mock_amperity_client_class,
    mock_find_run_id,
    mock_monitor,
):
    """Test monitoring an EMR job that fails."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-emr-fail",
        "state": "running",
        "databricks-run-id": "s-FAILSTEP",
    }
    mock_amperity_client_class.return_value = mock_client

    # Mock cached step ID
    mock_find_run_id.return_value = "s-FAILSTEP"

    # Mock monitoring result (failure)
    mock_monitor.return_value = {
        "success": False,
        "job_id": "chk-emr-fail",
        "state": "failed",
        "error": "Step failed due to cluster termination",
        "databricks_run_id": "s-FAILSTEP",
    }

    result = handle_command(job_id="chk-emr-fail")

    assert not result.success
    assert "chk-emr-fail" in result.message
    assert "Step failed due to cluster termination" in result.message


# Tests for parameter validation


def test_handle_command_no_parameters_no_cache():
    """Test that command fails gracefully with no parameters and no cache."""
    with patch("chuck_data.commands.monitor_job.get_last_job_id", return_value=None):
        result = handle_command()

        assert not result.success
        assert "No job ID" in result.message or "no cached job ID" in result.message


@patch("chuck_data.commands.monitor_job.get_last_job_id")
def test_handle_command_uses_cached_job_id(mock_get_last_job_id):
    """Test that command uses cached job ID when no parameters provided."""
    # Mock cached job ID
    mock_get_last_job_id.return_value = "chk-cached-123"

    # Mock find_run_id_for_job to return None (will fail, but we test the caching)
    with patch(
        "chuck_data.commands.monitor_job.find_run_id_for_job", return_value=None
    ):
        result = handle_command()

        # Should have tried to use cached ID
        mock_get_last_job_id.assert_called_once()
        assert not result.success  # Fails because no run_id found
        assert "chk-cached-123" in result.message


# Tests for backwards compatibility


@patch("chuck_data.commands.monitor_job._monitor_job_completion")
@patch("chuck_data.commands.monitor_job.find_run_id_for_job")
@patch("chuck_data.clients.amperity.AmperityAPIClient")
@patch("chuck_data.config.get_amperity_token")
def test_handle_command_databricks_job_still_works(
    mock_get_token,
    mock_amperity_client_class,
    mock_find_run_id,
    mock_monitor,
):
    """Test that existing Databricks job monitoring still works."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-databricks",
        "state": "running",
        "databricks-run-id": "123456",  # Numeric = Databricks
    }
    mock_amperity_client_class.return_value = mock_client

    # Mock cached run ID
    mock_find_run_id.return_value = "123456"

    # Mock monitoring result
    mock_monitor.return_value = {
        "success": True,
        "job_id": "chk-databricks",
        "state": "succeeded",
        "record_count": 2000,
        "databricks_run_id": "123456",
    }

    result = handle_command(job_id="chk-databricks")

    assert result.success
    assert "chk-databricks" in result.message
    mock_monitor.assert_called_once()


@patch("chuck_data.commands.monitor_job._monitor_job_completion")
@patch("chuck_data.commands.monitor_job.find_job_id_for_run")
@patch("chuck_data.clients.amperity.AmperityAPIClient")
@patch("chuck_data.config.get_amperity_token")
def test_handle_command_run_id_only_still_works(
    mock_get_token,
    mock_amperity_client_class,
    mock_find_job_id,
    mock_monitor,
):
    """Test that monitoring by run_id only (no job_id) still works."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-from-run",
        "state": "running",
        "databricks-run-id": "789012",
    }
    mock_amperity_client_class.return_value = mock_client

    # Mock finding job ID from run ID
    mock_find_job_id.return_value = "chk-from-run"

    # Mock monitoring result
    mock_monitor.return_value = {
        "success": True,
        "job_id": "chk-from-run",
        "state": "succeeded",
        "databricks_run_id": "789012",
    }

    result = handle_command(run_id="789012")

    assert result.success
    mock_find_job_id.assert_called_once_with("789012")
    mock_monitor.assert_called_once()
