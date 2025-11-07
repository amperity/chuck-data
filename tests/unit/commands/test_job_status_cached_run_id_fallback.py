"""Test fallback to cached run_id when Chuck backend returns UNSET_DATABRICKS_RUN_ID."""

from unittest.mock import Mock, patch
from chuck_data.commands.job_status import handle_command, _query_by_job_id


def test_query_by_job_id_uses_cached_run_id_when_backend_returns_unset():
    """Test that we fallback to cached run_id when Chuck backend returns UNSET."""
    mock_amperity_client = Mock()
    mock_databricks_client = Mock()

    # Chuck backend returns UNSET for databricks-run-id
    mock_amperity_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "succeeded",
        "databricks-run-id": "UNSET_DATABRICKS_RUN_ID",
        "record-count": 1000,
        "build": "test-build",
        "created-at": "2024-01-01T00:00:00Z",
        "start-time": "2024-01-01T00:01:00Z",
        "end-time": "2024-01-01T00:10:00Z",
    }

    # Databricks API returns successful run data
    mock_databricks_client.get_job_run_status.return_value = {
        "job_id": 123,
        "run_id": 456,
        "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
        "execution_duration": 864000,  # 14.4 minutes
        "tasks": [{"task_key": "task1"}],
    }

    with patch(
        "chuck_data.commands.job_status.get_amperity_token", return_value="token"
    ):
        with patch(
            "chuck_data.commands.job_status.get_workspace_url",
            return_value="https://test.databricks.com",
        ):
            with patch(
                "chuck_data.commands.job_status.find_run_id_for_job",
                return_value="cached-run-456",
            ):
                result = _query_by_job_id(
                    "chk-123",
                    amperity_client=mock_amperity_client,
                    client=mock_databricks_client,
                    fetch_live=True,
                )

    # Verify success
    assert result.success is True

    # Verify Databricks API was called with the CACHED run_id, not UNSET
    mock_databricks_client.get_job_run_status.assert_called_once_with("cached-run-456")

    # Verify the response includes live Databricks data
    assert "databricks_live" in result.data
    assert result.data["databricks_live"]["execution_duration"] == 864000

    # Verify the message includes Databricks section
    assert "Databricks:" in result.message
    assert "14m 24s" in result.message  # Execution duration formatted


def test_query_by_job_id_without_cached_run_id_skips_live_fetch():
    """Test that when no cached run_id exists, we skip live data fetch."""
    mock_amperity_client = Mock()

    # Chuck backend returns UNSET for databricks-run-id
    mock_amperity_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "succeeded",
        "databricks-run-id": "UNSET_DATABRICKS_RUN_ID",
        "record-count": 1000,
        "build": "test-build",
        "created-at": "2024-01-01T00:00:00Z",
        "start-time": "2024-01-01T00:01:00Z",
        "end-time": "2024-01-01T00:10:00Z",
    }

    with patch(
        "chuck_data.commands.job_status.get_amperity_token", return_value="token"
    ):
        with patch(
            "chuck_data.commands.job_status.get_workspace_url",
            return_value="https://test.databricks.com",
        ):
            # No cached run_id available
            with patch(
                "chuck_data.commands.job_status.find_run_id_for_job", return_value=None
            ):
                result = _query_by_job_id(
                    "chk-123",
                    amperity_client=mock_amperity_client,
                    client=None,
                    fetch_live=True,
                )

    # Verify success
    assert result.success is True

    # Verify NO live Databricks data was fetched
    assert "databricks_live" not in result.data

    # Verify the message does NOT include Databricks section
    assert "Databricks:" not in result.message


def test_handle_command_parameterless_uses_cached_and_fetches_live():
    """Test that parameterless query uses cached job_id and automatically fetches live data."""
    mock_amperity_client = Mock()
    mock_databricks_client = Mock()

    # Chuck backend returns data with UNSET run_id
    mock_amperity_client.get_job_status.return_value = {
        "job-id": "chk-cached-123",
        "state": "running",
        "databricks-run-id": "UNSET_DATABRICKS_RUN_ID",
        "record-count": 5000,
        "build": "test-build",
        "created-at": "2024-01-01T00:00:00Z",
        "start-time": "2024-01-01T00:01:00Z",
    }

    # Databricks API returns running job data
    mock_databricks_client.get_job_run_status.return_value = {
        "job_id": 123,
        "run_id": 999,
        "state": {"life_cycle_state": "RUNNING"},
        "tasks": [{"task_key": "task1"}],
    }

    with patch(
        "chuck_data.commands.job_status.get_last_job_id", return_value="chk-cached-123"
    ):
        with patch(
            "chuck_data.commands.job_status.find_run_id_for_job",
            return_value="cached-run-999",
        ):
            with patch(
                "chuck_data.commands.job_status.get_amperity_token",
                return_value="token",
            ):
                with patch(
                    "chuck_data.commands.job_status.get_workspace_url",
                    return_value="https://test.databricks.com",
                ):
                    # Call without parameters
                    result = handle_command(
                        client=mock_databricks_client,
                        amperity_client=mock_amperity_client,
                    )

    # Verify success
    assert result.success is True

    # Verify Databricks API was called (live fetch happened automatically)
    mock_databricks_client.get_job_run_status.assert_called_once_with("cached-run-999")

    # Verify live data is included
    assert "databricks_live" in result.data

    # Verify message includes both Chuck data and Databricks section
    assert "Records: 5,000" in result.message
    assert "Databricks:" in result.message


def test_handle_command_with_explicit_live_flag_uses_cached_run_id():
    """Test that explicit --live flag works with cached run_id fallback."""
    mock_amperity_client = Mock()
    mock_databricks_client = Mock()

    # Chuck backend returns UNSET
    mock_amperity_client.get_job_status.return_value = {
        "job-id": "chk-explicit-456",
        "state": "succeeded",
        "databricks-run-id": "UNSET_DATABRICKS_RUN_ID",
        "record-count": 7155216,
        "build": "stitch-service-build/7766-6a34055",
        "created-at": "2025-10-30T22:07:48.925Z",
        "start-time": "2025-10-30T22:16:54.461738141Z",
        "end-time": "2025-10-30T22:25:16.347324102Z",
    }

    # Databricks returns completed job
    mock_databricks_client.get_job_run_status.return_value = {
        "job_id": 123,
        "run_id": 500955362493539,
        "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
        "execution_duration": 1024000,  # Real duration from user's example
        "tasks": [{"task_key": "task1"}, {"task_key": "task2"}],
    }

    with patch(
        "chuck_data.commands.job_status.find_run_id_for_job",
        return_value="500955362493539",
    ):
        with patch(
            "chuck_data.commands.job_status.get_amperity_token", return_value="token"
        ):
            with patch(
                "chuck_data.commands.job_status.get_workspace_url",
                return_value="https://test.databricks.com",
            ):
                # Explicitly pass --live flag
                result = handle_command(
                    client=mock_databricks_client,
                    amperity_client=mock_amperity_client,
                    job_id="chk-explicit-456",
                    live=True,
                )

    # Verify success
    assert result.success is True

    # Verify Databricks API was called with cached run_id
    mock_databricks_client.get_job_run_status.assert_called_once_with("500955362493539")

    # Verify both Chuck and Databricks data are present
    assert "Records: 7,155,216" in result.message
    assert "Databricks:" in result.message
    assert "17m 4s" in result.message  # 1024000ms = 17m 4s
    assert "Tasks: 2" in result.message
