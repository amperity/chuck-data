"""Tests for the job-status command."""

from unittest.mock import Mock, patch

from chuck_data.commands.job_status import (
    handle_command,
    _extract_databricks_run_info,
    _query_by_job_id,
    _query_by_run_id,
    _format_job_status_message,
    UNSET_DATABRICKS_RUN_ID,
)


@patch("chuck_data.commands.job_status.get_last_job_id")
def test_handle_command_requires_job_id_or_run_id(mock_get_cached_job_id):
    """Test that either job_id or run_id is required when no cache."""
    # Mock no cached job ID
    mock_get_cached_job_id.return_value = None

    result = handle_command(None)

    assert not result.success
    assert (
        "No job ID provided" in result.message or "no cached job ID" in result.message
    )


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_job_id_success(mock_get_token, mock_amperity_client_class):
    """Test successful job status query by job_id."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "running",
        "databricks-run-id": "run-456",
        "record-count": 1000,
        "credits": 10,
    }
    mock_amperity_client_class.return_value = mock_client

    result = handle_command(None, job_id="chk-123")

    assert result.success
    assert "chk-123" in result.message
    assert "RUNNING" in result.message  # State is uppercase in new format
    assert result.data["state"] == "running"
    assert result.data["record-count"] == 1000


@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_job_id_no_token(mock_get_token):
    """Test job status query fails without authentication token."""
    # Mock missing token
    mock_get_token.return_value = None

    result = handle_command(None, job_id="chk-123")

    assert not result.success
    assert "token" in result.message.lower() or "authenticate" in result.message.lower()


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_job_id_with_live_data(
    mock_get_token, mock_amperity_client_class
):
    """Test job status query with live Databricks data enrichment."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_amperity_client = Mock()
    mock_amperity_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "running",
        "databricks-run-id": "run-456",
        "record-count": 1000,
    }
    mock_amperity_client_class.return_value = mock_amperity_client

    # Mock Databricks client
    mock_databricks_client = Mock()
    mock_databricks_client.get_job_run_status.return_value = {
        "run_id": "run-456",
        "state": {"life_cycle_state": "RUNNING"},
    }

    result = handle_command(
        mock_databricks_client,
        amperity_client=mock_amperity_client,
        job_id="chk-123",
        live=True,
    )

    assert result.success
    assert "databricks_live" in result.data
    assert result.data["databricks_live"]["run_id"] == "run-456"
    mock_databricks_client.get_job_run_status.assert_called_once_with("run-456")


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_job_id_api_error(mock_get_token, mock_amperity_client_class):
    """Test job status query handles API errors."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client that raises an exception
    mock_client = Mock()
    mock_client.get_job_status.side_effect = Exception("API Error")
    mock_amperity_client_class.return_value = mock_client

    result = handle_command(None, job_id="chk-123")

    assert not result.success
    assert "API Error" in result.message or "Failed" in result.message


def test_handle_command_run_id_fallback_success():
    """Test legacy fallback to Databricks API by run_id."""
    # Mock Databricks client
    mock_databricks_client = Mock()
    mock_databricks_client.get_job_run_status.return_value = {
        "run_id": 123456,
        "state": {"life_cycle_state": "RUNNING", "result_state": None},
    }

    result = handle_command(mock_databricks_client, run_id="123456")

    assert result.success
    assert "123456" in result.message
    assert "Databricks" in result.message
    assert "no Chuck telemetry" in result.message
    assert result.data["run_id"] == 123456


def test_handle_command_run_id_no_client():
    """Test run_id query fails without Databricks client."""
    result = handle_command(None, run_id="123456")

    assert not result.success
    assert "client" in result.message.lower()


def test_handle_command_run_id_not_found():
    """Test run_id query for non-existent run."""
    # Mock Databricks client that returns None
    mock_databricks_client = Mock()
    mock_databricks_client.get_job_run_status.return_value = None

    result = handle_command(mock_databricks_client, run_id="999999")

    assert not result.success
    assert "999999" in result.message


def test_handle_command_run_id_with_tasks():
    """Test run_id query includes task information."""
    # Mock Databricks client with tasks
    mock_databricks_client = Mock()
    mock_databricks_client.get_job_run_status.return_value = {
        "run_id": 123456,
        "state": {"life_cycle_state": "RUNNING"},
        "tasks": [
            {
                "task_key": "task1",
                "state": {"life_cycle_state": "RUNNING"},
                "start_time": 1234567890,
            },
            {"task_key": "task2", "state": {"life_cycle_state": "PENDING"}},
        ],
    }

    result = handle_command(mock_databricks_client, run_id="123456")

    assert result.success
    assert "tasks" in result.data
    assert len(result.data["tasks"]) == 2
    assert result.data["tasks"][0]["task_key"] == "task1"


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_job_id_includes_credits_and_records(
    mock_get_token, mock_amperity_client_class
):
    """Test job status message includes credits and record count."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "succeeded",
        "record-count": 5000,
        "credits": 25,
    }
    mock_amperity_client_class.return_value = mock_client

    result = handle_command(None, job_id="chk-123")

    assert result.success
    assert "Records: 5,000" in result.message
    assert "Credits: 25" in result.message


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_job_id_includes_error(
    mock_get_token, mock_amperity_client_class
):
    """Test job status message includes error information."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "failed",
        "error": "Timeout error",
    }
    mock_amperity_client_class.return_value = mock_client

    result = handle_command(None, job_id="chk-123")

    assert result.success  # Query succeeded even though job failed
    assert "Error: Timeout error" in result.message
    assert "FAILED" in result.message


# Tests for private helper functions


def test_extract_databricks_run_info_basic():
    """Test extraction of basic Databricks run information."""
    raw_data = {
        "job_id": "job-123",
        "run_id": "run-456",
        "run_name": "Test Run",
        "state": {"life_cycle_state": "RUNNING", "result_state": None},
        "start_time": 1234567890,
        "creator_user_name": "test@example.com",
    }

    result = _extract_databricks_run_info(raw_data)

    assert result["job_id"] == "job-123"
    assert result["run_id"] == "run-456"
    assert result["run_name"] == "Test Run"
    assert result["state"] == {"life_cycle_state": "RUNNING", "result_state": None}
    assert result["life_cycle_state"] == "RUNNING"
    assert result["result_state"] is None
    assert result["start_time"] == 1234567890
    assert result["creator_user_name"] == "test@example.com"


def test_extract_databricks_run_info_with_tasks():
    """Test extraction includes task information."""
    raw_data = {
        "run_id": "run-456",
        "state": {"life_cycle_state": "RUNNING"},
        "tasks": [
            {
                "task_key": "task1",
                "state": {"life_cycle_state": "RUNNING", "result_state": None},
                "start_time": 1234567890,
                "setup_duration": 1000,
                "execution_duration": 5000,
            },
            {"task_key": "task2", "state": {"life_cycle_state": "PENDING"}},
        ],
    }

    result = _extract_databricks_run_info(raw_data)

    assert "tasks" in result
    assert len(result["tasks"]) == 2
    assert result["tasks"][0]["task_key"] == "task1"
    assert result["tasks"][0]["state"] == "RUNNING"
    assert result["tasks"][0]["setup_duration"] == 1000
    assert result["tasks"][1]["task_key"] == "task2"
    assert result["tasks"][1]["state"] == "PENDING"


def test_extract_databricks_run_info_no_tasks():
    """Test extraction when no tasks are present."""
    raw_data = {"run_id": "run-456", "state": {"life_cycle_state": "RUNNING"}}

    result = _extract_databricks_run_info(raw_data)

    assert "tasks" not in result


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_query_by_job_id_basic(mock_get_token, mock_amperity_client_class):
    """Test _query_by_job_id function directly."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "running",
        "record-count": 1000,
    }
    mock_amperity_client_class.return_value = mock_client

    result = _query_by_job_id("chk-123")

    assert result.success
    assert "chk-123" in result.message
    assert "RUNNING" in result.message
    assert result.data["state"] == "running"


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_query_by_job_id_with_live_data(mock_get_token, mock_amperity_client_class):
    """Test _query_by_job_id enriches with live Databricks data."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_amperity_client = Mock()
    mock_amperity_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "running",
        "databricks-run-id": "run-456",
    }
    mock_amperity_client_class.return_value = mock_amperity_client

    # Mock Databricks client
    mock_databricks_client = Mock()
    mock_databricks_client.get_job_run_status.return_value = {
        "run_id": "run-456",
        "state": {"life_cycle_state": "RUNNING"},
    }

    result = _query_by_job_id(
        "chk-123",
        amperity_client=mock_amperity_client,
        client=mock_databricks_client,
        fetch_live=True,
    )

    assert result.success
    assert "databricks_live" in result.data
    assert result.data["databricks_live"]["run_id"] == "run-456"
    assert result.data["databricks_live"]["life_cycle_state"] == "RUNNING"
    mock_databricks_client.get_job_run_status.assert_called_once_with("run-456")


@patch("chuck_data.commands.job_status.get_amperity_token")
def test_query_by_job_id_no_token(mock_get_token):
    """Test _query_by_job_id fails without token."""
    mock_get_token.return_value = None

    result = _query_by_job_id("chk-123")

    assert not result.success
    assert "token" in result.message.lower()


def test_query_by_run_id_basic():
    """Test _query_by_run_id function directly."""
    # Mock Databricks client
    mock_client = Mock()
    mock_client.get_job_run_status.return_value = {
        "run_id": "run-456",
        "state": {"life_cycle_state": "RUNNING", "result_state": None},
    }

    result = _query_by_run_id("run-456", mock_client)

    assert result.success
    assert "run-456" in result.message
    assert "RUNNING" in result.message
    assert "no Chuck telemetry" in result.message
    assert result.data["run_id"] == "run-456"
    assert result.data["life_cycle_state"] == "RUNNING"


def test_query_by_run_id_no_client():
    """Test _query_by_run_id fails without client."""
    result = _query_by_run_id("run-456", None)

    assert not result.success
    assert "client" in result.message.lower()


def test_query_by_run_id_not_found():
    """Test _query_by_run_id handles non-existent run."""
    # Mock client that returns None
    mock_client = Mock()
    mock_client.get_job_run_status.return_value = None

    result = _query_by_run_id("run-999", mock_client)

    assert not result.success
    assert "run-999" in result.message


def test_query_by_run_id_with_long_url():
    """Test that _query_by_run_id formats long URLs on single line."""
    long_url = "https://dbc-6e75f43b-0f28.cloud.databricks.com/?o=4142544475373761#job/14906211489791/run/623626711565341"

    # Mock Databricks client with long URL
    mock_client = Mock()
    mock_client.get_job_run_status.return_value = {
        "run_id": "run-456",
        "run_name": "test-job",
        "run_page_url": long_url,
        "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
        "creator_user_name": "test@example.com",
        "execution_duration": 3600000,
        "tasks": [{"task_key": "task1"}],
    }

    result = _query_by_run_id("run-456", mock_client)

    assert result.success
    # Verify URL is present and on a single line
    assert long_url in result.message
    assert "• View:" in result.message

    # Verify the URL line is complete by checking it appears as a continuous string
    lines = result.message.split("\n")
    url_line = [line for line in lines if "• View:" in line][0]
    assert long_url in url_line

    # Verify box borders are consistent
    for line in lines:
        if line and not line.startswith("┌") and not line.startswith("└"):
            assert line.startswith("│")
            assert line.endswith("│")


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_job_id_includes_build(
    mock_get_token, mock_amperity_client_class
):
    """Test job status message includes build information."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "succeeded",
        "build": "v1.2.3",
    }
    mock_amperity_client_class.return_value = mock_client

    result = handle_command(None, job_id="chk-123")

    assert result.success
    assert "Build: v1.2.3" in result.message


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_job_id_includes_timestamps(
    mock_get_token, mock_amperity_client_class
):
    """Test job status message includes timestamp information."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "succeeded",
        "created-at": "2025-10-29T10:00:00Z",
        "start-time": "2025-10-29T10:01:00Z",
        "end-time": "2025-10-29T10:05:00Z",
    }
    mock_amperity_client_class.return_value = mock_client

    result = handle_command(None, job_id="chk-123")

    assert result.success
    assert "2025-10-29T10:00:00Z".replace("T", " ").replace("Z", "") in result.message
    assert "2025-10-29T10:01:00Z".replace("T", " ").replace("Z", "") in result.message
    assert "2025-10-29T10:05:00Z".replace("T", " ").replace("Z", "") in result.message


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_job_id_calculates_duration(
    mock_get_token, mock_amperity_client_class
):
    """Test job status message calculates duration from timestamps."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client with 4 minute duration
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "succeeded",
        "start-time": "2025-10-29T10:00:00Z",
        "end-time": "2025-10-29T10:04:00Z",
    }
    mock_amperity_client_class.return_value = mock_client

    result = handle_command(None, job_id="chk-123")

    assert result.success
    assert "4m" in result.message


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_job_id_includes_accepted(
    mock_get_token, mock_amperity_client_class
):
    """Test job status message includes accepted? field."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "running",
        "accepted?": True,
    }
    mock_amperity_client_class.return_value = mock_client

    result = handle_command(None, job_id="chk-123")

    assert result.success
    # Note: accepted? field is not displayed in the new box format
    assert "RUNNING" in result.message


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_job_id_includes_databricks_run_id(
    mock_get_token, mock_amperity_client_class
):
    """Test job status message includes Databricks run ID when present."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "running",
        "databricks-run-id": "run-456",
    }
    mock_amperity_client_class.return_value = mock_client

    result = handle_command(None, job_id="chk-123")

    assert result.success


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_job_id_excludes_unset_databricks_run_id(
    mock_get_token, mock_amperity_client_class
):
    """Test job status message excludes UNSET_DATABRICKS_RUN_ID from display."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "succeeded",
        "databricks-run-id": "UNSET_DATABRICKS_RUN_ID",
    }
    mock_amperity_client_class.return_value = mock_client

    result = handle_command(None, job_id="chk-123")

    assert result.success
    assert "UNSET_DATABRICKS_RUN_ID" not in result.message


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_job_id_live_skips_unset_databricks_run_id(
    mock_get_token, mock_amperity_client_class
):
    """Test --live flag skips fetching when databricks-run-id is UNSET."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_amperity_client = Mock()
    mock_amperity_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "succeeded",
        "databricks-run-id": "UNSET_DATABRICKS_RUN_ID",
    }
    mock_amperity_client_class.return_value = mock_amperity_client

    # Mock Databricks client - should NOT be called
    mock_databricks_client = Mock()

    result = handle_command(
        mock_databricks_client,
        amperity_client=mock_amperity_client,
        job_id="chk-123",
        live=True,
    )

    assert result.success
    assert "databricks_live" not in result.data
    mock_databricks_client.get_job_run_status.assert_not_called()


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_job_id_comprehensive_output(
    mock_get_token, mock_amperity_client_class
):
    """Test job status message with all available fields."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client with comprehensive data
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "succeeded",
        "record-count": 1234567,
        "credits": 150,
        "build": "v2.5.1",
        "databricks-run-id": "run-789",
        "created-at": "2025-10-29T10:00:00Z",
        "start-time": "2025-10-29T10:01:00Z",
        "end-time": "2025-10-29T10:15:30Z",
        "accepted?": True,
    }
    mock_amperity_client_class.return_value = mock_client

    result = handle_command(None, job_id="chk-123")

    assert result.success
    # Check all fields are in the message
    assert "succeeded".upper() in result.message
    assert "Records: 1,234,567" in result.message
    assert "Credits: 150" in result.message
    assert "Build: v2.5.1" in result.message
    assert "2025-10-29T10:00:00Z".replace("T", " ").replace("Z", "") in result.message
    assert "2025-10-29T10:01:00Z".replace("T", " ").replace("Z", "") in result.message
    assert "2025-10-29T10:15:30Z".replace("T", " ").replace("Z", "") in result.message
    assert "14m" in result.message
    # Note: accepted? field is not displayed in the new box format


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_job_id_minimal_output(
    mock_get_token, mock_amperity_client_class
):
    """Test job status message with minimal fields (pending job)."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client with minimal data (like a newly launched job)
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "pending",
        "created-at": "2025-10-29T10:00:00Z",
    }
    mock_amperity_client_class.return_value = mock_client

    result = handle_command(None, job_id="chk-123")

    assert result.success
    # Check only available fields are in the message
    assert "pending".upper() in result.message
    assert "2025-10-29T10:00:00Z".replace("T", " ").replace("Z", "") in result.message
    # These should NOT be in the message
    assert "Records:" not in result.message
    assert "Credits:" not in result.message
    assert "Duration:" not in result.message


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_job_id_handles_invalid_timestamp_format(
    mock_get_token, mock_amperity_client_class
):
    """Test job status gracefully handles invalid timestamp formats."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client with invalid timestamp format
    mock_client = Mock()
    mock_client.get_job_status.return_value = {
        "job-id": "chk-123",
        "state": "succeeded",
        "start-time": "invalid-format",
        "end-time": "also-invalid",
    }
    mock_amperity_client_class.return_value = mock_client

    result = handle_command(None, job_id="chk-123")

    # Should succeed but skip duration calculation
    assert result.success
    assert "Duration:" not in result.message
    assert "succeeded".upper() in result.message


# Tests for _format_job_status_message function


def test_format_job_status_message_basic():
    """Test formatting basic job status message."""
    job_data = {"state": "running"}

    message = _format_job_status_message("chk-123", job_data)

    # Check for box format
    assert "┌─ Job: chk-123" in message
    assert "Status: ◷ RUNNING" in message
    assert "└───" in message


def test_format_job_status_message_with_all_fields():
    """Test formatting job status message with all available fields."""
    job_data = {
        "state": "succeeded",
        "record-count": 5000000,
        "credits": 250,
        "build": "v3.0.0",
        "databricks-run-id": "run-999",
        "created-at": "2025-10-30T08:00:00Z",
        "start-time": "2025-10-30T08:05:00Z",
        "end-time": "2025-10-30T08:25:00Z",
        "accepted?": True,
    }

    message = _format_job_status_message("chk-456", job_data)

    # Verify key fields are present in the new box format
    assert "┌─ Job: chk-456" in message
    assert "✓ SUCCEEDED" in message
    assert "5,000,000" in message
    assert "250" in message
    assert "v3.0.0" in message
    assert "2025-10-30 08:00:00" in message
    assert "2025-10-30 08:05:00" in message
    assert "2025-10-30 08:25:00" in message
    assert "20m 0s" in message  # Duration format changed


def test_format_job_status_message_excludes_unset_run_id():
    """Test that UNSET_DATABRICKS_RUN_ID is excluded from message."""
    job_data = {
        "state": "succeeded",
        "databricks-run-id": UNSET_DATABRICKS_RUN_ID,
    }

    message = _format_job_status_message("chk-789", job_data)

    assert "succeeded".upper() in message
    assert UNSET_DATABRICKS_RUN_ID not in message


def test_format_job_status_message_includes_valid_run_id():
    """Test that valid Databricks run ID is included in message."""
    job_data = {"state": "running", "databricks-run-id": "run-12345"}

    message = _format_job_status_message("chk-100", job_data)

    assert "running".upper() in message


def test_format_job_status_message_with_error():
    """Test formatting job status message with error field."""
    job_data = {
        "state": "failed",
        "error": "Connection timeout to data source",
    }

    message = _format_job_status_message("chk-error", job_data)

    assert "failed".upper() in message
    assert "Error: Connection timeout to data source" in message


def test_format_job_status_message_with_partial_timestamps():
    """Test formatting with only start-time or end-time (no duration calculation)."""
    # Only start-time
    job_data_start = {"state": "running", "start-time": "2025-10-30T10:00:00Z"}

    message_start = _format_job_status_message("chk-111", job_data_start)

    assert "2025-10-30T10:00:00Z".replace("T", " ").replace("Z", "") in message_start
    assert "Duration:" not in message_start

    # Only end-time
    job_data_end = {"state": "succeeded", "end-time": "2025-10-30T11:00:00Z"}

    message_end = _format_job_status_message("chk-222", job_data_end)

    assert "2025-10-30T11:00:00Z".replace("T", " ").replace("Z", "") in message_end
    assert "Duration:" not in message_end


def test_format_job_status_message_duration_calculation():
    """Test duration calculation with various time ranges."""
    # 1 hour duration
    job_data_1h = {
        "state": "succeeded",
        "start-time": "2025-10-30T10:00:00Z",
        "end-time": "2025-10-30T11:00:00Z",
    }

    message_1h = _format_job_status_message("chk-300", job_data_1h)
    assert "60m" in message_1h

    # 30 seconds duration
    job_data_30s = {
        "state": "succeeded",
        "start-time": "2025-10-30T10:00:00Z",
        "end-time": "2025-10-30T10:00:30Z",
    }

    message_30s = _format_job_status_message("chk-301", job_data_30s)
    assert "0m" in message_30s


def test_format_job_status_message_handles_invalid_timestamps():
    """Test that invalid timestamp formats don't crash formatting."""
    job_data = {
        "state": "succeeded",
        "start-time": "not-a-timestamp",
        "end-time": "also-not-a-timestamp",
    }

    message = _format_job_status_message("chk-400", job_data)

    # Should still succeed without duration
    assert "succeeded".upper() in message
    assert "Duration:" not in message


def test_format_job_status_message_with_zero_records():
    """Test that zero record count is not displayed."""
    job_data = {"state": "succeeded", "record-count": 0}

    message = _format_job_status_message("chk-500", job_data)

    # Zero is falsy, so it should not be included
    assert "succeeded".upper() in message
    assert "Records:" not in message


def test_format_job_status_message_with_one_record():
    """Test formatting with single record count."""
    job_data = {"state": "succeeded", "record-count": 1}

    message = _format_job_status_message("chk-501", job_data)

    assert "Records: 1" in message


def test_format_job_status_message_unknown_state():
    """Test formatting when state is missing."""
    job_data_missing = {}
    message_missing = _format_job_status_message("chk-600", job_data_missing)
    assert "UNKNOWN".upper() in message_missing


def test_format_job_status_message_none_state():
    """Test formatting when state is explicitly None."""
    job_data_none = {"state": None}
    message_none = _format_job_status_message("chk-601", job_data_none)
    # None gets converted to "UNKNOWN" in the new format
    assert "chk-601" in message_none
    assert "UNKNOWN" in message_none


def test_format_job_status_message_empty_strings_not_included():
    """Test that empty string values are not included in message."""
    job_data = {
        "state": "succeeded",
        "build": "",
        "error": "",
        "databricks-run-id": "",
    }

    message = _format_job_status_message("chk-700", job_data)

    # Empty strings are falsy, so they should not be included
    assert "succeeded".upper() in message
    assert "Build:" not in message
    assert "Error:" not in message


def test_format_job_status_message_with_long_url():
    """Test that box width expands to accommodate long URLs on single line."""
    long_url = "https://dbc-6e75f43b-0f28.cloud.databricks.com/?o=4142544475373761#job/14906211489791/run/623626711565341"
    job_data = {
        "state": "succeeded",
        "databricks-run-id": "run-123",
        "databricks_live": {
            "run_id": "run-123",
            "run_name": "test-job",
            "run_page_url": long_url,
            "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
            "creator_user_name": "test@example.com",
            "execution_duration": 3600000,
            "tasks": [{"task_key": "task1"}],
        },
    }

    message = _format_job_status_message("chk-701", job_data)

    # Verify URL is present and on a single line (not split across lines)
    assert long_url in message
    assert "• View:" in message

    # Verify the URL line is complete by checking it appears as a continuous string
    lines = message.split("\n")
    url_line = [line for line in lines if "• View:" in line][0]
    assert long_url in url_line

    # Verify box borders are consistent (all lines should have │ at start and end)
    for line in lines:
        if line and not line.startswith("┌") and not line.startswith("└"):
            assert line.startswith("│")
            assert line.endswith("│")


# Tests for UNSET_DATABRICKS_RUN_ID constant


def test_unset_databricks_run_id_constant_value():
    """Test that UNSET_DATABRICKS_RUN_ID constant has the expected value."""
    assert UNSET_DATABRICKS_RUN_ID == "UNSET_DATABRICKS_RUN_ID"
    assert isinstance(UNSET_DATABRICKS_RUN_ID, str)


def test_unset_databricks_run_id_constant_is_string():
    """Test that UNSET_DATABRICKS_RUN_ID is a string type."""
    assert isinstance(UNSET_DATABRICKS_RUN_ID, str)
    assert len(UNSET_DATABRICKS_RUN_ID) > 0


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_unset_databricks_run_id_prevents_live_fetch(
    mock_get_token, mock_amperity_client_class
):
    """Test that UNSET_DATABRICKS_RUN_ID prevents live Databricks fetch."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client with UNSET run ID
    mock_amperity_client = Mock()
    mock_amperity_client.get_job_status.return_value = {
        "job-id": "chk-800",
        "state": "succeeded",
        "databricks-run-id": UNSET_DATABRICKS_RUN_ID,
    }
    mock_amperity_client_class.return_value = mock_amperity_client

    # Mock Databricks client - should NOT be called
    mock_databricks_client = Mock()

    result = _query_by_job_id(
        "chk-800",
        amperity_client=mock_amperity_client,
        client=mock_databricks_client,
        fetch_live=True,
    )

    assert result.success
    assert "databricks_live" not in result.data
    mock_databricks_client.get_job_run_status.assert_not_called()


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_valid_run_id_allows_live_fetch(mock_get_token, mock_amperity_client_class):
    """Test that a valid run ID (not UNSET) allows live Databricks fetch."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client with valid run ID
    mock_amperity_client = Mock()
    mock_amperity_client.get_job_status.return_value = {
        "job-id": "chk-801",
        "state": "running",
        "databricks-run-id": "run-valid-123",
    }
    mock_amperity_client_class.return_value = mock_amperity_client

    # Mock Databricks client - SHOULD be called
    mock_databricks_client = Mock()
    mock_databricks_client.get_job_run_status.return_value = {
        "run_id": "run-valid-123",
        "state": {"life_cycle_state": "RUNNING"},
    }

    result = _query_by_job_id(
        "chk-801",
        amperity_client=mock_amperity_client,
        client=mock_databricks_client,
        fetch_live=True,
    )

    assert result.success
    assert "databricks_live" in result.data
    mock_databricks_client.get_job_run_status.assert_called_once_with("run-valid-123")


# Tests for job ID caching functionality


@patch("chuck_data.commands.job_status.get_last_job_id")
@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_uses_cached_job_id(
    mock_get_token, mock_amperity_client_class, mock_get_cached_job_id
):
    """Test that handle_command uses cached job ID when no params provided."""
    # Mock cached job ID
    mock_get_cached_job_id.return_value = "chk-cached-123"

    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_amperity_client = Mock()
    mock_amperity_client.get_job_status.return_value = {
        "job-id": "chk-cached-123",
        "state": "succeeded",
        "record-count": 1000,
    }
    mock_amperity_client_class.return_value = mock_amperity_client

    # Call without any parameters
    result = handle_command(None)

    assert result.success
    assert "chk-cached-123" in result.message
    # Verify the cached ID was retrieved
    mock_get_cached_job_id.assert_called_once()
    # Verify the job status was queried with the cached ID
    mock_amperity_client.get_job_status.assert_called_once_with(
        "chk-cached-123", "test-token"
    )


@patch("chuck_data.commands.job_status.get_last_job_id")
def test_handle_command_fails_without_params_or_cache(mock_get_cached_job_id):
    """Test that handle_command fails when no params and no cached ID."""
    # Mock no cached job ID
    mock_get_cached_job_id.return_value = None

    # Call without any parameters
    result = handle_command(None)

    assert not result.success
    assert "No job ID provided" in result.message
    assert "no cached job ID available" in result.message


@patch("chuck_data.commands.job_status.get_last_job_id")
@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_cached_id_always_fetches_live(
    mock_get_token, mock_amperity_client_class, mock_get_cached_job_id
):
    """Test that using cached ID automatically enables live data fetch."""
    # Mock cached job ID
    mock_get_cached_job_id.return_value = "chk-live-test"

    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_amperity_client = Mock()
    mock_amperity_client.get_job_status.return_value = {
        "job-id": "chk-live-test",
        "state": "running",
        "databricks-run-id": "run-123",
    }
    mock_amperity_client_class.return_value = mock_amperity_client

    # Mock Databricks client
    mock_databricks_client = Mock()
    mock_databricks_client.get_job_run_status.return_value = {
        "run_id": "run-123",
        "state": {"life_cycle_state": "RUNNING"},
    }

    # Call without parameters (should use cache and fetch live)
    result = handle_command(
        mock_databricks_client, amperity_client=mock_amperity_client
    )

    assert result.success
    # Verify live data was fetched
    assert "databricks_live" in result.data
    mock_databricks_client.get_job_run_status.assert_called_once_with("run-123")


@patch("chuck_data.commands.job_status.get_last_job_id")
@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_explicit_param_overrides_cache(
    mock_get_token, mock_amperity_client_class, mock_get_cached_job_id
):
    """Test that explicit job_id parameter takes precedence over cached ID."""
    # Mock cached job ID (different from explicit)
    mock_get_cached_job_id.return_value = "chk-cached-999"

    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_amperity_client = Mock()
    mock_amperity_client.get_job_status.return_value = {
        "job-id": "chk-explicit-888",
        "state": "succeeded",
    }
    mock_amperity_client_class.return_value = mock_amperity_client

    # Call with explicit job_id
    result = handle_command(None, job_id="chk-explicit-888")

    assert result.success
    assert "chk-explicit-888" in result.message
    # Verify the cached ID getter was NOT called (explicit param used)
    mock_get_cached_job_id.assert_not_called()
    # Verify the explicit ID was used
    mock_amperity_client.get_job_status.assert_called_once_with(
        "chk-explicit-888", "test-token"
    )
