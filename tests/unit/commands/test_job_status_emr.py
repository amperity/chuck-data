"""Comprehensive tests for EMR support in job-status command."""

from unittest.mock import Mock, patch

from chuck_data.commands.job_status import (
    handle_command,
    _is_emr_step_id,
    _is_databricks_run_id,
    _extract_emr_step_info,
)

# Tests for ID detection functions


def test_is_emr_step_id_valid():
    """Test EMR step ID detection with valid IDs."""
    assert _is_emr_step_id("s-ABCD123456789")
    assert _is_emr_step_id("s-1234567890ABC")
    assert _is_emr_step_id("s-XXXXXXXXXXXXX")


def test_is_emr_step_id_invalid():
    """Test EMR step ID detection with invalid IDs."""
    assert not _is_emr_step_id("123456")  # Databricks format
    assert not _is_emr_step_id("run-123")  # Not EMR format
    assert not _is_emr_step_id("s-")  # Too short
    assert not _is_emr_step_id(
        "S-ABCD"
    )  # Uppercase S (should still work, but test lowercase)
    assert not _is_emr_step_id("")  # Empty
    assert not _is_emr_step_id(None)  # None


def test_is_databricks_run_id_valid():
    """Test Databricks run ID detection with valid IDs."""
    assert _is_databricks_run_id("123456")
    assert _is_databricks_run_id("999999999")
    assert _is_databricks_run_id("1")


def test_is_databricks_run_id_invalid():
    """Test Databricks run ID detection with invalid IDs."""
    assert not _is_databricks_run_id("s-ABCD123")  # EMR format
    assert not _is_databricks_run_id("run-123")  # Not numeric
    assert not _is_databricks_run_id("abc123")  # Mixed alphanumeric
    assert not _is_databricks_run_id("")  # Empty
    assert not _is_databricks_run_id(None)  # None


# Tests for EMR step info extraction


def test_extract_emr_step_info_basic():
    """Test extraction of basic EMR step information."""
    raw_data = {
        "step_id": "s-ABCD123",
        "cluster_id": "j-CLUSTER123",
        "status": "COMPLETED",
        "state_message": "Step completed successfully",
        "start_time": "2025-01-12T10:00:00Z",
        "end_time": "2025-01-12T10:15:00Z",
        "monitoring_url": "https://console.aws.amazon.com/emr/...",
    }

    result = _extract_emr_step_info(raw_data)

    assert result["step_id"] == "s-ABCD123"
    assert result["cluster_id"] == "j-CLUSTER123"
    assert result["status"] == "COMPLETED"
    assert result["state_message"] == "Step completed successfully"
    assert result["start_time"] == "2025-01-12T10:00:00Z"
    assert result["end_time"] == "2025-01-12T10:15:00Z"
    assert result["monitoring_url"] == "https://console.aws.amazon.com/emr/..."


def test_extract_emr_step_info_with_failure():
    """Test extraction includes failure information."""
    raw_data = {
        "step_id": "s-FAILED123",
        "cluster_id": "j-CLUSTER456",
        "status": "FAILED",
        "state_message": "Step failed due to error",
        "failure_reason": "APPLICATION_ERROR",
        "failure_message": "Java exception occurred",
        "start_time": "2025-01-12T11:00:00Z",
    }

    result = _extract_emr_step_info(raw_data)

    assert result["step_id"] == "s-FAILED123"
    assert result["status"] == "FAILED"
    assert result["failure_reason"] == "APPLICATION_ERROR"
    assert result["failure_message"] == "Java exception occurred"


def test_extract_emr_step_info_minimal():
    """Test extraction with minimal fields."""
    raw_data = {
        "status": "RUNNING",
    }

    result = _extract_emr_step_info(raw_data)

    assert result["status"] == "RUNNING"
    assert result["step_id"] is None
    assert result["cluster_id"] is None
    assert result["state_message"] == ""


# Tests for EMR job status with --live flag


@patch("chuck_data.clients.emr.EMRAPIClient")
@patch("chuck_data.config.get_aws_region")
@patch("chuck_data.config.get_emr_cluster_id")
@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_emr_job_with_live_data(
    mock_get_token,
    mock_amperity_client_class,
    mock_get_cluster_id,
    mock_get_region,
    mock_emr_client_class,
):
    """Test job status query with live EMR data enrichment."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_amperity_client = Mock()
    mock_amperity_client.get_job_status.return_value = {
        "job-id": "chk-emr-123",
        "state": "running",
        "databricks-run-id": "s-EMRSTEP123",  # EMR step ID
        "record-count": 1000,
    }
    mock_amperity_client_class.return_value = mock_amperity_client

    # Mock EMR config
    mock_get_cluster_id.return_value = "j-CLUSTER123"
    mock_get_region.return_value = "us-west-2"

    # Mock EMR client
    mock_emr_client = Mock()
    mock_emr_client.get_step_status.return_value = {
        "status": "RUNNING",
        "state_message": "Step is running",
        "start_time": "2025-01-12T10:00:00Z",
    }
    mock_emr_client.get_monitoring_url.return_value = (
        "https://console.aws.amazon.com/emr/..."
    )
    mock_emr_client_class.return_value = mock_emr_client

    result = handle_command(
        None,
        amperity_client=mock_amperity_client,
        job_id="chk-emr-123",
        live=True,
    )

    assert result.success
    assert "emr_live" in result.data
    assert result.data["emr_live"]["step_id"] == "s-EMRSTEP123"
    assert result.data["emr_live"]["cluster_id"] == "j-CLUSTER123"
    assert result.data["emr_live"]["status"] == "RUNNING"
    mock_emr_client.get_step_status.assert_called_once_with(
        "s-EMRSTEP123", "j-CLUSTER123"
    )
    mock_emr_client.get_monitoring_url.assert_called_once_with("j-CLUSTER123")


@patch("chuck_data.config.get_aws_region")
@patch("chuck_data.config.get_emr_cluster_id")
@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_emr_job_live_missing_config(
    mock_get_token,
    mock_amperity_client_class,
    mock_get_cluster_id,
    mock_get_region,
):
    """Test EMR live data fetch fails gracefully when config is missing."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_amperity_client = Mock()
    mock_amperity_client.get_job_status.return_value = {
        "job-id": "chk-emr-456",
        "state": "running",
        "databricks-run-id": "s-EMRSTEP456",
    }
    mock_amperity_client_class.return_value = mock_amperity_client

    # Mock missing EMR config
    mock_get_cluster_id.return_value = None  # Missing cluster ID
    mock_get_region.return_value = "us-west-2"

    result = handle_command(
        None,
        amperity_client=mock_amperity_client,
        job_id="chk-emr-456",
        live=True,
    )

    # Should succeed but without live EMR data
    assert result.success
    assert "emr_live" not in result.data


# Tests for --step-id fallback parameter


@patch("chuck_data.clients.emr.EMRAPIClient")
@patch("chuck_data.config.get_aws_region")
@patch("chuck_data.config.get_emr_cluster_id")
def test_handle_command_step_id_fallback(
    mock_get_cluster_id,
    mock_get_region,
    mock_emr_client_class,
):
    """Test query by EMR step ID without job ID."""
    # Mock EMR config
    mock_get_cluster_id.return_value = "j-CLUSTER789"
    mock_get_region.return_value = "us-east-1"

    # Mock EMR client
    mock_emr_client = Mock()
    mock_emr_client.get_step_status.return_value = {
        "status": "COMPLETED",
        "state_message": "Step completed",
        "start_time": "2025-01-12T12:00:00Z",
        "end_time": "2025-01-12T12:30:00Z",
    }
    mock_emr_client.get_monitoring_url.return_value = (
        "https://console.aws.amazon.com/emr/..."
    )
    mock_emr_client_class.return_value = mock_emr_client

    result = handle_command(None, step_id="s-DIRECTQUERY123")

    assert result.success
    assert result.data["step_id"] == "s-DIRECTQUERY123"
    assert result.data["cluster_id"] == "j-CLUSTER789"
    assert result.data["status"] == "COMPLETED"
    assert "EMR Step Status" in result.message
    assert "s-DIRECTQUERY123" in result.message


@patch("chuck_data.config.get_aws_region")
@patch("chuck_data.config.get_emr_cluster_id")
def test_handle_command_step_id_missing_config(
    mock_get_cluster_id,
    mock_get_region,
):
    """Test query by EMR step ID fails when config is missing."""
    # Mock missing EMR config
    mock_get_cluster_id.return_value = None
    mock_get_region.return_value = None

    result = handle_command(None, step_id="s-NOCONFIGSTEP")

    assert not result.success
    assert "cluster_id or region not configured" in result.message


# Tests for mixed Databricks and EMR scenarios


@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_databricks_job_not_confused_with_emr(
    mock_get_token,
    mock_amperity_client_class,
):
    """Test that Databricks jobs (numeric run_id) are not treated as EMR."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client with Databricks run ID (numeric)
    mock_amperity_client = Mock()
    mock_amperity_client.get_job_status.return_value = {
        "job-id": "chk-databricks-999",
        "state": "running",
        "databricks-run-id": "123456789",  # Numeric = Databricks
    }
    mock_amperity_client_class.return_value = mock_amperity_client

    result = handle_command(
        None,
        amperity_client=mock_amperity_client,
        job_id="chk-databricks-999",
        live=False,  # Don't fetch live to avoid needing Databricks client
    )

    assert result.success
    # Should not have EMR live data
    assert "emr_live" not in result.data


# Tests for EMR message formatting


@patch("chuck_data.clients.emr.EMRAPIClient")
@patch("chuck_data.config.get_aws_region")
@patch("chuck_data.config.get_emr_cluster_id")
@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_emr_message_format(
    mock_get_token,
    mock_amperity_client_class,
    mock_get_cluster_id,
    mock_get_region,
    mock_emr_client_class,
):
    """Test that EMR job status message includes EMR section."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_amperity_client = Mock()
    mock_amperity_client.get_job_status.return_value = {
        "job-id": "chk-emr-format",
        "state": "succeeded",
        "databricks-run-id": "s-FORMATTEST",
        "record-count": 5000,
        "credits": 25,
    }
    mock_amperity_client_class.return_value = mock_amperity_client

    # Mock EMR config
    mock_get_cluster_id.return_value = "j-FORMATCLUSTER"
    mock_get_region.return_value = "us-west-2"

    # Mock EMR client
    mock_emr_client = Mock()
    mock_emr_client.get_step_status.return_value = {
        "status": "COMPLETED",
        "state_message": "Step completed successfully",
        "start_time": "2025-01-12T14:00:00Z",
        "end_time": "2025-01-12T14:30:00Z",
    }
    mock_emr_client.get_monitoring_url.return_value = "https://us-west-2.console.aws.amazon.com/emr/home?region=us-west-2#/clusterDetails/j-FORMATCLUSTER"
    mock_emr_client_class.return_value = mock_emr_client

    result = handle_command(
        None,
        amperity_client=mock_amperity_client,
        job_id="chk-emr-format",
        live=True,
    )

    assert result.success
    # Check for EMR section in message
    assert "EMR:" in result.message
    assert "Step ID: s-FORMATTEST" in result.message
    assert "Cluster ID: j-FORMATCLUSTER" in result.message
    assert "Status: COMPLETED" in result.message
    assert "https://us-west-2.console.aws.amazon.com" in result.message


# Tests for EMR job caching integration


@patch("chuck_data.commands.job_status.find_run_id_for_job")
@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_uses_cached_emr_step_id(
    mock_get_token,
    mock_amperity_client_class,
    mock_find_run_id,
):
    """Test that cached EMR step ID is used when backend returns UNSET."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client returning UNSET
    mock_amperity_client = Mock()
    mock_amperity_client.get_job_status.return_value = {
        "job-id": "chk-cached-emr",
        "state": "running",
        "databricks-run-id": "UNSET_DATABRICKS_RUN_ID",
    }
    mock_amperity_client_class.return_value = mock_amperity_client

    # Mock cached EMR step ID
    mock_find_run_id.return_value = "s-CACHEDSTEP123"

    result = handle_command(
        None,
        amperity_client=mock_amperity_client,
        job_id="chk-cached-emr",
        live=False,
    )

    assert result.success
    # Should have used cached step ID
    assert result.data["databricks-run-id"] == "s-CACHEDSTEP123"
    mock_find_run_id.assert_called_once_with("chk-cached-emr")


# Tests for EMR error handling


@patch("chuck_data.clients.emr.EMRAPIClient")
@patch("chuck_data.config.get_aws_region")
@patch("chuck_data.config.get_emr_cluster_id")
@patch("chuck_data.commands.job_status.AmperityAPIClient")
@patch("chuck_data.commands.job_status.get_amperity_token")
def test_handle_command_emr_api_error(
    mock_get_token,
    mock_amperity_client_class,
    mock_get_cluster_id,
    mock_get_region,
    mock_emr_client_class,
):
    """Test that EMR API errors are handled gracefully."""
    # Mock token
    mock_get_token.return_value = "test-token"

    # Mock Amperity client
    mock_amperity_client = Mock()
    mock_amperity_client.get_job_status.return_value = {
        "job-id": "chk-emr-error",
        "state": "running",
        "databricks-run-id": "s-ERRORSTEP",
    }
    mock_amperity_client_class.return_value = mock_amperity_client

    # Mock EMR config
    mock_get_cluster_id.return_value = "j-ERRORCLUSTER"
    mock_get_region.return_value = "us-west-2"

    # Mock EMR client that raises an exception
    mock_emr_client = Mock()
    mock_emr_client.get_step_status.side_effect = Exception("EMR API Error")
    mock_emr_client_class.return_value = mock_emr_client

    result = handle_command(
        None,
        amperity_client=mock_amperity_client,
        job_id="chk-emr-error",
        live=True,
    )

    # Should still succeed (Chuck data is valid), just without EMR live data
    assert result.success
    assert "emr_live" not in result.data
