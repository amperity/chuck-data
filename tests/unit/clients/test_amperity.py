"""Tests for the Amperity API client."""

from unittest.mock import patch, Mock
import json

import pytest
import requests

from chuck_data.clients.amperity import AmperityAPIClient


@patch("chuck_data.clients.amperity.time.sleep")
@patch("chuck_data.clients.amperity.requests.get")
def test_poll_auth_state_stops_on_4xx(mock_get, mock_sleep):
    """Ensure polling stops when the API returns a 4xx response."""

    client = AmperityAPIClient()
    client.nonce = "nonce"

    resp = requests.Response()
    resp.status_code = 401
    mock_get.return_value = resp

    client._poll_auth_state()

    assert client.state == "error"
    mock_get.assert_called_once()


@patch("chuck_data.clients.amperity.requests.get")
def test_get_job_status_success(mock_get):
    """Test successful job status retrieval."""
    client = AmperityAPIClient()

    # Mock successful response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "status": 200,
        "job-id": "chk-123",
        "data": {
            "job-id": "chk-123",
            "state": "running",
            "databricks-run-id": "run-456",
            "start-time": "2025-06-05T12:00:00Z",
            "record-count": 1000,
            "credits": 10,
        },
    }
    mock_get.return_value = mock_response

    result = client.get_job_status("chk-123", "test-token")

    assert result["job-id"] == "chk-123"
    assert result["state"] == "running"
    assert result["databricks-run-id"] == "run-456"
    assert result["record-count"] == 1000
    assert result["credits"] == 10

    # Verify the request was made correctly
    mock_get.assert_called_once()
    call_args = mock_get.call_args
    assert "job/status/chk-123" in call_args[0][0]
    assert call_args[1]["headers"]["Authorization"] == "Bearer test-token"


@patch("chuck_data.clients.amperity.requests.get")
def test_get_job_status_404(mock_get):
    """Test job status retrieval for non-existent job."""
    client = AmperityAPIClient()

    # Mock 404 response
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.text = "Job not found"
    mock_get.return_value = mock_response

    with pytest.raises(Exception) as exc_info:
        client.get_job_status("nonexistent", "test-token")

    assert "404" in str(exc_info.value)


@patch("chuck_data.clients.amperity.requests.get")
def test_get_job_status_network_error(mock_get):
    """Test job status retrieval with network error."""
    client = AmperityAPIClient()

    # Mock network error
    mock_get.side_effect = requests.exceptions.ConnectionError("Network error")

    with pytest.raises(Exception):
        client.get_job_status("chk-123", "test-token")


@patch("chuck_data.clients.amperity.requests.post")
def test_record_job_submission_success(mock_post):
    """Test successful job submission recording with job_id."""
    client = AmperityAPIClient()

    # Mock successful response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_post.return_value = mock_response

    result = client.record_job_submission("run-456", "test-token", "chk-123")

    assert result is True

    # Verify the request was made correctly
    mock_post.assert_called_once()
    call_args = mock_post.call_args
    assert "job/record" in call_args[0][0]
    assert call_args[1]["headers"]["Authorization"] == "Bearer test-token"

    # Verify payload includes both databricks-run-id and job-id
    payload = json.loads(call_args[1]["data"])
    assert payload["databricks-run-id"] == "run-456"
    assert payload["job-id"] == "chk-123"


@patch("chuck_data.clients.amperity.requests.post")
def test_record_job_submission_201_accepted(mock_post):
    """Test job submission recording with 201 Created."""
    client = AmperityAPIClient()

    # Mock 201 response
    mock_response = Mock()
    mock_response.status_code = 201
    mock_post.return_value = mock_response

    result = client.record_job_submission("run-789", "test-token", "chk-456")

    assert result is True


@patch("chuck_data.clients.amperity.requests.post")
def test_record_job_submission_failure(mock_post):
    """Test job submission recording with failure response."""
    client = AmperityAPIClient()

    # Mock failure response
    mock_response = Mock()
    mock_response.status_code = 400
    mock_post.return_value = mock_response

    result = client.record_job_submission("run-456", "test-token", "chk-789")

    assert result is False


@patch("chuck_data.clients.amperity.requests.post")
def test_record_job_submission_network_error(mock_post):
    """Test job submission recording with network error."""
    client = AmperityAPIClient()

    # Mock network error
    mock_post.side_effect = requests.exceptions.ConnectionError("Network error")

    result = client.record_job_submission("run-456", "test-token", "chk-999")

    assert result is False


@patch("chuck_data.clients.amperity.requests.post")
def test_record_job_submission_with_job_id_in_payload(mock_post):
    """Test that job_id is included in the request payload when provided."""
    client = AmperityAPIClient()

    # Mock successful response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_post.return_value = mock_response

    # Call with job_id
    result = client.record_job_submission(
        databricks_run_id="run-123", token="test-token", job_id="chk-test-001"
    )

    assert result is True

    # Verify payload structure
    call_args = mock_post.call_args
    payload = json.loads(call_args[1]["data"])

    assert "databricks-run-id" in payload
    assert "job-id" in payload
    assert payload["databricks-run-id"] == "run-123"
    assert payload["job-id"] == "chk-test-001"


@patch("chuck_data.clients.amperity.requests.post")
def test_record_job_submission_payload_format(mock_post):
    """Test that the payload is correctly formatted for backend API."""
    client = AmperityAPIClient()

    # Mock successful response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_post.return_value = mock_response

    result = client.record_job_submission(
        databricks_run_id="run-abc", token="bearer-token", job_id="chk-xyz"
    )

    assert result is True

    # Verify request details
    call_args = mock_post.call_args

    # Check URL
    assert "job/record" in call_args[0][0]

    # Check headers
    headers = call_args[1]["headers"]
    assert headers["Authorization"] == "Bearer bearer-token"
    assert headers["Content-Type"] == "application/json"

    # Check payload uses kebab-case keys (backend convention)
    payload = json.loads(call_args[1]["data"])
    assert "databricks-run-id" in payload  # kebab-case, not snake_case
    assert "job-id" in payload  # kebab-case, not snake_case
