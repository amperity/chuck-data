"""Tests for job monitoring functionality."""

from unittest.mock import Mock, patch

from chuck_data.commands.monitor_job import _monitor_job_completion
from chuck_data.commands.stitch_tools import _helper_launch_stitch_job


class TestMonitorJobCompletion:
    """Test the _monitor_job_completion function."""

    @patch("time.sleep")
    @patch("chuck_data.config.get_amperity_token")
    @patch("chuck_data.clients.amperity.AmperityAPIClient")
    def test_monitor_job_success(
        self, mock_amperity_client_class, mock_get_token, mock_sleep
    ):
        """Test successful job monitoring - job completes successfully."""
        # Setup
        mock_get_token.return_value = "test-token"
        mock_client = Mock()
        mock_amperity_client_class.return_value = mock_client

        # Simulate job states: running -> succeeded
        mock_client.get_job_status.side_effect = [
            {"state": "running", "record-count": 100},
            {"state": "running", "record-count": 500},
            {"state": "succeeded", "record-count": 1000, "credits": 50},
        ]

        # Execute
        result = _monitor_job_completion(
            job_id="test-job-123",
            run_id="run-456",
            poll_interval=1,
            timeout=10,
        )

        # Verify
        assert result["success"] is True
        assert result["job_id"] == "test-job-123"
        assert result["state"] == "succeeded"
        assert result["record_count"] == 1000
        assert result["credits"] == 50
        assert result["databricks_run_id"] == "run-456"
        assert "job_data" in result

        # Should have polled 3 times
        assert mock_client.get_job_status.call_count == 3
        mock_client.get_job_status.assert_called_with("test-job-123", "test-token")

    @patch("time.sleep")
    @patch("chuck_data.config.get_amperity_token")
    @patch("chuck_data.clients.amperity.AmperityAPIClient")
    def test_monitor_job_failure(
        self, mock_amperity_client_class, mock_get_token, mock_sleep
    ):
        """Test monitoring when job fails."""
        # Setup
        mock_get_token.return_value = "test-token"
        mock_client = Mock()
        mock_amperity_client_class.return_value = mock_client

        # Simulate job states: running -> failed
        mock_client.get_job_status.side_effect = [
            {"state": "running", "record-count": 100},
            {"state": "failed", "error": "Out of memory", "record-count": 150},
        ]

        # Execute
        result = _monitor_job_completion(
            job_id="test-job-123",
            run_id="run-456",
            poll_interval=1,
            timeout=10,
        )

        # Verify
        assert result["success"] is False
        assert result["job_id"] == "test-job-123"
        assert result["state"] == "failed"
        assert result["error"] == "Out of memory"
        assert result["databricks_run_id"] == "run-456"

    @patch("time.time")
    @patch("time.sleep")
    @patch("chuck_data.config.get_amperity_token")
    @patch("chuck_data.clients.amperity.AmperityAPIClient")
    def test_monitor_job_timeout(
        self, mock_amperity_client_class, mock_get_token, mock_sleep, mock_time
    ):
        """Test monitoring timeout when job doesn't complete."""
        # Setup
        mock_get_token.return_value = "test-token"
        mock_client = Mock()
        mock_amperity_client_class.return_value = mock_client

        # Simulate time passing beyond timeout
        mock_time.side_effect = [0, 5, 10, 15, 20, 25]  # Start at 0, increment by 5

        # Job stays running
        mock_client.get_job_status.return_value = {
            "state": "running",
            "record-count": 100,
        }

        # Execute with 10 second timeout
        result = _monitor_job_completion(
            job_id="test-job-123",
            run_id="run-456",
            poll_interval=1,
            timeout=10,
        )

        # Verify
        assert result["success"] is False
        assert result["job_id"] == "test-job-123"
        assert result["state"] == "TIMEOUT"
        assert "timed out" in result["error"].lower()

    @patch("time.sleep")
    @patch("chuck_data.config.get_amperity_token")
    def test_monitor_job_no_token(self, mock_get_token, mock_sleep):
        """Test monitoring when no Amperity token is available."""
        # Setup
        mock_get_token.return_value = None

        # Execute
        result = _monitor_job_completion(
            job_id="test-job-123",
            run_id="run-456",
            poll_interval=1,
            timeout=10,
        )

        # Verify
        assert result["success"] is False
        assert result["state"] == "UNKNOWN"
        assert "No Amperity token" in result["error"]

    @patch("time.sleep")
    @patch("chuck_data.config.get_amperity_token")
    @patch("chuck_data.clients.amperity.AmperityAPIClient")
    def test_monitor_job_with_colon_states(
        self, mock_amperity_client_class, mock_get_token, mock_sleep
    ):
        """Test monitoring handles states with colons (e.g., :succeeded)."""
        # Setup
        mock_get_token.return_value = "test-token"
        mock_client = Mock()
        mock_amperity_client_class.return_value = mock_client

        # Simulate job with colon-prefixed state (backend format)
        mock_client.get_job_status.side_effect = [
            {"state": ":running", "record-count": 100},
            {"state": ":succeeded", "record-count": 1000, "credits": 50},
        ]

        # Execute
        result = _monitor_job_completion(
            job_id="test-job-123",
            run_id="run-456",
            poll_interval=1,
            timeout=10,
        )

        # Verify - should recognize :succeeded as terminal state
        assert result["success"] is True
        assert result["state"] == ":succeeded"
        assert result["record_count"] == 1000

    @patch("time.sleep")
    @patch("chuck_data.config.get_amperity_token")
    @patch("chuck_data.clients.amperity.AmperityAPIClient")
    def test_monitor_job_continues_on_transient_errors(
        self, mock_amperity_client_class, mock_get_token, mock_sleep
    ):
        """Test monitoring continues when API calls fail transiently."""
        # Setup
        mock_get_token.return_value = "test-token"
        mock_client = Mock()
        mock_amperity_client_class.return_value = mock_client

        # Simulate transient error then success
        mock_client.get_job_status.side_effect = [
            Exception("Network error"),  # First call fails
            {"state": "running", "record-count": 100},  # Second call succeeds
            {
                "state": "succeeded",
                "record-count": 1000,
                "credits": 50,
            },  # Job completes
        ]

        # Execute
        result = _monitor_job_completion(
            job_id="test-job-123",
            run_id="run-456",
            poll_interval=1,
            timeout=10,
        )

        # Verify - should still succeed despite transient error
        assert result["success"] is True
        assert result["state"] == "succeeded"
        assert result["record_count"] == 1000


class TestLaunchStitchJobWithMonitoring:
    """Test _helper_launch_stitch_job with monitoring enabled."""

    @patch("chuck_data.commands.monitor_job._monitor_job_completion")
    @patch("chuck_data.commands.stitch_tools._create_stitch_report_notebook")
    @patch("chuck_data.job_cache.cache_job")
    @patch("chuck_data.clients.amperity.AmperityAPIClient")
    @patch("chuck_data.config.get_amperity_token")
    def test_launch_with_monitoring_success(
        self,
        mock_get_token,
        mock_amperity_client_class,
        mock_cache_job,
        mock_create_notebook,
        mock_monitor,
    ):
        """Test job launch with monitoring enabled - success case."""
        # Setup
        mock_get_token.return_value = "test-token"
        mock_client = Mock()
        mock_client.upload_file.return_value = True
        mock_client.submit_job_run.return_value = {"run_id": 12345}

        mock_amperity_client = Mock()
        mock_amperity_client.record_job_submission.return_value = True
        mock_amperity_client_class.return_value = mock_amperity_client

        mock_create_notebook.return_value = {
            "success": True,
            "notebook_path": "/test/path",
        }

        mock_monitor.return_value = {
            "success": True,
            "job_id": "job-123",
            "state": "succeeded",
            "record_count": 5000,
            "credits": 100,
        }

        stitch_config = {
            "name": "test-stitch",
            "tables": [{"path": "cat.schema.table", "fields": []}],
        }

        metadata = {
            "target_catalog": "test_catalog",
            "target_schema": "test_schema",
            "stitch_job_name": "test-stitch",
            "config_file_path": "/path/to/config.json",
            "init_script_path": "/path/to/init.sh",
            "init_script_content": "#!/bin/bash\necho test",
            "job_id": "job-123",
            "pii_scan_output": {"message": "Scan complete"},
            "unsupported_columns": [],
        }

        # Execute launch (monitoring is now done separately by the caller)
        result = _helper_launch_stitch_job(mock_client, stitch_config, metadata)

        # Verify launch succeeded
        assert result["success"] is True
        assert result["job_id"] == "job-123"
        assert result["run_id"] == 12345

        # Monitoring should NOT be called by _helper_launch_stitch_job anymore
        mock_monitor.assert_not_called()

    @patch("chuck_data.commands.monitor_job._monitor_job_completion")
    @patch("chuck_data.commands.stitch_tools._create_stitch_report_notebook")
    @patch("chuck_data.job_cache.cache_job")
    @patch("chuck_data.clients.amperity.AmperityAPIClient")
    @patch("chuck_data.config.get_amperity_token")
    def test_launch_with_monitoring_failure(
        self,
        mock_get_token,
        mock_amperity_client_class,
        mock_cache_job,
        mock_create_notebook,
        mock_monitor,
    ):
        """Test job launch with monitoring enabled - job fails."""
        # Setup
        mock_get_token.return_value = "test-token"
        mock_client = Mock()
        mock_client.upload_file.return_value = True
        mock_client.submit_job_run.return_value = {"run_id": 12345}

        mock_amperity_client = Mock()
        mock_amperity_client.record_job_submission.return_value = True
        mock_amperity_client_class.return_value = mock_amperity_client

        mock_create_notebook.return_value = {
            "success": True,
            "notebook_path": "/test/path",
        }

        mock_monitor.return_value = {
            "success": False,
            "job_id": "job-123",
            "state": "failed",
            "error": "Out of memory error",
        }

        stitch_config = {
            "name": "test-stitch",
            "tables": [{"path": "cat.schema.table", "fields": []}],
        }

        metadata = {
            "target_catalog": "test_catalog",
            "target_schema": "test_schema",
            "stitch_job_name": "test-stitch",
            "config_file_path": "/path/to/config.json",
            "init_script_path": "/path/to/init.sh",
            "init_script_content": "#!/bin/bash\necho test",
            "job_id": "job-123",
            "pii_scan_output": {"message": "Scan complete"},
            "unsupported_columns": [],
        }

        # Execute launch (monitoring is now done separately by the caller)
        result = _helper_launch_stitch_job(mock_client, stitch_config, metadata)

        # Verify launch succeeded (no monitoring info in result)
        assert result["success"] is True
        assert result["job_id"] == "job-123"
        assert result["run_id"] == 12345

        # Monitoring should NOT be called by _helper_launch_stitch_job anymore
        mock_monitor.assert_not_called()

    @patch("chuck_data.commands.stitch_tools._create_stitch_report_notebook")
    @patch("chuck_data.job_cache.cache_job")
    @patch("chuck_data.clients.amperity.AmperityAPIClient")
    @patch("chuck_data.config.get_amperity_token")
    def test_launch_without_monitoring(
        self,
        mock_get_token,
        mock_amperity_client_class,
        mock_cache_job,
        mock_create_notebook,
    ):
        """Test job launch with monitoring disabled (default behavior)."""
        # Setup
        mock_get_token.return_value = "test-token"
        mock_client = Mock()
        mock_client.upload_file.return_value = True
        mock_client.submit_job_run.return_value = {"run_id": 12345}

        mock_amperity_client = Mock()
        mock_amperity_client.record_job_submission.return_value = True
        mock_amperity_client_class.return_value = mock_amperity_client

        mock_create_notebook.return_value = {
            "success": True,
            "notebook_path": "/test/path",
        }

        stitch_config = {
            "name": "test-stitch",
            "tables": [{"path": "cat.schema.table", "fields": []}],
        }

        metadata = {
            "target_catalog": "test_catalog",
            "target_schema": "test_schema",
            "stitch_job_name": "test-stitch",
            "config_file_path": "/path/to/config.json",
            "init_script_path": "/path/to/init.sh",
            "init_script_content": "#!/bin/bash\necho test",
            "job_id": "job-123",
            "pii_scan_output": {"message": "Scan complete"},
            "unsupported_columns": [],
        }

        # Execute (monitoring is not part of this function anymore)
        result = _helper_launch_stitch_job(mock_client, stitch_config, metadata)

        # Verify
        assert result["success"] is True
        assert result["job_id"] == "job-123"
        assert result["run_id"] == 12345
        assert "monitor_result" not in result  # No monitoring results
        # Should not contain monitoring success/failure messages
        assert "✓ Job completed successfully!" not in result["message"]
        assert "✗ Job failed or timed out" not in result["message"]
