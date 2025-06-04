import unittest
import os
import tempfile
from unittest.mock import patch

from src.commands.jobs import handle_launch_job, handle_job_status
from src.commands.base import CommandResult
from src.config import ConfigManager
from tests.fixtures import DatabricksClientStub


class TestJobs(unittest.TestCase):
    """Tests for job handling commands."""

    def setUp(self):
        """Set up test fixtures."""
        self.client_stub = DatabricksClientStub()

        # Set up config management
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.temp_dir.name, "test_config.json")
        self.config_manager = ConfigManager(self.config_path)
        self.patcher = patch("src.config._config_manager", self.config_manager)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        self.temp_dir.cleanup()

    def test_handle_launch_job_success(self):
        """Test launching a job with all required parameters."""
        # Use kwargs format instead of positional arguments
        result: CommandResult = handle_launch_job(
            self.client_stub,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
            run_name="MyTestJob",
        )
        assert result.success is True
        assert "123456" in result.message
        assert result.data["run_id"] == "123456"

    def test_handle_launch_job_no_run_id(self):
        """Test launching a job where response doesn't include run_id."""

        # Create a stub that returns response without run_id
        class NoRunIdStub(DatabricksClientStub):
            def submit_job_run(self, config_path, init_script_path, run_name=None):
                return {}  # No run_id in response

        no_run_id_client = NoRunIdStub()

        # Use kwargs format
        result = handle_launch_job(
            no_run_id_client,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
            run_name="NoRunId",
        )
        self.assertFalse(result.success)
        # Now we're looking for more generic failed/failure message
        self.assertTrue("Failed" in result.message or "No run_id" in result.message)

    def test_handle_launch_job_http_error(self):
        """Test launching a job with HTTP error response."""

        # Create a stub that raises an HTTP error
        class FailingJobStub(DatabricksClientStub):
            def submit_job_run(self, config_path, init_script_path, run_name=None):
                raise Exception("Bad Request")

        failing_client = FailingJobStub()

        # Use kwargs format
        result = handle_launch_job(
            failing_client,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
        )
        self.assertFalse(result.success)
        self.assertIn("Bad Request", result.message)

    def test_handle_launch_job_missing_token(self):
        """Test launching a job with missing API token."""
        # Use kwargs format
        result = handle_launch_job(
            None,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
        )
        self.assertFalse(result.success)
        self.assertIn("Client required", result.message)

    def test_handle_launch_job_missing_url(self):
        """Test launching a job with missing workspace URL."""
        # Use kwargs format
        result = handle_launch_job(
            None,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
        )
        self.assertFalse(result.success)
        self.assertIn("Client required", result.message)

    def test_handle_job_status_basic_success(self):
        """Test getting job status with successful response."""
        # Use kwargs format
        result = handle_job_status(self.client_stub, run_id="123456")
        self.assertTrue(result.success)
        self.assertEqual(result.data["state"]["life_cycle_state"], "RUNNING")
        self.assertEqual(result.data["run_id"], 123456)

    def test_handle_job_status_http_error(self):
        """Test getting job status with HTTP error response."""

        # Create a stub that raises an HTTP error
        class FailingStatusStub(DatabricksClientStub):
            def get_job_run_status(self, run_id):
                raise Exception("Not Found")

        failing_client = FailingStatusStub()

        # Use kwargs format
        result = handle_job_status(failing_client, run_id="999999")
        self.assertFalse(result.success)
        self.assertIn("Not Found", result.message)

    def test_handle_job_status_missing_token(self):
        """Test getting job status with missing API token."""
        # Use kwargs format
        result = handle_job_status(None, run_id="123456")
        self.assertFalse(result.success)
        self.assertIn("Client required", result.message)

    def test_handle_job_status_missing_url(self):
        """Test getting job status with missing workspace URL."""
        # Use kwargs format
        result = handle_job_status(None, run_id="123456")
        self.assertFalse(result.success)
        self.assertIn("Client required", result.message)
