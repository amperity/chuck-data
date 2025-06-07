from unittest.mock import patch

from chuck_data.commands.jobs import handle_launch_job, handle_job_status
from chuck_data.commands.base import CommandResult


def test_handle_launch_job_success(databricks_client_stub, temp_config):
    """Test launching a job with all required parameters."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Use kwargs format instead of positional arguments
        result: CommandResult = handle_launch_job(
            databricks_client_stub,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
            run_name="MyTestJob",
        )
        assert result.success is True
        assert "123456" in result.message
        assert result.data["run_id"] == "123456"


def test_handle_launch_job_no_run_id(databricks_client_stub, temp_config):
    """Test launching a job where response doesn't include run_id."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Configure stub to return response without run_id
        def submit_no_run_id(config_path, init_script_path, run_name=None):
            return {}  # No run_id in response

        databricks_client_stub.submit_job_run = submit_no_run_id

        # Use kwargs format
        result = handle_launch_job(
            databricks_client_stub,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
            run_name="NoRunId",
        )
        assert not result.success
        # Now we're looking for more generic failed/failure message
        assert "Failed" in result.message or "No run_id" in result.message


def test_handle_launch_job_http_error(databricks_client_stub, temp_config):
    """Test launching a job with HTTP error response."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Configure stub to raise an HTTP error
        def submit_failing(config_path, init_script_path, run_name=None):
            raise Exception("Bad Request")

        databricks_client_stub.submit_job_run = submit_failing

        # Use kwargs format
        result = handle_launch_job(
            databricks_client_stub,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
        )
        assert not result.success
        assert "Bad Request" in result.message


def test_handle_launch_job_missing_token(temp_config):
    """Test launching a job with missing API token."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Use kwargs format
        result = handle_launch_job(
            None,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
        )
        assert not result.success
        assert "Client required" in result.message


def test_handle_launch_job_missing_url(temp_config):
    """Test launching a job with missing workspace URL."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Use kwargs format
        result = handle_launch_job(
            None,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
        )
        assert not result.success
        assert "Client required" in result.message


def test_handle_job_status_basic_success(databricks_client_stub, temp_config):
    """Test getting job status with successful response."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Use kwargs format
        result = handle_job_status(databricks_client_stub, run_id="123456")
        assert result.success
        assert result.data["state"]["life_cycle_state"] == "RUNNING"
        assert result.data["run_id"] == 123456


def test_handle_job_status_http_error(databricks_client_stub, temp_config):
    """Test getting job status with HTTP error response."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Configure stub to raise an HTTP error
        def get_status_failing(run_id):
            raise Exception("Not Found")

        databricks_client_stub.get_job_run_status = get_status_failing

        # Use kwargs format
        result = handle_job_status(databricks_client_stub, run_id="999999")
        assert not result.success
        assert "Not Found" in result.message


def test_handle_job_status_missing_token(temp_config):
    """Test getting job status with missing API token."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Use kwargs format
        result = handle_job_status(None, run_id="123456")
        assert not result.success
        assert "Client required" in result.message


def test_handle_job_status_missing_url(temp_config):
    """Test getting job status with missing workspace URL."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Use kwargs format
        result = handle_job_status(None, run_id="123456")
        assert not result.success
        assert "Client required" in result.message
