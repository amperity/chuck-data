"""
Tests for job-related command handlers (/launch_job, /job_status).
Behavioral tests focused on command execution patterns, aligned with CLAUDE.MD.
"""

from unittest.mock import patch

from chuck_data.commands.jobs import handle_launch_job, handle_job_status
from chuck_data.commands.base import CommandResult
from chuck_data.agent.tool_executor import execute_tool


# --- Parameter Validation Tests ---


def test_direct_command_launch_job_failure_missing_config_path_parameter(
    databricks_client_stub, temp_config
):
    """Test launching a job with missing config_path parameter."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_launch_job(
            databricks_client_stub,
            init_script_path="/init/script.sh",
            run_name="TestMissingConfigPath",
            # config_path is intentionally omitted
        )
        assert not result.success
        assert (
            "config_path" in result.message.lower()
            or "parameter" in result.message.lower()
        )


def test_direct_command_launch_job_failure_missing_init_script_path_parameter(
    databricks_client_stub, temp_config
):
    """Test launching a job with missing init_script_path parameter."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_launch_job(
            databricks_client_stub,
            config_path="/Volumes/test/config.json",
            run_name="TestMissingInitScript",
            # init_script_path is intentionally omitted
        )
        assert not result.success
        assert (
            "init_script_path" in result.message.lower()
            or "parameter" in result.message.lower()
        )


def test_direct_command_job_status_failure_missing_run_id_parameter(
    databricks_client_stub, temp_config
):
    """Test getting job status with missing run_id parameter."""
    with patch("chuck_data.config._config_manager", temp_config):
        # run_id is intentionally omitted
        result = handle_job_status(databricks_client_stub)
        assert not result.success
        assert (
            "run_id" in result.message.lower() or "parameter" in result.message.lower()
        )


# --- Direct Command Execution Tests: handle_launch_job ---


def test_handle_launch_job_success(databricks_client_stub, temp_config):
    """Test launching a job with all required parameters."""
    with patch("chuck_data.config._config_manager", temp_config):
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

        def submit_no_run_id(config_path, init_script_path, run_name=None):
            return {}  # No run_id in response

        databricks_client_stub.submit_job_run = submit_no_run_id
        result = handle_launch_job(
            databricks_client_stub,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
            run_name="NoRunId",
        )
        assert not result.success
        assert "Failed" in result.message or "No run_id" in result.message


def test_handle_launch_job_http_error(databricks_client_stub, temp_config):
    """Test launching a job with HTTP error response."""
    with patch("chuck_data.config._config_manager", temp_config):

        def submit_failing(config_path, init_script_path, run_name=None):
            raise Exception("Bad Request")

        databricks_client_stub.submit_job_run = submit_failing
        result = handle_launch_job(
            databricks_client_stub,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
        )
        assert not result.success
        assert "Bad Request" in result.message


def test_handle_launch_job_missing_token(temp_config):
    """Test launching a job with missing API token (results in no client)."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_launch_job(
            None,  # Simulates client not being initializable
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
        )
        assert not result.success
        assert "Client required" in result.message


def test_handle_launch_job_missing_url(temp_config):
    """Test launching a job with missing workspace URL (results in no client)."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_launch_job(
            None,  # Simulates client not being initializable
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
        )
        assert not result.success
        assert "Client required" in result.message


# --- Direct Command Execution Tests: handle_job_status ---


def test_handle_job_status_basic_success(databricks_client_stub, temp_config):
    """Test getting job status with successful response."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_job_status(databricks_client_stub, run_id="123456")
        assert result.success
        assert result.data["state"]["life_cycle_state"] == "RUNNING"
        assert result.data["run_id"] == 123456


def test_handle_job_status_http_error(databricks_client_stub, temp_config):
    """Test getting job status with HTTP error response."""
    with patch("chuck_data.config._config_manager", temp_config):

        def get_status_failing(run_id):
            raise Exception("Not Found")

        databricks_client_stub.get_job_run_status = get_status_failing
        result = handle_job_status(databricks_client_stub, run_id="999999")
        assert not result.success
        assert "Not Found" in result.message


def test_handle_job_status_missing_token(temp_config):
    """Test getting job status with missing API token (results in no client)."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_job_status(
            None, run_id="123456"
        )  # Simulates client not being initializable
        assert not result.success
        assert "Client required" in result.message


def test_handle_job_status_missing_url(temp_config):
    """Test getting job status with missing workspace URL (results in no client)."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_job_status(
            None, run_id="123456"
        )  # Simulates client not being initializable
        assert not result.success
        assert "Client required" in result.message


# --- Agent-Specific Behavioral Tests: handle_launch_job ---


def test_agent_launch_job_success_shows_progress_steps(
    databricks_client_stub, temp_config
):
    """
    AGENT TEST: Launching a job successfully shows expected progress steps.
    """
    with patch("chuck_data.config._config_manager", temp_config):
        progress_steps = []

        def capture_progress(tool_name, data):
            assert tool_name == "Checking job progress"
            progress_steps.append(data.get("step", str(data)))

        # Ensure the default stub behavior provides a run_id for this success test
        # (The fixture default is run_id: "123456")

        result = handle_launch_job(
            databricks_client_stub,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
            run_name="AgentTestJob",
            tool_output_callback=capture_progress,
        )
        assert result.success is True
        assert "123456" in result.message
        assert len(progress_steps) == 2, "Expected two progress steps."
        assert progress_steps[0] == "Attempting to submit job."
        assert progress_steps[1] == "Job submitted successfully with run_id 123456."


def test_agent_launch_job_no_run_id_shows_progress_steps(
    databricks_client_stub, temp_config
):
    """
    AGENT TEST: Launching a job that returns no run_id shows expected progress steps.
    """
    with patch("chuck_data.config._config_manager", temp_config):
        progress_steps = []

        def capture_progress(tool_name, data):
            assert tool_name == "Checking job progress"
            progress_steps.append(data.get("step", str(data)))

        # Configure stub to return response without run_id
        def submit_no_run_id(config_path, init_script_path, run_name=None):
            return {}  # No run_id in response

        databricks_client_stub.submit_job_run = submit_no_run_id

        result = handle_launch_job(
            databricks_client_stub,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
            run_name="AgentNoRunIdJob",
            tool_output_callback=capture_progress,
        )
        assert not result.success  # Overall command should fail
        assert "Failed to submit job (no run_id)" in result.message
        assert (
            len(progress_steps) == 2
        ), "Expected two progress steps for no run_id scenario."
        assert progress_steps[0] == "Attempting to submit job."
        assert progress_steps[1] == "Failed to submit job, no run_id returned."


def test_agent_launch_job_callback_errors_bubble_up(
    databricks_client_stub, temp_config
):
    """
    AGENT TEST: Errors from tool_output_callback should result in a failed CommandResult.
    """
    with patch("chuck_data.config._config_manager", temp_config):

        def failing_callback(tool_name, data):
            # This callback will be called with tool_name "Checking job progress"
            assert tool_name == "Checking job progress"
            raise Exception("Agent display system crashed")

        # No need to change submit_job_run for this test, as the first callback should fail.

        result = handle_launch_job(
            databricks_client_stub,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
            run_name="AgentCallbackErrorJob",
            tool_output_callback=failing_callback,
        )
        assert (
            not result.success
        ), "Handler did not set success=False when callback failed."
        assert "Agent display system crashed" in result.message


# --- Agent-Specific Behavioral Tests: handle_job_status ---


def test_agent_job_status_success_shows_progress_steps(
    databricks_client_stub, temp_config
):
    """
    AGENT TEST: Getting job status successfully shows expected progress steps.
    """
    with patch("chuck_data.config._config_manager", temp_config):
        progress_steps = []

        def capture_progress(tool_name, data):
            assert tool_name == "job_status_progress"
            progress_steps.append(data.get("step", str(data)))

        # Ensure the default stub behavior provides a valid status for this success test
        # (The fixture default is RUNNING for run_id 123456)

        result = handle_job_status(
            databricks_client_stub,
            run_id="123456",
            tool_output_callback=capture_progress,
        )
        assert result.success is True
        assert result.data["state"]["life_cycle_state"] == "RUNNING"
        assert len(progress_steps) == 2, "Expected two progress steps."
        assert progress_steps[0] == "Attempting to get status for run ID 123456."
        assert progress_steps[1] == "Status for run ID 123456 retrieved."


def test_agent_job_status_callback_errors_bubble_up(
    databricks_client_stub, temp_config
):
    """
    AGENT TEST: Errors from tool_output_callback should result in a failed CommandResult for job status.
    """
    with patch("chuck_data.config._config_manager", temp_config):

        def failing_callback(tool_name, data):
            assert tool_name == "job_status_progress"
            raise Exception("Agent display system crashed during status")

        # No need to change get_job_run_status for this test, as the first callback should fail.

        result = handle_job_status(
            databricks_client_stub,
            run_id="123456",
            tool_output_callback=failing_callback,
        )
        assert (
            not result.success
        ), "Handler did not set success=False when callback failed during status."
        assert "Agent display system crashed during status" in result.message


# --- Agent Tool Executor Integration Tests ---


def test_agent_tool_executor_launch_job_integration(
    databricks_client_stub, temp_config
):
    """AGENT TEST: End-to-end integration for launching a job via execute_tool.
    Assumes 'launch-job' is the correct registered tool name.
    """
    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.submit_job_run = (
            lambda config_path, init_script_path, run_name=None: {"run_id": "789012"}
        )
        tool_args = {
            "config_path": "/Volumes/agent/config.json",
            "init_script_path": "/agent/init.sh",
            "run_name": "AgentExecutorTestJob",
        }
        agent_result = execute_tool(
            api_client=databricks_client_stub,
            tool_name="launch-job",
            tool_args=tool_args,
        )
        assert agent_result is not None
        assert agent_result.get("run_id") == "789012"


def test_agent_tool_executor_job_status_integration(
    databricks_client_stub, temp_config
):
    """AGENT TEST: End-to-end integration for getting job status via execute_tool.
    Assumes 'job-status' is the correct registered tool name.
    """
    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.get_job_run_status = lambda run_id: {
            "run_id": int(run_id),
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "Job finished.",
            },
            "job_id": 789,
            "task_run_stats": {},
        }
        tool_args = {"run_id": "777888"}
        agent_result = execute_tool(
            api_client=databricks_client_stub,
            tool_name="job-status",
            tool_args=tool_args,
        )
        assert agent_result is not None
        assert agent_result.get("run_id") == 777888
        assert agent_result.get("state") == "TERMINATED"
