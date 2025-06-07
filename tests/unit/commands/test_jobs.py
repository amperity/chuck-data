"""
Tests for job-related command handlers (/launch_job, /job_status).
Behavioral tests focused on command execution patterns, aligned with CLAUDE.MD.
"""
from unittest.mock import patch
import pytest # Added for pytest.skip

from chuck_data.commands.jobs import handle_launch_job, handle_job_status
from chuck_data.commands.base import CommandResult

# Imports for agent tests (assuming execute_tool location)
# You may need to adjust this import based on your project structure
try:
    from chuck_data.agent.tool_executor import execute_tool
except ImportError:
    execute_tool = None

# --- Parameter Validation Tests ---

def test_direct_command_launch_job_failure_missing_config_path_parameter(databricks_client_stub, temp_config):
    """Test launching a job with missing config_path parameter."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_launch_job(
            databricks_client_stub,
            init_script_path="/init/script.sh",
            run_name="TestMissingConfigPath"
            # config_path is intentionally omitted
        )
        assert not result.success
        assert "config_path" in result.message.lower() or "parameter" in result.message.lower()

def test_direct_command_launch_job_failure_missing_init_script_path_parameter(databricks_client_stub, temp_config):
    """Test launching a job with missing init_script_path parameter."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_launch_job(
            databricks_client_stub,
            config_path="/Volumes/test/config.json",
            run_name="TestMissingInitScript"
            # init_script_path is intentionally omitted
        )
        assert not result.success
        assert "init_script_path" in result.message.lower() or "parameter" in result.message.lower()

def test_direct_command_job_status_failure_missing_run_id_parameter(databricks_client_stub, temp_config):
    """Test getting job status with missing run_id parameter."""
    with patch("chuck_data.config._config_manager", temp_config):
        # run_id is intentionally omitted
        result = handle_job_status(databricks_client_stub)
        assert not result.success
        assert "run_id" in result.message.lower() or "parameter" in result.message.lower()


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
            None, # Simulates client not being initializable
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
        )
        assert not result.success
        assert "Client required" in result.message

def test_handle_launch_job_missing_url(temp_config):
    """Test launching a job with missing workspace URL (results in no client)."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_launch_job(
            None, # Simulates client not being initializable
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
        result = handle_job_status(None, run_id="123456") # Simulates client not being initializable
        assert not result.success
        assert "Client required" in result.message

def test_handle_job_status_missing_url(temp_config):
    """Test getting job status with missing workspace URL (results in no client)."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = handle_job_status(None, run_id="123456") # Simulates client not being initializable
        assert not result.success
        assert "Client required" in result.message


# --- Agent-Specific Behavioral Tests: handle_launch_job ---

def test_agent_launch_job_success_shows_progress_steps(databricks_client_stub, temp_config):
    """
    AGENT TEST (TEMPLATE): Launching a job successfully shows expected progress steps.
    """
    with patch("chuck_data.config._config_manager", temp_config):
        progress_steps = []
        def capture_progress(tool_name, data):
            progress_steps.append(data.get("step", str(data)))
        result = handle_launch_job(
            databricks_client_stub,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
            run_name="AgentTestJob",
            tool_output_callback=capture_progress
        )
        assert result.success is True
        assert "123456" in result.message # Assuming databricks_client_stub still returns this
        assert len(progress_steps) == 1, "Expected one progress step."
        assert progress_steps[0] == "Attempting to submit job."

def test_agent_launch_job_callback_errors_bubble_up(databricks_client_stub, temp_config):
    """
    AGENT TEST (TEMPLATE): Errors from tool_output_callback should result in a failed CommandResult.
    """
    with patch("chuck_data.config._config_manager", temp_config):
        def failing_callback(tool_name, data):
            raise Exception("Agent display system crashed")
        databricks_client_stub.submit_job_run = lambda config_path, init_script_path, run_name=None: {"run_id": "12345"}
        result = handle_launch_job(
            databricks_client_stub,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
            run_name="AgentCallbackErrorJob",
            tool_output_callback=failing_callback
        )
        assert not result.success, "Handler did not set success=False when callback failed."
        assert "Agent display system crashed" in result.message


# --- Agent-Specific Behavioral Tests: handle_job_status ---

def test_agent_job_status_success_shows_progress_steps(databricks_client_stub, temp_config):
    """
    AGENT TEST (TEMPLATE): Getting job status successfully shows expected progress steps.
    """
    with patch("chuck_data.config._config_manager", temp_config):
        progress_steps = []
        def capture_progress(tool_name, data):
            progress_steps.append(data.get("step", str(data)))
        result = handle_job_status(
            databricks_client_stub,
            run_id="123456",
            tool_output_callback=capture_progress
        )
        assert result.success is True
        assert result.data["state"]["life_cycle_state"] == "RUNNING"
        assert len(progress_steps) == 1, "Expected one progress step."
        assert progress_steps[0] == "Attempting to get status for run ID 123456."

def test_agent_job_status_callback_errors_bubble_up(databricks_client_stub, temp_config):
    """
    AGENT TEST (TEMPLATE): Errors from tool_output_callback should result in a failed CommandResult for job status.
    """
    with patch("chuck_data.config._config_manager", temp_config):
        def failing_callback(tool_name, data):
            raise Exception("Agent display system crashed during status")
        databricks_client_stub.get_job_run_status = lambda run_id: {
            "run_id": int(run_id),
            "state": {"life_cycle_state": "RUNNING", "state_message": "Job is running"},
        }
        result = handle_job_status(
            databricks_client_stub,
            run_id="123456",
            tool_output_callback=failing_callback
        )
        assert not result.success, "Handler did not set success=False when callback failed during status."
        assert "Agent display system crashed during status" in result.message


# --- Agent Tool Executor Integration Tests ---

def test_agent_tool_executor_launch_job_integration(databricks_client_stub, temp_config):
    """AGENT TEST: End-to-end integration for launching a job via execute_tool.
    This test currently FAILS if '/launch_job' is not the correct registered tool name.
    """
    if not execute_tool:
        pytest.skip("execute_tool not available, skipping integration test.")
    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.submit_job_run = lambda config_path, init_script_path, run_name=None: {
            "run_id": "789012"
        }
        tool_args = {
            "config_path": "/Volumes/agent/config.json",
            "init_script_path": "/agent/init.sh",
            "run_name": "AgentExecutorTestJob"
        }
        agent_result = execute_tool(
            api_client=databricks_client_stub,
            tool_name="/launch_job",
            tool_args=tool_args
        )
        assert agent_result is not None
        assert agent_result.get("run_id") == "789012"

def test_agent_tool_executor_job_status_integration(databricks_client_stub, temp_config):
    """AGENT TEST: End-to-end integration for getting job status via execute_tool.
    This test currently FAILS if '/job_status' is not the correct registered tool name.
    """
    if not execute_tool:
        pytest.skip("execute_tool not available, skipping integration test.")
    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.get_job_run_status = lambda run_id: {
            "run_id": int(run_id),
            "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS", "state_message": "Job finished."},
            "job_id": 789,
            "task_run_stats": {},
        }
        tool_args = {"run_id": "777888"}
        agent_result = execute_tool(
            api_client=databricks_client_stub,
            tool_name="/job_status",
            tool_args=tool_args
        )
        assert agent_result is not None
        assert agent_result.get("run_id") == 777888
        assert agent_result.get("state", {}).get("life_cycle_state") == "TERMINATED"
