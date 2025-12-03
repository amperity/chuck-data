"""
Tests for job-related command handlers (/launch_job).
Behavioral tests focused on command execution patterns, aligned with CLAUDE.MD.
"""

from unittest.mock import patch

from chuck_data.commands.jobs import handle_launch_job
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

        def submit_no_run_id(
            config_path, init_script_path, run_name=None, policy_id=None
        ):
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

        def submit_failing(
            config_path, init_script_path, run_name=None, policy_id=None
        ):
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


# --- policy_id Parameter Tests: handle_launch_job ---


def test_handle_launch_job_with_policy_id_success(databricks_client_stub, temp_config):
    """Test launching a job with policy_id passes it to the client."""
    with patch("chuck_data.config._config_manager", temp_config):
        result: CommandResult = handle_launch_job(
            databricks_client_stub,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
            run_name="MyTestJobWithPolicy",
            policy_id="000F957411D99C1F",
        )
        assert result.success is True
        assert "123456" in result.message
        assert result.data["run_id"] == "123456"

        # Verify policy_id was passed to the client
        assert len(databricks_client_stub.submit_job_run_calls) == 1
        call_args = databricks_client_stub.submit_job_run_calls[0]
        assert call_args["policy_id"] == "000F957411D99C1F"
        assert call_args["config_path"] == "/Volumes/test/config.json"
        assert call_args["init_script_path"] == "/init/script.sh"


def test_handle_launch_job_without_policy_id(databricks_client_stub, temp_config):
    """Test launching a job without policy_id passes None to the client."""
    with patch("chuck_data.config._config_manager", temp_config):
        result: CommandResult = handle_launch_job(
            databricks_client_stub,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
            run_name="MyTestJobNoPolicy",
        )
        assert result.success is True

        # Verify policy_id was passed as None
        assert len(databricks_client_stub.submit_job_run_calls) == 1
        call_args = databricks_client_stub.submit_job_run_calls[0]
        assert call_args["policy_id"] is None


def test_agent_launch_job_with_policy_id_success(databricks_client_stub, temp_config):
    """AGENT TEST: Launching a job with policy_id passes it correctly."""
    with patch("chuck_data.config._config_manager", temp_config):
        progress_steps = []

        def capture_progress(tool_name, data):
            progress_steps.append(data.get("step", str(data)))

        result = handle_launch_job(
            databricks_client_stub,
            config_path="/Volumes/test/config.json",
            init_script_path="/init/script.sh",
            run_name="AgentTestJobWithPolicy",
            policy_id="POLICY123ABC",
            tool_output_callback=capture_progress,
        )
        assert result.success is True
        assert "123456" in result.message

        # Verify policy_id was passed to the client
        assert len(databricks_client_stub.submit_job_run_calls) == 1
        call_args = databricks_client_stub.submit_job_run_calls[0]
        assert call_args["policy_id"] == "POLICY123ABC"


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
        def submit_no_run_id(
            config_path, init_script_path, run_name=None, policy_id=None
        ):
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


# --- Agent Tool Executor Integration Tests ---


def test_agent_tool_executor_launch_job_integration(
    databricks_client_stub, temp_config
):
    """AGENT TEST: End-to-end integration for launching a job via execute_tool.
    Assumes 'launch_job' is the correct registered tool name.
    """
    with patch("chuck_data.config._config_manager", temp_config):
        databricks_client_stub.submit_job_run = (
            lambda config_path, init_script_path, run_name=None, policy_id=None: {
                "run_id": "789012"
            }
        )
        tool_args = {
            "config_path": "/Volumes/agent/config.json",
            "init_script_path": "/agent/init.sh",
            "run_name": "AgentExecutorTestJob",
        }
        agent_result = execute_tool(
            api_client=databricks_client_stub,
            tool_name="launch_job",
            tool_args=tool_args,
        )
        assert agent_result is not None
        assert agent_result.get("run_id") == "789012"


def test_agent_tool_executor_launch_job_with_policy_id(
    databricks_client_stub, temp_config
):
    """AGENT TEST: End-to-end integration for launching a job with policy_id via execute_tool."""
    with patch("chuck_data.config._config_manager", temp_config):
        captured_policy_id = []

        def mock_submit_job_run(
            config_path, init_script_path, run_name=None, policy_id=None
        ):
            captured_policy_id.append(policy_id)
            return {"run_id": "789012"}

        databricks_client_stub.submit_job_run = mock_submit_job_run

        tool_args = {
            "config_path": "/Volumes/agent/config.json",
            "init_script_path": "/agent/init.sh",
            "run_name": "AgentExecutorTestJobWithPolicy",
            "policy_id": "AGENT_POLICY_ID_123",
        }
        agent_result = execute_tool(
            api_client=databricks_client_stub,
            tool_name="launch_job",
            tool_args=tool_args,
        )
        assert agent_result is not None
        assert agent_result.get("run_id") == "789012"
        # Verify policy_id was passed through the agent tool executor
        assert len(captured_policy_id) == 1
        assert captured_policy_id[0] == "AGENT_POLICY_ID_123"
