import logging
from typing import Optional

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.commands.base import CommandResult
from chuck_data.command_registry import CommandDefinition


def handle_launch_job(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """Submits a one-time Databricks job run.

    Args:
        client: API client instance
        **kwargs: config_path (str), init_script_path (str), run_name (str, optional), tool_output_callback (callable, optional)
    """
    config_path: str = kwargs.get("config_path")
    init_script_path: str = kwargs.get("init_script_path")
    run_name: Optional[str] = kwargs.get("run_name")
    tool_output_callback = kwargs.get("tool_output_callback")

    if not config_path or not init_script_path:
        return CommandResult(
            False, message="config_path and init_script_path are required."
        )
    if not client:
        return CommandResult(False, message="Client required to launch job.")
    try:
        if tool_output_callback:
            tool_output_callback(
                "Checking job progress", {"step": "Attempting to submit job."}
            )

        run_data = client.submit_job_run(
            config_path=config_path,
            init_script_path=init_script_path,
            run_name=run_name,
        )
        run_id = run_data.get("run_id")
        if run_id:
            if tool_output_callback:
                tool_output_callback(
                    "Checking job progress",
                    {"step": f"Job submitted successfully with run_id {run_id}."},
                )
            return CommandResult(
                True,
                data={"run_id": str(run_id)},
                message=f"Job submitted. Run ID: {run_id}",
            )
        else:
            logging.error(f"Failed to launch job, no run_id: {run_data}")
            if tool_output_callback:
                tool_output_callback(
                    "Checking job progress",
                    {"step": "Failed to submit job, no run_id returned."},
                )
            return CommandResult(
                False, message="Failed to submit job (no run_id).", data=run_data
            )
    except Exception as e:
        logging.error(f"Failed to submit job: {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


def handle_job_status(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """Query Databricks for the status of a one-time run.

    Args:
        client: API client instance
        **kwargs: run_id (str), tool_output_callback (callable, optional)
    """
    run_id_str: str = kwargs.get("run_id")
    tool_output_callback = kwargs.get("tool_output_callback")

    if not run_id_str:
        return CommandResult(False, message="run_id parameter is required.")
    if not client:
        return CommandResult(False, message="Client required to get job status.")
    try:
        if tool_output_callback:
            tool_output_callback(
                "job_status_progress",
                {"step": f"Attempting to get status for run ID {run_id_str}."},
            )

        data = client.get_job_run_status(run_id_str)
        status = data.get("status", data.get("state", {}))
        life_cycle_state = status.get("life_cycle_state", status.get("state"))
        result_state = status.get(
            "result_state", status.get("termination_details", {}).get("code")
        )
        state_message = status.get(
            "state_message", status.get("termination_details", {}).get("message")
        )
        run_page_url = data.get("run_page_url")
        msg = f"Run {run_id_str}: Status: {life_cycle_state or 'N/A'}, Result: {result_state or 'N/A'}, Message: {state_message or ''}"
        if run_page_url:
            msg += f" URL: {run_page_url}"

        if tool_output_callback:
            tool_output_callback(
                "job_status_progress",
                {
                    "step": f"Status for run ID {run_id_str} retrieved.",
                    "status_data": data,
                },
            )
        return CommandResult(True, data=data, message=msg)
    except Exception as e:
        logging.error(
            f"Failed to get job status for run '{run_id_str}': {e}", exc_info=True
        )
        return CommandResult(False, error=e, message=str(e))


LAUNCH_JOB_DEFINITION = CommandDefinition(
    name="launch-job",
    description="Launch a Databricks job using a config file",
    usage_hint="launch_job --config_path=/path/to/config.json --init_script_path=/init/script.sh",
    parameters={
        "config_path": {
            "type": "string",
            "description": "Path to the job configuration file",
        },
        "init_script_path": {
            "type": "string",
            "description": "Path to the init script",
        },
        "run_name": {"type": "string", "description": "Optional name for the job run"},
    },
    required_params=["config_path", "init_script_path"],
    handler=handle_launch_job,
    needs_api_client=True,
    visible_to_agent=True,
    tui_aliases=["launch-job"],
)

JOB_STATUS_DEFINITION = CommandDefinition(
    name="job-status",
    description="Get the status of a Databricks job run",
    usage_hint="job_status --run_id=123456",
    parameters={
        "run_id": {"type": "string", "description": "ID of the job run to check"},
    },
    required_params=["run_id"],
    handler=handle_job_status,
    needs_api_client=True,
    visible_to_agent=True,
    tui_aliases=["job-status"],
)

# Combined definition for module
DEFINITION = [LAUNCH_JOB_DEFINITION, JOB_STATUS_DEFINITION]
