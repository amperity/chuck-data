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
    config_path = kwargs.get("config_path")
    init_script_path = kwargs.get("init_script_path")
    run_name: Optional[str] = kwargs.get("run_name")
    tool_output_callback = kwargs.get("tool_output_callback")
    policy_id: Optional[str] = kwargs.get("policy_id")

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
            policy_id=policy_id,
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


LAUNCH_JOB_DEFINITION = CommandDefinition(
    name="launch_job",
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
        "policy_id": {
            "type": "string",
            "description": "Optional: cluster policy ID to use for the job run",
        },
        "run_name": {"type": "string", "description": "Optional name for the job run"},
    },
    required_params=["config_path", "init_script_path"],
    handler=handle_launch_job,
    needs_api_client=True,
    visible_to_agent=True,
    tui_aliases=["launch-job"],
)

DEFINITION = LAUNCH_JOB_DEFINITION
