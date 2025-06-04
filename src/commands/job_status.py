"""
Command for checking status of Databricks job runs.
"""

from typing import Optional, Any
from src.clients.databricks import DatabricksAPIClient
from src.commands.base import CommandResult
from src.command_registry import CommandDefinition
import logging


def handle_command(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """
    Check status of a Databricks job run.

    Args:
        client: DatabricksAPIClient instance for API calls
        **kwargs: Command parameters
            - run_id: The job run ID to check status for

    Returns:
        CommandResult with job run status details if successful
    """
    if not client:
        return CommandResult(
            False,
            message="No Databricks client available. Please set up your workspace first.",
        )

    # Extract parameters
    run_id = kwargs.get("run_id")

    try:
        # Get the job run status
        result = client.get_job_run_status(run_id)

        if not result:
            return CommandResult(False, message=f"No job run found with ID: {run_id}")

        # Extract key information
        run_info = {
            "job_id": result.get("job_id"),
            "run_id": result.get("run_id"),
            "run_name": result.get("run_name"),
            "state": result.get("state", {}).get("life_cycle_state"),
            "result_state": result.get("state", {}).get("result_state"),
            "start_time": result.get("start_time"),
            "setup_duration": result.get("setup_duration"),
            "execution_duration": result.get("execution_duration"),
            "cleanup_duration": result.get("cleanup_duration"),
            "creator_user_name": result.get("creator_user_name"),
        }

        # Add task status information if available
        tasks = result.get("tasks", [])
        if tasks:
            task_statuses = []
            for task in tasks:
                task_status = {
                    "task_key": task.get("task_key"),
                    "state": task.get("state", {}).get("life_cycle_state"),
                    "result_state": task.get("state", {}).get("result_state"),
                    "start_time": task.get("start_time"),
                    "setup_duration": task.get("setup_duration"),
                    "execution_duration": task.get("execution_duration"),
                    "cleanup_duration": task.get("cleanup_duration"),
                }
                task_statuses.append(task_status)

            run_info["tasks"] = task_statuses

        # Create a user-friendly message
        state_msg = f"{run_info['state']}"
        if run_info.get("result_state"):
            state_msg += f" ({run_info['result_state']})"

        message = f"Job run {run_id} is {state_msg}"

        return CommandResult(True, data=run_info, message=message)
    except Exception as e:
        logging.error(f"Error getting job run status: {str(e)}")
        return CommandResult(
            False, message=f"Failed to get job run status: {str(e)}", error=e
        )


DEFINITION = CommandDefinition(
    name="job-status",
    description="Check status of a Databricks job run.",
    handler=handle_command,
    parameters={
        "run_id": {
            "type": "string",
            "description": "The job run ID to check status for.",
        }
    },
    required_params=["run_id"],
    tui_aliases=["/job-status", "/job"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    usage_hint="Usage: /job-status --run_id <run_id>",
    condensed_action="Checking job status",
)
