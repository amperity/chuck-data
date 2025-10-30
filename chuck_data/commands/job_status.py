"""
Command for checking status of Chuck jobs.
"""

import logging
from typing import Optional, Any
from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.clients.amperity import AmperityAPIClient
from chuck_data.commands.base import CommandResult
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import get_amperity_token

# Constant for unset Databricks run ID
UNSET_DATABRICKS_RUN_ID = "UNSET_DATABRICKS_RUN_ID"


def _extract_databricks_run_info(result: dict) -> dict:
    """
    Extract and clean Databricks run information.

    Args:
        result: Raw result from Databricks get_job_run_status API

    Returns:
        Cleaned dictionary with structured run information
    """
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

    return run_info


def _format_job_status_message(job_id: str, job_data: dict) -> str:
    """
    Format a comprehensive job status message from job data.

    Args:
        job_id: Chuck job identifier
        job_data: Job data dictionary from Chuck backend

    Returns:
        Formatted status message string
    """
    state = job_data.get("state", "UNKNOWN")
    message_parts = [f"Job {job_id}: {state}"]

    # Add all available fields to the message
    if job_data.get("record-count"):
        message_parts.append(f"Records: {job_data.get('record-count'):,}")

    if job_data.get("credits"):
        message_parts.append(f"Credits: {job_data.get('credits')}")

    if job_data.get("build"):
        message_parts.append(f"Build: {job_data.get('build')}")

    if (
        job_data.get("databricks-run-id")
        and job_data.get("databricks-run-id") != UNSET_DATABRICKS_RUN_ID
    ):
        message_parts.append(f"Run ID: {job_data.get('databricks-run-id')}")

    # Timestamps
    if job_data.get("created-at"):
        message_parts.append(f"Created: {job_data.get('created-at')}")

    if job_data.get("start-time"):
        message_parts.append(f"Started: {job_data.get('start-time')}")

    if job_data.get("end-time"):
        message_parts.append(f"Ended: {job_data.get('end-time')}")

    # Calculate duration if we have start and end times
    if job_data.get("start-time") and job_data.get("end-time"):
        try:
            from datetime import datetime

            start = datetime.fromisoformat(
                job_data["start-time"].replace("Z", "+00:00")
            )
            end = datetime.fromisoformat(job_data["end-time"].replace("Z", "+00:00"))
            duration = end - start
            duration_mins = duration.total_seconds() / 60
            message_parts.append(f"Duration: {duration_mins:.1f}m")
        except Exception:
            pass  # Skip duration calculation if parsing fails

    if job_data.get("accepted?"):
        message_parts.append(f"Accepted: {job_data.get('accepted?')}")

    if job_data.get("error"):
        message_parts.append(f"Error: {job_data.get('error')}")

    return ", ".join(message_parts)


def _query_by_job_id(
    job_id: str,
    amperity_client: Optional[AmperityAPIClient] = None,
    client: Optional[DatabricksAPIClient] = None,
    fetch_live: Optional[bool] = None,
) -> CommandResult:
    """
    Query job status from Chuck backend using job-id (primary method).

    Args:
        job_id: Chuck job identifier
        amperity_client: AmperityAPIClient for backend calls (optional)
        client: DatabricksAPIClient for optional live data enrichment (optional)
        fetch_live: Whether to enrich with live Databricks data (optional, default: False)

    Returns:
        CommandResult with job status from Chuck backend
    """
    if not amperity_client:
        amperity_client = AmperityAPIClient()

    token = get_amperity_token()
    if not token:
        return CommandResult(
            False, message="No Amperity token found. Please authenticate first."
        )

    try:
        job_data = amperity_client.get_job_status(job_id, token)

        # Optionally enrich with live Databricks data
        databricks_run_id = job_data.get("databricks-run-id")
        if (
            fetch_live
            and databricks_run_id
            and databricks_run_id != UNSET_DATABRICKS_RUN_ID
            and client
        ):
            databricks_raw = client.get_job_run_status(databricks_run_id)
            job_data["databricks_live"] = _extract_databricks_run_info(databricks_raw)

        # Format output message - build a comprehensive summary
        message = _format_job_status_message(job_id, job_data)

        return CommandResult(True, data=job_data, message=message)

    except Exception as e:
        logging.error(f"Error querying Chuck backend: {str(e)}")
        return CommandResult(
            False, message=f"Failed to get job status from Chuck: {str(e)}", error=e
        )


def _query_by_run_id(
    run_id: str, client: Optional[DatabricksAPIClient] = None
) -> CommandResult:
    """
    Query job status from Databricks API using run-id (legacy fallback).

    Args:
        run_id: Databricks run identifier
        client: DatabricksAPIClient for API calls

    Returns:
        CommandResult with job status from Databricks API
    """
    if not client:
        return CommandResult(
            False, message="No Databricks client available to query job status"
        )

    result = client.get_job_run_status(run_id)

    if not result:
        return CommandResult(False, message=f"No job run found with ID: {run_id}")

    # Extract and clean Databricks run information
    run_info = _extract_databricks_run_info(result)

    # Create a user-friendly message with more details
    message_parts = [f"Job run {run_id}"]

    # State information
    state_msg = f"{run_info['state']}"
    if run_info.get("result_state"):
        state_msg += f" ({run_info['result_state']})"
    message_parts.append(f"Status: {state_msg}")

    # Job name
    if run_info.get("run_name"):
        message_parts.append(f"Name: {run_info['run_name']}")

    # Creator
    if run_info.get("creator_user_name"):
        message_parts.append(f"Creator: {run_info['creator_user_name']}")

    # Duration information
    if run_info.get("execution_duration"):
        duration_seconds = (
            run_info["execution_duration"] / 1000
        )  # Convert ms to seconds
        if duration_seconds >= 60:
            duration_mins = duration_seconds / 60
            message_parts.append(f"Execution: {duration_mins:.1f}m")
        else:
            message_parts.append(f"Execution: {duration_seconds:.1f}s")

    if run_info.get("run_duration"):
        total_seconds = run_info["run_duration"] / 1000
        if total_seconds >= 60:
            total_mins = total_seconds / 60
            message_parts.append(f"Total: {total_mins:.1f}m")
        else:
            message_parts.append(f"Total: {total_seconds:.1f}s")

    # Task information
    if run_info.get("tasks"):
        message_parts.append(f"Tasks: {len(run_info['tasks'])}")

    # Job URL
    if run_info.get("run_page_url"):
        message_parts.append(f"URL: {run_info['run_page_url']}")

    message_parts.append("(Databricks only - no Chuck telemetry)")
    message = ", ".join(message_parts)

    return CommandResult(True, data=run_info, message=message)


def handle_command(
    client: Optional[DatabricksAPIClient] = None,
    amperity_client: Optional[AmperityAPIClient] = None,
    **kwargs: Any,
) -> CommandResult:
    """
    Check status of a Chuck job.

    Args:
        client: DatabricksAPIClient instance for API calls (fallback for legacy run_id)
        amperity_client: AmperityAPIClient for Chuck backend queries
        **kwargs: Command parameters
            - job_id or job-id: Chuck job identifier (primary)
            - run_id or run-id: Databricks run ID (fallback for legacy)
            - live: Fetch live Databricks data (optional)

    Returns:
        CommandResult with job status details if successful
    """
    # Support both hyphen and underscore formats
    job_id = kwargs.get("job_id") or kwargs.get("job-id")
    run_id = kwargs.get("run_id") or kwargs.get("run-id")
    fetch_live = kwargs.get("live", False)

    if not job_id and not run_id:
        return CommandResult(False, message="Either --job-id or --run-id is required")

    try:
        # Primary: Query Chuck backend by job-id
        if job_id:
            return _query_by_job_id(job_id, amperity_client, client, fetch_live)

        # Fallback: Query Databricks API by run-id (legacy)
        elif run_id:
            return _query_by_run_id(run_id, client)

        else:
            return CommandResult(
                False, message="No client available to query job status"
            )

    except Exception as e:
        logging.error(f"Error getting job status: {str(e)}")
        return CommandResult(
            False, message=f"Failed to get job status: {str(e)}", error=e
        )


DEFINITION = CommandDefinition(
    name="job-status",
    description="Check status of a Chuck job via backend or Databricks.",
    handler=handle_command,
    parameters={
        "job-id": {
            "type": "string",
            "description": "Chuck job ID (primary parameter).",
        },
        "run-id": {
            "type": "string",
            "description": "Databricks run ID (legacy fallback).",
        },
        "live": {
            "type": "boolean",
            "description": "Fetch live Databricks data (optional).",
        },
    },
    required_params=[],
    tui_aliases=["/job-status", "/job"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    usage_hint="Usage: /job-status --job_id <job_id> [--live] OR /job-status --run_id <run_id>",
    condensed_action="Checking job status",
)
