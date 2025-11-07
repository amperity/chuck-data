"""
Command for monitoring Chuck jobs until completion.
"""

import logging
from typing import Optional, Any, Dict, Tuple
from chuck_data.commands.base import CommandResult
from chuck_data.command_registry import CommandDefinition
from chuck_data.job_cache import (
    get_last_job_id,
    find_run_id_for_job,
    find_job_id_for_run,
)
from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.clients.amperity import AmperityAPIClient


def _check_terminal_state(
    job_data: Dict[str, Any],
) -> Tuple[bool, Optional[CommandResult]]:
    """
    Check if a job is in a terminal state and return appropriate result.

    Args:
        job_data: Job data dictionary from Chuck backend

    Returns:
        Tuple of (is_terminal, optional_command_result)
        - If is_terminal=True, command_result will be populated
        - If is_terminal=False, command_result will be None
    """
    if not job_data:
        return True, CommandResult(
            False,
            message="Job not found in Chuck backend. "
            "The job may have been deleted or the ID may be incorrect.",
        )

    state = job_data.get("state", "").lower()
    normalized_state = state.replace(":", "")
    job_id = job_data.get("job-id", "unknown")

    # Check if job is already in a terminal state
    if normalized_state in ["succeeded", "success"]:
        record_count = job_data.get("record-count", 0)
        credits = job_data.get("credits")
        message = f"Job {job_id} has already completed successfully!"
        if record_count:
            message += f"\nRecords: {record_count:,}"
        if credits:
            message += f"\nCredits: {credits}"
        return True, CommandResult(True, message=message, data=job_data)

    if normalized_state in ["failed", "error", "unauthorized"]:
        error = job_data.get("error", "Job failed")
        return True, CommandResult(
            False,
            message=f"Job {job_id} has already failed: {error}",
            data=job_data,
        )

    # If state is unknown or empty, return error
    if not state or normalized_state in ["unknown"]:
        return True, CommandResult(
            False,
            message=f"Job {job_id} has unknown state: '{state}'. "
            "Cannot monitor a job with unknown state.",
        )

    # Not in terminal state - continue monitoring
    return False, None


def _monitor_job_completion(
    job_id: str,
    run_id: str,
    poll_interval: int = 30,
    timeout: int = 1800,
) -> Dict[str, Any]:
    """
    Poll job status from Chuck backend until completion or timeout.

    Args:
        job_id: Chuck job identifier
        run_id: Databricks run ID (for fallback/reference)
        poll_interval: Seconds between status checks (default: 30s)
        timeout: Maximum seconds to wait (default: 1800s = 30min)

    Returns:
        Dict with final job status from Chuck backend
    """
    import time
    from chuck_data.clients.amperity import AmperityAPIClient
    from chuck_data.config import get_amperity_token
    from chuck_data.ui.tui import get_console
    from chuck_data.ui.theme import INFO_STYLE, SUCCESS_STYLE, ERROR_STYLE

    amperity_client = AmperityAPIClient()
    token = get_amperity_token()
    console = get_console()

    if not token:
        error_msg = "No Amperity token available for monitoring"
        console.print(f"\n[{ERROR_STYLE}]{error_msg}[/{ERROR_STYLE}]")
        return {
            "success": False,
            "error": error_msg,
            "job_id": job_id,
            "state": "UNKNOWN",
        }

    monitor_start_time = time.time()
    job_start_time = None  # Will be set from job data
    console.print(
        f"\n[{INFO_STYLE}]Monitoring job progress... (Press Ctrl+C to exit)[/{INFO_STYLE}]"
    )
    logging.info(f"Monitoring job {job_id} for completion...")

    while True:
        elapsed = time.time() - monitor_start_time
        if elapsed > timeout:
            timeout_msg = f"Job monitoring timed out after {timeout}s"
            console.print(f"[{ERROR_STYLE}]{timeout_msg}[/{ERROR_STYLE}]")
            return {
                "success": False,
                "error": timeout_msg,
                "job_id": job_id,
                "state": "TIMEOUT",
            }

        # Query Chuck backend for job status
        try:
            job_data = amperity_client.get_job_status(job_id, token)

            # Use shared terminal state checker
            is_terminal, result = _check_terminal_state(job_data)
            if is_terminal:
                # Convert CommandResult to dict format and print appropriate message
                if not job_data:
                    error_msg = f"Job {job_id} not found in Chuck backend"
                    console.print(f"[{ERROR_STYLE}]{error_msg}[/{ERROR_STYLE}]")
                    return {
                        "success": False,
                        "error": error_msg,
                        "job_id": job_id,
                        "state": "NOT_FOUND",
                    }

                state = job_data.get("state", "").lower()
                normalized_state = state.replace(":", "")
                record_count = job_data.get("record-count", 0)

                if normalized_state in ["succeeded", "success"]:
                    success_msg = "Job completed successfully!"
                    if record_count:
                        success_msg += f" Records: {record_count:,}"
                    console.print(f"[{SUCCESS_STYLE}]{success_msg}[/{SUCCESS_STYLE}]")
                    return {
                        "success": True,
                        "job_id": job_id,
                        "state": state,
                        "record_count": record_count,
                        "credits": job_data.get("credits"),
                        "databricks_run_id": run_id,
                        "job_data": job_data,
                    }

                if normalized_state in ["failed", "error", "unauthorized"]:
                    error_msg = job_data.get("error", "Job failed")
                    console.print(
                        f"[{ERROR_STYLE}]Job failed: {error_msg}[/{ERROR_STYLE}]"
                    )
                    return {
                        "success": False,
                        "job_id": job_id,
                        "state": state,
                        "error": error_msg,
                        "databricks_run_id": run_id,
                        "job_data": job_data,
                    }

                # Unknown or other terminal states
                error_msg = (
                    result.message if result else "Job in unknown terminal state"
                )
                console.print(f"[{ERROR_STYLE}]{error_msg}[/{ERROR_STYLE}]")
                return {
                    "success": False,
                    "job_id": job_id,
                    "state": state,
                    "error": error_msg,
                    "databricks_run_id": run_id,
                    "job_data": job_data,
                }

            # Not terminal - continue monitoring
            state = job_data.get("state", "").lower()

            # Get job start time from job data (only once)
            if job_start_time is None and job_data.get("start-time"):
                try:
                    from datetime import datetime

                    start_time_str = job_data["start-time"].replace("Z", "+00:00")
                    job_start_dt = datetime.fromisoformat(start_time_str)
                    job_start_time = job_start_dt.timestamp()
                except Exception as e:
                    logging.warning(f"Could not parse job start time: {e}")
                    job_start_time = monitor_start_time  # Fallback to monitor start

            # Log progress
            record_count = job_data.get("record-count", 0)
            logging.debug(f"Job {job_id} state: {state}, records: {record_count}")

            # Show progress update for running job
            # Calculate elapsed time from job start (not monitor start)
            actual_elapsed = time.time() - (
                job_start_time if job_start_time else monitor_start_time
            )
            elapsed_mins = int(actual_elapsed) // 60
            elapsed_secs = int(actual_elapsed) % 60
            time_str = (
                f"{elapsed_mins}m {elapsed_secs}s"
                if elapsed_mins > 0
                else f"{elapsed_secs}s"
            )

            progress_msg = f"Job running... State: {state}, Elapsed: {time_str}"
            if record_count:
                progress_msg += f", Records: {record_count:,}"
            console.print(f"[{INFO_STYLE}]{progress_msg}[/{INFO_STYLE}]")

        except Exception as e:
            logging.warning(f"Error querying Chuck backend: {e}")
            console.print(
                f"[{INFO_STYLE}]Still monitoring... (elapsed: {int(elapsed)}s)[/{INFO_STYLE}]"
            )

        # Still running - wait and poll again
        logging.info(f"Job {job_id} still running... (elapsed: {int(elapsed)}s)")
        time.sleep(poll_interval)


def handle_command(
    client: Optional[DatabricksAPIClient] = None,
    amperity_client: Optional[AmperityAPIClient] = None,
    **kwargs: Any,
) -> CommandResult:
    """
    Monitor a Chuck job until completion with real-time progress updates.

    Args:
        **kwargs: Command parameters
            - job_id or job-id: Chuck job identifier (optional, uses cached if not provided)
            - run_id or run-id: Databricks run ID (optional, will attempt to find it)
            - poll_interval: Seconds between checks (default: 30)
            - timeout: Maximum seconds to wait (default: 1800 = 30 minutes)

    Returns:
        CommandResult with monitoring results
    """
    from chuck_data.clients.amperity import AmperityAPIClient
    from chuck_data.config import get_amperity_token

    # Support both hyphen and underscore formats
    job_id = kwargs.get("job_id") or kwargs.get("job-id")
    run_id = kwargs.get("run_id") or kwargs.get("run-id")
    poll_interval = kwargs.get("poll_interval", 30)
    timeout = kwargs.get("timeout", 1800)

    # If neither job_id nor run_id provided, try to use cached job ID
    if not job_id and not run_id:
        cached_job_id = get_last_job_id()
        if cached_job_id:
            job_id = cached_job_id
        else:
            return CommandResult(
                False,
                message="No job ID or run ID provided and no cached job ID available. "
                "Please specify --job-id or --run-id, or run a job first.",
            )

    # Validate and resolve IDs (1-to-1 correspondence)
    if job_id and run_id:
        # Both provided - verify they match
        cached_run_id = find_run_id_for_job(job_id)
        cached_job_id = find_job_id_for_run(run_id)

        # Check if they're consistent with cache
        if cached_run_id and cached_run_id != run_id:
            return CommandResult(
                False,
                message=f"Mismatch: job-id '{job_id}' is associated with run-id '{cached_run_id}', "
                f"not '{run_id}'. Please verify your IDs or provide only one.",
            )
        if cached_job_id and cached_job_id != job_id:
            return CommandResult(
                False,
                message=f"Mismatch: run-id '{run_id}' is associated with job-id '{cached_job_id}', "
                f"not '{job_id}'. Please verify your IDs or provide only one.",
            )
    elif job_id and not run_id:
        # Have job_id, need run_id
        run_id = find_run_id_for_job(job_id)
        if not run_id:
            return CommandResult(
                False,
                message=f"Cannot monitor job {job_id}: Databricks run ID not found. "
                "Please provide --run-id or ensure the job was launched by chuck.",
            )
    elif run_id and not job_id:
        # Have run_id, try to find job_id (optional, for better status checking)
        job_id = find_job_id_for_run(run_id)
        if not job_id:
            logging.info(
                f"Could not find job_id for run_id {run_id}, will monitor without pre-check"
            )

    # Final validation: ensure we have a valid run_id before monitoring
    if not run_id:
        return CommandResult(
            False,
            message="Cannot monitor job: No Databricks run ID available. "
            "Please provide --run-id or ensure the job was launched by chuck.",
        )

    try:
        # IMPORTANT: Always check the current job state before monitoring
        # This prevents monitoring jobs that are already in terminal states
        if not job_id:
            return CommandResult(
                False,
                message="Cannot monitor job: No job ID available. "
                "Please provide --job-id or ensure the job was launched by chuck.",
            )

        token = get_amperity_token()
        if not token:
            return CommandResult(
                False,
                message="Cannot monitor job: No Amperity token available. "
                "Please authenticate first using: chuck auth",
            )

        # Check current job state using the shared helper
        try:
            amperity_client = AmperityAPIClient()
            job_data = amperity_client.get_job_status(job_id, token)

            # Use shared terminal state checker
            is_terminal, result = _check_terminal_state(job_data)
            if is_terminal and result:
                return result

        except Exception as e:
            # If we can't check status, return error instead of continuing
            logging.error(f"Could not check job status before monitoring: {e}")

            # Parse common error messages for better user experience
            error_str = str(e)
            if "404" in error_str and "No job found" in error_str:
                return CommandResult(
                    False,
                    message=f"Job {job_id} not found. The job may have expired, been deleted, or the ID may be incorrect.",
                    error=e,
                )
            elif (
                "401" in error_str or "403" in error_str or "Unauthorized" in error_str
            ):
                return CommandResult(
                    False,
                    message="Authentication failed. Please authenticate using: chuck auth",
                    error=e,
                )
            else:
                return CommandResult(
                    False,
                    message=f"Failed to check job status: {error_str}",
                    error=e,
                )

        # Monitor the job (run_id is required at this point)
        monitor_result = _monitor_job_completion(
            job_id=job_id or "unknown",  # Provide fallback for type safety
            run_id=str(run_id),
            poll_interval=poll_interval,
            timeout=timeout,
        )

        # Build result message
        if monitor_result.get("success"):
            message = f"Job {job_id} completed successfully!"
            if monitor_result.get("record_count"):
                message += f"\nRecords: {monitor_result['record_count']:,}"
            if monitor_result.get("credits"):
                message += f"\nCredits: {monitor_result['credits']}"
        else:
            message = (
                f"Job {job_id} monitoring ended: "
                f"{monitor_result.get('error', 'Unknown error')}"
            )

        return CommandResult(
            monitor_result.get("success", False),
            message=message,
            data=monitor_result,
        )

    except Exception as e:
        logging.error(f"Error monitoring job: {str(e)}")
        return CommandResult(False, message=f"Failed to monitor job: {str(e)}", error=e)


DEFINITION = CommandDefinition(
    name="monitor_job",
    description="Monitor a Chuck job with real-time progress updates until completion. "
    "Provide either --job-id OR --run-id (not both), or omit both to monitor the last launched job.",
    handler=handle_command,
    parameters={
        "job_id": {
            "type": "string",
            "description": "Chuck job ID to monitor. Provide either job-id OR run-id, not both.",
        },
        "run_id": {
            "type": "string",
            "description": "Databricks run ID to monitor. Provide either job-id OR run-id, not both.",
        },
        "poll_interval": {
            "type": "number",
            "description": "Seconds between status checks (default: 30).",
        },
        "timeout": {
            "type": "number",
            "description": "Maximum seconds to wait (default: 1800).",
        },
    },
    required_params=[],
    tui_aliases=["/monitor-job", "/monitor"],
    needs_api_client=False,
    visible_to_user=True,
    visible_to_agent=True,
    usage_hint="Usage: /monitor-job [--job_id <id> | --run_id <id>] OR /monitor-job (monitors last job)",
    condensed_action="Monitoring job",
)
