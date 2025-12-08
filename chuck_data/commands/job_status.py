"""
Command for checking status of Chuck jobs.
"""

import logging
from typing import Optional, Any
from prettytable import PrettyTable
from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.clients.amperity import AmperityAPIClient
from chuck_data.commands.base import CommandResult
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import (
    get_amperity_token,
    get_workspace_url,
    get_databricks_token,
)
from chuck_data.job_cache import (
    get_last_job_id,
    find_run_id_for_job,
    get_all_cached_jobs,
)

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
    # Extract state info
    state_obj = result.get("state", {})

    run_info = {
        "job_id": result.get("job_id"),
        "run_id": result.get("run_id"),
        "run_name": result.get("run_name"),
        "state": state_obj,  # Keep full state object for nested access
        "life_cycle_state": (
            state_obj.get("life_cycle_state")
            if isinstance(state_obj, dict)
            else state_obj
        ),
        "result_state": (
            state_obj.get("result_state") if isinstance(state_obj, dict) else None
        ),
        "start_time": result.get("start_time"),
        "setup_duration": result.get("setup_duration"),
        "execution_duration": result.get("execution_duration"),
        "cleanup_duration": result.get("cleanup_duration"),
        "run_page_url": result.get("run_page_url"),
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


def _format_duration(duration_ms: int) -> str:
    """Format duration from milliseconds to human-readable string.

    Args:
        duration_ms: Duration in milliseconds

    Returns:
        Formatted duration string (e.g., "14m 22s")
    """
    seconds = duration_ms / 1000
    mins = int(seconds // 60)
    secs = int(seconds % 60)

    if mins > 0:
        return f"{mins}m {secs}s"
    else:
        return f"{secs}s"


def _format_box_line(content: str, width: int = 80) -> str:
    """Format a single line for the box with proper padding.

    Args:
        content: The text content (without borders)
        width: Total width of content area (default 80)

    Returns:
        Formatted line with borders and padding
    """
    # Ensure content doesn't exceed width
    if len(content) > width:
        content = content[:width]

    padding = width - len(content)
    return f"│{content}{' ' * padding}│"


def _format_job_status_message(job_id: str, job_data: dict) -> str:
    """
    Format a comprehensive job status message from job data with nice box display.

    Args:
        job_id: Chuck job identifier
        job_data: Job data dictionary from Chuck backend

    Returns:
        Formatted status message string with box drawing
    """
    # Box width constants
    BASE_BOX_WIDTH = 80  # Minimum width of the box content area

    state = (job_data.get("state") or "UNKNOWN").upper()

    # Status symbol
    status_symbol = "✓" if state == "SUCCEEDED" else "✗" if state == "FAILED" else "◷"

    # Check if we have live Databricks data
    has_live_data = "databricks_live" in job_data
    databricks_live = job_data.get("databricks_live", {})

    # Calculate actual box width needed (expand if URL is long)
    BOX_WIDTH = BASE_BOX_WIDTH
    if has_live_data and databricks_live.get("run_page_url"):
        url = databricks_live["run_page_url"]
        label = " • View: "
        url_line_length = len(label) + len(url)
        BOX_WIDTH = max(BOX_WIDTH, url_line_length)

    # Build the message
    lines = []

    # Header
    header_text = f"─ Job: {job_id} "
    lines.append("┌" + header_text + "─" * (BOX_WIDTH - len(header_text)) + "┐")

    # Status line
    status_text = f" Status: {status_symbol} {state}"
    lines.append("│" + status_text + " " * (BOX_WIDTH - len(status_text)) + "│")

    # Timestamps
    if job_data.get("created-at"):
        created = job_data["created-at"].replace("T", " ").replace("Z", "")
        lines.append(_format_box_line(f" Created: {created}", BOX_WIDTH))

    if job_data.get("start-time"):
        started = job_data["start-time"].replace("T", " ").replace("Z", "")
        lines.append(_format_box_line(f" Started: {started}", BOX_WIDTH))

    if job_data.get("end-time"):
        ended = job_data["end-time"].replace("T", " ").replace("Z", "")
        lines.append(_format_box_line(f" Ended:   {ended}", BOX_WIDTH))

    # Duration - always use Chuck timestamps (start-time to end-time)
    if job_data.get("start-time") and job_data.get("end-time"):
        try:
            from datetime import datetime

            start = datetime.fromisoformat(
                job_data["start-time"].replace("Z", "+00:00")
            )
            end = datetime.fromisoformat(job_data["end-time"].replace("Z", "+00:00"))
            duration = end - start
            duration_mins = int(duration.total_seconds() // 60)
            duration_secs = int(duration.total_seconds() % 60)
            duration_str = f"{duration_mins}m {duration_secs}s"
            lines.append(_format_box_line(f" Duration: {duration_str}", BOX_WIDTH))
        except Exception:
            pass

    # Processing section
    has_processing = (
        job_data.get("record-count") or job_data.get("credits") or job_data.get("build")
    )
    if has_processing:
        lines.append(_format_box_line("", BOX_WIDTH))
        # Use "Processed" if job is complete, "Processing" if still running
        processing_label = (
            "Processed:" if state in ["SUCCEEDED", "FAILED"] else "Processing:"
        )
        lines.append(_format_box_line(f" {processing_label}", BOX_WIDTH))

        if job_data.get("record-count"):
            records = f"{job_data['record-count']:,}"
            lines.append(_format_box_line(f" • Records: {records}", BOX_WIDTH))

        if job_data.get("credits"):
            lines.append(
                _format_box_line(f" • Credits: {job_data['credits']}", BOX_WIDTH)
            )

        if job_data.get("build"):
            build = job_data["build"]
            lines.append(_format_box_line(f" • Build: {build}", BOX_WIDTH))

    # Databricks section (only if --live flag was used)
    databricks_run_id = job_data.get("databricks-run-id")
    if (
        has_live_data
        and databricks_run_id
        and databricks_run_id != UNSET_DATABRICKS_RUN_ID
    ):
        lines.append(_format_box_line("", BOX_WIDTH))
        lines.append(_format_box_line(" Databricks:", BOX_WIDTH))

        # Status with result state
        db_state = databricks_live.get("state", {})
        lifecycle_state = db_state.get("life_cycle_state", "UNKNOWN")
        result_state = db_state.get("result_state")
        if result_state:
            status_text = f" Status: {lifecycle_state} ({result_state})"
        else:
            status_text = f" Status: {lifecycle_state}"
        lines.append(_format_box_line(status_text, BOX_WIDTH))

        # Run ID
        lines.append(_format_box_line(f" Run ID: {databricks_run_id}", BOX_WIDTH))

        # Run name
        if databricks_live.get("run_name"):
            lines.append(
                _format_box_line(f" Name: {databricks_live['run_name']}", BOX_WIDTH)
            )

        # Creator
        if databricks_live.get("creator_user_name"):
            lines.append(
                _format_box_line(
                    f" Creator: {databricks_live['creator_user_name']}", BOX_WIDTH
                )
            )

        # Execution time
        if databricks_live.get("execution_duration"):
            db_duration = _format_duration(databricks_live["execution_duration"])
            lines.append(_format_box_line(f" Execution: {db_duration}", BOX_WIDTH))

        # Task count
        if databricks_live.get("tasks"):
            task_count = len(databricks_live["tasks"])
            lines.append(_format_box_line(f" Tasks: {task_count}", BOX_WIDTH))

        # View URL - keep on single line for clickability
        if databricks_live.get("run_page_url"):
            url = databricks_live["run_page_url"]
            lines.append(_format_box_line("", BOX_WIDTH))
            # Put label and URL on same line (BOX_WIDTH already adjusted to fit)
            label = " • View: "
            lines.append(_format_box_line(f"{label}{url}", BOX_WIDTH))

    # Error section if any
    if job_data.get("error"):
        lines.append(_format_box_line("", BOX_WIDTH))
        error_msg = job_data["error"]
        lines.append(_format_box_line(f" ✗ Error: {error_msg}", BOX_WIDTH))

    lines.append("└" + "─" * BOX_WIDTH + "┘")

    return "\n".join(lines)


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

        # Add workspace URL for generating Databricks links
        workspace_url = get_workspace_url()
        if workspace_url:
            job_data["workspace_url"] = workspace_url

        # Optionally enrich with live Databricks data
        databricks_run_id = job_data.get("databricks-run-id")

        # If Chuck backend returns UNSET, try to use cached run_id
        if databricks_run_id == UNSET_DATABRICKS_RUN_ID:
            cached_run_id = find_run_id_for_job(job_id)
            if cached_run_id:
                databricks_run_id = cached_run_id
                # Update job_data so the formatting function sees the cached run_id
                job_data["databricks-run-id"] = cached_run_id

        if (
            fetch_live
            and databricks_run_id
            and databricks_run_id != UNSET_DATABRICKS_RUN_ID
        ):
            # Create Databricks client if not provided
            if not client:
                db_token = get_databricks_token()
                if workspace_url and db_token:
                    client = DatabricksAPIClient(workspace_url, db_token)

            if client:
                try:
                    databricks_raw = client.get_job_run_status(databricks_run_id)
                    job_data["databricks_live"] = _extract_databricks_run_info(
                        databricks_raw
                    )
                except Exception as e:
                    logging.error(
                        f"Failed to fetch live Databricks data: {e}", exc_info=True
                    )

        # Format output message - build a comprehensive summary
        message = _format_job_status_message(job_id, job_data)

        return CommandResult(True, data=job_data, message=message)

    except Exception as e:
        logging.error(f"Error querying Chuck backend: {str(e)}")
        return CommandResult(
            False, message=f"Failed to get job status from Chuck: {str(e)}", error=e
        )


def _format_databricks_only_message(run_id: str, run_info: dict) -> str:
    """
    Format Databricks-only job status message with box display.

    Args:
        run_id: Databricks run identifier
        run_info: Run information from Databricks

    Returns:
        Formatted status message string
    """
    BASE_BOX_WIDTH = 80

    state = (run_info.get("life_cycle_state") or "UNKNOWN").upper()
    result_state = (run_info.get("result_state") or "").upper()

    # Status symbol
    status_symbol = (
        "✓" if result_state == "SUCCESS" else "✗" if result_state == "FAILED" else "◷"
    )

    # Calculate actual box width needed (expand if URL is long)
    BOX_WIDTH = BASE_BOX_WIDTH
    if run_info.get("run_page_url"):
        url = run_info["run_page_url"]
        label = " • View: "
        url_line_length = len(label) + len(url)
        BOX_WIDTH = max(BOX_WIDTH, url_line_length)

    # Build the message
    lines = []

    # Header
    header_text = f"─ Databricks Run: {run_id} "
    lines.append("┌" + header_text + "─" * (BOX_WIDTH - len(header_text)) + "┐")

    # Status line
    if result_state:
        status_text = f" Status: {status_symbol} {state} ({result_state})"
    else:
        status_text = f" Status: {status_symbol} {state}"
    lines.append(_format_box_line(status_text, BOX_WIDTH))

    # Job name
    if run_info.get("run_name"):
        name = run_info["run_name"]
        lines.append(_format_box_line(f" Name: {name}", BOX_WIDTH))

    # Creator
    if run_info.get("creator_user_name"):
        creator = run_info["creator_user_name"]
        lines.append(_format_box_line(f" Creator: {creator}", BOX_WIDTH))

    # Duration
    if run_info.get("execution_duration"):
        duration_str = _format_duration(run_info["execution_duration"])
        lines.append(_format_box_line(f" Execution: {duration_str}", BOX_WIDTH))

    # Task information
    if run_info.get("tasks"):
        task_count = len(run_info["tasks"])
        lines.append(_format_box_line(f" Tasks: {task_count}", BOX_WIDTH))

    # Job URL - keep on single line for clickability
    if run_info.get("run_page_url"):
        url = run_info["run_page_url"]
        lines.append(_format_box_line("", BOX_WIDTH))
        # Put label and URL on same line (BOX_WIDTH already adjusted to fit)
        label = " • View: "
        lines.append(_format_box_line(f"{label}{url}", BOX_WIDTH))

    lines.append(_format_box_line("", BOX_WIDTH))
    lines.append(
        _format_box_line(" Note: Databricks data only - no Chuck telemetry", BOX_WIDTH)
    )
    lines.append("└" + "─" * BOX_WIDTH + "┘")

    return "\n".join(lines)


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

    # Format the message using the new box display
    message = _format_databricks_only_message(run_id, run_info)

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

    # If no parameters provided, try to use cached job ID with live data
    if not job_id and not run_id:
        cached_job_id = get_last_job_id()
        if cached_job_id:
            job_id = cached_job_id
            # Always fetch live data when using cached ID
            fetch_live = True
        else:
            return CommandResult(
                False,
                message="No job ID provided and no cached job ID available. "
                "Please specify --job-id or --run-id, or run a job first.",
            )

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
    name="job_status",
    description="Check status of a Chuck job via backend or Databricks. "
    "If no parameters provided, uses the last cached job ID.",
    handler=handle_command,
    parameters={
        "job_id": {
            "type": "string",
            "description": "Chuck job ID (primary parameter).",
        },
        "run_id": {
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
    usage_hint="Usage: /job-status [--job-id <job_id>] [--live] OR /job-status --run-id <run_id> OR /job-status (uses cached ID)",
    condensed_action="Checking job status",
)


# --- List Jobs Command ---


def _cache_job_data(
    job_id: str, run_id: Optional[str], job_data: dict, reason: str = ""
):
    """
    Helper to cache job data and log the action.

    Args:
        job_id: Chuck job identifier
        run_id: Optional Databricks run identifier
        job_data: Job data dictionary to cache
        reason: Optional reason for caching (for logging)
    """
    from chuck_data.job_cache import cache_job

    cache_job(job_id, run_id, job_data)
    state = job_data.get("state", "UNKNOWN")
    log_msg = f"Cached job {job_id} (state: {state})"
    if reason:
        log_msg += f" - {reason}"
    logging.debug(log_msg)


def handle_list_jobs(client=None, **kwargs) -> CommandResult:
    """List recent jobs from cache.

    Args:
        client: Optional API client (unused, for compatibility)
        **kwargs: Optional arguments (unused)

    Returns:
        CommandResult with table of recent jobs
    """
    import time

    start_time = time.time()

    # Get all cached jobs
    cached_jobs = get_all_cached_jobs()
    logging.debug(
        f"Loaded {len(cached_jobs)} jobs from cache in {time.time() - start_time:.3f}s"
    )

    if not cached_jobs:
        return CommandResult(
            True,
            message="No recent jobs found. Launch a job first to see it here.",
        )

    # Try to fetch job details from Chuck for each cached job
    client_init_start = time.time()
    client = AmperityAPIClient()
    token = get_amperity_token()
    logging.debug(f"Client initialization took {time.time() - client_init_start:.3f}s")

    if not token:
        # Still show cached job IDs even without token
        message = _format_jobs_table_minimal(cached_jobs)
        return CommandResult(
            True,
            message=message
            + "\n\nNote: Authenticate with Amperity to see full job details.",
        )

    # Fetch details for each job
    jobs_with_details = []
    cache_hits = 0
    api_calls = 0

    for job_entry in cached_jobs:
        job_id = job_entry.get("job_id")
        if not job_id:
            continue

        # Check if we have cached job data
        cached_job_data = job_entry.get("job_data")

        # If we have cached data for a terminal state, use it
        if cached_job_data and isinstance(cached_job_data, dict):
            state = (cached_job_data.get("state") or "").lower().replace(":", "")
            # Only use cache for terminal states (succeeded, failed, unknown)
            if state in ["succeeded", "success", "failed", "error", "unknown"]:
                cached_at = job_entry.get("cached_at", "unknown")
                logging.debug(
                    f"Using cached data for job {job_id} (state: {state}, cached_at: {cached_at})"
                )
                jobs_with_details.append(cached_job_data)
                cache_hits += 1
                continue

        # Otherwise fetch fresh data from API
        logging.debug(f"Fetching fresh data for job {job_id}")
        api_calls += 1
        run_id = job_entry.get("run_id")

        try:
            job_data = client.get_job_status(job_id, token)
            if job_data:
                jobs_with_details.append(job_data)

                # Cache the data if it's in a terminal state
                state = (job_data.get("state") or "").lower().replace(":", "")
                if state in ["succeeded", "success", "failed", "error"]:
                    _cache_job_data(job_id, run_id, job_data, "terminal state")
            else:
                # Job not found, cache as UNKNOWN to avoid retrying
                unknown_data = {"job-id": job_id, "state": "UNKNOWN"}
                jobs_with_details.append(unknown_data)
                _cache_job_data(job_id, run_id, unknown_data, "not found")
        except Exception as e:
            # On error, cache as UNKNOWN to avoid retrying
            logging.debug(f"Error fetching job {job_id}: {e}")
            unknown_data = {"job-id": job_id, "state": "UNKNOWN"}
            jobs_with_details.append(unknown_data)
            _cache_job_data(job_id, run_id, unknown_data, "error")

    logging.debug(f"Jobs list: {cache_hits} cache hits, {api_calls} API calls")

    # Format and display the table
    format_start = time.time()
    message = _format_jobs_table(jobs_with_details)
    logging.debug(f"Table formatting took {time.time() - format_start:.3f}s")

    total_time = time.time() - start_time
    logging.info(f"Total /jobs command time: {total_time:.3f}s")

    return CommandResult(
        True,
        message=message,
        data={"jobs": jobs_with_details},
    )


def _format_jobs_table_minimal(cached_jobs: list) -> str:
    """Format a minimal table with just job IDs."""
    table = PrettyTable()
    table.field_names = ["Job ID", "Status"]
    table.align["Job ID"] = "l"
    table.align["Status"] = "l"

    for job_entry in cached_jobs:
        job_id = job_entry.get("job_id", "N/A")
        table.add_row([job_id, "N/A"])

    return f"Recent Jobs:\n\n{table}"


def _format_jobs_table(jobs: list) -> str:
    """
    Format jobs data into a table using PrettyTable.

    Args:
        jobs: List of job data dictionaries

    Returns:
        Formatted table string
    """
    if not jobs:
        return "No jobs to display."

    # Sort jobs by date (most recent first)
    # Use start-time if available, otherwise created-at
    def get_sort_key(job):
        date_str = job.get("start-time") or job.get("created-at")
        if date_str:
            try:
                from datetime import datetime, timezone

                # Ensure timezone awareness for proper comparison
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                # Make sure it's timezone-aware (use UTC if naive)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass
        # If no date or parse error, sort to end (use timezone-aware min)
        from datetime import datetime, timezone

        return datetime.min.replace(tzinfo=timezone.utc)

    sorted_jobs = sorted(jobs, key=get_sort_key, reverse=True)

    table = PrettyTable()
    table.field_names = ["Job ID", "Status", "Started", "Records", "Credits"]
    table.align["Job ID"] = "l"
    table.align["Status"] = "l"
    table.align["Started"] = "l"
    table.align["Records"] = "r"
    table.align["Credits"] = "r"

    # Calculate total credits
    total_credits = 0

    # Add rows
    for job in sorted_jobs:
        job_id = job.get("job-id", "N/A")
        state = (job.get("state") or "UNKNOWN").upper()

        # Status symbol and text
        if state == "SUCCEEDED":
            status = "✓ Success"
        elif state == "FAILED":
            status = "✗ Failed"
        elif state == "RUNNING":
            status = "◷ Running"
        else:
            status = f"◷ {state.title()}"

        # Date - prefer start-time, fallback to created-at
        date_str = job.get("start-time") or job.get("created-at")
        if date_str:
            try:
                # Format as "YYYY-Mon-DD HH:MM" (e.g., "2025-Jan-15 14:30")
                from datetime import datetime

                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                formatted_date = dt.strftime("%Y-%b-%d %H:%M")
            except Exception:
                formatted_date = "-"
        else:
            formatted_date = "-"

        # Records (formatted with commas)
        record_count = job.get("record-count")
        if record_count is not None:
            records = f"{record_count:,}"
        else:
            records = "-"

        # Credits - show dash if not present
        credits = job.get("credits")
        if credits is not None:
            credits_str = str(credits)
            total_credits += credits
        else:
            credits_str = "-"

        table.add_row([job_id, status, formatted_date, records, credits_str])

    result = f"Recent Jobs:\n\n{table}"

    # Always add total credits line
    result += f"\n\nTotal credits used: {total_credits}"

    return result


LIST_JOBS_DEFINITION = CommandDefinition(
    name="jobs",
    description="List recent jobs from cache",
    parameters={},
    required_params=[],
    handler=handle_list_jobs,
    needs_api_client=False,
    visible_to_user=True,
    visible_to_agent=True,
    tui_aliases=["/jobs"],
    usage_hint="Usage: /jobs",
    condensed_action="Listing recent jobs",
)
