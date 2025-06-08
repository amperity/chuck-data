"""
Command handler for Stitch integration setup.

This module contains the handler for setting up a Stitch integration by scanning
for PII columns and creating a configuration file.
"""

import logging
from typing import Optional

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.llm.client import LLMClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import get_active_catalog, get_active_schema
from chuck_data.metrics_collector import get_metrics_collector
from chuck_data.interactive_context import InteractiveContext
from chuck_data.ui.theme import SUCCESS_STYLE, ERROR_STYLE, INFO_STYLE, WARNING
from chuck_data.ui.tui import get_console
from .base import CommandResult
from .stitch_tools import (
    _helper_setup_stitch_logic,
    _helper_prepare_stitch_config,
    _helper_modify_stitch_config,
    _helper_launch_stitch_job,
)


def _display_config_preview(console, stitch_config, metadata):
    """Display a preview of the Stitch configuration to the user."""
    console.print(f"\n[{INFO_STYLE}]Stitch Configuration Preview:[/{INFO_STYLE}]")

    # Show basic info
    console.print(f"• Target: {metadata['target_catalog']}.{metadata['target_schema']}")
    console.print(f"• Job Name: {metadata['stitch_job_name']}")
    console.print(f"• Config Path: {metadata['config_file_path']}")

    # Show tables and fields
    table_count = len(stitch_config["tables"])
    console.print(f"• Tables to process: {table_count}")

    total_fields = sum(len(table["fields"]) for table in stitch_config["tables"])
    console.print(f"• Total PII fields: {total_fields}")

    if table_count > 0:
        console.print("\nTables:")
        for table in stitch_config["tables"]:
            field_count = len(table["fields"])
            console.print(f"  - {table['path']} ({field_count} fields)")

            # Show all fields
            for field in table["fields"]:
                semantics = ", ".join(field.get("semantics", []))
                if semantics:
                    console.print(f"    • {field['field-name']} ({semantics})")
                else:
                    console.print(f"    • {field['field-name']}")

    # Show unsupported columns if any
    unsupported = metadata.get("unsupported_columns", [])
    if unsupported:
        console.print(
            f"\n[{WARNING}]Note: {sum(len(t['columns']) for t in unsupported)} columns excluded due to unsupported types[/{WARNING}]"
        )


def _display_confirmation_prompt(console):
    """Display the confirmation prompt to the user."""
    console.print(f"\n[{INFO_STYLE}]What would you like to do?[/{INFO_STYLE}]")
    console.print("• Type 'launch' or 'yes' to launch the job")
    console.print(
        "• Describe changes (e.g., 'remove table X', 'add email semantic to field Y')"
    )
    console.print("• Type 'cancel' to abort the setup")


def handle_command(
    client: Optional[DatabricksAPIClient],
    interactive_input: Optional[str] = None,
    auto_confirm: bool = False,
    **kwargs,
) -> CommandResult:
    """
    Set up a Stitch integration with interactive configuration review.

    Args:
        client: API client instance
        interactive_input: User input for interactive mode
        auto_confirm: Skip confirmation and launch immediately
        **kwargs:
            catalog_name (str, optional): Target catalog name
            schema_name (str, optional): Target schema name
    """
    catalog_name_arg: Optional[str] = kwargs.get("catalog_name")
    schema_name_arg: Optional[str] = kwargs.get("schema_name")

    if not client:
        return CommandResult(False, message="Client is required for Stitch setup.")

    # Determine if legacy auto-confirm mode was explicitly requested
    explicit_auto_confirm = auto_confirm or kwargs.get("auto_confirm") is True
    if explicit_auto_confirm:
        return _handle_legacy_setup(client, catalog_name_arg, schema_name_arg)

    # Interactive mode - use context management
    context = InteractiveContext()
    console = get_console()

    try:
        # Phase determination
        if not interactive_input:  # First call - Phase 1: Prepare config
            return _phase_1_prepare_config(
                client, context, console, catalog_name_arg, schema_name_arg
            )

        # Get stored context data
        builder_data = context.get_context_data("setup-stitch")
        if not builder_data:
            return CommandResult(
                False,
                message="Stitch setup context lost. Please run /setup-stitch again.",
            )

        current_phase = builder_data.get("phase", "review")

        if current_phase == "review":
            return _phase_2_handle_review(client, context, console, interactive_input)
        elif current_phase == "ready_to_launch":
            return _phase_3_launch_job(client, context, console, interactive_input)
        else:
            return CommandResult(
                False,
                message=f"Unknown phase: {current_phase}. Please run /setup-stitch again.",
            )

    except Exception as e:
        # Clear context on error
        context.clear_active_context("setup-stitch")
        logging.error(f"Stitch setup error: {e}", exc_info=True)
        return CommandResult(
            False, error=e, message=f"Error setting up Stitch: {str(e)}"
        )


def _handle_legacy_setup(
    client: DatabricksAPIClient,
    catalog_name_arg: Optional[str],
    schema_name_arg: Optional[str],
) -> CommandResult:
    """Handle auto-confirm mode using the legacy direct setup approach."""
    try:
        target_catalog = catalog_name_arg or get_active_catalog()
        target_schema = schema_name_arg or get_active_schema()

        if not target_catalog or not target_schema:
            return CommandResult(
                False,
                message="Target catalog and schema must be specified or active for Stitch setup.",
            )

        # Create a LLM client instance to pass to the helper
        llm_client = LLMClient()

        # Get metrics collector
        metrics_collector = get_metrics_collector()

        # Get the prepared configuration (doesn't launch job anymore)
        prep_result = _helper_setup_stitch_logic(
            client, llm_client, target_catalog, target_schema
        )
        if prep_result.get("error"):
            # Track error event
            metrics_collector.track_event(
                prompt="setup-stitch command",
                tools=[
                    {
                        "name": "setup_stitch",
                        "arguments": {
                            "catalog": target_catalog,
                            "schema": target_schema,
                        },
                    }
                ],
                error=prep_result.get("error"),
                additional_data={
                    "event_context": "direct_stitch_command",
                    "status": "error",
                },
            )

            return CommandResult(False, message=prep_result["error"], data=prep_result)

        # Now we need to explicitly launch the job since _helper_setup_stitch_logic no longer does it
        stitch_result_data = _helper_launch_stitch_job(
            client, prep_result["stitch_config"], prep_result["metadata"]
        )
        if stitch_result_data.get("error"):
            # Track error event for launch failure
            metrics_collector.track_event(
                prompt="setup_stitch command",
                tools=[
                    {
                        "name": "setup_stitch",
                        "arguments": {
                            "catalog": target_catalog,
                            "schema": target_schema,
                        },
                    }
                ],
                error=stitch_result_data.get("error"),
                additional_data={
                    "event_context": "direct_stitch_command",
                    "status": "launch_error",
                },
            )

            return CommandResult(
                False, message=stitch_result_data["error"], data=stitch_result_data
            )

        # Track successful stitch setup event
        metrics_collector.track_event(
            prompt="setup-stitch command",
            tools=[
                {
                    "name": "setup_stitch",
                    "arguments": {"catalog": target_catalog, "schema": target_schema},
                }
            ],
            additional_data={
                "event_context": "direct_stitch_command",
                "status": "success",
                **{k: v for k, v in stitch_result_data.items() if k != "message"},
            },
        )

        # Show detailed summary first as progress info for legacy mode too
        console = get_console()
        _display_detailed_summary(console, stitch_result_data)

        # Create the user guidance as the main result message
        result_message = _build_post_launch_guidance_message(
            stitch_result_data, prep_result["metadata"], client
        )

        return CommandResult(
            True,
            data=stitch_result_data,
            message=result_message,
        )
    except Exception as e:
        logging.error(f"Legacy stitch setup error: {e}", exc_info=True)
        return CommandResult(
            False, error=e, message=f"Error setting up Stitch: {str(e)}"
        )


def _phase_1_prepare_config(
    client: DatabricksAPIClient,
    context: InteractiveContext,
    console,
    catalog_name_arg: Optional[str],
    schema_name_arg: Optional[str],
) -> CommandResult:
    """Phase 1: Prepare the Stitch configuration."""
    target_catalog = catalog_name_arg or get_active_catalog()
    target_schema = schema_name_arg or get_active_schema()

    if not target_catalog or not target_schema:
        return CommandResult(
            False,
            message="Target catalog and schema must be specified or active for Stitch setup.",
        )

    # Set context as active for interactive mode
    context.set_active_context("setup-stitch")

    console.print(
        f"\n[{INFO_STYLE}]Preparing Stitch configuration for {target_catalog}.{target_schema}...[/{INFO_STYLE}]"
    )

    # Create LLM client
    llm_client = LLMClient()

    # Prepare the configuration
    prep_result = _helper_prepare_stitch_config(
        client, llm_client, target_catalog, target_schema
    )

    if prep_result.get("error"):
        context.clear_active_context("setup-stitch")
        return CommandResult(False, message=prep_result["error"])

    # Store the prepared data in context (don't store llm_client object)
    context.store_context_data("setup-stitch", "phase", "review")
    context.store_context_data(
        "setup-stitch", "stitch_config", prep_result["stitch_config"]
    )
    context.store_context_data("setup-stitch", "metadata", prep_result["metadata"])
    # Note: We'll recreate LLMClient in each phase instead of storing it

    # Display the configuration preview
    _display_config_preview(
        console, prep_result["stitch_config"], prep_result["metadata"]
    )
    _display_confirmation_prompt(console)

    return CommandResult(
        True, message=""  # Empty message - let the console output speak for itself
    )


def _phase_2_handle_review(
    client: DatabricksAPIClient, context: InteractiveContext, console, user_input: str
) -> CommandResult:
    """Phase 2: Handle user review and potential config modifications."""
    builder_data = context.get_context_data("setup-stitch")
    stitch_config = builder_data["stitch_config"]
    metadata = builder_data["metadata"]
    llm_client = LLMClient()  # Recreate instead of getting from context

    user_input_lower = user_input.lower().strip()

    # Check for launch commands
    if user_input_lower in ["launch", "yes", "y", "launch it", "go", "proceed"]:
        # Move to launch phase
        context.store_context_data("setup-stitch", "phase", "ready_to_launch")

        console.print(
            "When you launch Stitch it will create a job in Databricks and a notebook that will show you Stitch results when the job completes."
        )
        console.print(
            "Stitch will create a schema called stitch_outputs with two new tables called unified_coalesced and unified_scores."
        )
        console.print(
            "The unified_coalesced table will contain the standardized PII and amperity_ids."
        )
        console.print(
            "The unified_scores table will contain the links and confidence scores."
        )
        console.print("Be sure to check out the results in the Stitch Report notebook!")
        console.print(
            f"\n[{WARNING}]Ready to launch Stitch job. Type 'confirm' to proceed or 'cancel' to abort.[/{WARNING}]"
        )
        return CommandResult(
            True, message="Ready to launch. Type 'confirm' to proceed with job launch."
        )

    # Check for cancel
    if user_input_lower in ["cancel", "abort", "stop", "exit", "quit", "no"]:
        context.clear_active_context("setup-stitch")
        console.print(f"\n[{INFO_STYLE}]Stitch setup cancelled.[/{INFO_STYLE}]")
        return CommandResult(True, message="Stitch setup cancelled.")

    # Otherwise, treat as modification request
    console.print(
        f"\n[{INFO_STYLE}]Modifying configuration based on your request...[/{INFO_STYLE}]"
    )

    modify_result = _helper_modify_stitch_config(
        stitch_config, user_input, llm_client, metadata
    )

    if modify_result.get("error"):
        console.print(
            f"\n[{ERROR_STYLE}]Error modifying configuration: {modify_result['error']}[/{ERROR_STYLE}]"
        )
        console.print(
            "Please try rephrasing your request or type 'launch' to proceed with current config."
        )
        return CommandResult(
            True,
            message="Please try rephrasing your request or type 'launch' to proceed.",
        )

    # Update stored config
    updated_config = modify_result["stitch_config"]
    context.store_context_data("setup-stitch", "stitch_config", updated_config)

    console.print(f"\n[{SUCCESS_STYLE}]Configuration updated![/{SUCCESS_STYLE}]")
    if modify_result.get("modification_summary"):
        console.print(modify_result["modification_summary"])

    # Show updated preview
    _display_config_preview(console, updated_config, metadata)
    _display_confirmation_prompt(console)

    return CommandResult(
        True,
        message="Please review the updated configuration and choose: 'launch', more changes, or 'cancel'.",
    )


def _phase_3_launch_job(
    client: DatabricksAPIClient, context: InteractiveContext, console, user_input: str
) -> CommandResult:
    """Phase 3: Final confirmation and job launch."""
    builder_data = context.get_context_data("setup-stitch")
    stitch_config = builder_data["stitch_config"]
    metadata = builder_data["metadata"]

    user_input_lower = user_input.lower().strip()

    if user_input_lower in [
        "confirm",
        "yes",
        "y",
        "launch",
        "proceed",
        "go",
        "make it so",
    ]:
        console.print(f"\n[{INFO_STYLE}]Launching Stitch job...[/{INFO_STYLE}]")

        # Launch the job
        launch_result = _helper_launch_stitch_job(client, stitch_config, metadata)

        # Clear context after launch (success or failure)
        context.clear_active_context("setup-stitch")

        if launch_result.get("error"):
            # Track error event
            metrics_collector = get_metrics_collector()
            metrics_collector.track_event(
                prompt="setup-stitch command",
                tools=[
                    {
                        "name": "setup_stitch",
                        "arguments": {
                            "catalog": metadata["target_catalog"],
                            "schema": metadata["target_schema"],
                        },
                    }
                ],
                error=launch_result.get("error"),
                additional_data={
                    "event_context": "interactive_stitch_command",
                    "status": "error",
                },
            )
            return CommandResult(
                False, message=launch_result["error"], data=launch_result
            )

        # Track successful launch
        metrics_collector = get_metrics_collector()
        metrics_collector.track_event(
            prompt="setup-stitch command",
            tools=[
                {
                    "name": "setup_stitch",
                    "arguments": {
                        "catalog": metadata["target_catalog"],
                        "schema": metadata["target_schema"],
                    },
                }
            ],
            additional_data={
                "event_context": "interactive_stitch_command",
                "status": "success",
                **{k: v for k, v in launch_result.items() if k != "message"},
            },
        )

        console.print(
            f"\n[{SUCCESS_STYLE}]Stitch job launched successfully![/{SUCCESS_STYLE}]"
        )

        # Show detailed summary first as progress info
        _display_detailed_summary(console, launch_result)

        # Create the user guidance as the main result message
        result_message = _build_post_launch_guidance_message(
            launch_result, metadata, client
        )

        return CommandResult(
            True,
            data=launch_result,
            message=result_message,
        )

    elif user_input_lower in ["cancel", "abort", "stop", "no"]:
        context.clear_active_context("setup-stitch")
        console.print(f"\n[{INFO_STYLE}]Stitch job launch cancelled.[/{INFO_STYLE}]")
        return CommandResult(True, message="Stitch job launch cancelled.")

    else:
        console.print(
            f"\n[{WARNING}]Please type 'confirm' to launch the job or 'cancel' to abort.[/{WARNING}]"
        )
        return CommandResult(
            True, message="Please type 'confirm' to launch or 'cancel' to abort."
        )


def _display_post_launch_options(console, launch_result, metadata, client=None):
    """Display post-launch options and guidance to the user."""
    from chuck_data.config import get_workspace_url
    from chuck_data.databricks.url_utils import (
        get_full_workspace_url,
        detect_cloud_provider,
    )

    console.print(
        f"\n[{INFO_STYLE}]Stitch is now running in your Databricks workspace![/{INFO_STYLE}]"
    )
    console.print(
        "Running Stitch creates a job that will take at least a few minutes to complete."
    )
    console.print(
        "A Stitch report showing the results has been created to help you see the results."
    )
    console.print(
        f"[{WARNING}]The report will not work until Stitch is complete.[/{WARNING}]"
    )

    # Extract key information from launch result
    run_id = launch_result.get("run_id")
    notebook_result = launch_result.get("notebook_result")

    console.print(f"\n[{INFO_STYLE}]Choose from the following options:[/{INFO_STYLE}]")

    # Option 1: Check job status
    if run_id:
        console.print(
            f"• Check the status of the job: [bold]/job-status --run_id {run_id}[/bold]"
        )

    # Get workspace URL for constructing browser links
    workspace_url = get_workspace_url()
    if workspace_url:
        from chuck_data.databricks.url_utils import normalize_workspace_url

        # If workspace_url is already a full URL, normalize it to get just the workspace ID
        # If it's just the workspace ID, this will return it as-is
        workspace_id = normalize_workspace_url(workspace_url)
        cloud_provider = detect_cloud_provider(workspace_url)
        full_workspace_url = get_full_workspace_url(workspace_id, cloud_provider)

        # Option 2: Open job in browser
        if run_id and client:
            try:
                job_run_status = client.get_job_run_status(run_id)
                job_id = job_run_status.get("job_id")
                if job_id:
                    # Use proper URL format: https://workspace.domain.com/jobs/<job-id>/runs/<run-id>?o=<workspace-id>
                    job_url = f"{full_workspace_url}/jobs/{job_id}/runs/{run_id}?o={workspace_id}"
                    console.print(
                        f"• Open Databricks job in browser: [link]{job_url}[/link]"
                    )
            except Exception as e:
                logging.warning(f"Could not get job details for run {run_id}: {e}")

        # Option 3: Open notebook in browser
        if notebook_result and notebook_result.get("success"):
            notebook_path = notebook_result.get("notebook_path", "")
            if notebook_path:
                from urllib.parse import quote

                # Remove leading /Workspace if present, and construct proper URL
                clean_path = notebook_path.replace("/Workspace", "")
                # URL encode the path, especially spaces
                encoded_path = quote(clean_path, safe="/")
                # Construct URL with workspace ID: https://workspace.domain.com/?o=workspace_id#workspace/path
                notebook_url = (
                    f"{full_workspace_url}/?o={workspace_id}#workspace{encoded_path}"
                )
                console.print(
                    f"• Open Stitch Report notebook in browser: [link]{notebook_url}[/link]"
                )

        # Option 4: Open main workspace
        console.print(f"• Open Databricks workspace: [link]{full_workspace_url}[/link]")
    else:
        # Fallback when workspace URL is not configured
        if run_id:
            console.print(
                f"• Check the status of the job: [bold]/job-status --run_id {run_id}[/bold]"
            )
        console.print(
            "• Open your Databricks workspace to view the running job and report"
        )

    # Option 5: Do nothing
    console.print("• Do nothing for now - you can check the job status later")

    # Additional information about outputs
    console.print(f"\n[{INFO_STYLE}]What Stitch will create:[/{INFO_STYLE}]")
    target_catalog = metadata.get("target_catalog", "your_catalog")
    console.print(f"• Schema: [bold]{target_catalog}.stitch_outputs[/bold]")
    console.print(
        f"• Table: [bold]{target_catalog}.stitch_outputs.unified_coalesced[/bold] (standardized PII and amperity_ids)"
    )
    console.print(
        f"• Table: [bold]{target_catalog}.stitch_outputs.unified_scores[/bold] (links and confidence scores)"
    )


def _display_detailed_summary(console, launch_result):
    """Display the detailed technical summary after user guidance."""
    # Extract the original detailed message that was meant to be shown last
    detailed_message = launch_result.get("message", "")
    if detailed_message:
        console.print(f"\n[{INFO_STYLE}]Technical Summary:[/{INFO_STYLE}]")
        console.print(detailed_message)


def _build_post_launch_guidance_message(launch_result, metadata, client=None):
    """Build the post-launch guidance message as a string to return as CommandResult message."""
    from chuck_data.config import get_workspace_url
    from chuck_data.databricks.url_utils import (
        get_full_workspace_url,
        detect_cloud_provider,
        normalize_workspace_url,
    )

    lines = []
    lines.append("Stitch is now running in your Databricks workspace!")
    # Additional information about outputs
    lines.append("")
    lines.append(
        "Running Stitch creates a job that will take at least a few minutes to complete."
    )
    lines.append("")
    lines.append("What Stitch will create:")
    target_catalog = metadata.get("target_catalog", "your_catalog")
    lines.append(f"• Schema: {target_catalog}.stitch_outputs")
    lines.append(
        f"• Table: {target_catalog}.stitch_outputs.unified_coalesced (standardized PII and amperity_ids)"
    )
    lines.append(
        f"• Table: {target_catalog}.stitch_outputs.unified_scores (links and confidence scores)"
    )
    lines.append("")
    lines.append(
        "A Stitch report showing the results has been created to help you see the results."
    )
    lines.append("The report will not work until Stitch is complete.")

    # Extract key information from launch result
    run_id = launch_result.get("run_id")
    notebook_result = launch_result.get("notebook_result")

    lines.append("")
    lines.append("")
    lines.append("What you can do now:")

    # Option 1: Check job status
    if run_id:
        lines.append(f"• you can ask me about the status of the job (id {run_id})")

    # Get workspace URL for constructing browser links
    workspace_url = get_workspace_url()
    # If workspace_url is already a full URL, normalize it to get just the workspace ID
    # If it's just the workspace ID, this will return it as-is
    workspace_id = normalize_workspace_url(workspace_url)
    cloud_provider = detect_cloud_provider(workspace_url)
    full_workspace_url = get_full_workspace_url(workspace_id, cloud_provider)

    # Option 2: Open job in browser
    if run_id and client:
        try:
            job_run_status = client.get_job_run_status(run_id)
            job_id = job_run_status.get("job_id")
            if job_id:
                # Use proper URL format: https://workspace.domain.com/jobs/<job-id>/runs/<run-id>?o=<workspace-id>
                job_url = (
                    f"{full_workspace_url}/jobs/{job_id}/runs/{run_id}?o={workspace_id}"
                )
                lines.append(f"• Open Databricks job in browser: {job_url}")
        except Exception as e:
            logging.warning(f"Could not get job details for run {run_id}: {e}")

    # Option 3: Open notebook in browser
    if notebook_result and notebook_result.get("success"):
        notebook_path = notebook_result.get("notebook_path", "")
        if notebook_path:
            from urllib.parse import quote

            # Remove leading /Workspace if present, and construct proper URL
            clean_path = notebook_path.replace("/Workspace", "")
            # URL encode the path, especially spaces
            encoded_path = quote(clean_path, safe="/")
            # Construct URL with workspace ID: https://workspace.domain.com/?o=workspace_id#workspace/path
            notebook_url = (
                f"{full_workspace_url}/?o={workspace_id}#workspace{encoded_path}"
            )
            lines.append(f"• Open Stitch Report notebook in browser: {notebook_url}")

    # Option 4: Open main workspace
    lines.append(f"• Open Databricks workspace: {full_workspace_url}")

    return "\n".join(lines)


DEFINITION = CommandDefinition(
    name="setup-stitch",
    description="Interactively set up a Stitch integration with configuration review and modification",
    handler=handle_command,
    parameters={
        "catalog_name": {
            "type": "string",
            "description": "Optional: Name of the catalog. If not provided, uses the active catalog",
        },
        "schema_name": {
            "type": "string",
            "description": "Optional: Name of the schema. If not provided, uses the active schema",
        },
        "auto_confirm": {
            "type": "boolean",
            "description": "Optional: Skip interactive confirmation and launch job immediately (default: false)",
        },
    },
    required_params=[],
    tui_aliases=["/setup-stitch"],
    visible_to_user=True,
    visible_to_agent=True,
    supports_interactive_input=True,
    usage_hint="Example: /setup-stitch or /setup-stitch --auto-confirm to skip confirmation",
    condensed_action="Setting up Stitch integration",
)
