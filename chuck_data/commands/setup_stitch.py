"""
Command handler for Stitch integration setup.

This module contains the handler for setting up a Stitch integration by scanning
for PII columns and creating a configuration file.

Supports both Databricks Unity Catalog and AWS Redshift data sources.
"""

import logging
import os
import json
from datetime import datetime
from typing import Optional, List, Any, Dict, Union

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.clients.redshift import RedshiftAPIClient
from chuck_data.data_providers import is_redshift_client
from chuck_data.llm.factory import LLMProviderFactory
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import (
    get_active_catalog,
    get_active_schema,
    get_active_database,
    get_redshift_iam_role,
    get_redshift_s3_temp_dir,
    get_s3_bucket,
    get_redshift_region,
    get_amperity_token,
)
from chuck_data.metrics_collector import get_metrics_collector
from chuck_data.interactive_context import InteractiveContext
from chuck_data.ui.theme import SUCCESS_STYLE, ERROR_STYLE, INFO_STYLE, WARNING
from chuck_data.ui.tui import get_console
from chuck_data.storage.manifest import (
    save_manifest_to_file,
    upload_manifest_to_s3,
    validate_manifest,
)
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

    # Show target locations (single or multiple)
    target_locations = metadata.get("target_locations")
    if target_locations:
        console.print(f"• Scanned locations: {len(target_locations)}")
        for loc in target_locations:
            console.print(f"  - {loc['catalog']}.{loc['schema']}")
    else:
        # Backward compatible - single target
        console.print(
            f"• Target: {metadata['target_catalog']}.{metadata['target_schema']}"
        )

    console.print(
        f"• Output: {metadata.get('output_catalog', metadata.get('target_catalog'))}.stitch_outputs"
    )
    console.print(f"• Job Name: {metadata['stitch_job_name']}")
    console.print(f"• Config Path: {metadata['config_file_path']}")

    # Show scan summary if available
    scan_summary = metadata.get("scan_summary")
    if scan_summary:
        console.print("\nScan Results:")
        for summary in scan_summary:
            if summary["status"] == "success":
                console.print(
                    f"  ✓ {summary['location']} ({summary['tables']} tables, {summary['columns']} PII columns)"
                )
            else:
                console.print(
                    f"  ⚠ {summary['location']} (error: {summary.get('error', 'unknown')})"
                )

    # Show tables and fields
    table_count = len(stitch_config["tables"])
    console.print(f"\n• Tables to process: {table_count}")

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


def _submit_stitch_job_to_databricks(
    console,
    config_path: str,
    init_script_path: str,
    stitch_job_name: str,
    job_id: Optional[str] = None,
    policy_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Submit a Stitch job to Databricks (unified for both Unity Catalog and Redshift).

    Args:
        console: Console for output
        config_path: Path to config/manifest (S3 or /Volumes)
        init_script_path: Path to init script (S3 or /Volumes)
        stitch_job_name: Name for the Stitch job
        job_id: Optional Amperity job ID for tracking
        policy_id: Optional cluster policy ID

    Returns:
        Dict with 'success': bool, 'run_id': str, 'error': str
    """
    try:
        # Create Databricks client for job submission
        from chuck_data.config import get_workspace_url, get_databricks_token

        workspace_url = get_workspace_url()
        databricks_token = get_databricks_token()

        if not workspace_url or not databricks_token:
            return {
                "success": False,
                "error": "Databricks workspace not configured. Please run setup wizard.",
            }

        databricks_client = DatabricksAPIClient(
            workspace_url=workspace_url, token=databricks_token
        )

        # Submit job using Databricks client
        job_run_data = databricks_client.submit_job_run(
            config_path=config_path,
            init_script_path=init_script_path,
            run_name=f"Stitch Setup: {stitch_job_name}",
            policy_id=policy_id,
        )
        run_id = job_run_data.get("run_id")
        if not run_id:
            return {
                "success": False,
                "error": "Failed to launch job (no run_id returned)",
            }

        console.print(
            f"[{SUCCESS_STYLE}]✓ Stitch job submitted! Run ID: {run_id}[/{SUCCESS_STYLE}]"
        )

        # Record job submission to link job-id with databricks-run-id
        amperity_token = get_amperity_token()
        if amperity_token and job_id:
            try:
                from chuck_data.clients.amperity import AmperityAPIClient

                amperity_client = AmperityAPIClient()
                amperity_client.record_job_submission(
                    databricks_run_id=str(run_id),
                    token=amperity_token,
                    job_id=str(job_id),
                )
                logging.debug(f"Recorded job submission: job-id -> run_id {run_id}")

                # Cache the job ID and run ID for quick status lookups
                from chuck_data.job_cache import cache_job

                cache_job(job_id=str(job_id), run_id=str(run_id))
                logging.debug(f"Cached job ID: {job_id}, run ID: {run_id}")
            except Exception as e:
                logging.warning(f"Failed to record job submission: {e}")

        return {
            "success": True,
            "run_id": run_id,
            "databricks_client": databricks_client,
        }

    except Exception as e:
        logging.error(f"Error submitting Stitch job: {e}", exc_info=True)
        return {"success": False, "error": f"Failed to submit job: {str(e)}"}


def _create_stitch_report_notebook_unified(
    console,
    databricks_client: DatabricksAPIClient,
    manifest_or_config: Dict[str, Any],
    target_catalog: str,
    target_schema: str,
    stitch_job_name: str,
) -> Dict[str, Any]:
    """Create Stitch Report notebook (unified for both Unity Catalog and Redshift).

    Args:
        console: Console for output
        databricks_client: Databricks client instance
        manifest_or_config: Manifest or config dictionary
        target_catalog: Target catalog/database name
        target_schema: Target schema name
        stitch_job_name: Name of the Stitch job

    Returns:
        Dict with 'success': bool, 'notebook_path': str, 'error': str
    """
    console.print("\nCreating Stitch Report notebook...")

    from .stitch_tools import _create_stitch_report_notebook

    notebook_result = _create_stitch_report_notebook(
        databricks_client,
        manifest_or_config,
        target_catalog,
        target_schema,
        stitch_job_name,
    )

    if notebook_result.get("success"):
        console.print(
            f"[{SUCCESS_STYLE}]✓ Created Stitch Report notebook at {notebook_result.get('notebook_path')}[/{SUCCESS_STYLE}]"
        )
    else:
        console.print(
            f"[{WARNING}]⚠ Could not create Stitch Report notebook: {notebook_result.get('error', 'Unknown error')}[/{WARNING}]"
        )

    return notebook_result


def handle_command(
    client: Optional[Union[RedshiftAPIClient, DatabricksAPIClient]],
    interactive_input: Optional[str] = None,
    auto_confirm: bool = False,
    policy_id: Optional[str] = None,
    **kwargs,
) -> CommandResult:
    """
    Set up a Stitch integration with interactive configuration review.

    Automatically detects data provider (Databricks Unity Catalog or AWS Redshift)
    and routes to appropriate handler.

    Args:
        client: API client instance (DatabricksAPIClient or RedshiftAPIClient)
        interactive_input: User input for interactive mode
        auto_confirm: Skip interactive confirmation
        policy_id: Cluster policy ID for Databricks jobs
        **kwargs:
            catalog_name (str, optional): Single target catalog (Databricks)
            schema_name (str, optional): Single target schema
            database (str, optional): Database name (Redshift)
            targets (List[str], optional): Multiple targets ["cat.schema", ...]
            output_catalog (str, optional): Output catalog for multi-target
    """
    if not client:
        return CommandResult(False, message="Client is required for Stitch setup.")

    # Detect data provider
    is_redshift = is_redshift_client(client)

    # Create compute provider (currently only Databricks is supported)
    from chuck_data.provider_factory import ProviderFactory
    from chuck_data.config import get_workspace_url, get_databricks_token

    workspace_url = get_workspace_url()
    databricks_token = get_databricks_token()

    if not workspace_url or not databricks_token:
        return CommandResult(False, message="Databricks workspace not configured")

    # Determine data provider type for storage provider selection
    data_provider_type = "redshift" if is_redshift else "databricks"

    compute_provider = ProviderFactory.create_compute_provider(
        "databricks",
        {
            "workspace_url": workspace_url,
            "token": databricks_token,
            "data_provider_type": data_provider_type,
        },
    )

    # Route to appropriate handler
    if is_redshift:
        return _handle_redshift_stitch_setup(
            client, compute_provider, interactive_input, auto_confirm, **kwargs
        )
    else:
        # Databricks Unity Catalog path
        return _handle_databricks_stitch_setup(
            client,
            compute_provider,
            interactive_input,
            auto_confirm,
            policy_id,
            **kwargs,
        )


def _handle_databricks_stitch_setup(
    client: DatabricksAPIClient,
    compute_provider,  # ComputeProvider instance
    interactive_input: Optional[str],
    auto_confirm: bool,
    policy_id: Optional[str],
    **kwargs,
) -> CommandResult:
    """Handle Stitch setup for Databricks Unity Catalog."""
    catalog_name_arg: Optional[str] = kwargs.get("catalog_name")
    schema_name_arg: Optional[str] = kwargs.get("schema_name")
    targets_arg: Optional[List[str]] = kwargs.get("targets")
    output_catalog_arg: Optional[str] = kwargs.get("output_catalog")

    # Handle auto-confirm mode
    if auto_confirm:
        return _handle_legacy_setup(
            client, catalog_name_arg, schema_name_arg, policy_id
        )

    # Interactive mode - use context management
    context = InteractiveContext()
    console = get_console()

    try:
        # Phase determination
        if not interactive_input:  # First call - Phase 1: Prepare config
            return _phase_1_prepare_config(
                client,
                context,
                console,
                catalog_name_arg,
                schema_name_arg,
                targets_arg,
                output_catalog_arg,
                policy_id,
            )

        # Get stored context data
        builder_data = context.get_context_data("setup_stitch")
        if not builder_data:
            return CommandResult(
                False,
                message="Stitch setup context lost. Please run /setup-stitch again.",
            )

        current_phase = builder_data.get("phase", "review")

        if current_phase == "review":
            return _phase_2_handle_review(client, context, console, interactive_input)
        if current_phase == "ready_to_launch":
            return _phase_3_launch_job(client, context, console, interactive_input)
        return CommandResult(
            False,
            message=f"Unknown phase: {current_phase}. Please run /setup-stitch again.",
        )

    except Exception as e:
        # Clear context on error
        context.clear_active_context("setup_stitch")
        logging.error(f"Stitch setup error: {e}", exc_info=True)
        return CommandResult(
            False, error=e, message=f"Error setting up Stitch: {str(e)}"
        )


def _handle_legacy_setup(
    client: DatabricksAPIClient,
    catalog_name_arg: Optional[str],
    schema_name_arg: Optional[str],
    policy_id: Optional[str] = None,
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

        # Create a LLM provider instance using factory to pass to the helper
        llm_client = LLMProviderFactory.create()

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

        # Add policy_id to metadata if provided
        if policy_id:
            prep_result["metadata"]["policy_id"] = policy_id

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
    targets_arg: Optional[List[str]] = None,
    output_catalog_arg: Optional[str] = None,
    policy_id: Optional[str] = None,
) -> CommandResult:
    """Phase 1: Prepare the Stitch configuration for single or multiple targets."""

    # Set context as active for interactive mode
    context.set_active_context("setup_stitch")

    # Create LLM provider using factory
    llm_client = LLMProviderFactory.create()

    # Multi-target mode
    if targets_arg:
        target_locations = []
        for target in targets_arg:
            parts = target.split(".")
            if len(parts) != 2:
                context.clear_active_context("setup_stitch")
                return CommandResult(
                    False,
                    message=f"Invalid target format: '{target}'. Expected 'catalog.schema'",
                )
            target_locations.append({"catalog": parts[0], "schema": parts[1]})

        output_catalog = output_catalog_arg or target_locations[0]["catalog"]

        console.print(
            f"\n[{INFO_STYLE}]Preparing Stitch configuration for {len(target_locations)} locations...[/{INFO_STYLE}]"
        )
        for loc in target_locations:
            console.print(f"  • {loc['catalog']}.{loc['schema']}")

        prep_result = _helper_prepare_stitch_config(
            client,
            llm_client,
            target_locations=target_locations,
            output_catalog=output_catalog,
        )
    else:
        # Single target mode (backward compatible)
        target_catalog = catalog_name_arg or get_active_catalog()
        target_schema = schema_name_arg or get_active_schema()

        if not target_catalog or not target_schema:
            context.clear_active_context("setup_stitch")
            return CommandResult(
                False,
                message="Target catalog and schema must be specified or active for Stitch setup.",
            )

        console.print(
            f"\n[{INFO_STYLE}]Preparing Stitch configuration for {target_catalog}.{target_schema}...[/{INFO_STYLE}]"
        )

        prep_result = _helper_prepare_stitch_config(
            client, llm_client, target_catalog, target_schema
        )

    if prep_result.get("error"):
        context.clear_active_context("setup_stitch")
        return CommandResult(False, message=prep_result["error"])

    # Add policy_id to metadata if provided
    if policy_id:
        prep_result["metadata"]["policy_id"] = policy_id

    # Store the prepared data in context (don't store llm_client object)
    context.store_context_data("setup_stitch", "phase", "review")
    context.store_context_data(
        "setup_stitch", "stitch_config", prep_result["stitch_config"]
    )
    context.store_context_data("setup_stitch", "metadata", prep_result["metadata"])
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
    builder_data = context.get_context_data("setup_stitch")
    stitch_config = builder_data["stitch_config"]
    metadata = builder_data["metadata"]
    llm_client = LLMProviderFactory.create()  # Create provider using factory

    user_input_lower = user_input.lower().strip()

    # Check for launch commands
    if user_input_lower in ["launch", "yes", "y", "launch it", "go", "proceed"]:
        # Move to launch phase
        context.store_context_data("setup_stitch", "phase", "ready_to_launch")

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
        context.clear_active_context("setup_stitch")
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
    context.store_context_data("setup_stitch", "stitch_config", updated_config)

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
    builder_data = context.get_context_data("setup_stitch")
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
        context.clear_active_context("setup_stitch")

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

    if user_input_lower in ["cancel", "abort", "stop", "no"]:
        context.clear_active_context("setup_stitch")
        console.print(f"\n[{INFO_STYLE}]Stitch job launch cancelled.[/{INFO_STYLE}]")
        return CommandResult(True, message="Stitch job launch cancelled.")

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


def _build_post_launch_guidance_message(
    launch_result, metadata, client=None, is_redshift=False
):
    """Build the post-launch guidance message as a string to return as CommandResult message.

    Args:
        launch_result: Dict with run_id and optional notebook_result
        metadata: Dict with target_catalog, target_schema, job_id
        client: Optional Databricks client for fetching job details
        is_redshift: Boolean indicating if this is a Redshift job (no notebook created)
    """
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

    # Only mention notebook for non-Redshift jobs
    if not is_redshift:
        lines.append("")
        lines.append(
            "A Stitch report showing the results has been created to help you see the results."
        )
        lines.append("The report will not work until Stitch is complete.")

    # Extract key information from launch result
    run_id = launch_result.get("run_id")
    job_id = metadata.get("job_id")
    notebook_result = launch_result.get("notebook_result")

    lines.append("")
    lines.append("")
    lines.append("What you can do now:")

    # Option 1: Check job status
    if job_id:
        lines.append(
            f"• you can ask me about the status of the Chuck job (job-id: {job_id})"
        )
    if run_id:
        lines.append(
            f"• you can ask me about the status of the Databricks job run (run-id: {run_id})"
        )

    # Get workspace URL for constructing browser links
    workspace_url = get_workspace_url() or ""
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


def _redshift_phase_1_prepare(
    client: RedshiftAPIClient,
    compute_provider,  # ComputeProvider instance
    context: InteractiveContext,
    console,
    **kwargs,
) -> CommandResult:
    """Phase 1: Prepare manifest, upload to S3, and show preview."""
    # Set context as active for interactive mode
    context.set_active_context("setup_stitch")

    # Get database and schema
    database = kwargs.get("database") or get_active_database()
    schema_name = kwargs.get("schema_name") or get_active_schema()

    if not database or not schema_name:
        context.clear_active_context("setup_stitch")
        return CommandResult(
            False,
            message="Database and schema must be specified or active. Use /select-database and /select-schema commands.",
        )

    console.print(
        f"\n[{INFO_STYLE}]Preparing Stitch configuration for Redshift: {database}.{schema_name}...[/{INFO_STYLE}]"
    )

    # Step 1: Read semantic tags from chuck_metadata.semantic_tags table
    console.print("\nStep 1: Reading semantic tags from metadata table...")
    tags_result = client.read_semantic_tags(database, schema_name)

    if not tags_result["success"]:
        context.clear_active_context("setup_stitch")
        error_msg = tags_result.get("error", "Unknown error reading semantic tags")
        return CommandResult(False, message=error_msg)

    semantic_tags = tags_result["tags"]
    if not semantic_tags:
        context.clear_active_context("setup_stitch")
        return CommandResult(
            False,
            message=f"No semantic tags found for {database}.{schema_name}. Please run /tag-pii first.",
        )

    console.print(
        f"[{SUCCESS_STYLE}]✓ Found {len(semantic_tags)} tagged columns across {len(set(t['table'] for t in semantic_tags))} tables[/{SUCCESS_STYLE}]"
    )

    # Step 2: Read table schemas
    console.print("\nStep 2: Reading table schemas...")
    schema_result = client.read_table_schemas(database, schema_name, semantic_tags)

    if not schema_result["success"]:
        context.clear_active_context("setup_stitch")
        return CommandResult(False, message=schema_result["error"])

    tables = schema_result["tables"]
    console.print(
        f"[{SUCCESS_STYLE}]✓ Read schemas for {len(tables)} tables[/{SUCCESS_STYLE}]"
    )

    # Step 3: Generate manifest
    console.print("\nStep 3: Generating manifest...")
    manifest_result = _generate_redshift_manifest(
        database, schema_name, tables, semantic_tags, client
    )

    if not manifest_result["success"]:
        context.clear_active_context("setup_stitch")
        return CommandResult(False, message=manifest_result["error"])

    manifest = manifest_result["manifest"]

    # Validate manifest
    is_valid, error = validate_manifest(manifest)
    if not is_valid:
        context.clear_active_context("setup_stitch")
        return CommandResult(False, message=f"Generated manifest is invalid: {error}")

    console.print(
        f"[{SUCCESS_STYLE}]✓ Manifest generated successfully[/{SUCCESS_STYLE}]"
    )

    # Step 4: Save manifest locally
    console.print("\nStep 4: Saving manifest locally...")
    manifest_dir = os.path.expanduser("~/.chuck/manifests")
    os.makedirs(manifest_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    manifest_filename = f"redshift_{database}_{schema_name}_{timestamp}.json"
    manifest_path = os.path.join(manifest_dir, manifest_filename)

    if not save_manifest_to_file(manifest, manifest_path):
        context.clear_active_context("setup_stitch")
        return CommandResult(
            False, message=f"Failed to save manifest to {manifest_path}"
        )

    console.print(
        f"[{SUCCESS_STYLE}]✓ Saved manifest to {manifest_path}[/{SUCCESS_STYLE}]"
    )

    # Step 5: Upload manifest to S3
    console.print("\nStep 5: Uploading manifest to S3...")
    s3_bucket = get_s3_bucket()
    if not s3_bucket:
        context.clear_active_context("setup_stitch")
        return CommandResult(
            False,
            message="No S3 bucket configured. Please run setup wizard to configure S3 bucket.",
        )

    s3_path = f"s3://{s3_bucket}/chuck/manifests/{manifest_filename}"
    aws_profile = kwargs.get("aws_profile")

    if not upload_manifest_to_s3(manifest, s3_path, aws_profile):
        context.clear_active_context("setup_stitch")
        return CommandResult(False, message=f"Failed to upload manifest to {s3_path}")

    console.print(
        f"[{SUCCESS_STYLE}]✓ Uploaded manifest to {s3_path}[/{SUCCESS_STYLE}]"
    )

    # Store all data in context for next phase
    context.store_context_data("setup_stitch", "phase", "ready_to_launch")
    context.store_context_data("setup_stitch", "database", database)
    context.store_context_data("setup_stitch", "schema_name", schema_name)
    context.store_context_data("setup_stitch", "manifest", manifest)
    context.store_context_data("setup_stitch", "manifest_path", manifest_path)
    context.store_context_data("setup_stitch", "manifest_filename", manifest_filename)
    context.store_context_data("setup_stitch", "s3_path", s3_path)
    context.store_context_data("setup_stitch", "s3_bucket", s3_bucket)
    context.store_context_data("setup_stitch", "timestamp", timestamp)
    context.store_context_data("setup_stitch", "tables", tables)
    context.store_context_data("setup_stitch", "semantic_tags", semantic_tags)
    context.store_context_data("setup_stitch", "aws_profile", aws_profile)

    # Display confirmation prompt
    console.print(
        "\nWhen you launch Stitch it will create a job in Databricks that will process your Redshift data."
    )
    console.print(
        "Stitch will create a schema called stitch_outputs with two new tables: unified_coalesced and unified_scores."
    )
    console.print(
        f"\n[{WARNING}]Ready to launch Stitch job. Type 'confirm' to proceed or 'cancel' to abort.[/{WARNING}]"
    )

    return CommandResult(
        True, message="Ready to launch. Type 'confirm' to proceed with job launch."
    )


def _redshift_phase_2_confirm(
    client: RedshiftAPIClient,
    compute_provider,  # ComputeProvider instance
    context: InteractiveContext,
    console,
    user_input: str,
    **kwargs,
) -> CommandResult:
    """Phase 2: Handle user confirmation and launch job."""
    builder_data = context.get_context_data("setup_stitch")
    if not builder_data:
        return CommandResult(
            False,
            message="Stitch setup context lost. Please run /setup-stitch again.",
        )

    user_input_lower = user_input.lower().strip()

    # Check for cancel
    if user_input_lower in ["cancel", "abort", "stop", "exit", "quit", "no"]:
        context.clear_active_context("setup_stitch")
        console.print(f"\n[{INFO_STYLE}]Stitch setup cancelled.[/{INFO_STYLE}]")
        return CommandResult(True, message="Stitch setup cancelled.")

    # Check for confirm
    if user_input_lower not in [
        "confirm",
        "yes",
        "y",
        "launch",
        "proceed",
        "go",
        "make it so",
    ]:
        console.print(
            f"\n[{WARNING}]Please type 'confirm' to launch the job or 'cancel' to abort.[/{WARNING}]"
        )
        return CommandResult(
            True, message="Please type 'confirm' to launch or 'cancel' to abort."
        )

    # User confirmed - proceed with launch
    console.print(f"\n[{INFO_STYLE}]Launching Stitch job...[/{INFO_STYLE}]")

    # Retrieve stored data
    database = builder_data["database"]
    schema_name = builder_data["schema_name"]
    manifest_path = builder_data["manifest_path"]
    manifest_filename = builder_data["manifest_filename"]
    s3_path = builder_data["s3_path"]
    s3_bucket = builder_data["s3_bucket"]
    timestamp = builder_data["timestamp"]
    tables = builder_data["tables"]
    semantic_tags = builder_data["semantic_tags"]
    aws_profile = builder_data.get("aws_profile")

    try:
        # Step 6: Fetch and upload init script to S3
        console.print("\nStep 6: Fetching and uploading init script to S3...")

        # Fetch init script from Amperity
        amperity_token = get_amperity_token()
        if not amperity_token:
            context.clear_active_context("setup_stitch")
            return CommandResult(
                False, message="Amperity token not found. Please run /amp_login first."
            )

        try:
            from chuck_data.clients.amperity import AmperityAPIClient

            amperity_client = AmperityAPIClient()
            init_script_data = amperity_client.fetch_amperity_job_init(amperity_token)
            init_script_content = init_script_data.get("cluster-init")
            job_id = init_script_data.get("job-id")

            if not init_script_content:
                context.clear_active_context("setup_stitch")
                return CommandResult(
                    False,
                    message="Failed to get cluster init script from Amperity API.",
                )
            if not job_id:
                context.clear_active_context("setup_stitch")
                return CommandResult(
                    False, message="Failed to get job-id from Amperity API."
                )
        except Exception as e:
            context.clear_active_context("setup_stitch")
            return CommandResult(
                False, message=f"Error fetching Amperity init script: {str(e)}"
            )

        # Upload init script using storage provider (S3 for Redshift)
        init_script_filename = f"chuck-init-{timestamp}.sh"
        init_script_s3_path = (
            f"s3://{s3_bucket}/chuck/init-scripts/{init_script_filename}"
        )

        # Upload to S3
        try:
            import boto3

            s3_client = boto3.client("s3", region_name=get_redshift_region())
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=f"chuck/init-scripts/{init_script_filename}",
                Body=init_script_content.encode("utf-8"),
            )
            init_script_path = init_script_s3_path
            logging.debug(f"Init script uploaded to {init_script_path}")
        except Exception as e:
            context.clear_active_context("setup_stitch")
            return CommandResult(
                False, message=f"Failed to upload init script to S3: {str(e)}"
            )
        console.print(
            f"[{SUCCESS_STYLE}]✓ Uploaded init script to {init_script_path}[/{SUCCESS_STYLE}]"
        )

        # Step 7: Submit Stitch job to Databricks
        console.print("\nStep 7: Submitting Stitch job to Databricks...")

        stitch_job_name = f"stitch-redshift-{database}-{schema_name}"
        job_result = _submit_stitch_job_to_databricks(
            console,
            config_path=s3_path,
            init_script_path=init_script_path,
            stitch_job_name=stitch_job_name,
            job_id=job_id,
            policy_id=kwargs.get("policy_id"),
        )

        if not job_result["success"]:
            context.clear_active_context("setup_stitch")
            console.print(f"[{ERROR_STYLE}]✗ {job_result['error']}[/{ERROR_STYLE}]")
            return CommandResult(False, message=job_result["error"])

        run_id = job_result["run_id"]
        databricks_client = job_result["databricks_client"]

        # Clear context after successful launch
        context.clear_active_context("setup_stitch")

        # Step 8: Create Stitch Report notebook
        manifest = builder_data.get("manifest", {})
        notebook_result = _create_stitch_report_notebook_unified(
            console,
            databricks_client,
            manifest,
            database,  # target_catalog (database for Redshift)
            schema_name,  # target_schema
            stitch_job_name,
        )

        console.print(
            f"\n[{SUCCESS_STYLE}]Stitch job launched successfully![/{SUCCESS_STYLE}]"
        )

        # Build metadata for post-launch display
        metadata = {
            "target_catalog": database,  # Use database as catalog equivalent
            "target_schema": schema_name,
            "job_id": job_id,
        }

        # Build launch result data
        launch_result = {
            "run_id": run_id,
            "notebook_result": notebook_result,  # Add notebook result
            "message": f"Stitch job for Redshift {database}.{schema_name} launched successfully",
        }

        # Show detailed summary first as progress info
        _display_detailed_summary(console, launch_result)

        # Create the user guidance as the main result message
        result_message = _build_post_launch_guidance_message(
            launch_result, metadata, databricks_client, is_redshift=False
        )

        # Prepare return data
        result_data = {
            "database": database,
            "schema": schema_name,
            "manifest_path": manifest_path,
            "s3_path": s3_path,
            "tables": len(tables),
            "tagged_columns": len(semantic_tags),
            "run_id": str(run_id),
            "init_script_path": init_script_path,
        }

        return CommandResult(
            True,
            message=result_message,
            data=result_data,
        )

    except Exception as e:
        context.clear_active_context("setup_stitch")
        logging.error(f"Error launching Redshift Stitch job: {e}", exc_info=True)
        return CommandResult(
            False, error=e, message=f"Error launching Stitch job: {str(e)}"
        )


def _handle_redshift_stitch_setup(
    client: RedshiftAPIClient,
    compute_provider,  # ComputeProvider instance
    interactive_input: Optional[str],
    auto_confirm: bool,
    **kwargs,
) -> CommandResult:
    """
    Handle Stitch setup for AWS Redshift data source.

    Reads semantic tags from chuck_metadata.semantic_tags table,
    generates manifest JSON, uploads to S3.

    Supports interactive mode with review and confirmation, similar to Databricks flow.
    """
    console = get_console()
    context = InteractiveContext()

    # Force interactive mode for Redshift - always require explicit confirmation
    # Agent calls should go through Phase 1 -> Phase 2 flow
    if auto_confirm and not kwargs.get("force_auto_confirm"):
        logging.info(
            "Redshift setup: auto_confirm=True but forcing interactive mode (use force_auto_confirm=True to override)"
        )
        auto_confirm = False

    try:
        # Phase determination
        logging.info(
            f"Redshift setup - interactive_input: {interactive_input}, auto_confirm: {auto_confirm}"
        )

        if not interactive_input and not auto_confirm:
            # First call - Phase 1: Prepare manifest and show preview
            logging.info("Taking Phase 1 path: prepare manifest")
            return _redshift_phase_1_prepare(
                client, compute_provider, context, console, **kwargs
            )
        elif interactive_input and not auto_confirm:
            # Handle user input in interactive mode - Phase 2: Handle confirmation
            logging.info("Taking Phase 2 path: handle confirmation")
            return _redshift_phase_2_confirm(
                client, compute_provider, context, console, interactive_input, **kwargs
            )
        elif auto_confirm:
            logging.info("Taking auto-confirm path: execute all steps immediately")
            # Auto-confirm mode - execute directly (legacy path)
            # Get database and schema
            database = kwargs.get("database") or get_active_database()
            schema_name = kwargs.get("schema_name") or get_active_schema()

            if not database or not schema_name:
                return CommandResult(
                    False,
                    message="Database and schema must be specified or active. Use /select-database and /select-schema commands.",
                )

            console.print(
                f"[{INFO_STYLE}]Setting up Stitch for Redshift: {database}.{schema_name}[/{INFO_STYLE}]"
            )

            # Step 1: Read semantic tags from chuck_metadata.semantic_tags table
            console.print("\nStep 1: Reading semantic tags from metadata table...")
            tags_result = client.read_semantic_tags(database, schema_name)

            if not tags_result["success"]:
                return CommandResult(False, message=tags_result["error"])

            semantic_tags = tags_result["tags"]
            if not semantic_tags:
                return CommandResult(
                    False,
                    message=f"No semantic tags found for {database}.{schema_name}. Please run /tag-pii first.",
                )

            console.print(
                f"[{SUCCESS_STYLE}]✓ Found {len(semantic_tags)} tagged columns across {len(set(t['table'] for t in semantic_tags))} tables[/{SUCCESS_STYLE}]"
            )

            # Step 2: Read table schemas to get all columns (not just PII)
            console.print("\nStep 2: Reading table schemas...")
            schema_result = client.read_table_schemas(
                database, schema_name, semantic_tags
            )

            if not schema_result["success"]:
                return CommandResult(False, message=schema_result["error"])

            tables = schema_result["tables"]
            console.print(
                f"[{SUCCESS_STYLE}]✓ Read schemas for {len(tables)} tables[/{SUCCESS_STYLE}]"
            )

            # Step 3: Generate manifest
            console.print("\nStep 3: Generating manifest...")
            manifest_result = _generate_redshift_manifest(
                database, schema_name, tables, semantic_tags, client
            )

            if not manifest_result["success"]:
                return CommandResult(False, message=manifest_result["error"])

            manifest = manifest_result["manifest"]

            # Validate manifest
            is_valid, error = validate_manifest(manifest)
            if not is_valid:
                return CommandResult(
                    False, message=f"Generated manifest is invalid: {error}"
                )

            console.print(
                f"[{SUCCESS_STYLE}]✓ Manifest generated successfully[/{SUCCESS_STYLE}]"
            )

            # Step 4: Save manifest locally
            console.print("\nStep 4: Saving manifest locally...")
            manifest_dir = os.path.expanduser("~/.chuck/manifests")
            os.makedirs(manifest_dir, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            manifest_filename = f"redshift_{database}_{schema_name}_{timestamp}.json"
            manifest_path = os.path.join(manifest_dir, manifest_filename)

            if not save_manifest_to_file(manifest, manifest_path):
                return CommandResult(
                    False, message=f"Failed to save manifest to {manifest_path}"
                )

            console.print(
                f"[{SUCCESS_STYLE}]✓ Saved manifest to {manifest_path}[/{SUCCESS_STYLE}]"
            )

            # Step 5: Upload manifest to S3
            console.print("\nStep 5: Uploading manifest to S3...")
            s3_bucket = get_s3_bucket()
            if not s3_bucket:
                return CommandResult(
                    False,
                    message="No S3 bucket configured. Please run setup wizard to configure S3 bucket.",
                )

            s3_path = f"s3://{s3_bucket}/chuck/manifests/{manifest_filename}"
            aws_profile = kwargs.get("aws_profile")

            if not upload_manifest_to_s3(manifest, s3_path, aws_profile):
                return CommandResult(
                    False, message=f"Failed to upload manifest to {s3_path}"
                )

            console.print(
                f"[{SUCCESS_STYLE}]✓ Uploaded manifest to {s3_path}[/{SUCCESS_STYLE}]"
            )

            # Step 6: Fetch and upload init script to Volumes
            console.print("\nStep 6: Fetching and uploading init script to Volumes...")

            # Fetch init script from Amperity
            amperity_token = get_amperity_token()
            if not amperity_token:
                return CommandResult(
                    False,
                    message="Amperity token not found. Please run /amp_login first.",
                )

            try:
                from chuck_data.clients.amperity import AmperityAPIClient

                amperity_client = AmperityAPIClient()
                init_script_data = amperity_client.fetch_amperity_job_init(
                    amperity_token
                )
                init_script_content = init_script_data.get("cluster-init")
                job_id = init_script_data.get("job-id")

                if not init_script_content:
                    return CommandResult(
                        False,
                        message="Failed to get cluster init script from Amperity API.",
                    )
                if not job_id:
                    return CommandResult(
                        False, message="Failed to get job-id from Amperity API."
                    )
            except Exception as e:
                return CommandResult(
                    False, message=f"Error fetching Amperity init script: {str(e)}"
                )

            # Upload init script using storage provider (S3 for Redshift)
            init_script_filename = f"chuck-init-{timestamp}.sh"
            init_script_s3_path = (
                f"s3://{s3_bucket}/chuck/init-scripts/{init_script_filename}"
            )

            # Upload to S3
            try:
                import boto3

                s3_client = boto3.client("s3", region_name=get_redshift_region())
                s3_client.put_object(
                    Bucket=s3_bucket,
                    Key=f"chuck/init-scripts/{init_script_filename}",
                    Body=init_script_content.encode("utf-8"),
                )
                init_script_path = init_script_s3_path
                logging.debug(f"Init script uploaded to {init_script_path}")
            except Exception as e:
                return CommandResult(
                    False, message=f"Failed to upload init script to S3: {str(e)}"
                )

            console.print(
                f"[{SUCCESS_STYLE}]✓ Uploaded init script to {init_script_path}[/{SUCCESS_STYLE}]"
            )

            # Step 7: Submit Stitch job to Databricks
            console.print("\nStep 7: Submitting Stitch job to Databricks...")

            # Create Databricks client for job submission
            from chuck_data.config import get_workspace_url, get_databricks_token

            workspace_url = get_workspace_url()
            token = get_databricks_token()

            if not workspace_url or not token:
                console.print(
                    f"[{WARNING}]⚠ Databricks workspace not configured. Cannot submit job automatically.[/{WARNING}]"
                )
                console.print(
                    f"\n[{INFO_STYLE}]Manual submission required:[/{INFO_STYLE}]"
                )
                console.print(f"1. Manifest uploaded to: {s3_path}")
                console.print(
                    f"2. Use this manifest path when submitting Stitch job to Databricks"
                )
                console.print(
                    f"3. Job will use generic_main entry point which auto-detects Redshift backend"
                )
            else:
                # Submit job using unified function
                stitch_job_name = f"stitch-redshift-{database}-{schema_name}"
                job_result = _submit_stitch_job_to_databricks(
                    console,
                    config_path=s3_path,
                    init_script_path=init_script_path,
                    stitch_job_name=stitch_job_name,
                    job_id=job_id,
                    policy_id=kwargs.get("policy_id"),
                )

                if not job_result["success"]:
                    console.print(
                        f"[{ERROR_STYLE}]✗ {job_result['error']}[/{ERROR_STYLE}]"
                    )
                    return CommandResult(False, message=job_result["error"])

                run_id = job_result["run_id"]
                databricks_client = job_result["databricks_client"]

                # Step 8: Create Stitch Report notebook
                notebook_result = _create_stitch_report_notebook_unified(
                    console,
                    databricks_client,
                    manifest,
                    database,  # target_catalog (database for Redshift)
                    schema_name,  # target_schema
                    stitch_job_name,
                )

                console.print(
                    f"\n[{SUCCESS_STYLE}]Stitch job launched successfully![/{SUCCESS_STYLE}]"
                )

                # Build metadata for post-launch display
                metadata = {
                    "target_catalog": database,  # Use database as catalog equivalent
                    "target_schema": schema_name,
                    "job_id": job_id,
                }

                # Build launch result data
                launch_result = {
                    "run_id": run_id,
                    "notebook_result": notebook_result,  # Add notebook result
                    "message": f"Stitch job for Redshift {database}.{schema_name} launched successfully",
                }

                # Show detailed summary first as progress info
                _display_detailed_summary(console, launch_result)

                # Create the user guidance as the main result message
                logging.info(
                    f"Building post-launch guidance. launch_result: {launch_result}, metadata: {metadata}"
                )
                result_message = _build_post_launch_guidance_message(
                    launch_result, metadata, databricks_client, is_redshift=False
                )
                logging.info(f"Post-launch guidance message: {result_message}")

                # Fallback if message is empty
                if not result_message or not result_message.strip():
                    logging.warning(
                        "Post-launch guidance message is empty, using fallback"
                    )
                    result_message = f"Stitch job submitted successfully with run ID: {run_id}. Use /job-status --run-id {run_id} to check progress."

                # Prepare return data
                result_data = {
                    "database": database,
                    "schema": schema_name,
                    "manifest_path": manifest_path,
                    "s3_path": s3_path,
                    "tables": len(tables),
                    "tagged_columns": len(semantic_tags),
                    "run_id": str(run_id),  # Convert to string to avoid len() error
                    "init_script_path": init_script_path,
                }

                logging.info(f"Returning CommandResult with message: {result_message}")
                logging.info(f"Returning CommandResult with data: {result_data}")

                return CommandResult(
                    True,
                    message=result_message,
                    data=result_data,
                )

            # Return result even if no Databricks configured (manual submission case)
            return CommandResult(
                True,
                message=f"Stitch setup complete for {database}.{schema_name}. Manifest uploaded to S3.",
                data={
                    "database": database,
                    "schema": schema_name,
                    "manifest_path": manifest_path,
                    "s3_path": s3_path,
                    "tables": len(tables),
                    "tagged_columns": len(semantic_tags),
                },
            )
        else:
            # Unexpected state - this shouldn't happen
            return CommandResult(
                False,
                message="Invalid state: expected either interactive mode or auto-confirm mode. Please run /setup-stitch again.",
            )

    except Exception as e:
        logging.error(f"Error setting up Stitch for Redshift: {e}", exc_info=True)
        return CommandResult(False, error=e, message=f"Setup failed: {str(e)}")


def _generate_redshift_manifest(
    database: str, schema_name: str, tables: list, semantic_tags: list, client=None
) -> Dict[str, Any]:
    """Generate manifest JSON from table schemas with semantic tags.

    Follows the redshift-integration.md specification: semantics from chuck_metadata.semantic_tags
    table are included in the manifest for use during Stitch processing.
    """
    try:
        from datetime import datetime
        from chuck_data.config import (
            get_redshift_region,
            get_redshift_s3_temp_dir,
            get_redshift_iam_role,
            get_s3_bucket,
        )

        # Build lookup map of semantic tags: {table_name: {column_name: semantic_type}}
        semantic_map = {}
        for tag in semantic_tags:
            table = tag["table"]
            column = tag["column"]
            semantic_type = tag["semantic"]
            if table not in semantic_map:
                semantic_map[table] = {}
            semantic_map[table][column] = semantic_type

        manifest_tables = []
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")

        for table_info in tables:
            table_name = table_info["table_name"]
            columns = table_info["columns"]

            fields = []
            for col in columns:
                col_name = col["name"]
                col_type = col["type"]

                # Normalize type
                # normalize_redshift_type is defined below in this file
                normalized_type = normalize_redshift_type(col_type)

                # Get semantic tag for this column if it exists
                semantics = []
                if table_name in semantic_map and col_name in semantic_map[table_name]:
                    semantic_type = semantic_map[table_name][col_name]
                    semantics.append(semantic_type)

                fields.append(
                    {
                        "field-name": col_name,
                        "type": normalized_type.upper(),
                        "semantics": semantics,
                    }
                )

            manifest_tables.append({"path": table_name, "fields": fields})

        # Get connection details
        redshift_config = {"database": database, "schema": schema_name}

        if client and hasattr(client, "cluster_identifier"):
            if client.cluster_identifier:
                redshift_config["cluster_identifier"] = client.cluster_identifier
            if client.workgroup_name:
                redshift_config["workgroup_name"] = client.workgroup_name
            if client.region:
                redshift_config["region"] = client.region
        else:
            region = get_redshift_region()
            if region:
                redshift_config["region"] = region

        # Get S3 settings
        s3_temp_dir = get_redshift_s3_temp_dir()
        s3_bucket = get_s3_bucket()

        if (not s3_temp_dir or "None" in str(s3_temp_dir)) and s3_bucket:
            s3_temp_dir = f"s3://{s3_bucket}/redshift-temp/"

        iam_role = get_redshift_iam_role()

        if not s3_temp_dir or "None" in str(s3_temp_dir):
            return {
                "success": False,
                "error": "No S3 temp directory configured. Please configure s3_bucket in chuck config or set redshift_s3_temp_dir.",
            }

        if not iam_role or "123456789012" in str(iam_role):
            logging.warning(
                "IAM role appears to be a placeholder. Set redshift_iam_role in chuck config for production use."
            )
            if not iam_role:
                iam_role = "arn:aws:iam::ACCOUNT_ID:role/RedshiftRole"

        manifest_name = f"stitch-redshift-{timestamp}"

        manifest = {
            "name": manifest_name,
            "tables": manifest_tables,
            "settings": {
                "redshift_config": redshift_config,
                "s3_temp_dir": s3_temp_dir,
                "redshift_iam_role": iam_role,
                "output_database_name": database,
                "output_schema_name": "stitch_outputs",
            },
        }

        return {"success": True, "manifest": manifest}

    except Exception as e:
        logging.error(f"Error generating manifest: {e}", exc_info=True)
        return {"success": False, "error": f"Failed to generate manifest: {str(e)}"}


def normalize_redshift_type(redshift_type: str) -> str:
    """Normalize Redshift types to Spark types."""
    type_lower = redshift_type.lower()

    if any(t in type_lower for t in ["varchar", "char", "text", "character"]):
        return "string"

    if any(
        t in type_lower
        for t in ["int", "integer", "smallint", "bigint", "int2", "int4", "int8"]
    ):
        return "long"

    if any(t in type_lower for t in ["decimal", "numeric"]):
        return "decimal"

    if any(t in type_lower for t in ["float", "double", "real", "float4", "float8"]):
        return "double"

    if "bool" in type_lower:
        return "boolean"

    if "date" in type_lower:
        return "date"
    if "timestamp" in type_lower:
        return "timestamp"

    return "string"


DEFINITION = CommandDefinition(
    name="setup_stitch",
    description="Set up a Stitch integration for Databricks Unity Catalog or AWS Redshift. Automatically detects data provider and routes accordingly. For Databricks: scans catalog/schema for PII and creates job. For Redshift: reads PII tags from chuck_metadata.semantic_tags table, generates manifest JSON, and uploads to S3.",
    handler=handle_command,
    parameters={
        "catalog_name": {
            "type": "string",
            "description": "Optional: Single target catalog name (Databricks only)",
        },
        "database": {
            "type": "string",
            "description": "Optional: Database name (Redshift only, uses active database if not provided)",
        },
        "schema_name": {
            "type": "string",
            "description": "Optional: Schema name (uses active schema if not provided)",
        },
        "targets": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Optional: List of catalog.schema pairs to scan (e.g., ['prod.crm', 'prod.ecommerce', 'analytics.customers'])",
        },
        "output_catalog": {
            "type": "string",
            "description": "Optional: Catalog for outputs and volume storage (defaults to first target's catalog)",
        },
        "auto_confirm": {
            "type": "boolean",
            "description": "Optional: Skip interactive confirmation (default: false)",
        },
        "policy_id": {
            "type": "string",
            "description": "Optional: cluster policy ID to use for the Stitch job run",
        },
    },
    required_params=[],
    tui_aliases=["/setup-stitch"],
    visible_to_user=True,
    visible_to_agent=True,
    supports_interactive_input=True,
    usage_hint="Examples:\n  /setup-stitch (uses active catalog/schema)\n  /setup-stitch --catalog_name prod --schema_name crm\n  /setup-stitch --targets prod.crm,prod.ecommerce,analytics.customers --output_catalog prod",
    condensed_action="Setting up Stitch integration",
)
