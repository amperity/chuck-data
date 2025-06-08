"""
Bulk PII tagging command with interactive confirmation.

3-Phase Workflow:
1. Scan: Use scan-pii logic to find PII columns
2. Review: Show results, handle modifications/confirmations
3. Tag: Execute bulk tag-pii operations
"""

from chuck_data.interactive_context import InteractiveContext
from chuck_data.commands.base import CommandResult
from chuck_data.command_registry import CommandDefinition
from chuck_data.commands.pii_tools import _helper_scan_schema_for_pii_logic
from chuck_data.llm.client import LLMClient
from chuck_data.ui.tui import get_console
from chuck_data.ui.theme import INFO_STYLE, ERROR_STYLE, SUCCESS_STYLE
from chuck_data import config


def handle_bulk_tag_pii(client, **kwargs):
    """
    Handle bulk PII tagging with interactive confirmation.

    Args:
        client: Databricks client instance
        **kwargs: Command parameters including:
            - catalog_name: str (optional, uses active if not provided)
            - schema_name: str (optional, uses active if not provided)
            - auto_confirm: bool (optional, default False)
            - interactive_input: str (provided during interactive mode)
            - tool_output_callback: callable (for agent progress reporting)

    Returns:
        CommandResult: Success/failure with appropriate data
    """

    try:
        # Route to appropriate handler based on execution mode
        interactive_input = kwargs.get("interactive_input")
        auto_confirm = kwargs.get("auto_confirm", False)

        if interactive_input:
            # Handle user input during interactive session (skip validation - already validated)
            return _handle_interactive_input(client, interactive_input, **kwargs)

        # Parameter validation (for non-interactive commands)
        validation_result = _validate_parameters(client, **kwargs)
        if not validation_result.success:
            return validation_result

        if auto_confirm:
            # Direct execution without interaction
            return _execute_directly(client, **kwargs)
        else:
            # Start interactive workflow
            return _start_interactive_mode(client, **kwargs)

    except Exception as e:
        # Always cleanup context on any error
        context = InteractiveContext()
        context.clear_active_context("bulk-tag-pii")
        return CommandResult(False, error=e, message=f"Error: {str(e)}")


def _validate_parameters(client, **kwargs):
    """Comprehensive parameter validation."""
    errors = []

    # Get catalog name (explicit or from config)
    catalog_name = kwargs.get("catalog_name")
    if not catalog_name:
        try:
            catalog_name = config.get_active_catalog()
            if not catalog_name:
                errors.append("No catalog specified and no active catalog configured")
        except Exception:
            errors.append("No catalog specified and no active catalog configured")

    # Get schema name (explicit or from config)
    schema_name = kwargs.get("schema_name")
    if not schema_name:
        try:
            schema_name = config.get_active_schema()
            if not schema_name:
                errors.append("No schema specified and no active schema configured")
        except Exception:
            errors.append("No schema specified and no active schema configured")

    # Check warehouse configuration for SQL operations
    try:
        warehouse_id = config.get_warehouse_id()
        if not warehouse_id:
            errors.append(
                "No warehouse configured. Please configure a warehouse for SQL operations."
            )
    except Exception:
        errors.append(
            "No warehouse configured. Please configure a warehouse for SQL operations."
        )

    if errors:
        return CommandResult(
            False, message=f"Configuration errors: {'; '.join(errors)}"
        )

    # Validate catalog exists
    try:
        client.get_catalog(catalog_name)
    except Exception:
        try:
            catalogs_result = client.list_catalogs()
            catalog_names = [
                c.get("name", "Unknown") for c in catalogs_result.get("catalogs", [])
            ]
            available = ", ".join(catalog_names)
            return CommandResult(
                False,
                message=f"Catalog '{catalog_name}' not found. Available catalogs: {available}",
            )
        except Exception as e:
            return CommandResult(False, message=f"Unable to validate catalog: {str(e)}")

    # Validate schema exists
    try:
        client.get_schema(f"{catalog_name}.{schema_name}")
    except Exception:
        try:
            schemas_result = client.list_schemas(catalog_name)
            schemas = schemas_result.get("schemas", [])
            schema_names = [s.get("name", "Unknown") for s in schemas]
            available = ", ".join(schema_names)
            return CommandResult(
                False,
                message=f"Schema '{schema_name}' not found. Available schemas: {available}",
            )
        except Exception as e:
            return CommandResult(False, message=f"Unable to validate schema: {str(e)}")

    return CommandResult(True, message="Parameters valid")


def _execute_directly(client, **kwargs):
    """Execute workflow directly without interaction."""
    # Get parameters (validated already)
    catalog_name = kwargs.get("catalog_name") or config.get_active_catalog()
    schema_name = kwargs.get("schema_name") or config.get_active_schema()
    tool_output_callback = kwargs.get("tool_output_callback")

    # Assert non-None since validation has passed
    assert catalog_name is not None, "catalog_name should not be None after validation"
    assert schema_name is not None, "schema_name should not be None after validation"

    # Phase 1: Scan for PII using actual scan-pii logic
    _report_progress(
        f"Scanning schema {catalog_name}.{schema_name} for PII columns",
        tool_output_callback,
    )

    try:
        # Create LLM client for PII scanning
        llm_client = LLMClient()

        # Use actual scan-pii logic from pii_tools (show progress like scan-pii does)
        scan_summary_data = _helper_scan_schema_for_pii_logic(
            client, llm_client, catalog_name, schema_name, show_progress=True
        )

        # Check for scanning errors
        if scan_summary_data.get("error"):
            error_msg = scan_summary_data["error"]
            _report_progress(f"Scan failed: {error_msg}", tool_output_callback)
            return CommandResult(
                False, message=f"Error during PII scanning: {error_msg}"
            )

        # Extract scan results
        tables_with_pii = scan_summary_data.get("tables_with_pii", 0)
        total_pii_columns = scan_summary_data.get("total_pii_columns", 0)
        tables_processed = scan_summary_data.get("tables_successfully_processed", 0)
        tables_attempted = scan_summary_data.get("tables_scanned_attempted", 0)

        # Report scan completion with statistics
        _report_progress(
            f"Scan completed: {tables_processed}/{tables_attempted} tables processed, {total_pii_columns} PII columns found in {tables_with_pii} tables",
            tool_output_callback,
        )

        # Check if any PII was found
        if tables_with_pii == 0 or total_pii_columns == 0:
            return CommandResult(
                True,
                message="No PII columns found in schema - nothing to tag",
                data={
                    "catalog_name": catalog_name,
                    "schema_name": schema_name,
                    "tables_processed": tables_processed,
                    "columns_tagged": 0,
                    "scan_summary": scan_summary_data,
                },
            )

        # Phase 2: Execute bulk tagging
        _report_progress(
            f"Starting bulk tagging of {total_pii_columns} PII columns",
            tool_output_callback,
        )

        # Execute bulk tagging using scan results
        try:
            tagging_results = _execute_bulk_tagging(
                client, scan_summary_data, tool_output_callback
            )
        except Exception as e:
            _report_progress(f"Bulk tagging failed: {str(e)}", tool_output_callback)
            return CommandResult(
                False, message=f"Error during bulk tagging execution: {str(e)}"
            )

        # Count successful taggings
        columns_tagged = sum(
            1 for result in tagging_results if result.get("success", False)
        )
        failed_taggings = len(tagging_results) - columns_tagged

        # Check for critical errors (like warehouse not configured)
        warehouse_errors = [
            r
            for r in tagging_results
            if r.get("error") == "No warehouse configured for SQL execution"
        ]
        if warehouse_errors:
            return CommandResult(
                False,
                message="No warehouse configured for SQL execution. Please configure a warehouse first.",
            )

        # Report final results
        if failed_taggings > 0:
            _report_progress(
                f"Bulk tagging completed: {columns_tagged} successful, {failed_taggings} failed",
                tool_output_callback,
            )
            # Provide partial success result with details about failures
            failure_summary = _summarize_failures(tagging_results)
            message = f"Bulk PII tagging partially completed for {catalog_name}.{schema_name}. Tagged {columns_tagged} of {columns_tagged + failed_taggings} PII columns. {failure_summary}"
        else:
            _report_progress(
                f"Bulk tagging completed successfully: {columns_tagged} columns tagged",
                tool_output_callback,
            )
            message = f"Bulk PII tagging completed for {catalog_name}.{schema_name}. Tagged {columns_tagged} PII columns in {tables_with_pii} tables."

        return CommandResult(
            failed_taggings == 0,  # Success only if no failures
            message=message,
            data={
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "tables_processed": tables_processed,
                "tables_with_pii": tables_with_pii,
                "columns_tagged": columns_tagged,
                "columns_failed": failed_taggings,
                "scan_summary": scan_summary_data,
                "tagging_results": tagging_results,
            },
        )

    except Exception as e:
        return CommandResult(False, message=f"Error during bulk PII tagging: {str(e)}")


def _execute_bulk_tagging(client, scan_summary_data, tool_output_callback=None):
    """Execute bulk tagging based on scan results."""
    tagging_results = []

    # Get warehouse ID for SQL execution
    warehouse_id = config.get_warehouse_id()
    if not warehouse_id:
        return [
            {"error": "No warehouse configured for SQL execution", "success": False}
        ]

    # Extract detailed results from scan summary
    results_detail = scan_summary_data.get("results_detail", [])

    # Count tables with PII for progress tracking
    tables_with_pii = [
        r
        for r in results_detail
        if not r.get("error") and not r.get("skipped") and r.get("has_pii")
    ]
    current_table = 0

    for table_result in results_detail:
        # Skip tables with errors or no PII
        if (
            table_result.get("error")
            or table_result.get("skipped")
            or not table_result.get("has_pii")
        ):
            continue

        current_table += 1
        table_name = table_result.get("full_name")
        pii_columns = table_result.get("pii_columns", [])

        if not table_name or not pii_columns:
            continue

        # Progress report for each table with position
        table_short_name = table_name.split(".")[-1] if table_name else "unknown"
        _report_progress(
            f"Tagging {len(pii_columns)} PII columns in {table_short_name} ({current_table}/{len(tables_with_pii)})",
            tool_output_callback,
        )

        # Apply tags to each PII column in this table
        for column in pii_columns:
            column_name = column.get("name")
            semantic_type = column.get("semantic")

            if not column_name or not semantic_type:
                tagging_results.append(
                    {
                        "table": table_name,
                        "column": column_name or "unknown",
                        "success": False,
                        "error": "Missing column name or semantic type",
                    }
                )
                continue

            # Construct and execute the SQL ALTER TABLE statement
            sql = f"""
            ALTER TABLE {table_name} 
            ALTER COLUMN {column_name} 
            SET TAGS ('semantic' = '{semantic_type}')
            """

            try:
                result = client.submit_sql_statement(
                    sql_text=sql, warehouse_id=warehouse_id, wait_timeout="30s"
                )

                if result.get("status", {}).get("state") == "SUCCEEDED":
                    tagging_results.append(
                        {
                            "table": table_name,
                            "column": column_name,
                            "semantic_type": semantic_type,
                            "success": True,
                        }
                    )
                else:
                    # Extract detailed error information
                    status = result.get("status", {})
                    error_info = status.get("error", {})

                    if isinstance(error_info, dict):
                        error_message = error_info.get("message", "Unknown SQL error")
                        error_type = error_info.get("error_code", "UNKNOWN_ERROR")
                    else:
                        error_message = (
                            str(error_info) if error_info else "Unknown SQL error"
                        )
                        error_type = "UNKNOWN_ERROR"

                    tagging_results.append(
                        {
                            "table": table_name,
                            "column": column_name,
                            "semantic_type": semantic_type,
                            "success": False,
                            "error": error_message,
                            "error_type": error_type,
                        }
                    )
            except Exception as e:
                # Categorize common errors for better user feedback
                error_message = str(e)
                if "warehouse" in error_message.lower():
                    error_type = "WAREHOUSE_ERROR"
                elif (
                    "permission" in error_message.lower()
                    or "access" in error_message.lower()
                ):
                    error_type = "PERMISSION_ERROR"
                elif "timeout" in error_message.lower():
                    error_type = "TIMEOUT_ERROR"
                else:
                    error_type = "EXECUTION_ERROR"

                tagging_results.append(
                    {
                        "table": table_name,
                        "column": column_name,
                        "semantic_type": semantic_type,
                        "success": False,
                        "error": error_message,
                        "error_type": error_type,
                    }
                )

    return tagging_results


def _summarize_failures(tagging_results):
    """Summarize failure reasons for user feedback."""
    failed_results = [r for r in tagging_results if not r.get("success", False)]
    if not failed_results:
        return ""

    # Group failures by error type
    error_counts = {}
    for result in failed_results:
        error = result.get("error", "Unknown error")
        error_counts[error] = error_counts.get(error, 0) + 1

    # Create summary
    if len(error_counts) == 1:
        error, count = list(error_counts.items())[0]
        return f"Failures: {count} column(s) failed due to: {error}"
    else:
        error_list = [
            f"{count} failed due to {error}" for error, count in error_counts.items()
        ]
        return f"Failures: {', '.join(error_list)}"


def _display_pii_preview(console, scan_summary_data):
    """Display a detailed preview of PII findings like stitch setup."""
    results_detail = scan_summary_data.get("results_detail", [])

    # Filter to only tables with PII
    tables_with_pii = [
        table_result
        for table_result in results_detail
        if not table_result.get("error")
        and not table_result.get("skipped")
        and table_result.get("has_pii")
    ]

    if not tables_with_pii:
        console.print("No PII columns detected.")
        return

    console.print(f"\n[{INFO_STYLE}]PII Tagging Preview:[/{INFO_STYLE}]")

    total_pii_fields = sum(
        len(table.get("pii_columns", [])) for table in tables_with_pii
    )
    console.print(f"• Tables to tag: {len(tables_with_pii)}")
    console.print(f"• Total PII fields: {total_pii_fields}")

    if tables_with_pii:
        console.print("\nTables:")
        for table_result in tables_with_pii:
            table_name = table_result.get("table_name", "unknown")
            full_name = table_result.get("full_name", table_name)
            pii_columns = table_result.get("pii_columns", [])

            pii_count = len(pii_columns)
            console.print(f"  - {full_name} ({pii_count} PII fields)")

            # Show each PII column with its semantic tag
            for col in pii_columns:
                if col.get("semantic"):
                    console.print(f"    • {col['name']} ({col['semantic']})")


def _display_confirmation_prompt(console):
    """Display the confirmation prompt like stitch setup."""
    console.print(f"\n[{INFO_STYLE}]What would you like to do?[/{INFO_STYLE}]")
    console.print("• Type 'proceed' (or 'yes', 'go') to tag all PII columns")
    console.print(
        "• Describe changes (e.g., 'exclude table users', 'remove customer_data')"
    )
    console.print("• Type 'cancel' to abort the tagging")


def _report_progress(step_message, tool_output_callback=None):
    """Report progress for both agent integration and direct TUI usage."""
    if tool_output_callback:
        # Agent usage - report via callback
        tool_output_callback("bulk-tag-pii", {"step": step_message})
    else:
        # Direct TUI usage - show progress using console like scan-pii
        console = get_console()
        console.print(f"[dim]{step_message}[/dim]")


def _start_interactive_mode(client, **kwargs):
    """Start interactive workflow - Phase 1: Scan and Preview."""
    # Get parameters (validated already)
    catalog_name = kwargs.get("catalog_name") or config.get_active_catalog()
    schema_name = kwargs.get("schema_name") or config.get_active_schema()

    # Assert non-None since validation has passed
    assert catalog_name is not None, "catalog_name should not be None after validation"
    assert schema_name is not None, "schema_name should not be None after validation"

    try:
        # Phase 1: Scan for PII (let _helper_scan_schema_for_pii_logic show individual table progress)
        # Create LLM client for PII scanning
        llm_client = LLMClient()

        # Use actual scan-pii logic from pii_tools (show progress like scan-pii does)
        scan_summary_data = _helper_scan_schema_for_pii_logic(
            client, llm_client, catalog_name, schema_name, show_progress=True
        )

        # Check for scanning errors
        if scan_summary_data.get("error"):
            error_msg = scan_summary_data["error"]
            return CommandResult(
                False, message=f"Error during PII scanning: {error_msg}"
            )

        # Extract scan results
        tables_with_pii = scan_summary_data.get("tables_with_pii", 0)
        total_pii_columns = scan_summary_data.get("total_pii_columns", 0)
        tables_processed = scan_summary_data.get("tables_successfully_processed", 0)
        tables_attempted = scan_summary_data.get("tables_scanned_attempted", 0)

        # Initialize excluded tables count for interactive mode
        scan_summary_data["excluded_tables_count"] = 0

        # Check if any PII was found
        if tables_with_pii == 0 or total_pii_columns == 0:
            return CommandResult(
                True,
                message="No PII columns found in schema - nothing to tag",
                data={
                    "catalog_name": catalog_name,
                    "schema_name": schema_name,
                    "tables_processed": tables_processed,
                    "columns_tagged": 0,
                    "scan_summary": scan_summary_data,
                },
            )

        # Store context for interactive workflow
        context = InteractiveContext()
        context.set_active_context("bulk-tag-pii")
        context.store_context_data("bulk-tag-pii", "phase", "review")
        context.store_context_data("bulk-tag-pii", "catalog_name", catalog_name)
        context.store_context_data("bulk-tag-pii", "schema_name", schema_name)
        context.store_context_data("bulk-tag-pii", "scan_summary", scan_summary_data)
        context.store_context_data("bulk-tag-pii", "original_kwargs", kwargs)

        # Display the formatted preview like stitch setup
        console = get_console()

        # Show scan summary like scan-pii command
        scan_summary = f"Scanned {tables_processed}/{tables_attempted} tables in {catalog_name}.{schema_name}. Found {tables_with_pii} tables with {total_pii_columns} PII columns."
        console.print(scan_summary)

        # Display detailed PII preview
        _display_pii_preview(console, scan_summary_data)

        # Display confirmation prompt
        _display_confirmation_prompt(console)

        return CommandResult(
            True,
            message="",  # Empty message - let the console output speak for itself
            data={
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "tables_with_pii": tables_with_pii,
                "total_pii_columns": total_pii_columns,
                "scan_summary": scan_summary_data,
                "interactive": True,
                "awaiting_input": True,
            },
        )

    except Exception as e:
        # Cleanup context on error
        context = InteractiveContext()
        context.clear_active_context("bulk-tag-pii")
        return CommandResult(
            False, message=f"Error during interactive mode setup: {str(e)}"
        )


def _handle_interactive_input(client, user_input, **kwargs):
    """Handle user input during interactive mode."""
    # Get context
    context = InteractiveContext()
    if (
        not context.is_in_interactive_mode()
        or context.current_command != "bulk-tag-pii"
    ):
        return CommandResult(
            False,
            message="No active bulk-tag-pii session found. Please restart the command.",
        )

    # Extract context data
    context_data = context.get_context_data("bulk-tag-pii")
    tool_output_callback = kwargs.get("tool_output_callback")
    console = get_console()

    # Parse user input
    user_input_lower = user_input.strip().lower()

    try:
        # Check for proceed commands (like stitch's launch commands)
        if user_input_lower in [
            "proceed",
            "yes",
            "y",
            "go",
            "continue",
            "tag",
            "start",
        ]:
            # Execute bulk tagging with current scan results
            return _proceed_with_tagging(client, context_data, tool_output_callback)

        # Check for cancel commands (same as stitch)
        elif user_input_lower in ["cancel", "abort", "stop", "exit", "quit", "no"]:
            # Cancel and cleanup
            context.clear_active_context("bulk-tag-pii")
            console.print(f"\n[{INFO_STYLE}]Bulk PII tagging cancelled.[/{INFO_STYLE}]")
            return CommandResult(True, message="Bulk PII tagging cancelled.")

        # Check for exclude commands (modification requests like stitch)
        elif user_input_lower.startswith("exclude "):
            # Handle table exclusion
            table_name = user_input[8:].strip()  # Remove "exclude " prefix
            console.print(
                f"\n[{INFO_STYLE}]Excluding table based on your request...[/{INFO_STYLE}]"
            )
            return _exclude_table(context, context_data, table_name)

        else:
            # Try to handle as modification request - either parse it or provide helpful feedback
            return _handle_modification_request(
                context, context_data, user_input, console
            )

    except Exception as e:
        # Cleanup context on error
        context.clear_active_context("bulk-tag-pii")
        return CommandResult(
            False, message=f"Error during interactive input handling: {str(e)}"
        )


def _proceed_with_tagging(client, context_data, tool_output_callback):
    """Execute bulk tagging with current scan results."""
    catalog_name = context_data.get("catalog_name")
    schema_name = context_data.get("schema_name")
    scan_summary = context_data.get("scan_summary")

    # Clear context since we're proceeding
    context = InteractiveContext()
    context.clear_active_context("bulk-tag-pii")

    try:
        # Get updated statistics after any exclusions
        tables_with_pii = scan_summary.get("tables_with_pii", 0)
        total_pii_columns = scan_summary.get("total_pii_columns", 0)
        excluded_tables_count = scan_summary.get("excluded_tables_count", 0)

        # Execute bulk tagging
        _report_progress(
            f"Starting bulk tagging of {total_pii_columns} PII columns",
            tool_output_callback,
        )

        tagging_results = _execute_bulk_tagging(
            client, scan_summary, tool_output_callback
        )

        # Count successful taggings
        columns_tagged = sum(
            1 for result in tagging_results if result.get("success", False)
        )
        failed_taggings = len(tagging_results) - columns_tagged

        # Report final results
        if failed_taggings > 0:
            _report_progress(
                f"Bulk tagging completed: {columns_tagged} successful, {failed_taggings} failed",
                tool_output_callback,
            )
            failure_summary = _summarize_failures(tagging_results)
            exclusion_note = (
                f" ({excluded_tables_count} tables excluded)"
                if excluded_tables_count > 0
                else ""
            )
            message = f"Bulk PII tagging partially completed for {catalog_name}.{schema_name}. Tagged {columns_tagged} of {columns_tagged + failed_taggings} PII columns{exclusion_note}. {failure_summary}"
        else:
            _report_progress(
                f"Bulk tagging completed successfully: {columns_tagged} columns tagged",
                tool_output_callback,
            )
            exclusion_note = (
                f" ({excluded_tables_count} tables excluded)"
                if excluded_tables_count > 0
                else ""
            )
            message = f"Bulk PII tagging completed for {catalog_name}.{schema_name}. Tagged {columns_tagged} PII columns in {tables_with_pii} tables{exclusion_note}."

        return CommandResult(
            failed_taggings == 0,
            message=message,
            data={
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "tables_with_pii": tables_with_pii,
                "columns_tagged": columns_tagged,
                "columns_failed": failed_taggings,
                "excluded_tables_count": excluded_tables_count,
                "scan_summary": scan_summary,
                "tagging_results": tagging_results,
            },
        )

    except Exception as e:
        return CommandResult(
            False, message=f"Error during bulk tagging execution: {str(e)}"
        )


def _exclude_table(context, context_data, table_name):
    """Exclude a table from PII tagging and update the context."""
    scan_summary = context_data.get("scan_summary")
    results_detail = scan_summary.get("results_detail", [])

    # Find the table to exclude
    table_found = False
    updated_results = []

    for table_result in results_detail:
        if (
            table_result.get("table_name", "").lower() == table_name.lower()
            or table_result.get("full_name", "").split(".")[-1].lower()
            == table_name.lower()
        ):
            # Mark this table as excluded
            table_result = table_result.copy()
            table_result["excluded"] = True
            table_result["has_pii"] = False  # Remove from PII count
            table_found = True
        updated_results.append(table_result)

    if not table_found:
        # List available tables for the user
        available_tables = []
        for table_result in results_detail:
            if (
                not table_result.get("error")
                and not table_result.get("skipped")
                and table_result.get("has_pii")
            ):
                available_tables.append(table_result.get("table_name", "unknown"))

        available_list = ", ".join(available_tables) if available_tables else "none"
        return CommandResult(
            False,
            message=f"Table '{table_name}' not found or doesn't contain PII. Available tables with PII: {available_list}",
        )

    # Update scan summary with new results
    scan_summary["results_detail"] = updated_results

    # Recalculate statistics
    remaining_tables_with_pii = sum(
        1
        for r in updated_results
        if not r.get("error")
        and not r.get("skipped")
        and r.get("has_pii")
        and not r.get("excluded")
    )
    remaining_pii_columns = sum(
        r.get("pii_column_count", 0)
        for r in updated_results
        if not r.get("error")
        and not r.get("skipped")
        and r.get("has_pii")
        and not r.get("excluded")
    )
    excluded_tables_count = sum(1 for r in updated_results if r.get("excluded"))

    scan_summary["tables_with_pii"] = remaining_tables_with_pii
    scan_summary["total_pii_columns"] = remaining_pii_columns
    scan_summary["excluded_tables_count"] = excluded_tables_count

    # Update context with new scan summary
    context.store_context_data("bulk-tag-pii", "scan_summary", scan_summary)

    if remaining_tables_with_pii == 0:
        context.clear_active_context("bulk-tag-pii")
        return CommandResult(
            True,
            message=f"Table '{table_name}' excluded. No PII tables remaining - nothing to tag.",
        )

    # Display updated preview like the initial display
    console = get_console()
    console.print(
        f"Table '{table_name}' excluded. {remaining_pii_columns} PII columns remaining in {remaining_tables_with_pii} tables."
    )

    # Display updated PII preview
    _display_pii_preview(console, scan_summary)

    # Display confirmation prompt again
    _display_confirmation_prompt(console)

    return CommandResult(
        True,
        message="",  # Empty message - console output is the display
        data={
            "tables_with_pii": remaining_tables_with_pii,
            "total_pii_columns": remaining_pii_columns,
            "scan_summary": scan_summary,
            "interactive": True,
            "awaiting_input": True,
        },
    )


def _handle_modification_request(context, context_data, user_input, console):
    """Handle modification requests using LLM helper like setup-stitch."""
    scan_summary = context_data.get("scan_summary")

    console.print(
        f"\n[{INFO_STYLE}]Modifying PII configuration based on your request...[/{INFO_STYLE}]"
    )

    # Create LLM client for modification processing
    from chuck_data.llm.client import LLMClient

    llm_client = LLMClient()

    # Use LLM helper to modify the scan summary
    modify_result = _helper_modify_pii_config(scan_summary, user_input, llm_client)

    if modify_result.get("error"):
        console.print(
            f"\n[{ERROR_STYLE}]Error modifying configuration: {modify_result['error']}[/{ERROR_STYLE}]"
        )
        console.print(
            "Please try rephrasing your request or type 'proceed' to continue with current config."
        )
        return CommandResult(
            True,
            message="",
        )

    # Update stored scan summary
    updated_scan_summary = modify_result["scan_summary"]
    context.store_context_data("bulk-tag-pii", "scan_summary", updated_scan_summary)

    console.print(f"\n[{SUCCESS_STYLE}]Configuration updated![/{SUCCESS_STYLE}]")
    if modify_result.get("modification_summary"):
        console.print(modify_result["modification_summary"])

    # Show updated preview
    _display_pii_preview(console, updated_scan_summary)
    _display_confirmation_prompt(console)

    return CommandResult(
        True,
        message="",  # Empty message - console output is the display
        data={
            "tables_with_pii": updated_scan_summary.get("tables_with_pii", 0),
            "total_pii_columns": updated_scan_summary.get("total_pii_columns", 0),
            "scan_summary": updated_scan_summary,
            "interactive": True,
            "awaiting_input": True,
        },
    )


def _helper_modify_pii_config(scan_summary, user_request, llm_client):
    """Helper function to modify PII configuration using LLM."""
    import json

    try:
        # Build prompt for LLM to understand the modification request
        results_detail = scan_summary.get("results_detail", [])

        # Create a summary of current PII findings for the LLM
        pii_summary = []
        for table_result in results_detail:
            if (
                not table_result.get("error")
                and not table_result.get("skipped")
                and table_result.get("has_pii")
            ):
                table_name = table_result.get("table_name", "unknown")
                pii_columns = table_result.get("pii_columns", [])
                pii_summary.append(
                    {
                        "table": table_name,
                        "columns": [
                            {"name": col.get("name"), "semantic": col.get("semantic")}
                            for col in pii_columns
                        ],
                    }
                )

        if not pii_summary:
            return {"error": "No PII columns found to modify"}

        # Build LLM prompt
        prompt = f"""You are helping modify a PII tagging configuration. The user wants to make changes to which columns are tagged with PII semantic types.

Current PII configuration:
{pii_summary}

User request: "{user_request}"

Please interpret the user's request and provide modifications. Common requests include:
- Excluding entire tables: "remove table users", "skip pos_table"
- Excluding specific columns: "remove email column", "don't tag phone"
- Changing semantic types: "change address to street-address", "make phone_number a phone type"

Respond with a JSON object containing:
{{
    "action": "exclude_table" | "exclude_column" | "change_semantic" | "unknown",
    "target_table": "table_name" (if applicable),
    "target_column": "column_name" (if applicable), 
    "new_semantic": "semantic_type" (if changing semantic),
    "reasoning": "explanation of what you understood"
}}

If you cannot understand the request, set action to "unknown".
"""

        # Call LLM to get modification instructions
        llm_response = llm_client.chat(messages=[{"role": "user", "content": prompt}])

        if not llm_response or not llm_response.choices:
            return {"error": "Failed to get LLM response for modification request"}

        # Parse the LLM response as JSON
        try:
            response_text = llm_response.choices[0].message.content
            if not response_text or not isinstance(response_text, str):
                return {"error": "LLM returned invalid response format"}

            # Clean up response text (remove code blocks if present)
            response_text = response_text.strip()
            if response_text.startswith("```json"):
                response_text = response_text[7:-3].strip()
            elif response_text.startswith("```"):
                response_text = response_text[3:-3].strip()

            modification = json.loads(response_text)
        except json.JSONDecodeError as e:
            return {
                "error": f"Could not understand the modification request: {user_request}. LLM response parsing failed: {str(e)}"
            }

        # Apply the modification based on the LLM's interpretation
        action = modification.get("action")

        if action == "exclude_table":
            target_table = modification.get("target_table")
            if target_table:
                # Find and exclude the table
                updated_results = []
                table_found = False
                for table_result in results_detail:
                    if (
                        table_result.get("table_name", "").lower()
                        == target_table.lower()
                        or target_table.lower()
                        in table_result.get("table_name", "").lower()
                    ):
                        # Mark this table as excluded
                        table_result = table_result.copy()
                        table_result["excluded"] = True
                        table_result["has_pii"] = False
                        table_found = True
                    updated_results.append(table_result)

                if not table_found:
                    return {"error": f"Table '{target_table}' not found in PII results"}

                # Update scan summary with new results
                updated_scan_summary = scan_summary.copy()
                updated_scan_summary["results_detail"] = updated_results

                # Recalculate statistics
                remaining_tables_with_pii = sum(
                    1
                    for r in updated_results
                    if not r.get("error")
                    and not r.get("skipped")
                    and r.get("has_pii")
                    and not r.get("excluded")
                )
                remaining_pii_columns = sum(
                    r.get("pii_column_count", 0)
                    for r in updated_results
                    if not r.get("error")
                    and not r.get("skipped")
                    and r.get("has_pii")
                    and not r.get("excluded")
                )
                excluded_tables_count = sum(
                    1 for r in updated_results if r.get("excluded")
                )

                updated_scan_summary["tables_with_pii"] = remaining_tables_with_pii
                updated_scan_summary["total_pii_columns"] = remaining_pii_columns
                updated_scan_summary["excluded_tables_count"] = excluded_tables_count

                return {
                    "scan_summary": updated_scan_summary,
                    "modification_summary": f"Excluded table '{target_table}' from PII tagging. {remaining_pii_columns} PII columns remaining in {remaining_tables_with_pii} tables.",
                }

        elif action == "exclude_column":
            target_table = modification.get("target_table")
            target_column = modification.get("target_column")
            if target_column:
                # Find and exclude the column
                updated_results = []
                column_found = False
                for table_result in results_detail:
                    table_result = table_result.copy()
                    if (
                        target_table
                        and table_result.get("table_name", "").lower()
                        != target_table.lower()
                    ):
                        # Skip tables that don't match if table was specified
                        updated_results.append(table_result)
                        continue

                    # Check if this table has the target column
                    pii_columns = table_result.get("pii_columns", [])
                    updated_pii_columns = []

                    for col in pii_columns:
                        if col.get("name", "").lower() == target_column.lower():
                            # Found the column to exclude - don't add it to updated list
                            column_found = True
                        else:
                            updated_pii_columns.append(col)

                    # Update the table result with modified column list
                    table_result["pii_columns"] = updated_pii_columns
                    table_result["pii_column_count"] = len(updated_pii_columns)

                    # If no PII columns remain, mark table as not having PII
                    if len(updated_pii_columns) == 0:
                        table_result["has_pii"] = False

                    updated_results.append(table_result)

                if not column_found:
                    available_columns = []
                    for table_result in results_detail:
                        if (
                            not table_result.get("error")
                            and not table_result.get("skipped")
                            and table_result.get("has_pii")
                        ):
                            table_name = table_result.get("table_name", "")
                            for col in table_result.get("pii_columns", []):
                                available_columns.append(
                                    f"{table_name}.{col.get('name')}"
                                )

                    available_list = (
                        ", ".join(available_columns) if available_columns else "none"
                    )
                    return {
                        "error": f"Column '{target_column}' not found in PII results. Available PII columns: {available_list}"
                    }

                # Update scan summary with new results
                updated_scan_summary = scan_summary.copy()
                updated_scan_summary["results_detail"] = updated_results

                # Recalculate statistics
                remaining_tables_with_pii = sum(
                    1
                    for r in updated_results
                    if not r.get("error")
                    and not r.get("skipped")
                    and r.get("has_pii")
                    and not r.get("excluded")
                )
                remaining_pii_columns = sum(
                    len(r.get("pii_columns", []))
                    for r in updated_results
                    if not r.get("error")
                    and not r.get("skipped")
                    and r.get("has_pii")
                    and not r.get("excluded")
                )

                updated_scan_summary["tables_with_pii"] = remaining_tables_with_pii
                updated_scan_summary["total_pii_columns"] = remaining_pii_columns

                return {
                    "scan_summary": updated_scan_summary,
                    "modification_summary": f"Excluded column '{target_column}' from PII tagging. {remaining_pii_columns} PII columns remaining in {remaining_tables_with_pii} tables.",
                }
            else:
                return {"error": "No column specified for exclusion"}

        elif action == "change_semantic":
            target_table = modification.get("target_table")
            target_column = modification.get("target_column")
            new_semantic = modification.get("new_semantic")

            if target_column and new_semantic:
                # Find and change the semantic type of the column
                updated_results = []
                column_found = False
                for table_result in results_detail:
                    table_result = table_result.copy()
                    if (
                        target_table
                        and table_result.get("table_name", "").lower()
                        != target_table.lower()
                    ):
                        # Skip tables that don't match if table was specified
                        updated_results.append(table_result)
                        continue

                    # Check if this table has the target column
                    pii_columns = table_result.get("pii_columns", [])
                    updated_pii_columns = []

                    for col in pii_columns:
                        if col.get("name", "").lower() == target_column.lower():
                            # Found the column to update - change its semantic type
                            updated_col = col.copy()
                            updated_col["semantic"] = new_semantic
                            updated_pii_columns.append(updated_col)
                            column_found = True
                        else:
                            updated_pii_columns.append(col)

                    # Update the table result with modified column list
                    table_result["pii_columns"] = updated_pii_columns
                    updated_results.append(table_result)

                if not column_found:
                    available_columns = []
                    for table_result in results_detail:
                        if (
                            not table_result.get("error")
                            and not table_result.get("skipped")
                            and table_result.get("has_pii")
                        ):
                            table_name = table_result.get("table_name", "")
                            for col in table_result.get("pii_columns", []):
                                available_columns.append(
                                    f"{table_name}.{col.get('name')} ({col.get('semantic')})"
                                )

                    available_list = (
                        ", ".join(available_columns) if available_columns else "none"
                    )
                    return {
                        "error": f"Column '{target_column}' not found in PII results. Available PII columns: {available_list}"
                    }

                # Update scan summary with new results
                updated_scan_summary = scan_summary.copy()
                updated_scan_summary["results_detail"] = updated_results

                # Statistics remain the same (just changed semantic type, didn't add/remove columns)
                return {
                    "scan_summary": updated_scan_summary,
                    "modification_summary": f"Changed semantic type of column '{target_column}' to '{new_semantic}'.",
                }
            else:
                missing = []
                if not target_column:
                    missing.append("column name")
                if not new_semantic:
                    missing.append("new semantic type")
                return {
                    "error": f"Missing {' and '.join(missing)} for semantic type change"
                }

        else:
            reasoning = modification.get("reasoning", "Unknown request")
            return {
                "error": f"Could not understand the request: {reasoning}. You can try 'exclude [table_name]' to remove a table or 'proceed' to continue."
            }

    except Exception as e:
        return {"error": f"Error processing modification request: {str(e)}"}


DEFINITION = CommandDefinition(
    name="bulk-tag-pii",
    description="Scan schema for PII columns and bulk tag them with semantic tags after interactive confirmation",
    handler=handle_bulk_tag_pii,
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
            "description": "Optional: Skip interactive confirmation and proceed automatically. Default: false",
        },
    },
    required_params=[],
    supports_interactive_input=True,
    tui_aliases=["/tag-pii"],
    condensed_action="Bulk tagging PII columns",
    visible_to_user=True,
    visible_to_agent=True,
)
