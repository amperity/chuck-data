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
        # Parameter validation (always first)
        validation_result = _validate_parameters(client, **kwargs)
        if not validation_result.success:
            return validation_result

        # Route to appropriate handler based on execution mode
        interactive_input = kwargs.get("interactive_input")
        auto_confirm = kwargs.get("auto_confirm", False)

        if interactive_input:
            # Handle user input during interactive session
            return _handle_interactive_input(client, interactive_input, **kwargs)
        elif auto_confirm:
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

    # Phase 1: Scan for PII using actual scan-pii logic
    _report_progress(
        f"Scanning schema {catalog_name}.{schema_name} for PII columns",
        tool_output_callback,
    )

    try:
        # Create LLM client for PII scanning
        llm_client = LLMClient()

        # Use actual scan-pii logic from pii_tools
        scan_summary_data = _helper_scan_schema_for_pii_logic(
            client, llm_client, catalog_name, schema_name, show_progress=False
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


def _report_progress(step_message, tool_output_callback=None):
    """Report progress for agent integration."""
    if tool_output_callback:
        tool_output_callback("bulk-tag-pii", {"step": step_message})


def _start_interactive_mode(client, **kwargs):
    """Start interactive workflow."""
    # Minimal implementation to start failing tests
    return CommandResult(False, message="Not implemented yet")


def _handle_interactive_input(client, user_input, **kwargs):
    """Handle user input during interactive mode."""
    # Minimal implementation to start failing tests
    return CommandResult(False, message="Not implemented yet")


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
    tui_aliases=["/bulk-tag-pii"],
    agent_display="full",
    condensed_action="Bulk tagging PII columns",
    visible_to_user=True,
    visible_to_agent=True,
)
