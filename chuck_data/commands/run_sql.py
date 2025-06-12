"""
Command for executing SQL queries on a Databricks warehouse.
"""

from typing import Optional, Any, Dict
from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.commands.base import CommandResult
from chuck_data.config import get_warehouse_id, get_active_catalog
import logging


def handle_command(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """
    Execute a SQL query on a Databricks SQL warehouse.

    Args:
        client: DatabricksAPIClient instance for API calls
        **kwargs: Command parameters
            - warehouse_id: ID of the warehouse to run the query on
            - query: SQL query to execute
            - catalog: Optional catalog name to use
            - wait_timeout: How long to wait for query completion (default "30s")

    Returns:
        CommandResult with query results if successful
    """
    if not client:
        return CommandResult(
            False,
            message="No Databricks client available. Please set up your workspace first.",
        )

    # Extract parameters
    warehouse_id = kwargs.get("warehouse_id")
    query = kwargs.get("query")
    catalog = kwargs.get("catalog")
    wait_timeout = kwargs.get("wait_timeout", "30s")

    # Validate required parameters
    if not query:
        return CommandResult(
            False,
            message="No SQL query provided. Please specify a query to execute.",
        )

    # If warehouse_id not provided, try to use the configured warehouse
    if not warehouse_id:
        warehouse_id = get_warehouse_id()
        if not warehouse_id:
            return CommandResult(
                False,
                message="No warehouse ID specified and no active warehouse selected. Please provide a warehouse_id or select a warehouse first using /select-warehouse.",
            )

    # If catalog not provided, try to use active catalog
    if not catalog:
        catalog = get_active_catalog()
        # It's fine if catalog is None, the API will handle it

    try:
        # Execute the SQL query
        result = client.submit_sql_statement(
            sql_text=query,
            warehouse_id=warehouse_id,
            catalog=catalog,
            wait_timeout=wait_timeout,
        )

        # Check query status
        state = result.get("status", {}).get("state", result.get("state", ""))

        if state == "SUCCEEDED":
            result_data = result.get("result", {})
            data_array = result_data.get("data_array", [])
            external_links = result_data.get("external_links", [])
            manifest = result.get("manifest", {})

            # Extract column schema information
            column_infos = []
            schema_location = None

            # Try correct location: result.manifest.schema.columns (based on API response)
            if manifest.get("schema", {}).get("columns"):
                column_infos = manifest.get("schema", {}).get("columns", [])
                schema_location = "result.manifest.schema.columns"
            # Try secondary location: result_data.schema.columns
            elif result_data.get("schema", {}).get("columns"):
                column_infos = result_data.get("schema", {}).get("columns", [])
                schema_location = "result_data.schema.columns"
            # Try tertiary location: result_data.manifest.schema.columns
            elif result_data.get("manifest", {}).get("schema", {}).get("columns"):
                column_infos = (
                    result_data.get("manifest", {}).get("schema", {}).get("columns", [])
                )
                schema_location = "result_data.manifest.schema.columns"
            # Try direct columns in schema
            elif "schema" in result_data and isinstance(result_data["schema"], list):
                column_infos = result_data["schema"]
                schema_location = "result_data.schema (direct list)"
            # Try at top level
            elif result.get("schema", {}).get("columns"):
                column_infos = result.get("schema", {}).get("columns", [])
                schema_location = "result.schema.columns"

            # Extract column names
            columns = []
            if column_infos:
                columns = [
                    col.get("name") for col in column_infos if isinstance(col, dict)
                ]
                logging.debug(f"Found column schema at {schema_location}: {columns}")

            # Check if we have external links (large result set)
            if external_links:
                # Large result set - use external links for pagination
                total_row_count = manifest.get("total_row_count", 0)
                chunks = manifest.get("chunks", [])

                logging.info(
                    f"Large SQL result set detected: {total_row_count} total rows, {len(external_links)} chunks"
                )

                # If still no columns, create generic column names based on schema
                if not columns:
                    column_count = manifest.get("schema", {}).get("column_count", 0)
                    if column_count > 0:
                        columns = [f"column_{i+1}" for i in range(column_count)]
                        logging.warning(
                            f"No column names found, generated {len(columns)} generic column names"
                        )

                # Format the results for paginated display
                formatted_results = {
                    "columns": columns,
                    "external_links": external_links,
                    "manifest": manifest,
                    "total_row_count": total_row_count,
                    "chunks": chunks,
                    "execution_time_ms": result.get("execution_time_ms"),
                    "is_paginated": True,
                }
            else:
                # Small result set - traditional display with data_array
                # If still no columns but we have data, create generic column names
                if not columns and data_array:
                    first_row = data_array[0] if data_array else []
                    columns = [f"column_{i+1}" for i in range(len(first_row))]
                    logging.warning(
                        f"No column schema found in SQL result, generated {len(columns)} generic column names"
                    )

                # Format the results for display
                formatted_results = {
                    "columns": columns,
                    "rows": data_array,
                    "row_count": len(data_array),
                    "execution_time_ms": result.get("execution_time_ms"),
                    "is_paginated": False,
                }

            # Create enhanced result structure with both raw data and display formatting
            enhanced_result = {
                **formatted_results,  # Include all raw data fields
                "display_data": {
                    "formatted_table": _create_display_table(formatted_results),
                    "summary_info": {
                        "execution_time": formatted_results.get("execution_time_ms"),
                        "row_count": formatted_results.get(
                            "row_count", len(data_array)
                        ),
                        "column_count": len(formatted_results.get("columns", [])),
                    },
                },
            }

            return CommandResult(
                True,
                data=enhanced_result,
                message=f"Query executed successfully with {len(data_array)} result(s).",
            )
        elif state == "FAILED":
            error_message = (
                result.get("status", {})
                .get("error", {})
                .get("message", result.get("error", {}).get("message", "Unknown error"))
            )
            return CommandResult(
                False, message=f"Query execution failed: {error_message}"
            )
        elif state == "CANCELED":
            return CommandResult(False, message="Query execution was canceled.")
        else:
            return CommandResult(
                False,
                message=f"Query did not complete successfully. Final state: {state}",
            )
    except Exception as e:
        logging.error(f"Error executing SQL query: {str(e)}")
        return CommandResult(
            False, message=f"Failed to execute SQL query: {str(e)}", error=e
        )


def format_sql_results_for_agent(result: CommandResult) -> Dict[str, Any]:
    """
    Custom formatter for SQL results that provides consistent display.

    Since SQL results are already displayed via TUI when agent executes commands,
    this formatter provides a minimal success confirmation to avoid duplicate displays.

    Args:
        result: CommandResult containing SQL query results

    Returns:
        Dictionary with formatted results for agent consumption
    """
    if not result.success:
        return {"error": result.message or "SQL query failed"}

    if not result.data:
        return {
            "success": True,
            "message": result.message or "Query completed successfully",
            "results": "No data returned",
        }

    # Get basic result info for summary
    row_count = result.data.get("row_count", 0)
    execution_time = result.data.get("execution_time_ms")
    columns = result.data.get("columns", [])

    # For paginated results, get total count
    if result.data.get("is_paginated", False):
        row_count = result.data.get("total_row_count", row_count)

    # Return simple success confirmation since TUI handles the actual display
    return {
        "success": True,
        "message": result.message or "Query executed successfully",
        "summary": {
            "total_rows": row_count,
            "total_columns": len(columns),
            "execution_time_ms": execution_time,
        },
    }


def _create_display_table(formatted_results: Dict[str, Any]) -> str:
    """
    Create a formatted table string for display consistency between TUI and agent paths.

    Args:
        formatted_results: The formatted result data containing columns and rows

    Returns:
        String representation of the table for display
    """
    columns = formatted_results.get("columns", [])
    rows = formatted_results.get("rows", [])

    if not columns:
        return "No data to display"

    # Create table lines
    table_lines = []

    # Determine column widths dynamically
    col_widths = []
    for i, col in enumerate(columns):
        max_width = len(str(col))  # Start with header width
        # Check data widths (sample first 10 rows)
        sample_rows = rows[:10] if len(rows) > 10 else rows
        for row in sample_rows:
            if isinstance(row, list) and i < len(row):
                val_width = len(str(row[i] if row[i] is not None else ""))
                max_width = max(max_width, val_width)
        # Cap width at 25 characters for readability
        col_widths.append(min(max_width + 2, 25))

    # Add header
    header = " | ".join(str(col).ljust(col_widths[i]) for i, col in enumerate(columns))
    table_lines.append(header)
    table_lines.append("-" * len(header))

    # Add rows (limit to first 10 for readability)
    display_rows = rows[:10] if len(rows) > 10 else rows
    for row in display_rows:
        if isinstance(row, list):
            formatted_cells = []
            for i, val in enumerate(row[: len(columns)]):
                val_str = str(val if val is not None else "")
                # Truncate if too long
                if len(val_str) > col_widths[i] - 2:
                    val_str = val_str[: col_widths[i] - 5] + "..."
                formatted_cells.append(val_str.ljust(col_widths[i]))
            table_lines.append(" | ".join(formatted_cells))

    if len(rows) > 10:
        table_lines.append(f"\n... and {len(rows) - 10} more rows")

    return "\n".join(table_lines)


def _format_paginated_results_for_agent(result: CommandResult) -> Dict[str, Any]:
    """
    Format paginated SQL results for agent consumption.

    Since paginated results are displayed via TUI, this provides a simple success confirmation.
    """
    data = result.data
    columns = data.get("columns", [])
    total_row_count = data.get("total_row_count", 0)
    execution_time = data.get("execution_time_ms")

    return {
        "success": True,
        "message": result.message or "Large result set query executed successfully",
        "summary": {
            "total_rows": total_row_count,
            "total_columns": len(columns),
            "execution_time_ms": execution_time,
            "is_paginated": True,
            "note": "Large result set displayed in interactive table view.",
        },
    }


DEFINITION = CommandDefinition(
    name="run-sql",
    description="Execute a SQL query on a Databricks SQL warehouse.",
    handler=handle_command,
    parameters={
        "warehouse_id": {
            "type": "string",
            "description": "ID of the warehouse to run the query on.",
        },
        "query": {"type": "string", "description": "SQL query to execute."},
        "catalog": {
            "type": "string",
            "description": "Optional catalog name to use for the query.",
        },
        "wait_timeout": {
            "type": "string",
            "description": "How long to wait for query completion (e.g., '30s', '1m').",
            "default": "30s",
        },
    },
    required_params=[
        "query"
    ],  # Only query is required; warehouse_id can come from config
    tui_aliases=["/run-sql", "/sql"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="full",
    condensed_action="Running sql",
    output_formatter=format_sql_results_for_agent,
    usage_hint='Usage: /run-sql --query "SELECT * FROM my_table" [--warehouse_id <warehouse_id>] [--catalog <catalog>]\n(Uses active warehouse and catalog if not specified)',
)
