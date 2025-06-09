"""
Command handler for PII column tagging.

This module contains the handler for applying semantic tags to columns
containing Personally Identifiable Information (PII) in a table
using SQL commands. It applies tags to columns identified by the scan_pii command
rather than performing its own PII scanning.
"""

import logging
from typing import Optional, Dict, Any, List

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import get_warehouse_id, get_active_catalog, get_active_schema
from .base import CommandResult

# No need to import PII logic for scanning, we just apply the tags


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """
    Apply PII semantic tags to columns in a table using SQL.

    This command doesn't perform PII scanning itself - it applies the tags that were
    identified by the scan_pii command.

    Args:
        client: API client instance
        **kwargs:
            table_name (str): Name of the table to tag
            pii_columns (list): List of columns with PII semantic info
    """
    table_name: str = kwargs.get("table_name")
    pii_columns: List[Dict[str, Any]] = kwargs.get("pii_columns", [])

    if not table_name:
        return CommandResult(False, message="table_name parameter is required.")
    if not pii_columns:
        return CommandResult(
            False, message="pii_columns parameter is required with columns to tag."
        )
    if not client:
        return CommandResult(False, message="Client is required for PII tagging.")

    try:
        # Get warehouse ID from config for SQL execution
        warehouse_id = get_warehouse_id()
        if not warehouse_id:
            return CommandResult(
                False,
                message="No warehouse ID configured. Use /warehouse command to select a SQL warehouse first.",
            )

        # Get active catalog and schema from config if needed for table name resolution
        catalog_name = get_active_catalog()
        schema_name = get_active_schema()

        # Resolve the full table name
        full_table_name = table_name
        if "." not in table_name:
            if not catalog_name or not schema_name:
                return CommandResult(
                    False,
                    message="No active catalog and schema selected. Use /catalog and /schema commands first, or provide a fully qualified table name.",
                )
            full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

        # Validate the table exists
        try:
            table_info = client.get_table(full_name=full_table_name)
            if not table_info:
                return CommandResult(
                    False, message=f"Table {full_table_name} not found."
                )

            # Extract the actual full name from the table info
            full_table_name = table_info.get("full_name", full_table_name)
            table_name_only = full_table_name.split(".")[-1]
            column_count = len(table_info.get("columns", []))

        except Exception as e:
            return CommandResult(
                False, message=f"Failed to retrieve table details: {str(e)}"
            )

        # Apply tags for each provided PII column
        tagging_results = apply_semantic_tags(
            client, full_table_name, pii_columns, warehouse_id
        )

        # Prepare the result dictionary with table info
        pii_result_dict = {
            "table_name": table_name_only,
            "full_name": full_table_name,
            "column_count": column_count,
            "pii_column_count": len(pii_columns),
            "pii_columns": pii_columns,
            "tagging_results": tagging_results,
        }

        successfully_tagged = sum(1 for r in tagging_results if r.get("success", False))

        msg = f"Applied semantic tags to {successfully_tagged} of {len(pii_columns)} columns in {table_name_only}"
        return CommandResult(True, data=pii_result_dict, message=msg)
    except Exception as e:
        logging.error(f"handle_tag_pii error for '{table_name}': {e}", exc_info=True)
        return CommandResult(
            False, error=e, message=f"Unexpected error in PII tagging: {str(e)}"
        )


def apply_semantic_tags(
    client: DatabricksAPIClient,
    full_table_name: str,
    pii_columns: List[Dict[str, Any]],
    warehouse_id: str,
) -> List[Dict[str, Any]]:
    """
    Apply semantic tags to columns using SQL ALTER TABLE statements.

    Args:
        client: DatabricksAPIClient instance
        full_table_name: Full qualified table name (catalog.schema.table)
        pii_columns: List of columns with semantic tag information
        warehouse_id: ID of the SQL warehouse to execute statements

    Returns:
        List of result dictionaries for each tagging operation
    """
    tagging_results = []

    for column in pii_columns:
        column_name = column.get("name")
        semantic_type = column.get("semantic")

        if not column_name or not semantic_type:
            tagging_results.append(
                {
                    "column": column_name or "unknown",
                    "success": False,
                    "error": "Missing column name or semantic type",
                }
            )
            continue

        # Construct and execute the SQL ALTER TABLE statement
        sql = f"""
        ALTER TABLE {full_table_name} 
        ALTER COLUMN {column_name} 
        SET TAGS ('semantic' = '{semantic_type}')
        """

        try:
            logging.info(f"Applying tag '{semantic_type}' to column '{column_name}'")
            result = client.submit_sql_statement(
                sql_text=sql, warehouse_id=warehouse_id, wait_timeout="30s"
            )

            if result.get("status", {}).get("state") == "SUCCEEDED":
                tagging_results.append(
                    {
                        "column": column_name,
                        "semantic_type": semantic_type,
                        "success": True,
                    }
                )
            else:
                error = (
                    result.get("status", {})
                    .get("error", {})
                    .get("message", "Unknown error")
                )
                tagging_results.append(
                    {
                        "column": column_name,
                        "semantic_type": semantic_type,
                        "success": False,
                        "error": error,
                    }
                )
        except Exception as e:
            logging.error(f"Error applying tag to {column_name}: {str(e)}")
            tagging_results.append(
                {
                    "column": column_name,
                    "semantic_type": semantic_type,
                    "success": False,
                    "error": str(e),
                }
            )

    return tagging_results


DEFINITION = CommandDefinition(
    name="tag-pii-columns",
    description="Apply semantic tags to columns identified by the scan_pii command",
    handler=handle_command,
    parameters={
        "table_name": {
            "type": "string",
            "description": "Name of the table to tag (can be fully qualified or just the table name)",
        },
        "pii_columns": {
            "type": "array",
            "description": "List of columns with PII information in format [{'name': 'colname', 'semantic': 'pii-type'}]",
        },
    },
    required_params=["table_name", "pii_columns"],
    tui_aliases=["/tag-pii-columns"],
    visible_to_user=True,
    visible_to_agent=True,
    usage_hint='Example: /tag-pii --table_name my_table --pii_columns \'[{"name": "email", "semantic": "email"}]\'',
)
