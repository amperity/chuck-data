"""
Command for showing details of a specific Unity Catalog table.
"""

from typing import Optional, Any
from ..clients.databricks import DatabricksAPIClient
from ..commands.base import CommandResult
from ..catalogs import get_table as get_table_details
from ..config import get_active_catalog, get_active_schema
from ..command_registry import CommandDefinition
import logging


def handle_command(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """
    Get details of a specific table from Unity Catalog.

    Args:
        client: DatabricksAPIClient instance for API calls
        **kwargs: Command parameters
            - name: Name of the table to get details for
            - schema_name: Name of the schema containing the table (optional, uses active schema if not provided)
            - catalog_name: Name of the catalog containing the schema (optional, uses active catalog if not provided)
            - include_delta_metadata: Whether delta metadata should be included (optional)

    Returns:
        CommandResult with table details if successful
    """
    if not client:
        return CommandResult(
            False,
            message="No Databricks client available. Please set up your workspace first.",
        )

    # Extract parameters
    table_name = kwargs.get("name")
    schema_name = kwargs.get("schema_name")
    catalog_name = kwargs.get("catalog_name")
    include_delta_metadata = kwargs.get("include_delta_metadata", False)

    # If schema_name not provided, try to use active schema
    if not schema_name:
        schema_name = get_active_schema()
        if not schema_name:
            return CommandResult(
                False,
                message="No schema specified and no active schema selected. Please provide a schema_name or select a schema first.",
            )

    # If catalog_name not provided, try to use active catalog
    if not catalog_name:
        catalog_name = get_active_catalog()
        if not catalog_name:
            return CommandResult(
                False,
                message="No catalog specified and no active catalog selected. Please provide a catalog_name or select a catalog first.",
            )

    try:
        # Construct full name for table lookup
        full_name = f"{catalog_name}.{schema_name}.{table_name}"

        # Get table details
        table = get_table_details(
            client, full_name, include_delta_metadata=include_delta_metadata
        )

        if not table:
            return CommandResult(
                False,
                message=f"Table '{table_name}' not found in schema '{catalog_name}.{schema_name}'.",
            )

        # Extract column information in a user-friendly format
        columns = table.get("columns", [])
        formatted_columns = []
        for col in columns:
            formatted_column = {
                "name": col.get("name"),
                "type": col.get("type_text", col.get("type", {}).get("name", "")),
                "nullable": col.get("nullable", True),
                "comment": col.get("comment", ""),
                "position": col.get("position"),
            }
            formatted_columns.append(formatted_column)

        # Create a separate user-friendly table structure
        table_info = {
            "name": table.get("name"),
            "full_name": table.get("full_name"),
            "table_type": table.get("table_type", ""),
            "data_source_format": table.get("data_source_format", ""),
            "columns": formatted_columns,
            "column_count": len(formatted_columns),
            "comment": table.get("comment", ""),
            "created_at": table.get("created_at"),
            "created_by": table.get("created_by", ""),
            "owner": table.get("owner", ""),
            "properties": table.get("properties", {}),
        }

        # Include delta table details if available and requested
        if include_delta_metadata and "delta" in table:
            table_info["delta"] = table.get("delta", {})

        return CommandResult(
            True,
            data={
                "table": table_info,
                "raw_table": table,  # Include the raw table data as well for completeness
            },
            message=f"Table details for '{full_name}' ({len(formatted_columns)} columns).",
        )
    except Exception as e:
        logging.error(f"Error getting table details: {str(e)}")
        return CommandResult(
            False, message=f"Failed to get table details: {str(e)}", error=e
        )


DEFINITION = CommandDefinition(
    name="table",
    description="Show details of a specific Unity Catalog table.",
    handler=handle_command,
    parameters={
        "name": {
            "type": "string",
            "description": "Name of the table to get details for.",
        },
        "schema_name": {
            "type": "string",
            "description": "Name of the schema containing the table (uses active schema if not provided).",
        },
        "catalog_name": {
            "type": "string",
            "description": "Name of the catalog containing the schema (uses active catalog if not provided).",
        },
        "include_delta_metadata": {
            "type": "boolean",
            "description": "Whether Delta Lake metadata should be included.",
            "default": False,
        },
    },
    required_params=["name"],
    tui_aliases=["/table", "/show-table"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="full",  # Show full table details to agents
    usage_hint="Usage: /table --name <table_name> [--schema_name <schema_name>] [--catalog_name <catalog_name>] [--include_delta_metadata true|false]",
)
