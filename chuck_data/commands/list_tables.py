"""
Command for listing tables in a Unity Catalog schema.
"""

from typing import Optional, Any
from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.commands.base import CommandResult
from chuck_data.config import get_active_catalog, get_active_schema
import logging


def handle_command(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """
    List tables in a Unity Catalog schema.

    Args:
        client: DatabricksAPIClient instance for API calls
        **kwargs: Command parameters
            - catalog_name: Name of the catalog containing the schema
            - schema_name: Name of the schema to list tables from
            - include_delta_metadata: Whether delta metadata should be included (optional)
            - omit_columns: Whether to omit columns from the response (optional)
            - include_browse: Whether to include tables with selective metadata access (optional)
            - display: bool, whether to display the table (default: False)

    Returns:
        CommandResult with list of tables if successful
    """
    if not client:
        return CommandResult(
            False,
            message="No Databricks client available. Please set up your workspace first.",
        )

    # Check if display should be shown (default to False for agent calls)
    display = kwargs.get("display", False)

    # Extract parameters
    catalog_name = kwargs.get("catalog_name")
    schema_name = kwargs.get("schema_name")
    include_delta_metadata = kwargs.get("include_delta_metadata", False)
    omit_columns = kwargs.get("omit_columns", False)
    include_browse = kwargs.get("include_browse", False)

    # If catalog_name not provided, try to use active catalog
    if not catalog_name:
        catalog_name = get_active_catalog()
        if not catalog_name:
            return CommandResult(
                False,
                message="No catalog specified and no active catalog selected. Please provide a catalog_name or select a catalog first using /select-catalog.",
            )

    # If schema_name not provided, try to use active schema
    if not schema_name:
        schema_name = get_active_schema()
        if not schema_name:
            return CommandResult(
                False,
                message="No schema specified and no active schema selected. Please provide a schema_name or select a schema first using /select-schema.",
            )

    try:
        # List tables in the schema
        result = client.list_tables(
            catalog_name=catalog_name,
            schema_name=schema_name,
            include_delta_metadata=include_delta_metadata,
            omit_columns=omit_columns,
            include_browse=include_browse,
        )

        tables = result.get("tables", [])

        if not tables:
            return CommandResult(
                True,
                message=f"No tables found in schema '{catalog_name}.{schema_name}'.",
                data={
                    "tables": [],
                    "total_count": 0,
                    "catalog_name": catalog_name,
                    "schema_name": schema_name,
                    "display": display,
                },
            )

        # Format table information for display
        formatted_tables = []
        for table in tables:
            # Extract key information
            table_info = {
                "name": table.get("name"),
                "full_name": table.get("full_name"),
                "table_type": table.get("table_type", ""),
                "data_source_format": table.get("data_source_format", ""),
                "comment": table.get("comment", ""),
                "created_at": table.get("created_at"),
                "updated_at": table.get("updated_at"),
                "created_by": table.get("created_by", ""),
                "owner": table.get("owner", ""),
                "row_count": table.get("properties", {}).get(
                    "spark.sql.statistics.numRows", "-"
                ),
                "size_bytes": table.get("properties", {}).get("size_bytes", "Unknown"),
            }

            # Include columns if available and not omitted
            if not omit_columns:
                columns = table.get("columns", [])
                table_info["column_count"] = len(columns)
                if columns:
                    column_list = []
                    for col in columns:
                        column_list.append(
                            {
                                "name": col.get("name"),
                                "type": col.get(
                                    "type_text", col.get("type", {}).get("name", "")
                                ),
                                "nullable": col.get("nullable", True),
                            }
                        )
                    table_info["columns"] = column_list

            formatted_tables.append(table_info)

        return CommandResult(
            True,
            data={
                "tables": formatted_tables,
                "total_count": len(formatted_tables),
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "display": display,
            },
            message=f"Found {len(formatted_tables)} table(s) in '{catalog_name}.{schema_name}'.",
        )
    except Exception as e:
        logging.error(f"Error listing tables: {str(e)}")
        return CommandResult(False, message=f"Failed to list tables: {str(e)}", error=e)


DEFINITION = CommandDefinition(
    name="list-tables",
    description="List tables in a Unity Catalog schema. By default returns data without showing table. Use display=true when user asks to see tables.",
    handler=handle_command,
    parameters={
        "catalog_name": {
            "type": "string",
            "description": "Name of the catalog containing the schema.",
        },
        "schema_name": {
            "type": "string",
            "description": "Name of the schema to list tables from.",
        },
        "include_delta_metadata": {
            "type": "boolean",
            "description": "Whether delta metadata should be included.",
            "default": False,
        },
        "omit_columns": {
            "type": "boolean",
            "description": "Whether to omit columns from the response.",
            "default": False,
        },
        "include_browse": {
            "type": "boolean",
            "description": "Whether to include tables with selective metadata access.",
            "default": False,
        },
        "display": {
            "type": "boolean",
            "description": "Whether to display the table list to the user (default: false). Set to true when user asks to see tables.",
        },
    },
    required_params=[],  # Not required anymore as we'll try to get them from active config
    tui_aliases=["/list-tables", "/tables"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="conditional",  # Use conditional display based on display parameter
    display_condition=lambda result: result.get(
        "display", False
    ),  # Show full table only when display=True
    condensed_action="Listing tables",  # Friendly name for condensed display
    usage_hint="Usage: /list-tables [--catalog_name <catalog>] [--schema_name <schema>] [--display true|false]\n(Uses active catalog/schema if not specified)",
)
