"""
Command for listing schemas in a Unity Catalog catalog.
"""

from typing import Optional, Any
from src.clients.databricks import DatabricksAPIClient
from src.commands.base import CommandResult
from src.catalogs import list_schemas as get_schemas_list
from src.config import get_active_catalog, get_active_schema
from src.command_registry import CommandDefinition
import logging


def handle_command(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """
    Lists all schemas in the current catalog.

    Args:
        client: DatabricksAPIClient instance for API calls
        **kwargs:
            - display: bool, whether to display the table (default: False)
            - catalog_name: Name of the catalog to list schemas from (optional, uses active catalog if not provided)
            - include_browse: Whether to include schemas with selective metadata access (optional)
            - max_results: Maximum number of schemas to return (optional)
            - page_token: Opaque pagination token to go to next page (optional)

    Returns:
        CommandResult with list of schemas if successful
    """
    if not client:
        return CommandResult(
            False,
            message="No Databricks client available. Please set up your workspace first.",
        )

    # Extract parameters
    display = kwargs.get("display", False)
    catalog_name = kwargs.get("catalog_name")
    include_browse = kwargs.get("include_browse", False)
    max_results = kwargs.get("max_results")
    page_token = kwargs.get("page_token")

    # Get current schema for highlighting
    current_schema = get_active_schema()

    # If catalog_name not provided, try to use active catalog
    if not catalog_name:
        catalog_name = get_active_catalog()
        if not catalog_name:
            return CommandResult(
                False,
                message="No catalog specified and no active catalog selected. Please provide a catalog_name or select a catalog first.",
            )

    try:
        # List schemas in the catalog
        result = get_schemas_list(
            client=client,
            catalog_name=catalog_name,
            include_browse=include_browse,
            max_results=max_results,
            page_token=page_token,
        )

        schemas = result.get("schemas", [])
        next_page_token = result.get("next_page_token")

        if not schemas:
            return CommandResult(
                True,
                message=f"No schemas found in catalog '{catalog_name}'.",
                data={
                    "schemas": [],
                    "total_count": 0,
                    "display": display,
                    "current_schema": current_schema,
                    "catalog_name": catalog_name,
                },
            )

        # Format schema information for display
        formatted_schemas = []
        for schema in schemas:
            formatted_schema = {
                "name": schema.get("name"),
                "full_name": schema.get("full_name"),
                "catalog_name": schema.get("catalog_name"),
                "comment": schema.get("comment", ""),
                "created_at": schema.get("created_at"),
                "created_by": schema.get("created_by", ""),
                "owner": schema.get("owner", ""),
            }
            formatted_schemas.append(formatted_schema)

        return CommandResult(
            True,
            data={
                "schemas": formatted_schemas,
                "total_count": len(formatted_schemas),
                "display": display,  # Pass through to display logic
                "current_schema": current_schema,
                "catalog_name": catalog_name,
                "next_page_token": next_page_token,
            },
            message=f"Found {len(formatted_schemas)} schema(s) in catalog '{catalog_name}'."
            + (
                f" More schemas available with page token: {next_page_token}"
                if next_page_token
                else ""
            ),
        )
    except Exception as e:
        logging.error(f"Error listing schemas: {str(e)}")
        return CommandResult(
            False, message=f"Failed to list schemas: {str(e)}", error=e
        )


DEFINITION = CommandDefinition(
    name="list-schemas",
    description="Lists all schemas in the current catalog. By default returns data without showing table. Use display=true when user asks to see schemas.",
    handler=handle_command,
    parameters={
        "display": {
            "type": "boolean",
            "description": "Whether to display the schema table to the user (default: false). Set to true when user asks to see schemas.",
        },
        "catalog_name": {
            "type": "string",
            "description": "Name of the catalog to list schemas from (uses active catalog if not provided).",
        },
        "include_browse": {
            "type": "boolean",
            "description": "Whether to include schemas with selective metadata access.",
            "default": False,
        },
        "max_results": {
            "type": "integer",
            "description": "Maximum number of schemas to return.",
        },
        "page_token": {
            "type": "string",
            "description": "Opaque pagination token to go to next page.",
        },
    },
    required_params=[],
    tui_aliases=["/list-schemas", "/schemas"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="conditional",  # Use conditional display based on display parameter
    display_condition=lambda result: result.get(
        "display", False
    ),  # Show full table only when display=True
    condensed_action="Listing schemas",  # Friendly name for condensed display
    usage_hint="Usage: /list-schemas [--display true|false] [--catalog_name <catalog>]",
)
