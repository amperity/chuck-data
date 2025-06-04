"""
Command for showing details of a specific Unity Catalog schema.
"""

from typing import Optional, Any
from ..clients.databricks import DatabricksAPIClient
from ..commands.base import CommandResult
from ..catalogs import get_schema as get_schema_details
from ..config import get_active_catalog
from ..command_registry import CommandDefinition
import logging


def handle_command(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """
    Get details of a specific schema from Unity Catalog.

    Args:
        client: DatabricksAPIClient instance for API calls
        **kwargs: Command parameters
            - name: Name of the schema to get details for
            - catalog_name: Name of the catalog containing the schema (optional, uses active catalog if not provided)

    Returns:
        CommandResult with schema details if successful
    """
    if not client:
        return CommandResult(
            False,
            message="No Databricks client available. Please set up your workspace first.",
        )

    # Extract parameters
    schema_name = kwargs.get("name")
    catalog_name = kwargs.get("catalog_name")

    # If catalog_name not provided, try to use active catalog
    if not catalog_name:
        catalog_name = get_active_catalog()
        if not catalog_name:
            return CommandResult(
                False,
                message="No catalog specified and no active catalog selected. Please provide a catalog_name or select a catalog first.",
            )

    try:
        # Construct full name for schema lookup
        full_name = f"{catalog_name}.{schema_name}"

        # Get schema details
        schema = get_schema_details(client, full_name)

        if not schema:
            return CommandResult(
                False,
                message=f"Schema '{schema_name}' not found in catalog '{catalog_name}'.",
            )

        return CommandResult(
            True, data=schema, message=f"Schema details for '{full_name}'."
        )
    except Exception as e:
        logging.error(f"Error getting schema details: {str(e)}")
        return CommandResult(
            False, message=f"Failed to get schema details: {str(e)}", error=e
        )


DEFINITION = CommandDefinition(
    name="schema",
    description="Show details of a specific Unity Catalog schema. Used to retrieve schema information such as columns, types, and metadata.",
    handler=handle_command,
    parameters={
        "name": {
            "type": "string",
            "description": "Name of the schema to get details for.",
        },
        "catalog_name": {
            "type": "string",
            "description": "Name of the catalog containing the schema (uses active catalog if not provided).",
        },
    },
    required_params=["name"],
    tui_aliases=["/schema", "/schema-details"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="full",  # Show full schema details to agents
    usage_hint="Usage: /schema --name <schema_name> [--catalog_name <catalog_name>]",
)
