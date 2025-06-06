"""
Command for creating a new volume in Unity Catalog.
"""

from typing import Optional, Any
from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.commands.base import CommandResult
from chuck_data.config import get_active_catalog, get_active_schema
from chuck_data.command_registry import CommandDefinition
import logging


def handle_command(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """
    Create a new volume in Unity Catalog.

    Args:
        client: DatabricksAPIClient instance for API calls
        **kwargs: Command parameters
            - catalog_name: Name of the catalog where the volume will be created
            - schema_name: Name of the schema where the volume will be created
            - name: Name for the new volume
            - volume_type: Type of volume to create (default: "MANAGED")

    Returns:
        CommandResult with created volume details if successful
    """
    if not client:
        return CommandResult(
            False,
            message="No Databricks client available. Please set up your workspace first.",
        )

    # Extract parameters
    catalog_name = kwargs.get("catalog_name")
    schema_name = kwargs.get("schema_name")
    name = kwargs.get("name")
    volume_type = kwargs.get("volume_type", "MANAGED")

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
        # Create the volume
        result = client.create_volume(
            catalog_name=catalog_name,
            schema_name=schema_name,
            name=name,
            volume_type=volume_type,
        )

        return CommandResult(
            True,
            data=result,
            message=f"Successfully created volume '{catalog_name}.{schema_name}.{name}' of type '{volume_type}'.",
        )
    except Exception as e:
        logging.error(f"Error creating volume: {str(e)}")
        return CommandResult(
            False, message=f"Failed to create volume: {str(e)}", error=e
        )


DEFINITION = CommandDefinition(
    name="create-volume",
    description="Create a new volume in Unity Catalog.",
    handler=handle_command,
    parameters={
        "catalog_name": {
            "type": "string",
            "description": "Name of the catalog where the volume will be created.",
        },
        "schema_name": {
            "type": "string",
            "description": "Name of the schema where the volume will be created.",
        },
        "name": {"type": "string", "description": "Name for the new volume."},
        "volume_type": {
            "type": "string",
            "description": "Type of volume to create (MANAGED or EXTERNAL).",
            "default": "MANAGED",
        },
    },
    required_params=[
        "name"
    ],  # Only name is required; catalog and schema can come from active config
    tui_aliases=["/create-volume"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    usage_hint="Usage: /create-volume --name <volume_name> [--catalog_name <catalog>] [--schema_name <schema>] [--volume_type MANAGED|EXTERNAL]\n(Uses active catalog/schema if not specified)",
)
