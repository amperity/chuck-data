"""
Command for listing volumes in a Unity Catalog schema.
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
    List volumes in a Unity Catalog schema.

    Args:
        client: DatabricksAPIClient instance for API calls
        **kwargs: Command parameters
            - catalog_name: Name of the catalog containing the schema
            - schema_name: Name of the schema to list volumes from
            - include_browse: Whether to include volumes with selective metadata access (optional)

    Returns:
        CommandResult with list of volumes if successful
    """
    if not client:
        return CommandResult(
            False,
            message="No Databricks client available. Please set up your workspace first.",
        )

    # Extract parameters
    catalog_name = kwargs.get("catalog_name")
    schema_name = kwargs.get("schema_name")
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
        # List volumes in the schema
        result = client.list_volumes(
            catalog_name=catalog_name,
            schema_name=schema_name,
            include_browse=include_browse,
        )

        volumes = result.get("volumes", [])

        if not volumes:
            return CommandResult(
                True,
                message=f"No volumes found in schema '{catalog_name}.{schema_name}'.",
            )

        # Format volume information for display
        formatted_volumes = []
        for volume in volumes:
            formatted_volume = {
                "name": volume.get("name"),
                "full_name": volume.get("full_name"),
                "volume_type": volume.get("volume_type"),
                "comment": volume.get("comment", ""),
                "created_at": volume.get("created_at"),
                "created_by": volume.get("created_by", ""),
                "owner": volume.get("owner", ""),
            }
            formatted_volumes.append(formatted_volume)

        return CommandResult(
            True,
            data={
                "volumes": formatted_volumes,
                "total_count": len(formatted_volumes),
                "catalog_name": catalog_name,
                "schema_name": schema_name,
            },
            message=f"Found {len(formatted_volumes)} volume(s) in '{catalog_name}.{schema_name}'.",
        )
    except Exception as e:
        logging.error(f"Error listing volumes: {str(e)}")
        return CommandResult(
            False, message=f"Failed to list volumes: {str(e)}", error=e
        )


DEFINITION = CommandDefinition(
    name="list-volumes",
    description="List volumes in a Unity Catalog schema.",
    handler=handle_command,
    parameters={
        "catalog_name": {
            "type": "string",
            "description": "Name of the catalog containing the schema.",
        },
        "schema_name": {
            "type": "string",
            "description": "Name of the schema to list volumes from.",
        },
        "include_browse": {
            "type": "boolean",
            "description": "Whether to include volumes with selective metadata access.",
            "default": False,
        },
    },
    required_params=[],  # Not required anymore as we'll try to get from active config
    tui_aliases=["/list-volumes", "/volumes"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="full",  # Show full volume list in tables
    usage_hint="Usage: /list-volumes [--catalog_name <catalog>] [--schema_name <schema>] [--include_browse true|false]\n(Uses active catalog/schema if not specified)",
)
