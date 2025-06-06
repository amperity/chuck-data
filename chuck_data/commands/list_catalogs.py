"""
Command for listing Unity Catalog catalogs.
"""

from typing import Optional, Any
from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.commands.base import CommandResult
from chuck_data.catalogs import list_catalogs as get_catalogs_list
from chuck_data.command_registry import CommandDefinition
import logging


def handle_command(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """
    List catalogs in Unity Catalog.

    Args:
        client: DatabricksAPIClient instance for API calls
        **kwargs: Command parameters
            - display: bool, whether to display the table (default: False)
            - include_browse: Whether to include catalogs with selective metadata access (optional)
            - max_results: Maximum number of catalogs to return (optional)
            - page_token: Opaque pagination token to go to next page (optional)

    Returns:
        CommandResult with list of catalogs if successful
    """
    if not client:
        return CommandResult(
            False,
            message="No Databricks client available. Please set up your workspace first.",
        )

    # Check if display should be shown (default to False for agent calls)
    display = kwargs.get("display", False)

    # Get current catalog for highlighting
    from chuck_data.config import get_active_catalog

    current_catalog = get_active_catalog()

    # Extract parameters
    include_browse = kwargs.get("include_browse", False)
    max_results = kwargs.get("max_results")
    page_token = kwargs.get("page_token")

    try:
        # List catalogs in Unity Catalog
        result = get_catalogs_list(
            client=client,
            include_browse=include_browse,
            max_results=max_results,
            page_token=page_token,
        )

        catalogs = result.get("catalogs", [])
        next_page_token = result.get("next_page_token")

        if not catalogs:
            return CommandResult(
                True,
                message="No catalogs found in this workspace.",
                data={
                    "catalogs": [],
                    "total_count": 0,
                    "display": display,
                    "current_catalog": current_catalog,
                },
            )

        # Format catalog information for display
        formatted_catalogs = []
        for catalog in catalogs:
            formatted_catalog = {
                "name": catalog.get("name"),
                "type": catalog.get("type", ""),
                "comment": catalog.get("comment", ""),
                "provider": catalog.get("provider", {}).get("name", ""),
                "created_at": catalog.get("created_at"),
                "created_by": catalog.get("created_by", ""),
                "owner": catalog.get("owner", ""),
            }
            formatted_catalogs.append(formatted_catalog)

        return CommandResult(
            True,
            data={
                "catalogs": formatted_catalogs,
                "total_count": len(formatted_catalogs),
                "next_page_token": next_page_token,
                "display": display,  # Pass through to display logic
                "current_catalog": current_catalog,
            },
            message=f"Found {len(formatted_catalogs)} catalog(s)."
            + (
                f" More catalogs available with page token: {next_page_token}"
                if next_page_token
                else ""
            ),
        )
    except Exception as e:
        logging.error(f"Error listing catalogs: {str(e)}")
        return CommandResult(
            False, message=f"Failed to list catalogs: {str(e)}", error=e
        )


DEFINITION = CommandDefinition(
    name="list-catalogs",
    description="Lists all catalogs in the current workspace. By default returns data without showing table. Use display=true when user asks to see catalogs.",
    handler=handle_command,
    parameters={
        "display": {
            "type": "boolean",
            "description": "Whether to display the catalog table to the user (default: false). Set to true when user asks to see catalogs.",
        },
        "include_browse": {
            "type": "boolean",
            "description": "Whether to include catalogs with selective metadata access.",
            "default": False,
        },
        "max_results": {
            "type": "integer",
            "description": "Maximum number of catalogs to return.",
        },
        "page_token": {
            "type": "string",
            "description": "Opaque pagination token to go to next page.",
        },
    },
    required_params=[],
    tui_aliases=["/list-catalogs", "/catalogs"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="conditional",  # Use conditional display based on display parameter
    display_condition=lambda result: result.get(
        "display", False
    ),  # Show full table only when display=True
    condensed_action="Listing catalogs",  # Friendly name for condensed display
    usage_hint="Usage: /list-catalogs [--display true|false] [--include_browse true|false] [--max_results <number>] [--page_token <token>]",
)
