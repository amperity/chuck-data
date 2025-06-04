"""
Command handler for adding a Stitch Report notebook.

This module contains the handler for creating a Stitch report notebook
based on a unified table.
"""

import logging
from typing import Optional, Any

from ..clients.databricks import DatabricksAPIClient
from ..command_registry import CommandDefinition
from ..metrics_collector import get_metrics_collector
from .base import CommandResult


def handle_command(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """
    Create a Stitch report notebook for a unified table.

    Args:
        client: API client instance
        **kwargs:
            table_path (str): Fully qualified table path in format catalog.schema.table
            name (str, optional): Optional custom name for the notebook
    """
    table_path: Optional[str] = kwargs.get("table_path")
    custom_name: Optional[str] = kwargs.get("name")
    rest_args: Optional[str] = kwargs.get("rest")

    # If rest is provided, use it as the notebook name (overrides name parameter)
    if rest_args:
        custom_name = rest_args

    if not client:
        return CommandResult(
            False, message="Client is required for creating Stitch report notebooks."
        )

    try:
        if not table_path:
            return CommandResult(
                False,
                message="Table path must be provided in format 'catalog.schema.table'.",
            )

        # Validate the table path format
        path_parts = table_path.split(".")
        if len(path_parts) != 3:
            return CommandResult(
                False,
                message="Table path must be in the format 'catalog.schema.table'.",
            )

        # Get metrics collector
        metrics_collector = get_metrics_collector()

        try:
            # Call the create_stitch_notebook function with optional name
            result = client.create_stitch_notebook(table_path, custom_name)

            # Track successful event
            metrics_collector.track_event(
                prompt="add-stitch-report command",
                tools=[
                    {
                        "name": "add-stitch-report",
                        "arguments": {
                            "table_path": table_path,
                            "notebook_name": custom_name,
                        },
                    }
                ],
                additional_data={
                    "event_context": "direct_add-stitch-report_command",
                    "status": "success",
                    "notebook_path": result.get("path", ""),
                },
            )

            return CommandResult(
                True,
                data=result,
                message=f"Successfully created Stitch report notebook at {result.get('path')}",
            )
        except Exception as e:
            # Track error event
            metrics_collector.track_event(
                prompt="add-stitch-report command",
                tools=[
                    {
                        "name": "add-stitch-report",
                        "arguments": {
                            "table_path": table_path,
                            "notebook_name": custom_name,
                        },
                    }
                ],
                error=str(e),
                additional_data={
                    "event_context": "direct_add-stitch-report_command",
                    "status": "error",
                },
            )
            raise

    except Exception as e:
        logging.error(f"Error creating Stitch report: {e}", exc_info=True)
        return CommandResult(
            False, error=e, message=f"Error creating Stitch report: {str(e)}"
        )


DEFINITION = CommandDefinition(
    name="add-stitch-report",
    description="Create a Stitch report notebook for a unified table",
    handler=handle_command,
    parameters={
        "table_path": {
            "type": "string",
            "description": "Fully qualified table path in format 'catalog.schema.table'",
        },
        "name": {
            "type": "string",
            "description": "Optional: Custom name for the notebook",
        },
        "rest": {
            "type": "string",
            "description": "Additional arguments will be treated as part of the notebook name",
        },
    },
    required_params=["table_path"],
    tui_aliases=["/add-stitch-report"],
    visible_to_user=True,
    visible_to_agent=True,
    needs_api_client=True,
    usage_hint="/add-stitch-report catalog.schema.table [notebook name]",
)
