"""
Command handler for displaying getting started tips and examples.

This module contains the handler for displaying helpful information
about how to use Chuck effectively.
"""

import logging
from typing import Optional

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.command_registry import CommandDefinition
from .base import CommandResult


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """
    Display getting started tips and examples to help users.

    Args:
        client: API client instance (not used by this handler)
        **kwargs: No parameters required
    """
    try:
        # Create content for the getting started message
        getting_started_text = """
[bold]Getting Started with Chuck[/bold]


[bold]1. Make sure you select a warehouse so that Chuck can run SQL.[/bold]
- [bold]/status[/bold] - Verify your current application context
- [bold]/list-warehouses[/bold] - View available SQL warehouses
- [bold]/select-warehouse <warehouse_id>[/bold] - Set the active SQL warehouse

[bold]2. Many commands use catalog and schema from your application context. Make sure you set them correctly.[/bold]
- [bold]/list-catalogs[/bold] - View available catalogs
- [bold]/catalog <catalog_name>[/bold] - Set the active catalog
- [bold]/list-schemas[/bold] - View schemas in the current catalog
- [bold]/schema <schema_name>[/bold] - Set the active schema
- [bold]/list-tables[/bold] - View tables in the current schema

[bold]3. Chuck is agentic so you do not need to worry about syntax. Try natural language prompts. Examples:[/bold]
- "What customer data is in my CATALOG_NAME catalog?"
- "Find and tag PII in Unity Catalog for my table CATALOG.SCHEMA.TABLE"
- "Run stitch on my tables in my catalog CATALOG_NAME and schema SCHEMA_NAME"

[bold]4. Chuck also has a library of direct commands if you want to be more explicit. Examples:[/bold]
- [bold]/run-sql "select * from CATALOG.SCHEMA.TABLE limit 10"[/bold]
- [bold]/scan-pii --catalog_name CATALOG --schema_name SCHEMA[/bold]

[bold]Need More Help?[/bold]
Type [bold]/help[/bold] for a complete list of commands
Type [bold]/discord[/bold] to join our discord community and ask experts!
"""
        return CommandResult(True, data={"getting_started_text": getting_started_text})
    except Exception as e:
        logging.error(f"Error generating getting started tips: {e}", exc_info=True)
        return CommandResult(
            False, error=e, message="Error generating tips and examples."
        )


DEFINITION = CommandDefinition(
    name="getting-started",
    description="Display helpful tips and examples for using Chuck",
    handler=handle_command,
    parameters={},
    required_params=[],
    tui_aliases=["/getting-started", "/examples", "/tips"],
    visible_to_user=True,
    visible_to_agent=True,
)
