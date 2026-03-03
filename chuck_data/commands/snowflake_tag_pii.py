"""
Command for applying PII semantic tags to Snowflake table columns.

Snowflake does not expose native column tags through the Spark connector, so tags
are stored in the chuck_metadata.semantic_tags metadata table (same pattern as
Redshift). The SnowflakeProviderAdapter.tag_columns() handles this transparently.
"""

import logging
from typing import Optional, Any

from chuck_data.clients.snowflake import SnowflakeAPIClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.commands.base import CommandResult
from chuck_data.config import get_active_database, get_active_schema
from chuck_data.data_providers.adapters import SnowflakeProviderAdapter


def handle_command(
    client: Optional[SnowflakeAPIClient], **kwargs: Any
) -> CommandResult:
    """
    Apply semantic PII tags to columns in a Snowflake table.

    Tags are stored in chuck_metadata.semantic_tags (not as native Snowflake tags)
    because native object tags are not visible through the Spark connector used by
    stitch-standalone.

    Args:
        client: SnowflakeAPIClient instance
        **kwargs:
            table (str): Table name to tag
            column (str): Column name to tag
            semantic_type (str): Semantic type (e.g. 'pii/email', 'pii/phone')
            database (str, optional): Database (uses active database if not provided)
            schema (str, optional): Schema (uses active schema if not provided)
    """
    if not client:
        return CommandResult(False, message="No Snowflake client available.")

    table = kwargs.get("table")
    column = kwargs.get("column")
    semantic_type = kwargs.get("semantic_type")

    if not table or not column or not semantic_type:
        return CommandResult(
            False,
            message="table, column, and semantic_type are all required.",
        )

    database = kwargs.get("database") or get_active_database()
    schema = kwargs.get("schema") or get_active_schema()

    if not schema:
        return CommandResult(
            False,
            message="No schema specified and no active schema set. Use /select-schema first.",
        )

    try:
        # Build a SnowflakeProviderAdapter around the existing client so we can
        # call the standard tag_columns() interface (metadata table approach).
        adapter = SnowflakeProviderAdapter.__new__(SnowflakeProviderAdapter)
        adapter.client = client

        result = adapter.tag_columns(
            tags=[{"table": table, "column": column, "semantic_type": semantic_type}],
            catalog=database,
            schema=schema,
        )

        if result.get("success"):
            return CommandResult(
                True,
                message=(
                    f"Tagged column '{column}' in '{table}' as '{semantic_type}'. "
                    f"Stored in chuck_metadata.semantic_tags."
                ),
                data=result,
            )
        else:
            errors = result.get("errors", [])
            error_msg = (
                errors[0].get("error", "Unknown error") if errors else "Unknown error"
            )
            return CommandResult(
                False, message=f"Failed to apply tag: {error_msg}", data=result
            )

    except Exception as e:
        logging.error(f"Error tagging Snowflake column: {e}", exc_info=True)
        return CommandResult(False, error=e, message=str(e))


DEFINITION = CommandDefinition(
    name="snowflake_tag_pii",
    description=(
        "Apply a semantic PII tag to a column in a Snowflake table. "
        "Tags are stored in chuck_metadata.semantic_tags and used by stitch-standalone "
        "during identity resolution."
    ),
    handler=handle_command,
    parameters={
        "table": {"type": "string", "description": "Table name to tag"},
        "column": {"type": "string", "description": "Column name to tag"},
        "semantic_type": {
            "type": "string",
            "description": "Semantic type (e.g. 'pii/email', 'pii/phone', 'pk')",
        },
        "database": {
            "type": "string",
            "description": "Database name (uses active database if not provided)",
        },
        "schema": {
            "type": "string",
            "description": "Schema name (uses active schema if not provided)",
        },
    },
    required_params=["table", "column", "semantic_type"],
    tui_aliases=["/tag-pii", "/sf-tag-pii"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="condensed",
    condensed_action="Tagging Snowflake column:",
    usage_hint="Usage: /tag-pii --table <table> --column <col> --semantic_type <type>",
    provider="snowflake",
)
