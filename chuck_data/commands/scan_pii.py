"""
Command handler for bulk PII scanning.

This module contains the handler for scanning all tables in a schema
for Personally Identifiable Information (PII).
"""

import logging
from typing import Optional

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.llm.client import LLMClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import get_active_catalog, get_active_schema
from .base import CommandResult
from .pii_tools import _helper_scan_schema_for_pii_logic


def handle_command(client: Optional[DatabricksAPIClient], **kwargs) -> CommandResult:
    """
    Scan all tables in a schema for PII data.

    Args:
        client: API client instance
        **kwargs:
            catalog_name (str, optional): Name of the catalog
            schema_name (str, optional): Name of the schema
    """
    catalog_name_arg: Optional[str] = kwargs.get("catalog_name")
    schema_name_arg: Optional[str] = kwargs.get("schema_name")

    if not client:
        return CommandResult(False, message="Client is required for bulk PII scan.")

    try:
        effective_catalog = catalog_name_arg or get_active_catalog()
        effective_schema = schema_name_arg or get_active_schema()

        if not effective_catalog or not effective_schema:
            return CommandResult(
                False,
                message="Catalog and schema must be specified or active for bulk PII scan.",
            )

        # Create a LLM client instance to pass to the helper
        llm_client = LLMClient()

        scan_summary_data = _helper_scan_schema_for_pii_logic(
            client, llm_client, effective_catalog, effective_schema
        )
        if scan_summary_data.get("error"):
            return CommandResult(
                False, message=scan_summary_data["error"], data=scan_summary_data
            )

        msg = (
            f"Scanned {scan_summary_data.get('tables_successfully_processed',0)}/"
            f"{scan_summary_data.get('tables_scanned_attempted',0)} tables in {effective_catalog}.{effective_schema}. "
            f"Found {scan_summary_data.get('tables_with_pii',0)} tables with {scan_summary_data.get('total_pii_columns',0)} PII columns."
        )
        return CommandResult(True, data=scan_summary_data, message=msg)
    except Exception as e:
        logging.error(f"Bulk PII scan error: {e}", exc_info=True)
        return CommandResult(
            False, error=e, message=f"Error during bulk PII scan: {str(e)}"
        )


DEFINITION = CommandDefinition(
    name="scan-schema-for-pii",
    description="Scan all tables in the current schema (or specified catalog/schema) for PII and/or customer data",
    handler=handle_command,
    parameters={
        "catalog_name": {
            "type": "string",
            "description": "Optional: Name of the catalog. If not provided, uses the active catalog",
        },
        "schema_name": {
            "type": "string",
            "description": "Optional: Name of the schema. If not provided, uses the active schema",
        },
    },
    required_params=[],
    tui_aliases=["/scan-pii"],
    visible_to_user=True,
    visible_to_agent=True,
)
