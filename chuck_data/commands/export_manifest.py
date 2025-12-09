"""
Command handler for exporting Redshift manifests.

This module contains the handler for generating and exporting stitch-standalone
manifests for Redshift integration. The manifest contains semantic tags from
PII scans and Redshift connection configuration.
"""

import logging
from typing import Optional, Union

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.clients.redshift import RedshiftAPIClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import get_active_database, get_active_schema
from chuck_data.storage.manifest import (
    generate_manifest_from_scan,
    upload_manifest_to_s3,
    save_manifest_to_file,
    validate_manifest,
)
from chuck_data.commands.pii_tools import _helper_scan_schema_for_pii_logic
from chuck_data.llm.factory import LLMProviderFactory
from .base import CommandResult


def handle_command(
    client: Optional[Union[DatabricksAPIClient, RedshiftAPIClient]], **kwargs
) -> CommandResult:
    """
    Generate and export a Redshift manifest for stitch-standalone.

    This command:
    1. Scans tables for PII (or uses provided scan results)
    2. Generates manifest JSON with semantic tags
    3. Optionally uploads to S3 or saves to local file

    Args:
        client: RedshiftAPIClient instance
        **kwargs:
            database (str, optional): Redshift database name
            schema_name (str, optional): Redshift schema name
            output_file (str, optional): Local file path to save manifest
            s3_path (str, optional): S3 path to upload manifest
            s3_temp_dir (str): S3 temp directory for Spark-Redshift
            iam_role (str): IAM role ARN for Redshift
            redshift_host (str): Redshift cluster host
            redshift_port (int, optional): Redshift port (default: 5439)
            redshift_user (str): Redshift username
            redshift_password (str, optional): Redshift password
            aws_profile (str, optional): AWS profile for S3 upload
            scan_results (dict, optional): Pre-computed scan results
    """
    # Validate client is Redshift
    if not isinstance(client, RedshiftAPIClient):
        return CommandResult(
            False,
            message="export-manifest is only supported for Redshift. Use Databricks native tags for Unity Catalog.",
        )

    # Get database and schema
    database = kwargs.get("database") or get_active_database()
    schema_name = kwargs.get("schema_name") or get_active_schema()

    if not database or not schema_name:
        return CommandResult(
            False,
            message="Database and schema must be specified or active for manifest export.",
        )

    try:
        # Get or generate scan results
        scan_results = kwargs.get("scan_results")

        if not scan_results:
            # Scan for PII
            llm_client = LLMProviderFactory.create()
            scan_results = _helper_scan_schema_for_pii_logic(
                client, llm_client, database, schema_name, show_progress=True
            )

            if scan_results.get("error"):
                return CommandResult(
                    False,
                    message=f"PII scan failed: {scan_results['error']}",
                    data=scan_results,
                )

        # Check if any PII was found
        tables_with_pii = scan_results.get("tables_with_pii", 0)
        if tables_with_pii == 0:
            return CommandResult(
                False,
                message="No PII columns found - cannot generate manifest without semantic tags.",
                data={"scan_results": scan_results},
            )

        # Build Redshift config
        redshift_config = {
            "host": kwargs.get("redshift_host"),
            "port": kwargs.get("redshift_port", 5439),
            "database": database,
            "schema": schema_name,
            "user": kwargs.get("redshift_user"),
        }

        # Add password if provided
        if "redshift_password" in kwargs:
            redshift_config["password"] = kwargs["redshift_password"]

        # Validate required Redshift config
        required_fields = ["host", "user"]
        missing_fields = [f for f in required_fields if not redshift_config.get(f)]
        if missing_fields:
            return CommandResult(
                False,
                message=f"Missing required Redshift configuration: {', '.join(missing_fields)}",
            )

        # Build S3 config
        s3_config = {
            "temp_dir": kwargs.get("s3_temp_dir"),
            "iam_role": kwargs.get("iam_role"),
        }

        # Validate required S3 config
        if not s3_config["temp_dir"] or not s3_config["iam_role"]:
            return CommandResult(
                False,
                message="Both s3_temp_dir and iam_role are required for manifest generation.",
            )

        # Generate manifest
        manifest = generate_manifest_from_scan(scan_results, redshift_config, s3_config)

        # Validate manifest
        is_valid, error_msg = validate_manifest(manifest)
        if not is_valid:
            return CommandResult(
                False,
                message=f"Generated manifest is invalid: {error_msg}",
                data={"manifest": manifest},
            )

        # Determine output
        output_file = kwargs.get("output_file")
        s3_path = kwargs.get("s3_path")
        aws_profile = kwargs.get("aws_profile")

        results = {
            "manifest": manifest,
            "tables_count": len(manifest["tables"]),
            "total_pii_columns": scan_results.get("total_pii_columns", 0),
            "scan_results": scan_results,
        }

        # Save to file if requested
        if output_file:
            success = save_manifest_to_file(manifest, output_file)
            if not success:
                return CommandResult(
                    False,
                    message=f"Failed to save manifest to {output_file}",
                    data=results,
                )
            results["output_file"] = output_file

        # Upload to S3 if requested
        if s3_path:
            success = upload_manifest_to_s3(manifest, s3_path, aws_profile)
            if not success:
                return CommandResult(
                    False,
                    message=f"Failed to upload manifest to {s3_path}",
                    data=results,
                )
            results["s3_path"] = s3_path

        # Build success message
        msg_parts = [
            f"Generated manifest for {database}.{schema_name}",
            f"with {results['tables_count']} tables",
            f"and {results['total_pii_columns']} PII columns",
        ]

        if output_file:
            msg_parts.append(f"saved to {output_file}")

        if s3_path:
            msg_parts.append(f"uploaded to {s3_path}")

        message = " ".join(msg_parts) + "."

        return CommandResult(True, data=results, message=message)

    except Exception as e:
        logging.error(f"Manifest export error: {e}", exc_info=True)
        return CommandResult(
            False, error=e, message=f"Error during manifest export: {str(e)}"
        )


DEFINITION = CommandDefinition(
    name="export_manifest",
    description="Generate and export a Redshift manifest for stitch-standalone integration. The manifest contains semantic PII tags and connection configuration.",
    handler=handle_command,
    parameters={
        "database": {
            "type": "string",
            "description": "Redshift database name. If not provided, uses the active database",
        },
        "schema_name": {
            "type": "string",
            "description": "Redshift schema name. If not provided, uses the active schema",
        },
        "output_file": {
            "type": "string",
            "description": "Local file path to save manifest JSON (e.g., './manifest.json')",
        },
        "s3_path": {
            "type": "string",
            "description": "S3 path to upload manifest (e.g., 's3://bucket/path/manifest.json')",
        },
        "s3_temp_dir": {
            "type": "string",
            "description": "S3 temp directory for Spark-Redshift connector (e.g., 's3://bucket/temp/')",
        },
        "iam_role": {
            "type": "string",
            "description": "IAM role ARN for Redshift (e.g., 'arn:aws:iam::123:role/RedshiftRole')",
        },
        "redshift_host": {
            "type": "string",
            "description": "Redshift cluster host (e.g., 'cluster.region.redshift.amazonaws.com')",
        },
        "redshift_port": {
            "type": "integer",
            "description": "Redshift port (default: 5439)",
        },
        "redshift_user": {
            "type": "string",
            "description": "Redshift username",
        },
        "redshift_password": {
            "type": "string",
            "description": "Redshift password (optional if using IAM auth)",
        },
        "aws_profile": {
            "type": "string",
            "description": "AWS profile name for S3 upload (optional)",
        },
        "scan_results": {
            "type": "object",
            "description": "Pre-computed PII scan results (internal use)",
        },
    },
    required_params=["s3_temp_dir", "iam_role", "redshift_host", "redshift_user"],
    tui_aliases=["/export-manifest"],
    agent_display="full",
    condensed_action="Exporting Redshift manifest",
    visible_to_user=True,
    visible_to_agent=True,
    needs_api_client=True,
)
