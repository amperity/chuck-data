"""
Manifest generation for Redshift Stitch integration.

This module generates manifest JSON files compatible with the stitch-standalone
Redshift integration. The manifest contains semantic tags for PII columns and
Redshift connection configuration.

Manifest format matches the specification in:
app/service/stitch/stitch-standalone/doc/redshift-integration.md
"""

import json
import logging
from typing import Dict, Any, List, Optional
import boto3
from botocore.exceptions import ClientError


def generate_manifest_from_scan(
    scan_results: Dict[str, Any],
    redshift_config: Dict[str, str],
    s3_config: Dict[str, str],
) -> Dict[str, Any]:
    """
    Generate a stitch-standalone manifest from PII scan results.

    Args:
        scan_results: Results from _helper_scan_schema_for_pii_logic
            Expected structure:
            {
                "catalog": "database_name",
                "schema": "schema_name",
                "results_detail": [
                    {
                        "table_name": "customers",
                        "full_name": "dev.public.customers",
                        "has_pii": True,
                        "pii_columns": [
                            {"name": "email", "type": "varchar", "semantic": "email"},
                            {"name": "phone", "type": "varchar", "semantic": "phone"}
                        ],
                        "columns": [...]  # All columns with tags
                    }
                ]
            }

        redshift_config: Redshift connection configuration
            For Serverless with IAM auth:
            {
                "database": "dev",
                "schema": "public"
            }
            For provisioned clusters with username/password:
            {
                "host": "cluster.region.redshift.amazonaws.com",
                "port": 5439,
                "database": "dev",
                "schema": "public",
                "user": "username",
                "password": "password"  # optional
            }

        s3_config: S3 configuration for Spark-Redshift connector
            {
                "temp_dir": "s3://bucket/temp/",
                "iam_role": "arn:aws:iam::123456789:role/RedshiftRole"
            }

    Returns:
        Manifest dictionary in stitch-standalone format
    """
    tables = []

    # Extract results detail from scan
    results_detail = scan_results.get("results_detail", [])

    for table_result in results_detail:
        # Skip tables with errors, skipped tables, or tables without PII
        if (
            table_result.get("error")
            or table_result.get("skipped")
            or not table_result.get("has_pii")
        ):
            continue

        table_name = table_result.get("table_name")
        columns = table_result.get("columns", [])

        if not table_name:
            logging.warning(f"Skipping table with no name: {table_result}")
            continue

        # Build field list with semantic tags
        fields = []
        for col in columns:
            col_name = col.get("name")
            col_type = col.get("type", "string")
            semantic = col.get("semantic")

            if not col_name:
                continue

            # Build semantics list
            semantics = []
            if semantic:
                semantics.append(semantic)
                # Add general 'pii' marker for all PII columns
                if "pii" not in semantics:
                    semantics.append("pii")

            fields.append(
                {
                    "field-name": col_name,
                    "type": _normalize_type(col_type),
                    "semantics": semantics,
                }
            )

        if fields:  # Only add table if it has fields
            tables.append({"path": table_name, "fields": fields})

    # Build the complete manifest
    manifest = {
        "tables": tables,
        "settings": {
            "redshift_config": {
                "host": redshift_config.get("host"),
                "port": redshift_config.get("port", 5439),
                "database": redshift_config.get("database"),
                "schema": redshift_config.get("schema"),
                "user": redshift_config.get("user"),
            },
            "s3_temp_dir": s3_config.get("temp_dir"),
            "redshift_iam_role": s3_config.get("iam_role"),
        },
    }

    # Add password if present (optional if using IAM auth)
    if "password" in redshift_config:
        manifest["settings"]["redshift_config"]["password"] = redshift_config[
            "password"
        ]

    return manifest


def _normalize_type(type_str: str) -> str:
    """
    Normalize database types to simple types for manifest.

    Maps database-specific types to generic types used by Spark.
    """
    type_lower = type_str.lower()

    # String types
    if any(t in type_lower for t in ["varchar", "char", "text", "string"]):
        return "string"

    # Integer types
    if any(t in type_lower for t in ["int", "integer", "smallint", "bigint"]):
        return "long"

    # Decimal types
    if any(t in type_lower for t in ["decimal", "numeric"]):
        return "decimal"

    # Float types
    if any(t in type_lower for t in ["float", "double", "real"]):
        return "double"

    # Boolean
    if "bool" in type_lower:
        return "boolean"

    # Date/Time types
    if "date" in type_lower:
        return "date"
    if "timestamp" in type_lower:
        return "timestamp"

    # Default to string for unknown types
    return "string"


def upload_manifest_to_s3(
    manifest: Dict[str, Any], s3_path: str, aws_profile: Optional[str] = None
) -> bool:
    """
    Upload manifest JSON to S3.

    Args:
        manifest: Manifest dictionary to upload
        s3_path: Full S3 path (e.g., "s3://bucket/path/manifest.json")
        aws_profile: Optional AWS profile name to use

    Returns:
        True if upload successful, False otherwise
    """
    try:
        # Parse S3 path
        if not s3_path.startswith("s3://"):
            logging.error(f"Invalid S3 path: {s3_path}. Must start with 's3://'")
            return False

        path_parts = s3_path[5:].split("/", 1)
        if len(path_parts) != 2:
            logging.error(f"Invalid S3 path format: {s3_path}")
            return False

        bucket_name = path_parts[0]
        object_key = path_parts[1]

        # Create S3 client
        session_kwargs = {}
        if aws_profile:
            session_kwargs["profile_name"] = aws_profile

        session = boto3.Session(**session_kwargs)
        s3_client = session.client("s3")

        # Convert manifest to JSON
        manifest_json = json.dumps(manifest, indent=2)

        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=manifest_json.encode("utf-8"),
            ContentType="application/json",
        )

        logging.info(f"Successfully uploaded manifest to {s3_path}")
        return True

    except ClientError as e:
        logging.error(f"Failed to upload manifest to S3: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error uploading manifest: {e}", exc_info=True)
        return False


def validate_manifest(manifest: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Validate manifest structure.

    Args:
        manifest: Manifest dictionary to validate

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check top-level structure
    if "tables" not in manifest:
        return False, "Missing 'tables' key in manifest"

    if "settings" not in manifest:
        return False, "Missing 'settings' key in manifest"

    # Check settings
    settings = manifest["settings"]
    if "redshift_config" not in settings:
        return False, "Missing 'redshift_config' in settings"

    if "s3_temp_dir" not in settings:
        return False, "Missing 's3_temp_dir' in settings"

    if "redshift_iam_role" not in settings:
        return False, "Missing 'redshift_iam_role' in settings"

    # Check redshift_config
    redshift_config = settings["redshift_config"]
    # Only database and schema are truly required
    # host/user are optional (not needed for Serverless with IAM auth)
    required_fields = ["database", "schema"]
    for field in required_fields:
        if field not in redshift_config:
            return False, f"Missing '{field}' in redshift_config"

    # Check tables structure
    tables = manifest["tables"]
    if not isinstance(tables, list):
        return False, "'tables' must be a list"

    for i, table in enumerate(tables):
        if "path" not in table:
            return False, f"Table {i} missing 'path' field"

        if "fields" not in table:
            return (
                False,
                f"Table {i} ({table.get('path', 'unknown')}) missing 'fields' field",
            )

        fields = table["fields"]
        if not isinstance(fields, list):
            return (
                False,
                f"Table {i} ({table.get('path', 'unknown')}) 'fields' must be a list",
            )

        for j, field in enumerate(fields):
            if "field-name" not in field:
                return False, f"Table {i} field {j} missing 'field-name'"

            if "type" not in field:
                return (
                    False,
                    f"Table {i} field {j} ({field.get('field-name', 'unknown')}) missing 'type'",
                )

            if "semantics" not in field:
                return (
                    False,
                    f"Table {i} field {j} ({field.get('field-name', 'unknown')}) missing 'semantics'",
                )

            if not isinstance(field["semantics"], list):
                return (
                    False,
                    f"Table {i} field {j} ({field.get('field-name', 'unknown')}) 'semantics' must be a list",
                )

    return True, None


def save_manifest_to_file(manifest: Dict[str, Any], file_path: str) -> bool:
    """
    Save manifest to a local JSON file.

    Args:
        manifest: Manifest dictionary to save
        file_path: Local file path to save to

    Returns:
        True if save successful, False otherwise
    """
    try:
        with open(file_path, "w") as f:
            json.dump(manifest, f, indent=2)

        logging.info(f"Successfully saved manifest to {file_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to save manifest to file: {e}", exc_info=True)
        return False


def load_manifest_from_file(file_path: str) -> Optional[Dict[str, Any]]:
    """
    Load manifest from a local JSON file.

    Args:
        file_path: Local file path to load from

    Returns:
        Manifest dictionary or None if load failed
    """
    try:
        with open(file_path, "r") as f:
            manifest = json.load(f)

        logging.info(f"Successfully loaded manifest from {file_path}")
        return manifest
    except Exception as e:
        logging.error(f"Failed to load manifest from file: {e}", exc_info=True)
        return None
