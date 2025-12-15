"""
Validation utilities for command handlers.

This module contains reusable validation functions for command parameters
and configurations, following the provider abstraction pattern.
"""

from typing import Dict, Any, List, Optional


def validate_single_target_params(
    catalog: Optional[str], schema: Optional[str]
) -> Dict[str, Any]:
    """Validate single target catalog and schema parameters.

    Args:
        catalog: Catalog name
        schema: Schema name

    Returns:
        Dict with 'valid' boolean and optional 'error' message
    """
    if not catalog or not schema:
        return {
            "valid": False,
            "error": "Target catalog and schema must be specified or active for Stitch setup.",
        }
    return {"valid": True}


def validate_multi_target_params(
    targets: List[str], output_catalog: Optional[str] = None
) -> Dict[str, Any]:
    """Validate multi-target parameters format.

    Args:
        targets: List of target strings in format "catalog.schema"
        output_catalog: Optional output catalog name

    Returns:
        Dict with:
        - 'valid': boolean
        - 'error': error message if invalid
        - 'target_locations': parsed target locations if valid
        - 'output_catalog': determined output catalog if valid
    """
    if not targets or not isinstance(targets, list):
        return {"valid": False, "error": "Targets must be a non-empty list"}

    target_locations = []
    for target in targets:
        if not isinstance(target, str):
            return {
                "valid": False,
                "error": f"Invalid target type: {type(target)}. Expected string.",
            }

        parts = target.split(".")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            return {
                "valid": False,
                "error": f"Invalid target format: '{target}'. Expected 'catalog.schema'",
            }

        target_locations.append({"catalog": parts[0], "schema": parts[1]})

    # Determine output catalog
    if not output_catalog:
        output_catalog = target_locations[0]["catalog"]

    return {
        "valid": True,
        "target_locations": target_locations,
        "output_catalog": output_catalog,
    }


def validate_stitch_config_structure(config: Dict[str, Any]) -> Dict[str, Any]:
    """Validate that a Stitch configuration has the required structure.

    Args:
        config: Stitch configuration dictionary

    Returns:
        Dict with 'valid' boolean and optional 'error' message
    """
    if not isinstance(config, dict):
        return {"valid": False, "error": "Configuration must be a dictionary"}

    # Check required top-level keys
    required_keys = ["name", "tables", "settings"]
    for key in required_keys:
        if key not in config:
            return {
                "valid": False,
                "error": f"Configuration missing required key: {key}",
            }

    # Validate tables structure
    if not isinstance(config["tables"], list):
        return {"valid": False, "error": "Configuration 'tables' must be a list"}

    # Validate each table
    for i, table in enumerate(config["tables"]):
        if not isinstance(table, dict):
            return {
                "valid": False,
                "error": f"Table at index {i} must be a dictionary",
            }

        if "path" not in table:
            return {"valid": False, "error": f"Table at index {i} missing 'path'"}

        if "fields" not in table:
            return {"valid": False, "error": f"Table at index {i} missing 'fields'"}

        if not isinstance(table["fields"], list):
            return {
                "valid": False,
                "error": f"Table at index {i} 'fields' must be a list",
            }

        # Validate each field
        for j, field in enumerate(table["fields"]):
            if not isinstance(field, dict):
                return {
                    "valid": False,
                    "error": f"Field at index {j} in table {i} must be a dictionary",
                }

            required_field_keys = ["field-name", "type", "semantics"]
            for fkey in required_field_keys:
                if fkey not in field:
                    return {
                        "valid": False,
                        "error": f"Field at index {j} in table {i} missing required key: {fkey}",
                    }

            if not isinstance(field["semantics"], list):
                return {
                    "valid": False,
                    "error": f"Field at index {j} in table {i} 'semantics' must be a list",
                }

    # Validate settings structure
    if not isinstance(config["settings"], dict):
        return {
            "valid": False,
            "error": "Configuration 'settings' must be a dictionary",
        }

    required_settings = ["output_catalog_name", "output_schema_name"]
    for key in required_settings:
        if key not in config["settings"]:
            return {
                "valid": False,
                "error": f"Configuration 'settings' missing required key: {key}",
            }

    return {"valid": True}


def validate_provider_required(
    data_provider: Optional[Any] = None, compute_provider: Optional[Any] = None
) -> Dict[str, Any]:
    """Validate that required providers are present.

    Args:
        data_provider: Optional data provider instance
        compute_provider: Optional compute provider instance

    Returns:
        Dict with 'valid' boolean and optional 'error' message
    """
    if data_provider is None:
        return {
            "valid": False,
            "error": "Data provider is required for Stitch setup.",
        }

    if compute_provider is None:
        return {
            "valid": False,
            "error": "Compute provider is required for Stitch setup.",
        }

    return {"valid": True}


def validate_amperity_token(token: Optional[str]) -> Dict[str, Any]:
    """Validate that Amperity token is present and non-empty.

    Args:
        token: Amperity authentication token

    Returns:
        Dict with 'valid' boolean and optional 'error' message
    """
    if not token or not isinstance(token, str) or not token.strip():
        return {
            "valid": False,
            "error": "Amperity token not found. Please run /amp_login first.",
        }

    return {"valid": True}
