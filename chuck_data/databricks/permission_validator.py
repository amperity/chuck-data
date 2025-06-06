"""
Module for validating Databricks API token permissions.
Provides functions to check access levels for different Databricks resources.
"""

import logging
from typing import Dict, Any


def validate_all_permissions(client) -> Dict[str, Dict[str, Any]]:
    """
    Run all permission checks and return detailed results.

    Args:
        client: API client instance with token

    Returns:
        Dict of permission check results by resource area
    """
    results = {
        "basic_connectivity": check_basic_connectivity(client),
        "unity_catalog": check_unity_catalog(client),
        "sql_warehouse": check_sql_warehouse(client),
        "jobs": check_jobs(client),
        "models": check_models(client),
        "volumes": check_volumes(client),
    }

    return results


def check_basic_connectivity(client):
    """
    Check basic API connectivity using identity API.
    """
    try:
        response = client.get("/api/2.0/preview/scim/v2/Me")
        username = response.get("userName", "unknown")
        return {
            "authorized": True,
            "details": f"Connected as {username}",
            "api_path": "/api/2.0/preview/scim/v2/Me",
        }
    except Exception as e:
        logging.debug(f"Basic connectivity check failed: {e}")
        return {
            "authorized": False,
            "error": str(e),
            "api_path": "/api/2.0/preview/scim/v2/Me",
        }


def check_unity_catalog(client):
    """
    Check Unity Catalog access permission.
    """
    try:
        # Try listing catalogs with minimal results
        response = client.get("/api/2.1/unity-catalog/catalogs?max_results=1")
        catalog_count = len(response.get("catalogs", []))
        return {
            "authorized": True,
            "details": f"Unity Catalog access granted ({catalog_count} catalogs visible)",
            "api_path": "/api/2.1/unity-catalog/catalogs",
        }
    except Exception as e:
        logging.debug(f"Unity Catalog check failed: {e}")
        return {
            "authorized": False,
            "error": str(e),
            "api_path": "/api/2.1/unity-catalog/catalogs",
        }


def check_sql_warehouse(client):
    """
    Check SQL warehouse access permission.
    """
    try:
        response = client.get("/api/2.0/sql/warehouses?page_size=1")
        warehouse_count = len(response.get("warehouses", []))
        return {
            "authorized": True,
            "details": f"SQL Warehouse access granted ({warehouse_count} warehouses visible)",
            "api_path": "/api/2.0/sql/warehouses",
        }
    except Exception as e:
        logging.debug(f"SQL Warehouse check failed: {e}")
        return {
            "authorized": False,
            "error": str(e),
            "api_path": "/api/2.0/sql/warehouses",
        }


def check_jobs(client):
    """
    Check Jobs access permission.
    """
    try:
        response = client.get("/api/2.1/jobs/list?limit=1")
        job_count = len(response.get("jobs", []))
        return {
            "authorized": True,
            "details": f"Jobs access granted ({job_count} jobs visible)",
            "api_path": "/api/2.1/jobs/list",
        }
    except Exception as e:
        logging.debug(f"Jobs check failed: {e}")
        return {"authorized": False, "error": str(e), "api_path": "/api/2.1/jobs/list"}


def check_models(client):
    """
    Check ML models access permission.
    """
    try:
        response = client.get("/api/2.0/mlflow/registered-models/list?max_results=1")
        model_count = len(response.get("registered_models", []))
        return {
            "authorized": True,
            "details": f"ML Models access granted ({model_count} models visible)",
            "api_path": "/api/2.0/mlflow/registered-models/list",
        }
    except Exception as e:
        logging.debug(f"Models check failed: {e}")
        return {
            "authorized": False,
            "error": str(e),
            "api_path": "/api/2.0/mlflow/registered-models/list",
        }


def check_volumes(client):
    """
    Check Volumes access permission in Unity Catalog.
    """
    # For volumes, we need a catalog and schema
    try:
        # First get a catalog
        catalog_response = client.get("/api/2.1/unity-catalog/catalogs?max_results=1")
        catalogs = catalog_response.get("catalogs", [])

        if not catalogs:
            return {
                "authorized": False,
                "error": "No catalogs available to check volumes access",
                "api_path": "/api/2.1/unity-catalog/volumes",
            }

        catalog_name = catalogs[0].get("name")

        # Then get a schema
        schema_response = client.get(
            f"/api/2.1/unity-catalog/schemas?catalog_name={catalog_name}&max_results=1"
        )
        schemas = schema_response.get("schemas", [])

        if not schemas:
            return {
                "authorized": False,
                "error": f"No schemas available in catalog '{catalog_name}' to check volumes access",
                "api_path": "/api/2.1/unity-catalog/volumes",
            }

        schema_name = schemas[0].get("name")

        # Now check volumes
        volume_response = client.get(
            f"/api/2.1/unity-catalog/volumes?catalog_name={catalog_name}&schema_name={schema_name}"
        )
        volume_count = len(volume_response.get("volumes", []))

        return {
            "authorized": True,
            "details": f"Volumes access granted in {catalog_name}.{schema_name} ({volume_count} volumes visible)",
            "api_path": "/api/2.1/unity-catalog/volumes",
        }
    except Exception as e:
        logging.debug(f"Volumes check failed: {e}")
        return {
            "authorized": False,
            "error": str(e),
            "api_path": "/api/2.1/unity-catalog/volumes",
        }
