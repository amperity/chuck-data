"""
Cluster init script upload helper for stitch setup.

This module contains the upload logic for cluster init scripts with versioning
to prevent conflicts between concurrent stitch runs.
"""

import logging
import datetime
from typing import Dict, Any

from src.clients.databricks import DatabricksAPIClient


def _helper_upload_cluster_init_logic(
    client: DatabricksAPIClient,
    target_catalog: str,
    target_schema: str,
    init_script_content: str,
) -> Dict[str, Any]:
    """Internal logic for uploading cluster init script."""
    if not target_catalog or not target_schema:
        return {
            "error": "Target catalog and schema are required for cluster init upload."
        }

    if not init_script_content.strip():
        return {"error": "Init script content cannot be empty."}

    # Step 1: Check/Create "chuck" volume (same as stitch)
    volume_name = "chuck"
    volume_exists = False

    # Check if volume exists
    try:
        volumes_response = client.list_volumes(
            catalog_name=target_catalog, schema_name=target_schema
        )
        for volume_info in volumes_response.get("volumes", []):
            if volume_info.get("name") == volume_name:
                volume_exists = True
                break
    except Exception as e:
        return {"error": f"Failed to list volumes: {str(e)}"}

    if not volume_exists:
        logging.debug(
            f"Volume '{volume_name}' not found in {target_catalog}.{target_schema}. Attempting to create."
        )
        try:
            volume_response = client.create_volume(
                catalog_name=target_catalog, schema_name=target_schema, name=volume_name
            )
            if not volume_response:
                return {"error": f"Failed to create volume '{volume_name}'"}
            logging.debug(f"Volume '{volume_name}' created successfully.")
        except Exception as e:
            return {"error": f"Failed to create volume '{volume_name}': {str(e)}"}

    # Step 2: Generate timestamped filename (matching stitch pattern)
    current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
    init_script_filename = f"cluster_init-{current_datetime}.sh"

    # Step 3: Upload the init script to the volume
    volume_path = f"/Volumes/{target_catalog}/{target_schema}/{volume_name}/{init_script_filename}"

    try:
        # Upload file to volume
        upload_response = client.upload_file(
            path=volume_path, content=init_script_content, overwrite=True
        )
        if not upload_response:
            return {"error": f"Failed to upload init script to {volume_path}"}

        logging.info(f"Cluster init script uploaded successfully to {volume_path}")

        return {
            "success": True,
            "volume_path": volume_path,
            "filename": init_script_filename,
            "timestamp": current_datetime,
        }

    except Exception as e:
        return {"error": f"Failed to upload init script: {str(e)}"}
