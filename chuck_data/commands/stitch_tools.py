"""
Stitch integration helper functions for command handlers.

This module contains utilities for setting up Stitch integration
with Databricks catalogs and schemas.
"""

import logging
import json
import datetime
from typing import Dict, Any, List, Optional

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.llm.provider import LLMProvider
from chuck_data.config import get_amperity_token
from .pii_tools import _helper_scan_schema_for_pii_logic
from .cluster_init_tools import _helper_upload_cluster_init_logic

UNSUPPORTED_TYPES = [
    "INTERVAL",
    "VOID",
    "ARRAY",
    "MAP",
    "STRUCT",
    "VARIANT",
    "OBJECT",
    "GEOGRAPHY",
    "GEOMETRY",
]

# Numeric types that don't support semantic tags in Stitch
NUMERIC_TYPES = [
    "LONG",
    "BIGINT",
    "INT",
    "INTEGER",
    "SMALLINT",
    "TINYINT",
    "DOUBLE",
    "FLOAT",
    "DECIMAL",
    "NUMERIC",
]


def validate_multi_location_access(
    client: DatabricksAPIClient, locations: List[Dict[str, str]]
) -> List[Dict[str, Any]]:
    """Check read permissions on all target locations before scanning.

    Args:
        client: DatabricksAPIClient instance
        locations: List of {"catalog": "...", "schema": "..."} dictionaries

    Returns:
        List of results with structure:
        [{"location": {"catalog": "...", "schema": "..."}, "accessible": bool, "error": str}]
    """
    results = []
    for loc in locations:
        catalog = loc.get("catalog")
        schema = loc.get("schema")

        if not catalog or not schema:
            results.append(
                {
                    "location": loc,
                    "accessible": False,
                    "error": "Missing catalog or schema in location specification",
                }
            )
            continue

        try:
            # Try to get schema to verify access
            client.get_schema(f"{catalog}.{schema}")
            results.append({"location": loc, "accessible": True})
        except Exception as e:
            results.append({"location": loc, "accessible": False, "error": str(e)})
    return results


def _helper_setup_stitch_logic(
    client: DatabricksAPIClient,
    llm_client_instance: LLMProvider,
    target_catalog: str,
    target_schema: str,
) -> Dict[str, Any]:
    """Legacy function for backward compatibility. Calls prepare phase only.

    IMPORTANT: This has been modified to only run the preparation phase and not
    automatically launch the job, which is now handled by the interactive flow.
    """
    # Phase 1: Prepare config only
    prep_result = _helper_prepare_stitch_config(
        client, llm_client_instance, target_catalog, target_schema
    )
    if prep_result.get("error"):
        return prep_result

    # Return the prepared config for further processing
    # No longer automatically launching the job
    return prep_result


def _helper_prepare_stitch_config(
    client: DatabricksAPIClient,
    llm_client_instance: LLMProvider,
    target_catalog: Optional[str] = None,
    target_schema: Optional[str] = None,
    target_locations: Optional[List[Dict[str, str]]] = None,
    output_catalog: Optional[str] = None,
) -> Dict[str, Any]:
    """Phase 1: Prepare Stitch configuration without launching job.

    Supports both single-location (backward compatible) and multi-location modes.

    Args:
        target_catalog: Single target catalog (backward compatible)
        target_schema: Single target schema (backward compatible)
        target_locations: List of {"catalog": "...", "schema": "..."} for multi-target
        output_catalog: Catalog for outputs (defaults to first target's catalog)

    Returns:
        Dictionary with success/error status, stitch_config, and metadata
    """
    # Backward compatibility: single catalog/schema
    if target_catalog and target_schema and not target_locations:
        target_locations = [{"catalog": target_catalog, "schema": target_schema}]
        output_catalog = output_catalog or target_catalog

    # Multi-location path
    if target_locations:
        if not output_catalog:
            output_catalog = target_locations[0]["catalog"]

        return _helper_prepare_multi_location_stitch_config(
            client, llm_client_instance, target_locations, output_catalog
        )

    # Original single-location logic (fallback for legacy calls)
    if not target_catalog or not target_schema:
        return {"error": "Target catalog and schema are required for Stitch setup."}

    # Step 1: Scan for PII data (using the helper for this logic)
    pii_scan_output = _helper_scan_schema_for_pii_logic(
        client, llm_client_instance, target_catalog, target_schema
    )
    if pii_scan_output.get("error"):
        return {
            "error": f"PII Scan failed during Stitch setup: {pii_scan_output['error']}"
        }

    # Step 2: Check/Create "chuck" volume
    volume_name = "chuck"
    volume_exists = False

    # Check if volume exists - direct API call
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
            # Direct API call to create volume
            volume_response = client.create_volume(
                catalog_name=target_catalog, schema_name=target_schema, name=volume_name
            )
            if not volume_response:
                return {"error": f"Failed to create volume '{volume_name}'"}
            logging.debug(f"Volume '{volume_name}' created successfully.")
        except Exception as e:
            return {"error": f"Failed to create volume '{volume_name}': {str(e)}"}

    # Step 3: Generate Stitch configuration
    current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
    stitch_job_name = f"stitch-{current_datetime}"
    stitch_config = {
        "name": stitch_job_name,
        "tables": [],
        "settings": {
            "output_catalog_name": target_catalog,
            "output_schema_name": "stitch_outputs",
        },
    }

    # Track unsupported columns for user feedback
    unsupported_columns = []

    for table_pii_data in pii_scan_output.get("results_detail", []):
        if (
            table_pii_data.get("error")
            or table_pii_data.get("skipped")
            or not table_pii_data.get("has_pii")
        ):
            continue  # Only include successfully scanned tables with PII

        table_cfg = {"path": table_pii_data["full_name"], "fields": []}
        table_unsupported = []

        for col_data in table_pii_data.get("columns", []):
            if col_data["type"] not in UNSUPPORTED_TYPES:
                field_cfg = {
                    "field-name": col_data["name"],
                    "type": col_data["type"],
                    "semantics": [],
                }
                # Only add semantics for non-numeric types (Stitch doesn't support semantics on LONG, etc.)
                if (
                    col_data.get("semantic")
                    and col_data["type"].upper() not in NUMERIC_TYPES
                ):
                    field_cfg["semantics"].append(col_data["semantic"])
                table_cfg["fields"].append(field_cfg)
            else:
                # Track unsupported column
                table_unsupported.append(
                    {
                        "column": col_data["name"],
                        "type": col_data["type"],
                        "semantic": col_data.get("semantic"),
                    }
                )

        # Add unsupported columns for this table if any
        if table_unsupported:
            unsupported_columns.append(
                {"table": table_pii_data["full_name"], "columns": table_unsupported}
            )

        # Only add table if it has at least one supported field
        if table_cfg["fields"]:
            stitch_config["tables"].append(table_cfg)

    if not stitch_config["tables"]:
        return {
            "error": "No tables with PII found to include in Stitch configuration.",
            "pii_scan_output": pii_scan_output,
        }

    # Step 4: Prepare file paths and get Amperity token
    config_file_path = f"/Volumes/{target_catalog}/{target_schema}/{volume_name}/{stitch_job_name}.json"
    init_script_volume_path = (
        f"/Volumes/{target_catalog}/{target_schema}/{volume_name}/cluster_init.sh"
    )

    amperity_token = get_amperity_token()
    if not amperity_token:
        return {"error": "Amperity token not found. Please run /amp_login first."}

    # Fetch init script content and job-id from Amperity API
    try:
        init_script_data = client.fetch_amperity_job_init(amperity_token)
        init_script_content = init_script_data.get("cluster-init")
        job_id = init_script_data.get("job-id")

        if not init_script_content:
            return {"error": "Failed to get cluster init script from Amperity API."}
        if not job_id:
            return {"error": "Failed to get job-id from Amperity API."}
    except Exception as e_fetch_init:
        logging.error(
            f"Error fetching Amperity init script: {e_fetch_init}", exc_info=True
        )
        return {"error": f"Error fetching Amperity init script: {str(e_fetch_init)}"}

    # Upload cluster init script with versioning
    upload_result = _helper_upload_cluster_init_logic(
        client=client,
        target_catalog=target_catalog,
        target_schema=target_schema,
        init_script_content=init_script_content,
    )
    if upload_result.get("error"):
        return upload_result

    # Use the versioned init script path
    init_script_volume_path = upload_result["volume_path"]
    logging.debug(
        f"Versioned cluster init script uploaded to {init_script_volume_path}"
    )

    return {
        "success": True,
        "stitch_config": stitch_config,
        "metadata": {
            "target_catalog": target_catalog,
            "target_schema": target_schema,
            "volume_name": volume_name,
            "stitch_job_name": stitch_job_name,
            "config_file_path": config_file_path,
            "init_script_path": init_script_volume_path,
            "init_script_content": init_script_content,
            "amperity_token": amperity_token,
            "job_id": job_id,
            "pii_scan_output": pii_scan_output,
            "unsupported_columns": unsupported_columns,
        },
    }


def _helper_prepare_multi_location_stitch_config(
    client: DatabricksAPIClient,
    llm_client_instance: LLMProvider,
    target_locations: List[Dict[str, str]],
    output_catalog: str,
) -> Dict[str, Any]:
    """Scan multiple catalog/schema locations for PII and create unified Stitch config.

    Args:
        client: DatabricksAPIClient instance
        llm_client_instance: LLMProvider instance for PII scanning
        target_locations: List of {"catalog": "...", "schema": "..."} to scan
        output_catalog: Catalog where outputs and volume will be stored

    Returns:
        Dictionary with success/error status, stitch_config, and metadata
    """
    # Validate access to all locations first
    access_results = validate_multi_location_access(client, target_locations)
    accessible_locations = [r for r in access_results if r["accessible"]]

    if not accessible_locations:
        inaccessible_details = [
            f"{r['location']['catalog']}.{r['location']['schema']}: {r.get('error', 'unknown error')}"
            for r in access_results
            if not r["accessible"]
        ]
        return {
            "error": f"No accessible locations found to scan. Issues: {'; '.join(inaccessible_details)}"
        }

    # Aggregate PII results from all locations
    all_pii_results = []
    scan_summary = []

    for location_result in accessible_locations:
        location = location_result["location"]
        catalog = location["catalog"]
        schema = location["schema"]

        logging.info(f"Scanning {catalog}.{schema} for PII...")

        pii_scan = _helper_scan_schema_for_pii_logic(
            client, llm_client_instance, catalog, schema
        )

        if pii_scan.get("error"):
            logging.warning(f"Failed to scan {catalog}.{schema}: {pii_scan['error']}")
            scan_summary.append(
                {
                    "location": f"{catalog}.{schema}",
                    "status": "error",
                    "error": pii_scan["error"],
                }
            )
            continue

        results = pii_scan.get("results_detail", [])
        all_pii_results.extend(results)

        table_count = len([t for t in results if t.get("has_pii")])
        column_count = sum(
            len(t.get("columns", [])) for t in results if t.get("has_pii")
        )

        scan_summary.append(
            {
                "location": f"{catalog}.{schema}",
                "status": "success",
                "tables": table_count,
                "columns": column_count,
            }
        )

    if not all_pii_results or not any(r.get("has_pii") for r in all_pii_results):
        return {
            "error": "No PII found in any of the scanned locations",
            "scan_summary": scan_summary,
        }

    # Build stitch_config with aggregated tables from all locations
    current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
    stitch_job_name = f"stitch-multi-{current_datetime}"

    stitch_config = {
        "name": stitch_job_name,
        "tables": [],
        "settings": {
            "output_catalog_name": output_catalog,
            "output_schema_name": "stitch_outputs",
        },
    }

    unsupported_columns = []

    for table_pii_data in all_pii_results:
        if not table_pii_data.get("has_pii"):
            continue

        table_cfg = {
            "path": table_pii_data[
                "full_name"
            ],  # Already fully qualified: "catalog.schema.table"
            "fields": [],
        }

        table_unsupported = []
        for col_data in table_pii_data.get("columns", []):
            if col_data["type"] not in UNSUPPORTED_TYPES:
                field_cfg = {
                    "field-name": col_data["name"],
                    "type": col_data["type"],
                    "semantics": [],
                }
                if col_data.get("semantic"):
                    field_cfg["semantics"].append(col_data["semantic"])
                table_cfg["fields"].append(field_cfg)
            else:
                table_unsupported.append(
                    {
                        "column": col_data["name"],
                        "type": col_data["type"],
                        "semantic": col_data.get("semantic"),
                    }
                )

        if table_unsupported:
            unsupported_columns.append(
                {"table": table_pii_data["full_name"], "columns": table_unsupported}
            )

        if table_cfg["fields"]:
            stitch_config["tables"].append(table_cfg)

    if not stitch_config["tables"]:
        return {
            "error": "No tables with supported PII columns found to include in Stitch configuration.",
            "scan_summary": scan_summary,
        }

    # Create/verify volume in output_catalog
    # Use first accessible location's schema for volume
    output_schema = accessible_locations[0]["location"]["schema"]
    volume_name = "chuck"

    # Check if volume exists
    try:
        volumes_response = client.list_volumes(
            catalog_name=output_catalog, schema_name=output_schema
        )
        volume_exists = any(
            v.get("name") == volume_name for v in volumes_response.get("volumes", [])
        )
    except Exception as e:
        return {"error": f"Failed to list volumes: {str(e)}"}

    if not volume_exists:
        try:
            client.create_volume(
                catalog_name=output_catalog, schema_name=output_schema, name=volume_name
            )
            logging.debug(f"Volume '{volume_name}' created successfully.")
        except Exception as e:
            return {"error": f"Failed to create volume '{volume_name}': {str(e)}"}

    # Prepare file paths
    config_file_path = f"/Volumes/{output_catalog}/{output_schema}/{volume_name}/{stitch_job_name}.json"

    # Get Amperity token
    amperity_token = get_amperity_token()
    if not amperity_token:
        return {"error": "Amperity token not found. Please run /amp_login first."}

    # Fetch init script content
    try:
        init_script_data = client.fetch_amperity_job_init(amperity_token)
        init_script_content = init_script_data.get("cluster-init")
        job_id = init_script_data.get("job-id")
        if not init_script_content:
            return {"error": "Failed to get cluster init script from Amperity API."}
        if not job_id:
            return {"error": "Failed to get job-id from Amperity API."}
    except Exception as e:
        return {"error": f"Error fetching Amperity init script: {str(e)}"}

    # Upload cluster init script with versioning
    upload_result = _helper_upload_cluster_init_logic(
        client=client,
        target_catalog=output_catalog,
        target_schema=output_schema,
        init_script_content=init_script_content,
    )
    if upload_result.get("error"):
        return upload_result

    init_script_volume_path = upload_result["volume_path"]

    return {
        "success": True,
        "stitch_config": stitch_config,
        "metadata": {
            "target_locations": target_locations,
            "output_catalog": output_catalog,
            "output_schema": "stitch_outputs",
            "volume_name": volume_name,
            "stitch_job_name": stitch_job_name,
            "config_file_path": config_file_path,
            "init_script_path": init_script_volume_path,
            "init_script_content": init_script_content,
            "amperity_token": amperity_token,
            "job_id": job_id,
            "scan_summary": scan_summary,
            "unsupported_columns": unsupported_columns,
            # Store first location for backward compatibility with launch logic
            "target_catalog": output_catalog,
            "target_schema": output_schema,
            # Add pii_scan_output for launch compatibility (summary of all locations)
            "pii_scan_output": {
                "message": f"Multi-location PII scan complete across {len(target_locations)} locations"
            },
        },
    }


def _helper_modify_stitch_config(
    current_config: Dict[str, Any],
    modification_request: str,
    llm_client_instance: LLMProvider,
    metadata: Dict[str, Any],
) -> Dict[str, Any]:
    """Phase 2: Modify Stitch configuration based on user request using LLM."""
    try:
        # Create a prompt for the LLM to modify the config
        prompt = f"""You are helping modify a Stitch integration configuration based on user requests.

Current configuration:
{json.dumps(current_config, indent=2)}

User modification request: "{modification_request}"

Please modify the configuration according to the user's request and return ONLY the updated JSON configuration.
Ensure the JSON is valid and follows the same structure.

Important rules:
- Keep the same overall structure (name, tables, settings)
- Each table should have "path" and "fields" arrays
- Each field should have "field-name", "type", and "semantics" arrays
- Only include tables and fields that make sense based on the original PII scan data
- If removing tables/fields, just omit them from the output
- If adding semantics, use standard PII types like "email", "name", "phone", "ssn", etc.
"""

        # Call LLM to get modified config
        llm_response = llm_client_instance.chat(
            messages=[{"role": "user", "content": prompt}]
        )

        if not llm_response or not llm_response.choices:
            return {"error": "Failed to get LLM response for config modification"}

        # Parse the LLM response as JSON
        try:
            response_text = llm_response.choices[0].message.content
            if not response_text or not isinstance(response_text, str):
                return {"error": "LLM returned invalid response format"}

            # Clean up response text (remove code blocks if present)
            response_text = response_text.strip()
            if response_text.startswith("```json"):
                response_text = response_text[7:-3].strip()
            elif response_text.startswith("```"):
                response_text = response_text[3:-3].strip()

            modified_config = json.loads(response_text)
        except json.JSONDecodeError as e:
            return {"error": f"LLM returned invalid JSON: {str(e)}"}

        # Basic validation of the modified config
        if not isinstance(modified_config, dict):
            return {"error": "Modified config must be a JSON object"}

        required_keys = ["name", "tables", "settings"]
        for key in required_keys:
            if key not in modified_config:
                return {"error": f"Modified config missing required key: {key}"}

        if not isinstance(modified_config["tables"], list):
            return {"error": "Modified config 'tables' must be an array"}

        # Validate each table structure
        for table in modified_config["tables"]:
            if (
                not isinstance(table, dict)
                or "path" not in table
                or "fields" not in table
            ):
                return {"error": "Each table must have 'path' and 'fields' properties"}

            if not isinstance(table["fields"], list):
                return {"error": "Table 'fields' must be an array"}

            for field in table["fields"]:
                if not isinstance(field, dict):
                    return {"error": "Each field must be an object"}
                required_field_keys = ["field-name", "type", "semantics"]
                for fkey in required_field_keys:
                    if fkey not in field:
                        return {"error": f"Field missing required key: {fkey}"}

        return {
            "success": True,
            "stitch_config": modified_config,
            "modification_summary": f"Configuration modified based on request: {modification_request}",
        }

    except Exception as e:
        logging.error(f"Error modifying Stitch config: {e}", exc_info=True)
        return {"error": f"Error modifying configuration: {str(e)}"}


def _create_stitch_report_notebook(
    client: DatabricksAPIClient,
    stitch_config: Dict[str, Any],
    target_catalog: str,
    target_schema: str,
    stitch_job_name: str,
) -> Dict[str, Any]:
    """Helper function to create a Stitch report notebook automatically.

    This uses the DatabricksAPIClient.create_stitch_notebook method but with datasources
    extracted from the stitch_config tables' paths and a table_path constructed from the
    target catalog and schema.

    Args:
        client: DatabricksAPIClient instance
        stitch_config: The Stitch configuration dictionary
        target_catalog: Target catalog name
        target_schema: Target schema name
        stitch_job_name: Name of the Stitch job (used for notebook naming)

    Returns:
        Dictionary with success/error status and notebook path if successful
    """
    try:
        # Construct table path in the required format
        table_path = f"{target_catalog}.stitch_outputs.unified_coalesced"

        # Construct a descriptive notebook name
        notebook_name = f"Stitch Report: {target_catalog}.{target_schema}"

        # Call the create_stitch_notebook method with our parameters
        try:
            result = client.create_stitch_notebook(
                table_path=table_path,
                notebook_name=notebook_name,
                stitch_config=stitch_config,
            )

            # If we get here, the notebook was created successfully, even if result doesn't have notebook_path
            notebook_path = result.get(
                "notebook_path", f"/Workspace/Users/unknown/{notebook_name}"
            )

            return {
                "success": True,
                "notebook_path": notebook_path,
                "message": f"Successfully created Stitch report notebook at {notebook_path}",
            }
        except Exception as e:
            # Only return an error if there was an actual exception
            return {"success": False, "error": str(e)}
    except Exception as e:
        logging.error(f"Error creating Stitch report notebook: {str(e)}", exc_info=True)
        return {"success": False, "error": str(e)}


def _helper_launch_stitch_job(
    client: DatabricksAPIClient, stitch_config: Dict[str, Any], metadata: Dict[str, Any]
) -> Dict[str, Any]:
    """Phase 3: Write final config and launch Stitch job."""
    try:
        # Extract metadata
        target_catalog = metadata["target_catalog"]
        target_schema = metadata["target_schema"]
        stitch_job_name = metadata["stitch_job_name"]
        config_file_path = metadata["config_file_path"]
        init_script_path = metadata["init_script_path"]
        init_script_content = metadata["init_script_content"]
        pii_scan_output = metadata["pii_scan_output"]
        unsupported_columns = metadata["unsupported_columns"]

        # Write final config file to volume
        config_content_json = json.dumps(stitch_config, indent=2)
        try:
            upload_success = client.upload_file(
                path=config_file_path, content=config_content_json, overwrite=True
            )
            if not upload_success:
                return {
                    "error": f"Failed to write Stitch config to '{config_file_path}'"
                }
            logging.debug(f"Stitch config written to {config_file_path}")
        except Exception as e:
            return {
                "error": f"Failed to write Stitch config '{config_file_path}': {str(e)}"
            }

        # Write init script to volume
        try:
            upload_init_success = client.upload_file(
                path=init_script_path, content=init_script_content, overwrite=True
            )
            if not upload_init_success:
                return {"error": f"Failed to write init script to '{init_script_path}'"}
            logging.debug(f"Cluster init script written to {init_script_path}")
        except Exception as e:
            return {
                "error": f"Failed to write init script '{init_script_path}': {str(e)}"
            }

        # Launch the Stitch job
        try:
            # Extract policy_id if present
            policy_id = metadata.get("policy_id")

            job_run_data = client.submit_job_run(
                config_path=config_file_path,
                init_script_path=init_script_path,
                run_name=f"Stitch Setup: {stitch_job_name}",
                policy_id=policy_id,
            )
            run_id = job_run_data.get("run_id")
            if not run_id:
                return {"error": "Failed to launch job (no run_id returned)"}
        except Exception as e:
            return {"error": f"Failed to launch Stitch job: {str(e)}"}

        # Record job submission to link job-id with databricks-run-id
        try:
            # Get CLI token and job-id from metadata
            amperity_token = metadata.get("amperity_token") or get_amperity_token()
            job_id = metadata.get("job_id")

            if amperity_token and job_id:
                from chuck_data.clients.amperity import AmperityAPIClient

                amperity_client = AmperityAPIClient()
                amperity_client.record_job_submission(
                    databricks_run_id=str(run_id),
                    token=amperity_token,
                    job_id=str(job_id),
                )
                logging.debug(f"Recorded job submission: job-id -> run_id {run_id}")

                # Cache the job ID and run ID for quick status lookups
                from chuck_data.job_cache import cache_job

                cache_job(job_id=str(job_id), run_id=str(run_id))
                logging.debug(f"Cached job ID: {job_id}, run ID: {run_id}")
            else:
                logging.warning("No Amperity token available to record job submission")
        except Exception as e:
            # Log warning but don't fail the job launch
            logging.warning(f"Failed to record job submission: {e}")

        # Build success message
        summary_msg_lines = [
            f"Stitch setup for {target_catalog}.{target_schema} initiated."
        ]
        summary_msg_lines.append(f"Config: {config_file_path}")

        # Extract job_id from metadata
        job_id = metadata.get("job_id")
        if job_id:
            summary_msg_lines.append(f"Chuck Job ID: {job_id}")
        summary_msg_lines.append(f"Databricks Job Run ID: {run_id}")

        # Add unsupported columns information if any
        if unsupported_columns:
            summary_msg_lines.append("")
            summary_msg_lines.append(
                "Note: Some columns were excluded due to unsupported data types:"
            )
            for table_info in unsupported_columns:
                summary_msg_lines.append(f"  Table: {table_info['table']}")
                for col_info in table_info["columns"]:
                    semantic_info = (
                        f" (semantic: {col_info['semantic']})"
                        if col_info["semantic"]
                        else ""
                    )
                    summary_msg_lines.append(
                        f"    - {col_info['column']} ({col_info['type']}){semantic_info}"
                    )

        # Automatically create stitch report notebook
        notebook_result = _create_stitch_report_notebook(
            client=client,
            stitch_config=stitch_config,
            target_catalog=target_catalog,
            target_schema=target_schema,
            stitch_job_name=stitch_job_name,
        )

        # Add notebook creation information to the summary
        if notebook_result.get("success"):
            summary_msg_lines.append("\nCreated Stitch Report notebook:")
            summary_msg_lines.append(
                f"Notebook Path: {notebook_result.get('notebook_path', 'Unknown')}"
            )
        else:
            # If notebook creation failed, log the error but don't fail the overall job
            error_msg = notebook_result.get("error", "Unknown error")
            summary_msg_lines.append(
                f"\nNote: Could not create Stitch Report notebook: {error_msg}"
            )
            logging.warning(f"Failed to create Stitch Report notebook: {error_msg}")

        final_summary = "\n".join(summary_msg_lines)

        return {
            "success": True,
            "message": final_summary,
            "stitch_job_name": stitch_job_name,
            "job_id": job_id,
            "run_id": run_id,
            "config_path": config_file_path,
            "init_script_path": init_script_path,
            "pii_scan_summary": pii_scan_output.get("message", "PII scan performed."),
            "unsupported_columns": unsupported_columns,
            "notebook_result": (
                notebook_result if "notebook_result" in locals() else None
            ),
        }

    except Exception as e:
        logging.error(f"Error launching Stitch job: {e}", exc_info=True)
        return {"error": f"Error launching Stitch job: {str(e)}"}
