"""Databricks Compute Provider.

Runs Stitch jobs on Databricks clusters.
"""

import datetime
import json
import logging
from typing import Dict, Any, Optional

from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.commands.cluster_init_tools import _helper_upload_cluster_init_logic
from chuck_data.config import get_amperity_token
from chuck_data.compute_providers.provider import ComputeProvider


# Unsupported column types for Stitch (from stitch_tools.py)
UNSUPPORTED_TYPES = [
    "ARRAY",
    "MAP",
    "STRUCT",
    "BINARY",
    "INTERVAL",
]

# Numeric types that don't support semantics (from stitch_tools.py)
NUMERIC_TYPES = [
    "BIGINT",
    "INT",
    "SMALLINT",
    "TINYINT",
    "LONG",
    "DOUBLE",
    "FLOAT",
    "DECIMAL",
]


class DatabricksComputeProvider(ComputeProvider):
    """Run Stitch jobs on Databricks clusters.

    This compute provider can process data from:
    - Databricks Unity Catalog (direct access)
    - AWS Redshift (via Spark-Redshift connector)

    Implements the ComputeProvider protocol.
    """

    def __init__(
        self,
        workspace_url: str,
        token: str,
        storage_provider: Any,
        **kwargs,
    ):
        """Initialize Databricks compute provider.

        Args:
            workspace_url: Databricks workspace URL
            token: Authentication token
            storage_provider: StorageProvider instance for uploading artifacts.
                            Must be provided - use ProviderFactory.create_storage_provider()
            **kwargs: Additional configuration options
        """
        if storage_provider is None:
            raise ValueError(
                "storage_provider is required. Use ProviderFactory.create_storage_provider() "
                "to create the appropriate storage provider instance."
            )

        self.workspace_url = workspace_url
        self.token = token
        self.config = kwargs
        self.client = DatabricksAPIClient(workspace_url=workspace_url, token=token)
        self.storage_provider = storage_provider

    def prepare_stitch_job(
        self,
        manifest: Dict[str, Any],
        data_provider: Any,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Prepare job artifacts for Stitch execution.

        Uploads manifests and init scripts via data_provider methods:
        - Databricks data → data_provider.upload_manifest() to /Volumes
        - Redshift data → data_provider.upload_manifest() to S3

        Args:
            manifest: Stitch configuration with tables and semantic tags
            data_provider: Data source provider (handles uploads to appropriate storage)
            config: Job configuration including:
                - target_catalog: Target catalog for Stitch outputs
                - target_schema: Target schema for Stitch outputs
                - llm_client: LLM provider for PII scanning
                - policy_id: Optional cluster policy ID

        Returns:
            Preparation results with:
                - success: Boolean indicating success/failure
                - stitch_config: Generated Stitch configuration
                - metadata: Job metadata including paths, tokens, etc.
                - error: Error message if preparation failed

        Raises:
            ValueError: If required configuration is missing
        """
        # Extract configuration
        target_catalog = config.get("target_catalog")
        target_schema = config.get("target_schema")
        llm_client_instance = config.get("llm_client")
        policy_id = config.get("policy_id")

        if not target_catalog or not target_schema:
            return {"error": "Target catalog and schema are required for Stitch setup."}

        if not llm_client_instance:
            return {"error": "LLM client is required for PII scanning."}

        # Import here to avoid circular dependencies
        from chuck_data.commands.pii_tools import _helper_scan_schema_for_pii_logic

        # Step 1: Scan for PII data
        pii_scan_output = _helper_scan_schema_for_pii_logic(
            self.client, llm_client_instance, target_catalog, target_schema
        )
        if pii_scan_output.get("error"):
            return {
                "error": f"PII Scan failed during Stitch setup: {pii_scan_output['error']}"
            }

        # Step 2: Check/Create "chuck" volume
        volume_name = "chuck"
        volume_exists = False

        try:
            volumes_response = self.client.list_volumes(
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
                volume_response = self.client.create_volume(
                    catalog_name=target_catalog,
                    schema_name=target_schema,
                    name=volume_name,
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
                    # Only add semantics for non-numeric types
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

        amperity_token = get_amperity_token()
        if not amperity_token:
            return {"error": "Amperity token not found. Please run /amp_login first."}

        # Fetch init script content and job-id from Amperity API
        try:
            from chuck_data.clients.amperity import AmperityAPIClient

            amperity_client = AmperityAPIClient()
            init_script_data = amperity_client.fetch_amperity_job_init(amperity_token)
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
            return {
                "error": f"Error fetching Amperity init script: {str(e_fetch_init)}"
            }

        # Upload cluster init script with versioning
        upload_result = _helper_upload_cluster_init_logic(
            client=self.client,
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
                "policy_id": policy_id,
                "pii_scan_output": pii_scan_output,
                "unsupported_columns": unsupported_columns,
            },
        }

    def launch_stitch_job(self, preparation: Dict[str, Any]) -> Dict[str, Any]:
        """Launch the Stitch job on Databricks.

        Args:
            preparation: Results from prepare_stitch_job() containing:
                - success: Boolean indicating preparation success
                - stitch_config: Stitch configuration to execute
                - metadata: Job metadata (paths, tokens, etc.)

        Returns:
            Job execution results with:
                - success: Boolean indicating launch success
                - message: Summary message
                - run_id: Databricks job run ID
                - job_id: Chuck job ID
                - config_path: Path to config file
                - stitch_job_name: Name of the Stitch job
                - error: Error message if launch failed
        """
        if not preparation.get("success"):
            return {
                "error": "Cannot launch job: preparation failed",
                "preparation_error": preparation.get("error"),
            }

        stitch_config = preparation["stitch_config"]
        metadata = preparation["metadata"]

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

            # Write final config file to volume using storage provider
            config_content_json = json.dumps(stitch_config, indent=2)
            try:
                upload_success = self.storage_provider.upload_file(
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

            # Write init script to volume using storage provider
            try:
                upload_init_success = self.storage_provider.upload_file(
                    path=init_script_path, content=init_script_content, overwrite=True
                )
                if not upload_init_success:
                    return {
                        "error": f"Failed to write init script to '{init_script_path}'"
                    }
                logging.debug(f"Cluster init script written to {init_script_path}")
            except Exception as e:
                return {
                    "error": f"Failed to write init script '{init_script_path}': {str(e)}"
                }

            # Launch the Stitch job
            try:
                # Extract policy_id if present
                policy_id = metadata.get("policy_id")

                job_run_data = self.client.submit_job_run(
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
                    logging.warning(
                        "No Amperity token available to record job submission"
                    )
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
            notebook_result = self._create_stitch_report_notebook(
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
                "pii_scan_summary": pii_scan_output.get(
                    "message", "PII scan performed."
                ),
                "unsupported_columns": unsupported_columns,
                "notebook_result": notebook_result,
            }

        except Exception as e:
            logging.error(f"Error launching Stitch job: {str(e)}", exc_info=True)
            return {"error": f"Error launching Stitch job: {str(e)}"}

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get job status.

        Args:
            job_id: Job identifier (Databricks run ID)

        Returns:
            Job status information including:
                - success: Boolean indicating if status was retrieved
                - status: Job status (PENDING, RUNNING, TERMINATED, etc.)
                - state_message: Detailed state message
                - life_cycle_state: Life cycle state
                - result_state: Result state (SUCCESS, FAILED, CANCELLED, etc.)
                - error: Error message if retrieval failed
        """
        try:
            status_data = self.client.get_job_run_status(job_id)

            # Extract key status fields
            state = status_data.get("state", {})
            life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
            result_state = state.get("result_state")
            state_message = state.get("state_message", "")

            return {
                "success": True,
                "run_id": job_id,
                "status": life_cycle_state,
                "result_state": result_state,
                "state_message": state_message,
                "full_status": status_data,
            }
        except Exception as e:
            logging.error(
                f"Error getting job status for {job_id}: {str(e)}", exc_info=True
            )
            return {"success": False, "error": f"Failed to get job status: {str(e)}"}

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job.

        Args:
            job_id: Job identifier (Databricks run ID)

        Returns:
            True if cancellation succeeded

        Raises:
            NotImplementedError: Cancellation requires adding cancel_run to DatabricksAPIClient
        """
        # Note: This requires adding a cancel_run method to DatabricksAPIClient
        # The Databricks API endpoint is POST /api/2.2/jobs/runs/cancel with {"run_id": job_id}
        raise NotImplementedError(
            "DatabricksComputeProvider.cancel_job() requires adding "
            "cancel_run() method to DatabricksAPIClient. "
            "API endpoint: POST /api/2.2/jobs/runs/cancel with payload {'run_id': job_id}"
        )

    def _create_stitch_report_notebook(
        self,
        stitch_config: Dict[str, Any],
        target_catalog: str,
        target_schema: str,
        stitch_job_name: str,  # noqa: ARG002 - kept for API consistency
    ) -> Dict[str, Any]:
        """Helper function to create a Stitch report notebook automatically.

        This uses the DatabricksAPIClient.create_stitch_notebook method with datasources
        extracted from the stitch_config tables' paths.

        Args:
            stitch_config: The Stitch configuration dictionary
            target_catalog: Target catalog name
            target_schema: Target schema name
            stitch_job_name: Name of the Stitch job (kept for potential future use)

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
                result = self.client.create_stitch_notebook(
                    table_path=table_path,
                    notebook_name=notebook_name,
                    stitch_config=stitch_config,
                )

                # If we get here, the notebook was created successfully
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
            logging.error(
                f"Error creating Stitch report notebook: {str(e)}", exc_info=True
            )
            return {"success": False, "error": str(e)}
