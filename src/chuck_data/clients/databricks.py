"""
Reusable Databricks API client for authentication and requests.
"""

import base64
import json
import logging
import os
import requests
import time
import urllib.parse
from datetime import datetime
from ..config import get_warehouse_id
from ..clients.amperity import get_amperity_url
from ..databricks.url_utils import (
    detect_cloud_provider,
    normalize_workspace_url,
)


class DatabricksAPIClient:
    """Reusable Databricks API client for authentication and requests."""

    def __init__(self, workspace_url, token):
        """
        Initialize the API client.

        Args:
            workspace_url: Databricks workspace URL (with or without protocol/domain)
            token: Databricks API token
        """
        self.original_url = workspace_url
        self.workspace_url = self._normalize_workspace_url(workspace_url)
        self.cloud_provider = detect_cloud_provider(workspace_url)
        self.base_domain = self._get_base_domain()
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "User-Agent": "amperity",
        }

    def _normalize_workspace_url(self, url):
        """
        Normalize the workspace URL to the format needed for API calls.

        Args:
            url: Input workspace URL that might be in various formats

        Returns:
            Cleaned workspace URL (workspace ID only)
        """
        return normalize_workspace_url(url)

    def _get_base_domain(self):
        """
        Get the appropriate base domain based on the cloud provider.

        Returns:
            Base domain string for the detected cloud provider
        """
        from ..databricks.url_utils import DATABRICKS_DOMAIN_MAP

        return DATABRICKS_DOMAIN_MAP.get(
            self.cloud_provider, DATABRICKS_DOMAIN_MAP["AWS"]
        )

    def get_compute_node_type(self):
        """
        Get the appropriate compute node type based on the cloud provider.

        Returns:
            Node type string for the detected cloud provider
        """
        node_type_map = {
            "AWS": "r5d.4xlarge",
            "Azure": "Standard_E16ds_v4",
            "GCP": "n2-standard-16",  # Default GCP node type
            "Generic": "r5d.4xlarge",  # Default to AWS
        }
        return node_type_map.get(self.cloud_provider, "r5d.4xlarge")

    def get_cloud_attributes(self):
        """
        Get cloud-specific attributes for cluster configuration.

        Returns:
            Dictionary containing cloud-specific attributes
        """
        if self.cloud_provider == "AWS":
            return {
                "aws_attributes": {
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK",
                    "zone_id": "us-west-2b",
                    "spot_bid_price_percent": 100,
                    "ebs_volume_count": 0,
                }
            }
        elif self.cloud_provider == "Azure":
            return {
                "azure_attributes": {
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK_AZURE",
                    "spot_bid_max_price": -1,
                }
            }
        elif self.cloud_provider == "GCP":
            return {
                "gcp_attributes": {
                    "use_preemptible_executors": True,
                    "google_service_account": None,
                }
            }
        else:
            # Default to AWS
            return {
                "aws_attributes": {
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK",
                    "zone_id": "us-west-2b",
                    "spot_bid_price_percent": 100,
                    "ebs_volume_count": 0,
                }
            }

    #
    # Base API request methods
    #

    def get(self, endpoint):
        """
        Send a GET request to the Databricks API.

        Args:
            endpoint: API endpoint (starting with /)

        Returns:
            JSON response from the API

        Raises:
            ValueError: If an HTTP error occurs
            ConnectionError: If a connection error occurs
        """
        url = f"https://{self.workspace_url}.{self.base_domain}{endpoint}"
        logging.debug(f"GET request to: {url}")

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logging.debug(f"HTTP error: {e}, Response: {response.text}")
            raise ValueError(f"HTTP error occurred: {e}, Response: {response.text}")
        except requests.RequestException as e:
            logging.debug(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    def get_with_params(self, endpoint, params=None):
        """
        Send a GET request to the Databricks API with query parameters.

        Args:
            endpoint: API endpoint (starting with /)
            params: Dictionary of query parameters

        Returns:
            JSON response from the API

        Raises:
            ValueError: If an HTTP error occurs
            ConnectionError: If a connection error occurs
        """
        url = f"https://{self.workspace_url}.{self.base_domain}{endpoint}"
        logging.debug(f"GET request with params to: {url}")

        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logging.debug(f"HTTP error: {e}, Response: {response.text}")
            raise ValueError(f"HTTP error occurred: {e}, Response: {response.text}")
        except requests.RequestException as e:
            logging.debug(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    def post(self, endpoint, data):
        """
        Send a POST request to the Databricks API.

        Args:
            endpoint: API endpoint (starting with /)
            data: JSON data to send in the request body

        Returns:
            JSON response from the API

        Raises:
            ValueError: If an HTTP error occurs
            ConnectionError: If a connection error occurs
        """
        url = f"https://{self.workspace_url}.{self.base_domain}{endpoint}"
        logging.debug(f"POST request to: {url}")

        try:
            response = requests.post(url, headers=self.headers, json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logging.debug(f"HTTP error: {e}, Response: {response.text}")
            raise ValueError(f"HTTP error occurred: {e}, Response: {response.text}")
        except requests.RequestException as e:
            logging.debug(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    #
    # Authentication methods
    #

    def validate_token(self):
        """
        Validate the current token by calling the SCIM Me endpoint.

        Returns:
            True if the token is valid, False otherwise
        """
        try:
            response = self.get("/api/2.0/preview/scim/v2/Me")
            return True if response else False
        except Exception as e:
            logging.debug(f"Token validation failed: {e}")
            return False

    #
    # Unity Catalog methods
    #

    def list_catalogs(self, include_browse=False, max_results=None, page_token=None):
        """
        Gets an array of catalogs in the metastore.

        Args:
            include_browse: Whether to include catalogs for which the principal can only access selective metadata
            max_results: Maximum number of catalogs to return (optional)
            page_token: Opaque pagination token to go to next page (optional)

        Returns:
            Dictionary containing:
            - catalogs: List of catalogs
            - next_page_token: Token for retrieving the next page (if available)
        """
        params = {}
        if include_browse:
            params["include_browse"] = "true"
        if max_results is not None:
            params["max_results"] = str(max_results)
        if page_token:
            params["page_token"] = page_token

        if params:
            return self.get_with_params("/api/2.1/unity-catalog/catalogs", params)
        return self.get("/api/2.1/unity-catalog/catalogs")

    def get_catalog(self, catalog_name):
        """
        Gets a catalog from Unity Catalog.

        Args:
            catalog_name: Name of the catalog

        Returns:
            Catalog information
        """
        return self.get(f"/api/2.1/unity-catalog/catalogs/{catalog_name}")

    def list_schemas(
        self, catalog_name, include_browse=False, max_results=None, page_token=None
    ):
        """
        Gets an array of schemas for a catalog in the metastore.

        Args:
            catalog_name: Parent catalog for schemas of interest (required)
            include_browse: Whether to include schemas for which the principal can only access selective metadata
            max_results: Maximum number of schemas to return (optional)
            page_token: Opaque pagination token to go to next page (optional)

        Returns:
            Dictionary containing:
            - schemas: List of schemas
            - next_page_token: Token for retrieving the next page (if available)
        """
        params = {"catalog_name": catalog_name}
        if include_browse:
            params["include_browse"] = "true"
        if max_results is not None:
            params["max_results"] = str(max_results)
        if page_token:
            params["page_token"] = page_token

        return self.get_with_params("/api/2.1/unity-catalog/schemas", params)

    def get_schema(self, full_name):
        """
        Gets a schema from Unity Catalog.

        Args:
            full_name: Full name of the schema in the format 'catalog_name.schema_name'

        Returns:
            Schema information
        """
        return self.get(f"/api/2.1/unity-catalog/schemas/{full_name}")

    def list_tables(
        self,
        catalog_name,
        schema_name,
        max_results=None,
        page_token=None,
        include_delta_metadata=False,
        omit_columns=False,
        omit_properties=False,
        omit_username=False,
        include_browse=False,
        include_manifest_capabilities=False,
    ):
        """
        Gets an array of all tables for the current metastore under the parent catalog and schema.

        Args:
            catalog_name: Name of parent catalog for tables of interest (required)
            schema_name: Parent schema of tables (required)
            max_results: Maximum number of tables to return (optional)
            page_token: Opaque token to send for the next page of results (optional)
            include_delta_metadata: Whether delta metadata should be included (optional)
            omit_columns: Whether to omit columns from the response (optional)
            omit_properties: Whether to omit properties from the response (optional)
            omit_username: Whether to omit username from the response (optional)
            include_browse: Whether to include tables with selective metadata access (optional)
            include_manifest_capabilities: Whether to include table capabilities (optional)

        Returns:
            Dictionary containing:
            - tables: List of tables
            - next_page_token: Token for retrieving the next page (if available)
        """
        params = {"catalog_name": catalog_name, "schema_name": schema_name}

        if max_results is not None:
            params["max_results"] = str(max_results)
        if page_token:
            params["page_token"] = page_token
        if include_delta_metadata:
            params["include_delta_metadata"] = "true"
        if omit_columns:
            params["omit_columns"] = "true"
        if omit_properties:
            params["omit_properties"] = "true"
        if omit_username:
            params["omit_username"] = "true"
        if include_browse:
            params["include_browse"] = "true"
        if include_manifest_capabilities:
            params["include_manifest_capabilities"] = "true"

        return self.get_with_params("/api/2.1/unity-catalog/tables", params)

    def get_table(
        self,
        full_name,
        include_delta_metadata=False,
        include_browse=False,
        include_manifest_capabilities=False,
    ):
        """
        Gets a table from the metastore for a specific catalog and schema.

        Args:
            full_name: Full name of the table in format 'catalog_name.schema_name.table_name'
            include_delta_metadata: Whether delta metadata should be included (optional)
            include_browse: Whether to include tables with selective metadata access (optional)
            include_manifest_capabilities: Whether to include table capabilities (optional)

        Returns:
            Table information
        """
        params = {}
        if include_delta_metadata:
            params["include_delta_metadata"] = "true"
        if include_browse:
            params["include_browse"] = "true"
        if include_manifest_capabilities:
            params["include_manifest_capabilities"] = "true"

        if params:
            return self.get_with_params(
                f"/api/2.1/unity-catalog/tables/{full_name}", params
            )
        return self.get(f"/api/2.1/unity-catalog/tables/{full_name}")

    def list_volumes(
        self,
        catalog_name,
        schema_name,
        max_results=None,
        page_token=None,
        include_browse=False,
    ):
        """
        Gets an array of volumes for the current metastore under the parent catalog and schema.

        Args:
            catalog_name: Name of parent catalog (required)
            schema_name: Name of parent schema (required)
            max_results: Maximum number of volumes to return (optional)
            page_token: Opaque token for pagination (optional)
            include_browse: Whether to include volumes with selective metadata access (optional)

        Returns:
            Dictionary containing:
            - volumes: List of volumes
            - next_page_token: Token for retrieving the next page (if available)
        """
        params = {"catalog_name": catalog_name, "schema_name": schema_name}
        if max_results is not None:
            params["max_results"] = str(max_results)
        if page_token:
            params["page_token"] = page_token
        if include_browse:
            params["include_browse"] = "true"

        return self.get_with_params("/api/2.1/unity-catalog/volumes", params)

    def create_volume(self, catalog_name, schema_name, name, volume_type="MANAGED"):
        """
        Create a new volume in Unity Catalog.

        Args:
            catalog_name: The name of the catalog where the volume will be created
            schema_name: The name of the schema where the volume will be created
            name: The name of the volume to create
            volume_type: The type of volume to create (default: "MANAGED")

        Returns:
            Dict containing the created volume information
        """
        data = {
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "name": name,
            "volume_type": volume_type,
        }
        return self.post("/api/2.1/unity-catalog/volumes", data)

    #
    # Models and Serving methods
    #

    def list_models(self):
        """
        Fetch a list of models from the Databricks Serving API.

        Returns:
            List of available model endpoints
        """
        response = self.get("/api/2.0/serving-endpoints")
        return response.get("endpoints", [])

    def get_model(self, model_name):
        """
        Get details of a specific model from Databricks Serving API.

        Args:
            model_name: Name of the model to retrieve

        Returns:
            Model details if found
        """
        try:
            return self.get(f"/api/2.0/serving-endpoints/{model_name}")
        except ValueError as e:
            if "404" in str(e):
                logging.warning(f"Model '{model_name}' not found")
                return None
            raise

    #
    # Warehouse methods
    #

    def list_warehouses(self):
        """
        Lists all SQL warehouses in the Databricks workspace.

        Returns:
            List of warehouses
        """
        response = self.get("/api/2.0/sql/warehouses")
        return response.get("warehouses", [])

    def get_warehouse(self, warehouse_id):
        """
        Gets information about a specific SQL warehouse.

        Args:
            warehouse_id: ID of the SQL warehouse

        Returns:
            Warehouse information
        """
        return self.get(f"/api/2.0/sql/warehouses/{warehouse_id}")

    def create_warehouse(self, opts):
        """
        Creates a new SQL warehouse.

        Args:
            opts: Dictionary containing warehouse configuration options

        Returns:
            Created warehouse information
        """
        return self.post("/api/2.0/sql/warehouses", opts)

    def submit_sql_statement(
        self,
        sql_text,
        warehouse_id,
        catalog=None,
        wait_timeout="30s",
        on_wait_timeout="CONTINUE",
    ):
        """
        Submit a SQL statement to Databricks SQL warehouse and wait for completion.

        Args:
            sql_text: SQL statement to execute
            warehouse_id: ID of the SQL warehouse
            catalog: Optional catalog name
            wait_timeout: How long to wait for query completion (default "30s")
            on_wait_timeout: What to do on timeout ("CONTINUE" or "CANCEL")

        Returns:
            Dictionary containing the SQL statement execution result
        """
        data = {
            "statement": sql_text,
            "warehouse_id": warehouse_id,
            "wait_timeout": wait_timeout,
            "on_wait_timeout": on_wait_timeout,
        }

        if catalog:
            data["catalog"] = catalog

        # Submit the SQL statement
        response = self.post("/api/2.0/sql/statements", data)
        statement_id = response.get("statement_id")

        # Poll until complete
        while True:
            status = self.get(f"/api/2.0/sql/statements/{statement_id}")
            state = status.get("status", {}).get("state", status.get("state"))
            if state not in ["PENDING", "RUNNING"]:
                break
            time.sleep(1)

        return status

    #
    # Jobs methods
    #

    def submit_job_run(self, config_path, init_script_path, run_name=None):
        """
        Submit a one-time Databricks job run using the /runs/submit endpoint.

        Args:
            config_path: Path to the configuration file for the job in the Volume
            init_script_path: Path to the initialization script
            run_name: Optional name for the run. If None, a default name will be generated.

        Returns:
            Dict containing the job run information (including run_id)
        """
        if not run_name:
            run_name = (
                f"Chuck AI One-Time Run {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

        # Define the task and cluster for the one-time run
        # Create base cluster configuration
        cluster_config = {
            "cluster_name": "",
            "spark_version": "16.0.x-cpu-ml-scala2.12",
            "init_scripts": [
                {
                    "volumes": {
                        "destination": init_script_path,
                    }
                }
            ],
            "node_type_id": self.get_compute_node_type(),
            "custom_tags": {
                "stack": "aws-dev",
                "sys": "chuck",
                "tenant": "amperity",
            },
            "spark_env_vars": {
                "JNAME": "zulu17-ca-amd64",
                "CHUCK_API_URL": f"https://{get_amperity_url()}",
                "DEBUG_INIT_SRIPT_URL": init_script_path,
                "DEBUG_CONFIG_PATH": config_path,
            },
            "enable_elastic_disk": False,
            "data_security_mode": "SINGLE_USER",
            "runtime_engine": "STANDARD",
            "autoscale": {"min_workers": 10, "max_workers": 50},
        }

        # Add cloud-specific attributes
        cluster_config.update(self.get_cloud_attributes())

        run_payload = {
            "run_name": run_name,
            "tasks": [
                {
                    "task_key": "Run_Stitch",
                    "run_if": "ALL_SUCCESS",
                    "spark_jar_task": {
                        "jar_uri": "",
                        "main_class_name": os.environ.get(
                            "MAIN_CLASS", "amperity.stitch_standalone.chuck_main"
                        ),
                        "parameters": [
                            "",
                            config_path,
                        ],
                        "run_as_repl": True,
                    },
                    "libraries": [{"jar": "file:///opt/amperity/job.jar"}],
                    "timeout_seconds": 0,
                    "email_notifications": {},
                    "webhook_notifications": {},
                    "new_cluster": cluster_config,
                },
            ],
            "timeout_seconds": 0,
        }

        return self.post("/api/2.2/jobs/runs/submit", run_payload)

    def get_job_run_status(self, run_id):
        """
        Get the status of a Databricks job run.

        Args:
            run_id: The job run ID (as str or int)

        Returns:
            Dict containing the job run status information
        """
        params = {"run_id": run_id}
        return self.get_with_params("/api/2.2/jobs/runs/get", params)

    #
    # File system methods
    #

    def upload_file(self, path, file_path=None, content=None, overwrite=False):
        """
        Upload a file using the /api/2.0/fs/files endpoint.

        Args:
            path: The destination path (e.g., "/Volumes/my-catalog/my-schema/my-volume/file.txt")
            file_path: Local file path to upload (mutually exclusive with content)
            content: String content to upload (mutually exclusive with file_path)
            overwrite: Whether to overwrite an existing file

        Returns:
            True if successful (API returns no content on success)

        Raises:
            ValueError: If both file_path and content are provided or neither is provided
            ValueError: If an HTTP error occurs
            ConnectionError: If a connection error occurs
        """
        if (file_path and content) or (not file_path and not content):
            raise ValueError("Exactly one of file_path or content must be provided")

        # URL encode the path and make sure it starts with a slash
        if not path.startswith("/"):
            path = f"/{path}"

        # Remove duplicate slashes if any
        while "//" in path:
            path = path.replace("//", "/")

        # URL encode path components but preserve the slashes
        encoded_path = "/".join(
            urllib.parse.quote(component) for component in path.split("/") if component
        )
        encoded_path = f"/{encoded_path}"

        url = f"https://{self.workspace_url}.{self.base_domain}/api/2.0/fs/files{encoded_path}"

        if overwrite:
            url += "?overwrite=true"

        logging.debug(f"File upload request to: {url}")

        headers = self.headers.copy()
        headers.update({"Content-Type": "application/octet-stream"})

        # Get binary data to upload
        if file_path:
            with open(file_path, "rb") as f:
                binary_data = f.read()
        else:
            # Convert string content to bytes
            binary_data = content.encode("utf-8")

        try:
            # Use PUT request with raw binary data in the body
            response = requests.put(url, headers=headers, data=binary_data)
            response.raise_for_status()
            # API returns 204 No Content on success
            return True
        except requests.exceptions.HTTPError as e:
            logging.debug(f"HTTP error: {e}, Response: {response.text}")
            raise ValueError(f"HTTP error occurred: {e}, Response: {response.text}")
        except requests.RequestException as e:
            logging.debug(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    def store_dbfs_file(self, path, contents, overwrite=True):
        """
        Store content to DBFS using the /api/2.0/dbfs/put endpoint.

        Args:
            path: Path in DBFS to store the file
            contents: String content to store (will be JSON encoded)
            overwrite: Whether to overwrite an existing file

        Returns:
            True if successful
        """
        # Encode the content as base64
        encoded_contents = (
            base64.b64encode(contents.encode()).decode()
            if isinstance(contents, str)
            else base64.b64encode(contents).decode()
        )

        # Prepare the request with file content and path
        request_data = {
            "path": path,
            "contents": encoded_contents,
            "overwrite": overwrite,
        }

        # Call DBFS API
        self.post("/api/2.0/dbfs/put", request_data)
        return True

    #
    # Amperity-specific methods
    #

    def fetch_amperity_job_init(self, token, api_url: str | None = None):
        """
        Fetch initialization script for Amperity jobs.

        Args:
            token: Amperity authentication token
            api_url: Optional override for the job init endpoint

        Returns:
            Dict containing the initialization script data
        """
        try:
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            if not api_url:
                api_url = f"https://{get_amperity_url()}/api/job/launch"

            response = requests.post(api_url, headers=headers, json={})
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            response = e.response
            resp_text = response.text if response else ""
            logging.debug(f"HTTP error: {e}, Response: {resp_text}")
            if response is not None:
                try:
                    message = response.json().get("message", resp_text)
                except ValueError:
                    message = resp_text
                raise ValueError(
                    f"{response.status_code} Error: {message}. Please /logout and /login again"
                )
            raise ValueError(
                f"HTTP error occurred: {e}. Please /logout and /login again"
            )
        except requests.RequestException as e:
            logging.debug(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    def get_current_user(self):
        """
        Get the current user's username from Databricks API.

        Returns:
            Username string from the current user
        """
        try:
            response = self.get("/api/2.0/preview/scim/v2/Me")
            username = response.get("userName")
            if not username:
                logging.debug("Username not found in response")
                raise ValueError("Username not found in API response")
            return username
        except Exception as e:
            logging.debug(f"Error getting current user: {e}")
            raise

    def create_stitch_notebook(
        self, table_path, notebook_name=None, stitch_config=None, datasources=None
    ):
        """
        Create a stitch notebook for the given table path.

        This function will:
        1. Load the stitch notebook template
        2. Extract the notebook name from metadata (or use provided name)
        3. Get datasources either from provided list, stitch_config, or query the table
        4. Replace template fields with appropriate values
        5. Import the notebook to Databricks

        Args:
            table_path: Full path to the unified table in format catalog.schema.table
            notebook_name: Optional name for the notebook. If not provided, the name will be
                           extracted from the template's metadata.
            stitch_config: Optional stitch configuration dictionary. If provided, datasources will be
                           extracted from stitch_config["tables"][*]["path"] values.
            datasources: Optional list of datasource values. If provided, these will be used
                         instead of querying the table.

        Returns:
            Dictionary containing the notebook path and status
        """
        # 1. Load the template notebook
        template_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "assets",
            "stitch_notebook_template.ipynb",
        )

        try:
            with open(template_path, "r") as f:
                notebook_content = json.load(f)

            # 2. Extract notebook name from metadata (if not provided)
            extracted_name = "Stitch Results"
            for cell in notebook_content.get("cells", []):
                if cell.get("cell_type") == "markdown":
                    source = cell.get("source", [])
                    if source and source[0].startswith("# "):
                        extracted_name = source[0].replace("# ", "").strip()
                        break

            # Use the provided notebook name if available, otherwise use the extracted name
            final_notebook_name = notebook_name if notebook_name else extracted_name

            # 3. Get distinct datasource values
            # First check if datasources were directly provided
            if not datasources:
                # Check if we should extract datasources from stitch_config
                if (
                    stitch_config
                    and "tables" in stitch_config
                    and stitch_config["tables"]
                ):
                    # Extract unique datasources from the path values in stitch_config tables
                    datasource_set = set()
                    for table in stitch_config["tables"]:
                        if "path" in table:
                            datasource_set.add(table["path"])

                    datasources = list(datasource_set)
                    # Extract datasources successfully from stitch config

                # If we still don't have datasources, query the table directly
                if not datasources:
                    # Get the configured warehouse ID
                    warehouse_id = get_warehouse_id()

                    # If no warehouse ID is configured, try to find a default one
                    if not warehouse_id:
                        warehouses = self.list_warehouses()
                        if not warehouses:
                            raise ValueError(
                                "No SQL warehouses found and no warehouse configured. Please select a warehouse using /warehouse_selection."
                            )

                        # Use the first available warehouse
                        warehouse_id = warehouses[0]["id"]
                        logging.warning(
                            f"No warehouse configured. Using first available warehouse: {warehouse_id}"
                        )

                    # Query for distinct datasource values
                    sql_query = f"SELECT DISTINCT datasource FROM {table_path}"
                    # Execute SQL query to get datasources
                    result = self.submit_sql_statement(sql_query, warehouse_id)

                    # Extract the results from the query response
                    if (
                        result
                        and result.get("result")
                        and result["result"].get("data_array")
                    ):
                        datasources = [row[0] for row in result["result"]["data_array"]]
                        # Successfully extracted datasources from query

            # If we still don't have datasources, use a default value
            if not datasources:
                logging.warning(f"No datasources found for {table_path}")
                datasources = ["default_source"]
                # Use default datasource as a fallback

            # 4. Create the source names JSON mapping
            source_names_json = {}
            for source in datasources:
                source_names_json[source] = source

            # Convert to JSON string (formatted nicely)
            source_names_str = json.dumps(source_names_json, indent=4)
            # Source mapping created for template

            # 5. Replace the template fields
            # Replace template placeholders with actual values

            # Need to directly modify the notebook cells rather than doing string replacement on the JSON
            for cell in notebook_content.get("cells", []):
                if cell.get("cell_type") == "code":
                    source_lines = cell.get("source", [])
                    for i, line in enumerate(source_lines):
                        if (
                            '"{UNIFIED_PATH}"' in line
                            or "unified_coalesced_path = " in line
                        ):
                            # Found placeholder in notebook
                            # Replace the line with our table path
                            source_lines[i] = (
                                f'unified_coalesced_path = "{table_path}"\n'
                            )
                            break

            # Replace source semantic mapping in the cells
            replaced_mapping = False
            for cell in notebook_content.get("cells", []):
                if cell.get("cell_type") == "code":
                    source_lines = cell.get("source", [])
                    # Look for the source_semantic_mapping definition
                    for i, line in enumerate(source_lines):
                        if "source_semantic_mapping =" in line and not replaced_mapping:
                            # Found source names placeholder

                            # Find the closing brace
                            closing_index = None
                            opening_count = 0
                            for j in range(i, len(source_lines)):
                                if "{" in source_lines[j]:
                                    opening_count += 1
                                if "}" in source_lines[j]:
                                    opening_count -= 1
                                    if opening_count == 0:
                                        closing_index = j
                                        break

                            if closing_index is not None:
                                # Replace the mapping with our custom mapping
                                mapping_start = i
                                mapping_end = closing_index + 1
                                new_line = (
                                    f"source_semantic_mapping = {source_names_str}\n"
                                )
                                source_lines[mapping_start:mapping_end] = [new_line]
                                replaced_mapping = True
                                # Import notebook to workspace
                                break

            if not replaced_mapping:
                logging.warning(
                    "Could not find source_semantic_mapping in the notebook template to replace"
                )

            # 6. Get the current user's username
            username = self.get_current_user()

            # 7. Construct the notebook path
            notebook_path = f"/Workspace/Users/{username}/{final_notebook_name}"

            # 8. Convert the notebook to base64 for API call
            notebook_content_str = json.dumps(notebook_content)
            encoded_content = base64.b64encode(notebook_content_str.encode()).decode()

            # 9. Import the notebook to Databricks
            import_data = {
                "path": notebook_path,
                "content": encoded_content,
                "format": "JUPYTER",
                "overwrite": True,
            }

            self.post("/api/2.0/workspace/import", import_data)

            # 10. Log success and return the path
            # Notebook created successfully

            return {"notebook_path": notebook_path, "status": "success"}

        except Exception as e:
            logging.debug(f"Error creating stitch notebook: {e}")
            raise
