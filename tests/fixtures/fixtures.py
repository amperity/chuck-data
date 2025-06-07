"""Test fixtures for Chuck tests."""


class AmperityClientStub:
    """Comprehensive stub for AmperityAPIClient with predictable responses."""

    def __init__(self):
        self.base_url = "chuck.amperity.com"
        self.nonce = None
        self.token = None
        self.state = "pending"
        self.auth_thread = None

        # Test configuration
        self.should_fail_auth_start = False
        self.should_fail_auth_completion = False
        self.should_fail_metrics = False
        self.should_fail_bug_report = False
        self.should_raise_exception = False
        self.auth_completion_delay = 0

        # Track method calls for testing
        self.metrics_calls = []

    def start_auth(self) -> tuple[bool, str]:
        """Start the authentication process."""
        if self.should_fail_auth_start:
            return False, "Failed to start auth: 500 - Server Error"

        self.nonce = "test-nonce-123"
        self.state = "started"
        return True, "Authentication started. Please log in via the browser."

    def get_auth_status(self) -> dict:
        """Return the current authentication status."""
        return {"state": self.state, "nonce": self.nonce, "has_token": bool(self.token)}

    def wait_for_auth_completion(
        self, poll_interval: int = 1, timeout: int = None
    ) -> tuple[bool, str]:
        """Wait for authentication to complete in a blocking manner."""
        if not self.nonce:
            return False, "Authentication not started"

        if self.should_fail_auth_completion:
            self.state = "error"
            return False, "Authentication failed: error"

        # Simulate successful authentication
        self.state = "success"
        self.token = "test-auth-token-456"
        return True, "Authentication completed successfully."

    def submit_metrics(self, payload: dict, token: str) -> bool:
        """Send usage metrics to the Amperity API."""
        # Track the call
        self.metrics_calls.append((payload, token))

        if self.should_raise_exception:
            raise Exception("Test exception")

        if self.should_fail_metrics:
            return False

        # Validate basic payload structure
        if not isinstance(payload, dict):
            return False

        if not token:
            return False

        return True

    def submit_bug_report(self, payload: dict, token: str) -> tuple[bool, str]:
        """Send a bug report to the Amperity API."""
        if self.should_fail_bug_report:
            return False, "Failed to submit bug report: 500"

        # Validate basic payload structure
        if not isinstance(payload, dict):
            return False, "Invalid payload format"

        if not token:
            return False, "Authentication token required"

        return True, "Bug report submitted successfully"

    def _poll_auth_state(self) -> None:
        """Poll the auth state endpoint until authentication is complete."""
        # In stub, this is a no-op since we control state directly
        pass

    # Helper methods for test configuration
    def set_auth_start_failure(self, should_fail: bool = True):
        """Configure whether start_auth should fail."""
        self.should_fail_auth_start = should_fail

    def set_auth_completion_failure(self, should_fail: bool = True):
        """Configure whether wait_for_auth_completion should fail."""
        self.should_fail_auth_completion = should_fail

    def set_metrics_failure(self, should_fail: bool = True):
        """Configure whether submit_metrics should fail."""
        self.should_fail_metrics = should_fail

    def set_bug_report_failure(self, should_fail: bool = True):
        """Configure whether submit_bug_report should fail."""
        self.should_fail_bug_report = should_fail

    def reset(self):
        """Reset all state to initial values."""
        self.nonce = None
        self.token = None
        self.state = "pending"
        self.auth_thread = None
        self.should_fail_auth_start = False
        self.should_fail_auth_completion = False
        self.should_fail_metrics = False
        self.should_fail_bug_report = False
        self.auth_completion_delay = 0


class DatabricksClientStub:
    """Comprehensive stub for DatabricksAPIClient with predictable responses."""

    def __init__(self):
        # Initialize with default data
        self.catalogs = []
        self.schemas = {}  # catalog_name -> [schemas]
        self.tables = {}  # (catalog, schema) -> [tables]
        self.models = []
        self.warehouses = []
        self.volumes = {}  # catalog_name -> [volumes]
        self.connection_status = "connected"
        self.permissions = {}
        self.sql_results = {}  # sql -> results mapping
        self.pii_scan_results = {}  # table_name -> pii results

        # Call tracking
        self.create_stitch_notebook_calls = []
        self.list_catalogs_calls = []
        self.get_catalog_calls = []
        self.list_schemas_calls = []
        self.get_schema_calls = []
        self.list_tables_calls = []
        self.get_table_calls = []

    # Catalog operations
    def list_catalogs(self, include_browse=False, max_results=None, page_token=None):
        # Track the call
        self.list_catalogs_calls.append((include_browse, max_results, page_token))
        return {"catalogs": self.catalogs}

    def get_catalog(self, catalog_name):
        # Track the call
        self.get_catalog_calls.append((catalog_name,))
        catalog = next((c for c in self.catalogs if c["name"] == catalog_name), None)
        if not catalog:
            raise Exception(f"Catalog {catalog_name} not found")
        return catalog

    # Schema operations
    def list_schemas(
        self,
        catalog_name,
        include_browse=False,
        max_results=None,
        page_token=None,
        **kwargs,
    ):
        # Track the call
        self.list_schemas_calls.append(
            (catalog_name, include_browse, max_results, page_token)
        )
        return {"schemas": self.schemas.get(catalog_name, [])}

    def get_schema(self, full_name):
        # Track the call
        self.get_schema_calls.append((full_name,))
        # Parse full_name in format "catalog_name.schema_name"
        parts = full_name.split(".")
        if len(parts) != 2:
            raise Exception("Invalid schema name format")

        catalog_name, schema_name = parts
        schemas = self.schemas.get(catalog_name, [])
        schema = next((s for s in schemas if s["name"] == schema_name), None)
        if not schema:
            raise Exception(f"Schema {full_name} not found")
        return schema

    # Table operations
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
        **kwargs,
    ):
        # Track the call
        self.list_tables_calls.append(
            (
                catalog_name,
                schema_name,
                max_results,
                page_token,
                include_delta_metadata,
                omit_columns,
                omit_properties,
                omit_username,
                include_browse,
                include_manifest_capabilities,
            )
        )
        key = (catalog_name, schema_name)
        tables = self.tables.get(key, [])
        return {"tables": tables, "next_page_token": None}

    def get_table(
        self,
        full_name,
        include_delta_metadata=False,
        include_browse=False,
        include_manifest_capabilities=False,
        full_table_name=None,
        **kwargs,
    ):
        # Track the call
        self.get_table_calls.append(
            (
                full_name or full_table_name,
                include_delta_metadata,
                include_browse,
                include_manifest_capabilities,
            )
        )
        # Support both parameter names for compatibility
        table_name = full_name or full_table_name
        if not table_name:
            raise Exception("Table name is required")

        # Parse full_table_name and return table details
        parts = table_name.split(".")
        if len(parts) != 3:
            raise Exception("Invalid table name format")

        catalog, schema, table = parts
        key = (catalog, schema)
        tables = self.tables.get(key, [])
        table_info = next((t for t in tables if t["name"] == table), None)
        if not table_info:
            raise Exception(f"Table {table_name} not found")
        return table_info

    # Model operations
    def list_models(self, **kwargs):
        if hasattr(self, "_list_models_error"):
            raise self._list_models_error
        return self.models

    def get_model(self, model_name):
        if hasattr(self, "_get_model_error"):
            raise self._get_model_error
        model = next((m for m in self.models if m["name"] == model_name), None)
        return model

    # Warehouse operations
    def list_warehouses(self, **kwargs):
        return self.warehouses

    def get_warehouse(self, warehouse_id):
        warehouse = next((w for w in self.warehouses if w["id"] == warehouse_id), None)
        if not warehouse:
            raise Exception(f"Warehouse {warehouse_id} not found")
        return warehouse

    def start_warehouse(self, warehouse_id):
        warehouse = self.get_warehouse(warehouse_id)
        warehouse["state"] = "STARTING"
        return warehouse

    def stop_warehouse(self, warehouse_id):
        warehouse = self.get_warehouse(warehouse_id)
        warehouse["state"] = "STOPPING"
        return warehouse

    # Volume operations
    def list_volumes(self, catalog_name, **kwargs):
        return {"volumes": self.volumes.get(catalog_name, [])}

    def create_volume(
        self, catalog_name, schema_name, volume_name, volume_type="MANAGED", **kwargs
    ):
        key = catalog_name
        if key not in self.volumes:
            self.volumes[key] = []

        volume = {
            "name": volume_name,
            "full_name": f"{catalog_name}.{schema_name}.{volume_name}",
            "volume_type": volume_type,
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            **kwargs,
        }
        self.volumes[key].append(volume)
        return volume

    # SQL operations
    def execute_sql(self, sql, **kwargs):
        # Return pre-configured results or default
        if sql in self.sql_results:
            return self.sql_results[sql]

        # Default response
        return {
            "result": {
                "data_array": [["row1_col1", "row1_col2"], ["row2_col1", "row2_col2"]],
                "column_names": ["col1", "col2"],
            },
            "next_page_token": kwargs.get("return_next_page") and "next_token" or None,
        }

    def submit_sql_statement(self, sql_text=None, sql=None, **kwargs):
        # Support both parameter names for compatibility
        # Return successful SQL submission by default
        return {"status": {"state": "SUCCEEDED"}}

    # PII scanning
    def scan_table_pii(self, table_name):
        if table_name in self.pii_scan_results:
            return self.pii_scan_results[table_name]

        return {
            "table_name": table_name,
            "pii_columns": ["email", "phone"],
            "scan_timestamp": "2023-01-01T00:00:00Z",
        }

    def tag_columns_pii(self, table_name, columns, pii_type):
        return {
            "table_name": table_name,
            "tagged_columns": columns,
            "pii_type": pii_type,
            "status": "success",
        }

    # Connection/status
    def test_connection(self):
        if self.connection_status == "connected":
            return {"status": "success", "workspace": "test-workspace"}
        else:
            raise Exception("Connection failed")

    def get_current_user(self):
        return {"userName": "test.user@example.com", "displayName": "Test User"}

    # File upload operations
    def upload_file(self, file_path, destination_path):
        return {
            "source_path": file_path,
            "destination_path": destination_path,
            "status": "uploaded",
            "size_bytes": 1024,
        }

    # Job operations
    def list_jobs(self, **kwargs):
        return {"jobs": []}

    def get_job(self, job_id):
        return {
            "job_id": job_id,
            "settings": {"name": f"test_job_{job_id}"},
            "state": "TERMINATED",
        }

    def run_job(self, job_id):
        return {"run_id": f"run_{job_id}_001", "job_id": job_id, "state": "RUNNING"}

    def submit_job_run(self, config_path, init_script_path, run_name=None):
        """Submit a job run and return run_id."""
        from datetime import datetime

        if not run_name:
            run_name = (
                f"Chuck AI One-Time Run {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

        # Return a successful job submission
        return {"run_id": 123456}

    def get_job_run_status(self, run_id):
        """Get job run status."""
        return {
            "state": {"life_cycle_state": "RUNNING"},
            "run_id": int(run_id),
            "run_name": "Test Run",
            "creator_user_name": "test@example.com",
        }

    # Helper methods to set up test data
    def add_catalog(self, name, catalog_type="MANAGED", **kwargs):
        catalog = {"name": name, "type": catalog_type, **kwargs}
        self.catalogs.append(catalog)
        return catalog

    def add_schema(self, catalog_name, schema_name, **kwargs):
        if catalog_name not in self.schemas:
            self.schemas[catalog_name] = []
        schema = {"name": schema_name, "catalog_name": catalog_name, **kwargs}
        self.schemas[catalog_name].append(schema)
        return schema

    def add_table(
        self, catalog_name, schema_name, table_name, table_type="MANAGED", **kwargs
    ):
        key = (catalog_name, schema_name)
        if key not in self.tables:
            self.tables[key] = []

        table = {
            "name": table_name,
            "full_name": f"{catalog_name}.{schema_name}.{table_name}",
            "table_type": table_type,
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "comment": kwargs.get("comment", ""),
            "created_at": kwargs.get("created_at", "2023-01-01T00:00:00Z"),
            "created_by": kwargs.get("created_by", "test.user@example.com"),
            "owner": kwargs.get("owner", "test.user@example.com"),
            "columns": kwargs.get("columns", []),
            "properties": kwargs.get("properties", {}),
            **kwargs,
        }
        self.tables[key].append(table)
        return table

    def add_model(self, name, status="READY", **kwargs):
        model = {"name": name, "status": status, **kwargs}
        self.models.append(model)
        return model

    def add_warehouse(
        self,
        warehouse_id=None,
        name="Test Warehouse",
        state="RUNNING",
        size="SMALL",
        enable_serverless_compute=False,
        warehouse_type="PRO",
        creator_name="test.user@example.com",
        auto_stop_mins=60,
        **kwargs,
    ):
        if warehouse_id is None:
            warehouse_id = f"warehouse_{len(self.warehouses)}"

        warehouse = {
            "id": warehouse_id,
            "name": name,
            "state": state,
            "size": size,  # Use size instead of cluster_size for the main field
            "cluster_size": size,  # Keep cluster_size for backward compatibility
            "enable_serverless_compute": enable_serverless_compute,
            "warehouse_type": warehouse_type,
            "creator_name": creator_name,
            "auto_stop_mins": auto_stop_mins,
            "jdbc_url": f"jdbc:databricks://test.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=/sql/1.0/warehouses/{warehouse_id}",
            **kwargs,
        }
        self.warehouses.append(warehouse)
        return warehouse

    def add_volume(
        self, catalog_name, schema_name, volume_name, volume_type="MANAGED", **kwargs
    ):
        key = catalog_name
        if key not in self.volumes:
            self.volumes[key] = []

        volume = {
            "name": volume_name,
            "full_name": f"{catalog_name}.{schema_name}.{volume_name}",
            "volume_type": volume_type,
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            **kwargs,
        }
        self.volumes[key].append(volume)
        return volume

    def set_sql_result(self, sql, result):
        """Set a specific result for a SQL query."""
        self.sql_results[sql] = result

    def set_pii_scan_result(self, table_name, result):
        """Set a specific PII scan result for a table."""
        self.pii_scan_results[table_name] = result

    def set_connection_status(self, status):
        """Set the connection status for testing."""
        self.connection_status = status

    def set_list_models_error(self, error):
        """Configure list_models to raise an error."""
        self._list_models_error = error

    def set_get_model_error(self, error):
        """Configure get_model to raise an error."""
        self._get_model_error = error

    def create_stitch_notebook(self, *args, **kwargs):
        """Create a stitch notebook (simulate successful creation)."""
        # Track the call
        self.create_stitch_notebook_calls.append((args, kwargs))

        if hasattr(self, "_create_stitch_notebook_result"):
            return self._create_stitch_notebook_result
        if hasattr(self, "_create_stitch_notebook_error"):
            raise self._create_stitch_notebook_error
        return {
            "notebook_id": "test-notebook-123",
            "path": "/Workspace/Stitch/test_notebook.py",
        }

    def set_create_stitch_notebook_result(self, result):
        """Configure create_stitch_notebook return value."""
        self._create_stitch_notebook_result = result

    def set_create_stitch_notebook_error(self, error):
        """Configure create_stitch_notebook to raise error."""
        self._create_stitch_notebook_error = error

    def reset(self):
        """Reset all data to initial state."""
        self.catalogs = []
        self.schemas = {}
        self.tables = {}
        self.models = []
        self.warehouses = []
        self.volumes = {}
        self.connection_status = "connected"
        self.permissions = {}
        self.sql_results = {}
        self.pii_scan_results = {}


# Model response fixtures
MODEL_FIXTURES = {
    "endpoints": [
        {
            "name": "databricks-llama-4-maverick",
            "config": {
                "served_entities": [
                    {
                        "name": "databricks-llama-4-maverick",
                        "foundation_model": {"name": "Llama 4 Maverick"},
                    }
                ],
            },
        },
        {
            "name": "databricks-claude-3-7-sonnet",
            "config": {
                "served_entities": [
                    {
                        "name": "databricks-claude-3-7-sonnet",
                        "foundation_model": {"name": "Claude 3.7 Sonnet"},
                    }
                ],
            },
        },
    ]
}

# Expected model list after parsing
EXPECTED_MODEL_LIST = [
    {
        "name": "databricks-llama-4-maverick",
        "config": {
            "served_entities": [
                {
                    "name": "databricks-llama-4-maverick",
                    "foundation_model": {"name": "Llama 4 Maverick"},
                }
            ],
        },
    },
    {
        "name": "databricks-claude-3-7-sonnet",
        "config": {
            "served_entities": [
                {
                    "name": "databricks-claude-3-7-sonnet",
                    "foundation_model": {"name": "Claude 3.7 Sonnet"},
                }
            ],
        },
    },
]

# Empty model response
EMPTY_MODEL_RESPONSE = {"endpoints": []}

# For TUI tests
SIMPLE_MODEL_LIST = [
    {"name": "databricks-llama-4-maverick"},
    {"name": "databricks-claude-3-7-sonnet"},
]


class LLMClientStub:
    """Comprehensive stub for LLMClient with predictable responses."""

    def __init__(self):
        self.databricks_token = "test-token"
        self.base_url = "https://test.databricks.com"

        # Test configuration
        self.should_fail_chat = False
        self.should_raise_exception = False
        self.response_content = "Test LLM response"
        self.tool_calls = []
        self.streaming_responses = []

        # Track method calls for testing
        self.chat_calls = []

        # Pre-configured responses for specific scenarios
        self.configured_responses = {}

    def chat(self, messages, model=None, tools=None, stream=False, tool_choice="auto"):
        """Simulate LLM chat completion."""
        # Track the call
        call_info = {
            "messages": messages,
            "model": model,
            "tools": tools,
            "stream": stream,
            "tool_choice": tool_choice,
        }
        self.chat_calls.append(call_info)

        if self.should_raise_exception:
            raise Exception("Test LLM exception")

        if self.should_fail_chat:
            raise Exception("LLM API error")

        # Check for configured response based on messages
        messages_key = str(messages)
        if messages_key in self.configured_responses:
            return self.configured_responses[messages_key]

        # Create mock response structure
        mock_choice = MockChoice()
        mock_choice.message = MockMessage()

        if self.tool_calls:
            # Return tool calls if configured
            mock_choice.message.tool_calls = self.tool_calls
            mock_choice.message.content = None
        else:
            # Return content response
            mock_choice.message.content = self.response_content
            mock_choice.message.tool_calls = None

        mock_response = MockChatResponse()
        mock_response.choices = [mock_choice]

        return mock_response

    def set_response_content(self, content):
        """Set the content for the next chat response."""
        self.response_content = content

    def set_tool_calls(self, tool_calls):
        """Set tool calls for the next chat response."""
        self.tool_calls = tool_calls

    def configure_response_for_messages(self, messages, response):
        """Configure a specific response for specific messages."""
        self.configured_responses[str(messages)] = response

    def set_chat_failure(self, should_fail=True):
        """Configure chat to fail."""
        self.should_fail_chat = should_fail

    def set_exception(self, should_raise=True):
        """Configure chat to raise exception."""
        self.should_raise_exception = should_raise


class MockMessage:
    """Mock LLM message object."""

    def __init__(self):
        self.content = None
        self.tool_calls = None


class MockChoice:
    """Mock LLM choice object."""

    def __init__(self):
        self.message = None


class MockChatResponse:
    """Mock LLM chat response object."""

    def __init__(self):
        self.choices = []


class MockToolCall:
    """Mock LLM tool call object."""

    def __init__(self, id="test-id", name="test-function", arguments="{}"):
        self.id = id
        self.function = MockFunction(name, arguments)


class MockFunction:
    """Mock LLM function object."""

    def __init__(self, name, arguments):
        self.name = name
        self.arguments = arguments


class MetricsCollectorStub:
    """Comprehensive stub for MetricsCollector with predictable responses."""

    def __init__(self):
        # Track method calls for testing
        self.track_event_calls = []

        # Test configuration
        self.should_fail_track_event = False
        self.should_return_false = False

    def track_event(
        self,
        prompt=None,
        tools=None,
        conversation_history=None,
        error=None,
        additional_data=None,
    ):
        """Track an event (simulate metrics collection)."""
        call_info = {
            "prompt": prompt,
            "tools": tools,
            "conversation_history": conversation_history,
            "error": error,
            "additional_data": additional_data,
        }
        self.track_event_calls.append(call_info)

        if self.should_fail_track_event:
            raise Exception("Metrics collection failed")

        return not self.should_return_false

    def set_track_event_failure(self, should_fail=True):
        """Configure track_event to fail."""
        self.should_fail_track_event = should_fail

    def set_return_false(self, should_return_false=True):
        """Configure track_event to return False."""
        self.should_return_false = should_return_false


class ConfigManagerStub:
    """Comprehensive stub for ConfigManager with predictable responses."""

    def __init__(self):
        self.config = ConfigStub()

    def get_config(self):
        """Return the config stub."""
        return self.config


class ConfigStub:
    """Comprehensive stub for Config objects with predictable responses."""

    def __init__(self):
        # Default config values
        self.workspace_url = "https://test.databricks.com"
        self.active_catalog = "test_catalog"
        self.active_schema = "test_schema"
        self.active_model = "test_model"
        self.usage_tracking_consent = True

        # Additional config properties as needed
        self.databricks_token = "test-token"
        self.host = "test.databricks.com"
