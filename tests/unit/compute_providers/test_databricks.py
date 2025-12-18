"""Unit tests for DatabricksComputeProvider."""

from unittest.mock import Mock, patch, MagicMock
import pytest
from chuck_data.compute_providers.databricks import DatabricksComputeProvider


@pytest.fixture
def mock_storage_provider():
    """Mock storage provider for testing."""
    mock_storage = Mock()
    mock_storage.upload_manifest.return_value = "s3://bucket/path/manifest.json"
    mock_storage.upload_init_script.return_value = "s3://bucket/path/init.sh"
    return mock_storage


class TestDatabricksComputeProviderInit:
    """Tests for DatabricksComputeProvider initialization."""

    def test_requires_storage_provider(self):
        """Test that storage_provider is required."""
        with pytest.raises(ValueError) as exc_info:
            DatabricksComputeProvider(
                workspace_url="https://test.databricks.com",
                token="test-token",
                storage_provider=None,
            )

        assert "storage_provider is required" in str(exc_info.value)
        assert "ProviderFactory.create_storage_provider" in str(exc_info.value)

    def test_can_instantiate(self, mock_storage_provider):
        """Test that DatabricksComputeProvider can be instantiated."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )
        assert provider.workspace_url == "https://test.databricks.com"
        assert provider.token == "test-token"
        assert provider.client is not None

    def test_accepts_additional_kwargs(self, mock_storage_provider):
        """Test that additional kwargs are stored in config."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
            cluster_size="small",
            custom_setting="value",
        )

        assert provider.config["cluster_size"] == "small"
        assert provider.config["custom_setting"] == "value"

    def test_creates_databricks_client(self, mock_storage_provider):
        """Test that DatabricksAPIClient is instantiated."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )
        # Client is created with the provided workspace_url
        assert provider.client is not None
        assert hasattr(provider.client, "workspace_url")


class TestPrepareStitchJob:
    """Tests for prepare_stitch_job method."""

    @patch("chuck_data.compute_providers.databricks.get_amperity_token")
    @patch("chuck_data.compute_providers.databricks._helper_upload_cluster_init_logic")
    def test_prepare_stitch_job_success(
        self,
        mock_upload_init,
        mock_get_token,
        mock_databricks_client,
        mock_llm_client,
        mock_storage_provider,
    ):
        """Test successful Stitch job preparation."""
        # Setup mocks
        mock_get_token.return_value = "test-amperity-token"
        mock_upload_init.return_value = {
            "success": True,
            "volume_path": "/Volumes/test_cat/test_schema/chuck/cluster_init-2024-01-01_12-00.sh",
        }

        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )
        provider.client = mock_databricks_client

        # Mock PII scan results
        pii_scan_result = {
            "success": True,
            "message": "Scan complete",
            "results_detail": [
                {
                    "full_name": "test_cat.test_schema.customers",
                    "has_pii": True,
                    "columns": [
                        {"name": "email", "type": "STRING", "semantic": "email"},
                        {"name": "phone", "type": "STRING", "semantic": "phone"},
                    ],
                }
            ],
        }

        # Mock client methods
        mock_databricks_client.list_volumes.return_value = {"volumes": []}
        mock_databricks_client.create_volume.return_value = {"name": "chuck"}

        config = {
            "target_catalog": "test_cat",
            "target_schema": "test_schema",
            "llm_client": mock_llm_client,
        }

        with patch(
            "chuck_data.commands.pii_tools._helper_scan_schema_for_pii_logic"
        ) as mock_scan:
            mock_scan.return_value = pii_scan_result

            with patch(
                "chuck_data.clients.amperity.AmperityAPIClient.fetch_amperity_job_init"
            ) as mock_fetch_init:
                mock_fetch_init.return_value = {
                    "cluster-init": "#!/bin/bash\necho 'init'",
                    "job-id": "test-job-123",
                }

                result = provider.prepare_stitch_job(
                    manifest={}, data_provider=None, config=config
                )

        # Assertions
        assert result["success"] is True
        assert "stitch_config" in result
        assert "metadata" in result
        assert result["stitch_config"]["name"].startswith("stitch-")
        assert len(result["stitch_config"]["tables"]) == 1
        assert result["metadata"]["job_id"] == "test-job-123"
        assert result["metadata"]["target_catalog"] == "test_cat"

    def test_prepare_stitch_job_missing_catalog(self, mock_storage_provider):
        """Test that missing catalog returns error."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )

        config = {"target_schema": "test_schema", "llm_client": Mock()}

        result = provider.prepare_stitch_job(
            manifest={}, data_provider=None, config=config
        )

        assert "error" in result
        assert "catalog and schema are required" in result["error"]

    def test_prepare_stitch_job_missing_llm_client(self, mock_storage_provider):
        """Test that missing LLM client returns error."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )

        config = {"target_catalog": "test_cat", "target_schema": "test_schema"}

        result = provider.prepare_stitch_job(
            manifest={}, data_provider=None, config=config
        )

        assert "error" in result
        assert "LLM client is required" in result["error"]

    @patch("chuck_data.compute_providers.databricks.get_amperity_token")
    def test_prepare_stitch_job_pii_scan_fails(
        self, mock_databricks_client, mock_llm_client, mock_storage_provider
    ):
        """Test handling of PII scan failure."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )
        provider.client = mock_databricks_client

        config = {
            "target_catalog": "test_cat",
            "target_schema": "test_schema",
            "llm_client": mock_llm_client,
        }

        with patch(
            "chuck_data.commands.pii_tools._helper_scan_schema_for_pii_logic"
        ) as mock_scan:
            mock_scan.return_value = {"error": "PII scan failed"}

            result = provider.prepare_stitch_job(
                manifest={}, data_provider=None, config=config
            )

        assert "error" in result
        assert "PII Scan failed" in result["error"]

    @patch("chuck_data.compute_providers.databricks.get_amperity_token")
    def test_prepare_stitch_job_no_pii_tables(
        self,
        mock_get_token,
        mock_databricks_client,
        mock_llm_client,
        mock_storage_provider,
    ):
        """Test handling when no tables with PII are found."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )
        provider.client = mock_databricks_client

        pii_scan_result = {
            "success": True,
            "message": "Scan complete",
            "results_detail": [
                {"full_name": "test_cat.test_schema.customers", "has_pii": False}
            ],
        }

        mock_databricks_client.list_volumes.return_value = {"volumes": []}

        config = {
            "target_catalog": "test_cat",
            "target_schema": "test_schema",
            "llm_client": mock_llm_client,
        }

        with patch(
            "chuck_data.commands.pii_tools._helper_scan_schema_for_pii_logic"
        ) as mock_scan:
            mock_scan.return_value = pii_scan_result

            result = provider.prepare_stitch_job(
                manifest={}, data_provider=None, config=config
            )

        assert "error" in result
        assert "No tables with PII found" in result["error"]

    @patch("chuck_data.compute_providers.databricks.get_amperity_token")
    def test_prepare_stitch_job_no_amperity_token(
        self,
        mock_get_token,
        mock_databricks_client,
        mock_llm_client,
        mock_storage_provider,
    ):
        """Test handling when Amperity token is not found."""
        mock_get_token.return_value = None

        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )
        provider.client = mock_databricks_client

        pii_scan_result = {
            "success": True,
            "message": "Scan complete",
            "results_detail": [
                {
                    "full_name": "test_cat.test_schema.customers",
                    "has_pii": True,
                    "columns": [
                        {"name": "email", "type": "STRING", "semantic": "email"}
                    ],
                }
            ],
        }

        mock_databricks_client.list_volumes.return_value = {"volumes": []}
        mock_databricks_client.create_volume.return_value = {"name": "chuck"}

        config = {
            "target_catalog": "test_cat",
            "target_schema": "test_schema",
            "llm_client": mock_llm_client,
        }

        with patch(
            "chuck_data.commands.pii_tools._helper_scan_schema_for_pii_logic"
        ) as mock_scan:
            mock_scan.return_value = pii_scan_result

            result = provider.prepare_stitch_job(
                manifest={}, data_provider=None, config=config
            )

        assert "error" in result
        assert "Amperity token not found" in result["error"]

    @patch("chuck_data.compute_providers.databricks.get_amperity_token")
    @patch("chuck_data.compute_providers.databricks._helper_upload_cluster_init_logic")
    def test_prepare_stitch_job_filters_unsupported_types(
        self,
        mock_upload_init,
        mock_get_token,
        mock_databricks_client,
        mock_llm_client,
        mock_storage_provider,
    ):
        """Test that unsupported column types are filtered out."""
        mock_get_token.return_value = "test-amperity-token"
        mock_upload_init.return_value = {
            "success": True,
            "volume_path": "/Volumes/test_cat/test_schema/chuck/cluster_init.sh",
        }

        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )
        provider.client = mock_databricks_client

        pii_scan_result = {
            "success": True,
            "message": "Scan complete",
            "results_detail": [
                {
                    "full_name": "test_cat.test_schema.customers",
                    "has_pii": True,
                    "columns": [
                        {"name": "email", "type": "STRING", "semantic": "email"},
                        {"name": "metadata", "type": "ARRAY", "semantic": None},
                        {"name": "config", "type": "MAP", "semantic": None},
                    ],
                }
            ],
        }

        mock_databricks_client.list_volumes.return_value = {
            "volumes": [{"name": "chuck"}]
        }

        config = {
            "target_catalog": "test_cat",
            "target_schema": "test_schema",
            "llm_client": mock_llm_client,
        }

        with patch(
            "chuck_data.commands.pii_tools._helper_scan_schema_for_pii_logic"
        ) as mock_scan:
            mock_scan.return_value = pii_scan_result

            with patch(
                "chuck_data.clients.amperity.AmperityAPIClient.fetch_amperity_job_init"
            ) as mock_fetch_init:
                mock_fetch_init.return_value = {
                    "cluster-init": "#!/bin/bash",
                    "job-id": "test-job-123",
                }

                result = provider.prepare_stitch_job(
                    manifest={}, data_provider=None, config=config
                )

        # Only email should be included (STRING type)
        assert len(result["stitch_config"]["tables"][0]["fields"]) == 1
        assert (
            result["stitch_config"]["tables"][0]["fields"][0]["field-name"] == "email"
        )

        # Unsupported columns should be tracked
        assert len(result["metadata"]["unsupported_columns"]) == 1
        assert len(result["metadata"]["unsupported_columns"][0]["columns"]) == 2


class TestLaunchStitchJob:
    """Tests for launch_stitch_job method."""

    def test_launch_stitch_job_preparation_failed(self, mock_storage_provider):
        """Test that job launch fails if preparation failed."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )

        preparation = {"success": False, "error": "Preparation failed"}

        result = provider.launch_stitch_job(preparation)

        assert "error" in result
        assert "preparation failed" in result["error"]

    @patch("chuck_data.compute_providers.databricks.get_amperity_token")
    def test_launch_stitch_job_success(
        self, mock_get_token, mock_databricks_client, mock_storage_provider
    ):
        """Test successful Stitch job launch."""
        mock_get_token.return_value = "test-amperity-token"

        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )
        provider.client = mock_databricks_client

        # Mock storage provider
        mock_storage = Mock()
        mock_storage.upload_file.return_value = True
        provider.storage_provider = mock_storage

        # Mock client methods
        mock_databricks_client.submit_job_run.return_value = {"run_id": 12345}
        mock_databricks_client.create_stitch_notebook.return_value = {
            "notebook_path": "/Workspace/Users/test/notebook"
        }

        # Mock AmperityAPIClient (imported inside the function)
        with patch("chuck_data.clients.amperity.AmperityAPIClient") as mock_amperity:
            with patch("chuck_data.job_cache.cache_job") as mock_cache:
                mock_amperity_instance = Mock()
                mock_amperity.return_value = mock_amperity_instance

                preparation = {
                    "success": True,
                    "stitch_config": {
                        "name": "stitch-2024-01-01_12-00",
                        "tables": [{"path": "test_cat.test_schema.customers"}],
                    },
                    "metadata": {
                        "target_catalog": "test_cat",
                        "target_schema": "test_schema",
                        "stitch_job_name": "stitch-2024-01-01_12-00",
                        "config_file_path": "/Volumes/test_cat/test_schema/chuck/config.json",
                        "init_script_path": "/Volumes/test_cat/test_schema/chuck/init.sh",
                        "init_script_content": "#!/bin/bash",
                        "amperity_token": "test-token",
                        "job_id": "test-job-123",
                        "pii_scan_output": {"message": "Scan complete"},
                        "unsupported_columns": [],
                    },
                }

                result = provider.launch_stitch_job(preparation)

        # Assertions
        assert result["success"] is True
        assert result["run_id"] == 12345
        assert result["job_id"] == "test-job-123"
        assert "Stitch setup for test_cat.test_schema initiated" in result["message"]

    def test_launch_stitch_job_config_upload_fails(
        self, mock_databricks_client, mock_storage_provider
    ):
        """Test handling of config file upload failure."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )
        provider.client = mock_databricks_client

        # Mock storage provider to return False
        mock_storage = Mock()
        mock_storage.upload_file.return_value = False
        provider.storage_provider = mock_storage

        preparation = {
            "success": True,
            "stitch_config": {"name": "test-stitch"},
            "metadata": {
                "target_catalog": "test_cat",
                "target_schema": "test_schema",
                "stitch_job_name": "stitch-test",
                "config_file_path": "/Volumes/test_cat/test_schema/chuck/config.json",
                "init_script_path": "/Volumes/test_cat/test_schema/chuck/init.sh",
                "init_script_content": "#!/bin/bash",
                "pii_scan_output": {},
                "unsupported_columns": [],
            },
        }

        result = provider.launch_stitch_job(preparation)

        assert "error" in result
        assert "Failed to write Stitch config" in result["error"]

    @patch("chuck_data.compute_providers.databricks.get_amperity_token")
    def test_launch_stitch_job_with_unsupported_columns(
        self, mock_get_token, mock_databricks_client, mock_storage_provider
    ):
        """Test that unsupported columns are included in the summary."""
        mock_get_token.return_value = "test-amperity-token"

        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )
        provider.client = mock_databricks_client

        # Mock storage provider
        mock_storage = Mock()
        mock_storage.upload_file.return_value = True
        provider.storage_provider = mock_storage

        mock_databricks_client.submit_job_run.return_value = {"run_id": 12345}
        mock_databricks_client.create_stitch_notebook.return_value = {
            "notebook_path": "/Workspace/test"
        }

        with patch("chuck_data.clients.amperity.AmperityAPIClient"):
            with patch("chuck_data.job_cache.cache_job"):
                preparation = {
                    "success": True,
                    "stitch_config": {"name": "test-stitch", "tables": []},
                    "metadata": {
                        "target_catalog": "test_cat",
                        "target_schema": "test_schema",
                        "stitch_job_name": "stitch-test",
                        "config_file_path": "/Volumes/test/config.json",
                        "init_script_path": "/Volumes/test/init.sh",
                        "init_script_content": "#!/bin/bash",
                        "amperity_token": "test-token",
                        "job_id": "test-job-123",
                        "pii_scan_output": {},
                        "unsupported_columns": [
                            {
                                "table": "test.table",
                                "columns": [
                                    {
                                        "column": "metadata",
                                        "type": "ARRAY",
                                        "semantic": None,
                                    }
                                ],
                            }
                        ],
                    },
                }

                result = provider.launch_stitch_job(preparation)

        assert "unsupported data types" in result["message"]
        assert "metadata" in result["message"]


class TestGetJobStatus:
    """Tests for get_job_status method."""

    def test_get_job_status_success(
        self, mock_databricks_client, mock_storage_provider
    ):
        """Test successful job status retrieval."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )
        provider.client = mock_databricks_client

        mock_databricks_client.get_job_run_status.return_value = {
            "run_id": 12345,
            "state": {
                "life_cycle_state": "RUNNING",
                "result_state": None,
                "state_message": "Job is running",
            },
        }

        result = provider.get_job_status("12345")

        assert result["success"] is True
        assert result["status"] == "RUNNING"
        assert result["run_id"] == "12345"

    def test_get_job_status_failure(
        self, mock_databricks_client, mock_storage_provider
    ):
        """Test job status retrieval failure."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )
        provider.client = mock_databricks_client

        mock_databricks_client.get_job_run_status.side_effect = Exception("API error")

        result = provider.get_job_status("12345")

        assert result["success"] is False
        assert "error" in result


class TestCancelJob:
    """Tests for cancel_job method."""

    def test_cancel_job_not_implemented(self, mock_storage_provider):
        """Test that cancel_job raises NotImplementedError."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )

        with pytest.raises(NotImplementedError) as exc_info:
            provider.cancel_job("test-job-id")

        assert "cancel_run() method to DatabricksAPIClient" in str(exc_info.value)


class TestCreateStitchReportNotebook:
    """Tests for _create_stitch_report_notebook helper method."""

    def test_create_notebook_success(
        self, mock_databricks_client, mock_storage_provider
    ):
        """Test successful notebook creation."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )
        provider.client = mock_databricks_client

        mock_databricks_client.create_stitch_notebook.return_value = {
            "notebook_path": "/Workspace/Users/test/Stitch Report"
        }

        stitch_config = {"name": "test-stitch", "tables": []}

        result = provider._create_stitch_report_notebook(
            stitch_config=stitch_config,
            target_catalog="test_cat",
            target_schema="test_schema",
            stitch_job_name="stitch-2024-01-01",
        )

        assert result["success"] is True
        assert "notebook_path" in result

    def test_create_notebook_failure(
        self, mock_databricks_client, mock_storage_provider
    ):
        """Test notebook creation failure."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )
        provider.client = mock_databricks_client

        mock_databricks_client.create_stitch_notebook.side_effect = Exception(
            "Notebook creation failed"
        )

        result = provider._create_stitch_report_notebook(
            stitch_config={},
            target_catalog="test_cat",
            target_schema="test_schema",
            stitch_job_name="stitch-test",
        )

        assert result["success"] is False
        assert "error" in result


# Fixtures
@pytest.fixture
def mock_databricks_client():
    """Create a mock DatabricksAPIClient."""
    client = MagicMock()
    client.workspace_url = "https://test.databricks.com"
    return client


@pytest.fixture
def mock_llm_client():
    """Create a mock LLM client."""
    return MagicMock()
