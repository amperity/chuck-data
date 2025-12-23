"""Tests for Workspace API methods and init script handling."""

import pytest
from unittest.mock import patch, MagicMock, call
from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.clients.amperity import AmperityAPIClient


class TestDatabricksWorkspaceAPIs:
    """Tests for DatabricksAPIClient workspace methods."""

    @pytest.fixture
    def client(self):
        """Create a DatabricksAPIClient for testing."""
        return DatabricksAPIClient("test-workspace", "fake-token")

    def test_workspace_mkdirs(self, client):
        """Test workspace_mkdirs creates directory."""
        with patch.object(client, "post") as mock_post:
            mock_post.return_value = {}

            result = client.workspace_mkdirs(
                "/Users/user@example.com/.chuck/init-scripts"
            )

            mock_post.assert_called_once_with(
                "/api/2.0/workspace/mkdirs",
                {"path": "/Users/user@example.com/.chuck/init-scripts"},
            )
            assert result == {}

    def test_workspace_import_with_defaults(self, client):
        """Test workspace_import with default parameters."""
        with patch.object(client, "post") as mock_post:
            mock_post.return_value = {}

            result = client.workspace_import(
                "/Users/user@example.com/script.sh",
                "IyEvYmluL2Jhc2gK",  # base64 encoded content
            )

            mock_post.assert_called_once_with(
                "/api/2.0/workspace/import",
                {
                    "path": "/Users/user@example.com/script.sh",
                    "content": "IyEvYmluL2Jhc2gK",
                    "format": "AUTO",
                    "overwrite": True,
                },
            )
            assert result == {}

    def test_workspace_import_with_custom_params(self, client):
        """Test workspace_import with custom format and overwrite."""
        with patch.object(client, "post") as mock_post:
            mock_post.return_value = {}

            result = client.workspace_import(
                "/Users/user@example.com/notebook.ipynb",
                "encoded_notebook_content",
                format="JUPYTER",
                overwrite=False,
            )

            mock_post.assert_called_once_with(
                "/api/2.0/workspace/import",
                {
                    "path": "/Users/user@example.com/notebook.ipynb",
                    "content": "encoded_notebook_content",
                    "format": "JUPYTER",
                    "overwrite": False,
                },
            )
            assert result == {}


class TestSubmitJobRunInitScripts:
    """Tests for submit_job_run init script configuration."""

    @pytest.fixture
    def client(self):
        """Create a DatabricksAPIClient for testing."""
        return DatabricksAPIClient("test-workspace", "fake-token")

    def test_submit_job_run_with_volumes_init_script(self, client):
        """Test submit_job_run uses volumes format for Volumes paths."""
        with patch.object(client, "post") as mock_post:
            mock_post.return_value = {"run_id": 123}

            result = client.submit_job_run(
                config_path="/Volumes/catalog/schema/volume/config.json",
                init_script_path="/Volumes/catalog/schema/volume/init.sh",
                run_name="Test Run",
            )

            # Verify the call was made
            assert mock_post.called
            call_args = mock_post.call_args

            # Extract the payload
            payload = call_args[0][1]

            # Verify init_scripts configuration uses volumes format
            init_scripts = payload["tasks"][0]["new_cluster"]["init_scripts"]
            assert len(init_scripts) == 1
            assert "volumes" in init_scripts[0]
            assert (
                init_scripts[0]["volumes"]["destination"]
                == "/Volumes/catalog/schema/volume/init.sh"
            )
            assert "s3" not in init_scripts[0]

            assert result == {"run_id": 123}

    @patch("chuck_data.config.get_redshift_region")
    def test_submit_job_run_with_s3_init_script(self, mock_get_region, client):
        """Test submit_job_run uses s3 format for S3 paths."""
        mock_get_region.return_value = "us-east-1"

        with patch.object(client, "post") as mock_post:
            mock_post.return_value = {"run_id": 456}

            result = client.submit_job_run(
                config_path="s3://bucket/manifests/config.json",
                init_script_path="s3://bucket/init-scripts/init.sh",
                run_name="Redshift Test Run",
            )

            # Verify the call was made
            assert mock_post.called
            call_args = mock_post.call_args

            # Extract the payload
            payload = call_args[0][1]

            # Verify init_scripts configuration uses s3 format
            init_scripts = payload["tasks"][0]["new_cluster"]["init_scripts"]
            assert len(init_scripts) == 1
            assert "s3" in init_scripts[0]
            assert (
                init_scripts[0]["s3"]["destination"]
                == "s3://bucket/init-scripts/init.sh"
            )
            assert init_scripts[0]["s3"]["region"] == "us-east-1"
            assert "volumes" not in init_scripts[0]

            assert result == {"run_id": 456}

    @patch("chuck_data.config.get_redshift_region")
    def test_submit_job_run_s3_default_region(self, mock_get_region, client):
        """Test submit_job_run uses default region when config returns None."""
        mock_get_region.return_value = None

        with patch.object(client, "post") as mock_post:
            mock_post.return_value = {"run_id": 789}

            client.submit_job_run(
                config_path="s3://bucket/config.json",
                init_script_path="s3://bucket/init.sh",
            )

            # Extract the payload
            payload = mock_post.call_args[0][1]
            init_scripts = payload["tasks"][0]["new_cluster"]["init_scripts"]

            # Should default to us-west-2
            assert init_scripts[0]["s3"]["region"] == "us-west-2"

    def test_submit_job_run_with_policy_id(self, client):
        """Test submit_job_run includes policy_id when provided."""
        with patch.object(client, "post") as mock_post:
            mock_post.return_value = {"run_id": 999}

            client.submit_job_run(
                config_path="/Volumes/cat/schema/vol/config.json",
                init_script_path="/Volumes/cat/schema/vol/init.sh",
                policy_id="test-policy-123",
            )

            payload = mock_post.call_args[0][1]
            cluster_config = payload["tasks"][0]["new_cluster"]

            assert cluster_config["policy_id"] == "test-policy-123"


class TestAmperityFetchInitScript:
    """Tests for AmperityAPIClient.fetch_amperity_job_init."""

    @pytest.fixture
    def client(self):
        """Create an AmperityAPIClient for testing."""
        return AmperityAPIClient()

    @patch("chuck_data.clients.amperity.requests.post")
    def test_fetch_amperity_job_init_success(self, mock_post, client):
        """Test successful fetch of init script and job-id."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "cluster-init": "#!/bin/bash\necho 'init script'",
            "job-id": "job-123",
        }
        mock_post.return_value = mock_response

        result = client.fetch_amperity_job_init("test-token")

        assert result["cluster-init"] == "#!/bin/bash\necho 'init script'"
        assert result["job-id"] == "job-123"

        # Verify request was made correctly
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args[1]
        assert call_kwargs["headers"]["Authorization"] == "Bearer test-token"
        assert call_kwargs["headers"]["Content-Type"] == "application/json"

    @patch("chuck_data.clients.amperity.requests.post")
    def test_fetch_amperity_job_init_with_custom_url(self, mock_post, client):
        """Test fetch with custom API URL."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"cluster-init": "script", "job-id": "456"}
        mock_post.return_value = mock_response

        custom_url = "https://custom.amperity.com/api/job/launch"
        result = client.fetch_amperity_job_init("token", api_url=custom_url)

        assert result["job-id"] == "456"
        mock_post.assert_called_once()
        assert mock_post.call_args[0][0] == custom_url

    @patch("chuck_data.clients.amperity.requests.post")
    def test_fetch_amperity_job_init_http_error(self, mock_post, client):
        """Test handling of HTTP errors."""
        import requests

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_response.json.return_value = {"message": "Invalid token"}

        mock_post.side_effect = requests.exceptions.HTTPError(response=mock_response)

        with pytest.raises(ValueError, match="401 Error.*Invalid token"):
            client.fetch_amperity_job_init("bad-token")

    @patch("chuck_data.clients.amperity.requests.post")
    def test_fetch_amperity_job_init_connection_error(self, mock_post, client):
        """Test handling of connection errors."""
        import requests

        mock_post.side_effect = requests.RequestException("Connection failed")

        with pytest.raises(ConnectionError, match="Connection error occurred"):
            client.fetch_amperity_job_init("token")


class TestProtocolImplementation:
    """Tests for protocol implementations."""

    def test_databricks_compute_provider_implements_protocol(self):
        """Test DatabricksComputeProvider implements ComputeProvider protocol."""
        from unittest.mock import Mock
        from chuck_data.compute_providers import (
            DatabricksComputeProvider,
            ComputeProvider,
        )

        # Create mock storage provider
        mock_storage = Mock()

        # Create instance
        provider = DatabricksComputeProvider(
            workspace_url="test-workspace",
            token="test-token",
            storage_provider=mock_storage,
        )

        # Verify it's recognized as implementing the protocol
        assert isinstance(provider, ComputeProvider)

    def test_emr_compute_provider_implements_protocol(self):
        """Test EMRComputeProvider implements ComputeProvider protocol."""
        from unittest.mock import Mock
        from chuck_data.compute_providers import EMRComputeProvider, ComputeProvider

        # Create mock storage provider
        mock_storage = Mock()

        # Create instance
        provider = EMRComputeProvider(
            region="us-west-2",
            storage_provider=mock_storage,
            aws_access_key_id="key",
            aws_secret_access_key="secret",
        )

        # Verify it's recognized as implementing the protocol
        assert isinstance(provider, ComputeProvider)

    def test_databricks_provider_adapter_implements_protocol(self):
        """Test DatabricksProviderAdapter implements DataProvider protocol."""
        from chuck_data.data_providers import DatabricksProviderAdapter, DataProvider

        adapter = DatabricksProviderAdapter(
            workspace_url="test-workspace", token="test-token"
        )

        assert isinstance(adapter, DataProvider)

    def test_redshift_provider_adapter_implements_protocol(self):
        """Test RedshiftProviderAdapter implements DataProvider protocol."""
        from chuck_data.data_providers import RedshiftProviderAdapter, DataProvider

        adapter = RedshiftProviderAdapter(
            aws_access_key_id="key",
            aws_secret_access_key="secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        assert isinstance(adapter, DataProvider)


class TestDependencyInjection:
    """Tests for dependency injection pattern in setup_stitch."""

    @patch("chuck_data.provider_factory.ProviderFactory.create_compute_provider")
    @patch("chuck_data.config.get_workspace_url")
    @patch("chuck_data.config.get_databricks_token")
    @patch("chuck_data.data_providers.is_redshift_client")
    def test_compute_provider_created_once_in_handle_command(
        self, mock_is_redshift, mock_get_token, mock_get_url, mock_factory
    ):
        """Test compute provider uses factory pattern instead of direct instantiation."""
        from chuck_data.commands.setup_stitch import handle_command
        from chuck_data.clients.databricks import DatabricksAPIClient

        # Setup mocks
        mock_get_url.return_value = "https://workspace.databricks.com"
        mock_get_token.return_value = "test-token"
        mock_is_redshift.return_value = False

        mock_provider_instance = MagicMock()
        mock_factory.return_value = mock_provider_instance

        client = MagicMock(spec=DatabricksAPIClient)

        # Call handle_command
        with patch(
            "chuck_data.commands.setup_stitch._handle_databricks_stitch_setup"
        ) as mock_handler:
            mock_handler.return_value = MagicMock()
            handle_command(client, auto_confirm=True)

        # Verify factory was called exactly once
        assert mock_factory.call_count == 1
        # Verify factory was called with correct parameters
        mock_factory.assert_called_once_with(
            "databricks",
            {
                "workspace_url": "https://workspace.databricks.com",
                "token": "test-token",
                "data_provider_type": "databricks",
            },
        )

        # Verify it was passed to the handler
        mock_handler.assert_called_once()
        call_args = mock_handler.call_args[0]
        assert call_args[1] == mock_provider_instance


class TestRedshiftS3Upload:
    """Tests for Redshift init script S3 upload."""

    @patch("boto3.client")
    @patch("chuck_data.commands.setup_stitch.get_redshift_region")
    @patch("chuck_data.clients.amperity.AmperityAPIClient")
    @patch("chuck_data.commands.setup_stitch.get_amperity_token")
    def test_redshift_phase_2_uploads_init_script_to_s3(
        self, mock_get_token, mock_amperity_class, mock_get_region, mock_boto_client
    ):
        """Test that Redshift phase 2 uploads init script to S3."""
        from chuck_data.commands.setup_stitch import _redshift_phase_2_confirm
        from chuck_data.interactive_context import InteractiveContext
        from chuck_data.ui.tui import get_console

        # Setup mocks
        mock_get_token.return_value = "amp-token"
        mock_get_region.return_value = "us-east-1"

        mock_amperity_instance = MagicMock()
        mock_amperity_instance.fetch_amperity_job_init.return_value = {
            "cluster-init": "#!/bin/bash\necho 'test'",
            "job-id": "job-123",
        }
        mock_amperity_class.return_value = mock_amperity_instance

        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client

        # Setup context with stored data
        context = InteractiveContext()
        context.store_context_data("setup_stitch", "database", "testdb")
        context.store_context_data("setup_stitch", "schema_name", "testschema")
        context.store_context_data("setup_stitch", "manifest_path", "/path/to/manifest")
        context.store_context_data("setup_stitch", "manifest_filename", "manifest.json")
        context.store_context_data(
            "setup_stitch", "s3_path", "s3://bucket/manifest.json"
        )
        context.store_context_data("setup_stitch", "s3_bucket", "test-bucket")
        context.store_context_data("setup_stitch", "timestamp", "20231215_120000")
        context.store_context_data("setup_stitch", "tables", [])
        context.store_context_data("setup_stitch", "semantic_tags", [])
        context.store_context_data("setup_stitch", "manifest", {})

        client = MagicMock()
        console = get_console()

        # Mock the submission function and guidance message builder
        with (
            patch(
                "chuck_data.commands.setup_stitch._submit_stitch_job_to_databricks"
            ) as mock_submit,
            patch(
                "chuck_data.commands.setup_stitch._build_post_launch_guidance_message"
            ) as mock_guidance,
        ):
            mock_submit.return_value = {
                "success": True,
                "run_id": 123,
                "databricks_client": MagicMock(),
            }
            mock_guidance.return_value = "Mock guidance message"

            # Call the function
            result = _redshift_phase_2_confirm(context, console, "confirm")

        # Verify S3 upload was called
        mock_s3_client.put_object.assert_called_once()
        call_kwargs = mock_s3_client.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "test-bucket"
        assert "chuck/init-scripts/" in call_kwargs["Key"]
        assert call_kwargs["Body"] == b"#!/bin/bash\necho 'test'"
