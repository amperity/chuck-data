"""Unit tests for EMRComputeProvider."""

import pytest
from unittest.mock import Mock, patch, MagicMock

from chuck_data.compute_providers.emr import EMRComputeProvider


@pytest.fixture
def mock_storage_provider():
    """Create a mock storage provider."""
    provider = Mock()
    provider.upload_file.return_value = True
    return provider


@pytest.fixture
def mock_emr_client():
    """Create a mock EMR client."""
    with patch("chuck_data.compute_providers.emr.EMRAPIClient") as mock_class:
        mock_client = Mock()
        mock_client.validate_connection.return_value = True
        mock_client.get_cluster_status.return_value = "WAITING"
        mock_client.submit_spark_redshift_job.return_value = "s-REDSHIFT123"
        mock_client.submit_spark_databricks_job.return_value = "s-DATABRICKS123"
        mock_client.submit_spark_job.return_value = "s-GENERIC123"
        mock_client.get_monitoring_url.return_value = "https://us-west-2.console.aws.amazon.com/emr/home?region=us-west-2#/clusters/j-TEST123"
        mock_client.submit_bash_script.return_value = "s-INIT123"
        mock_class.return_value = mock_client
        yield mock_client


@pytest.fixture
def emr_provider(mock_storage_provider, mock_emr_client):
    """Create an EMRComputeProvider instance with mocked dependencies."""
    return EMRComputeProvider(
        region="us-west-2",
        cluster_id="j-TEST123",
        storage_provider=mock_storage_provider,
        s3_bucket="test-bucket",
    )


class TestEMRComputeProviderInit:
    """Test EMRComputeProvider initialization."""

    def test_init_with_required_params(self, mock_storage_provider, mock_emr_client):
        """Test initialization with required parameters."""
        provider = EMRComputeProvider(
            region="us-west-2",
            cluster_id="j-TEST123",
            storage_provider=mock_storage_provider,
        )
        assert provider.region == "us-west-2"
        assert provider.cluster_id == "j-TEST123"
        assert provider.storage_provider == mock_storage_provider

    def test_init_without_storage_provider(self):
        """Test initialization fails without storage provider."""
        with pytest.raises(ValueError, match="storage_provider is required"):
            EMRComputeProvider(
                region="us-west-2", cluster_id="j-TEST123", storage_provider=None
            )

    def test_init_with_optional_params(self, mock_storage_provider, mock_emr_client):
        """Test initialization with optional parameters."""
        with patch("boto3.Session"):
            provider = EMRComputeProvider(
                region="us-west-2",
                cluster_id="j-TEST123",
                storage_provider=mock_storage_provider,
                aws_profile="my-profile",
                s3_bucket="my-bucket",
            )
            assert provider.aws_profile == "my-profile"
            assert provider.s3_bucket == "my-bucket"


class TestLaunchStitchJobRedshift:
    """Test launching Stitch jobs with Redshift data source."""

    def test_launch_redshift_job_success(
        self, emr_provider, mock_emr_client, mock_storage_provider
    ):
        """Test successful launch of Stitch job with Redshift connector."""
        # Prepare test data
        stitch_config = {
            "name": "test-stitch",
            "tables": [
                {
                    "path": "customers",
                    "fields": [
                        {
                            "field-name": "email",
                            "type": "STRING",
                            "semantics": ["email"],
                        }
                    ],
                }
            ],
        }

        metadata = {
            "stitch_job_name": "stitch-redshift-test",
            "s3_config_path": "s3://test-bucket/config.json",
            "init_script_path": "s3://test-bucket/chuck/init-scripts/chuck-init-123.sh",
            "s3_jar_path": "s3://test-bucket/chuck/jars/job-123.jar",
            "main_class": "amperity.stitch_standalone.chuck_main",
            "s3_temp_dir": "s3://test-bucket/temp/",
            "redshift_jdbc_url": "jdbc:redshift://cluster.redshift.amazonaws.com:5439/dev",
            "aws_iam_role": "arn:aws:iam::123456789012:role/RedshiftRole",
            "job_id": "job-123",
            "amperity_token": "token-456",
        }

        preparation = {
            "success": True,
            "stitch_config": stitch_config,
            "metadata": metadata,
        }

        # Mock submit_bash_script to return init step ID
        mock_emr_client.submit_bash_script.return_value = "s-INIT123"

        # Mock Amperity API client
        with (
            patch("chuck_data.config.get_amperity_token") as mock_token,
            patch("chuck_data.clients.amperity.AmperityAPIClient") as mock_amp_client,
        ):
            mock_token.return_value = "token-456"
            mock_amp_instance = Mock()
            mock_amp_client.return_value = mock_amp_instance

            # Execute
            result = emr_provider.launch_stitch_job(preparation)

            # Verify
            assert (
                result["success"] is True
            ), f"Expected success=True, got result={result}"
            assert result["step_id"] == "s-REDSHIFT123"
            assert result["cluster_id"] == "j-TEST123"
            assert result["job_id"] == "job-123"
            assert result["s3_config_path"] == "s3://test-bucket/config.json"

            # Verify config was uploaded
            mock_storage_provider.upload_file.assert_called_once()
            call_args = mock_storage_provider.upload_file.call_args
            assert call_args[1]["path"] == "s3://test-bucket/config.json"
            assert call_args[1]["overwrite"] is True

            # Verify init script was executed first with environment variables
            mock_emr_client.submit_bash_script.assert_called_once()
            bash_call_args = mock_emr_client.submit_bash_script.call_args[1]
            assert bash_call_args["name"] == "Download Stitch JAR: stitch-redshift-test"
            assert (
                bash_call_args["script_s3_path"]
                == "s3://test-bucket/chuck/init-scripts/chuck-init-123.sh"
            )
            assert bash_call_args["action_on_failure"] == "CANCEL_AND_WAIT"
            # Verify environment variables are set (similar to Databricks spark_env_vars)
            assert "env_vars" in bash_call_args
            env_vars = bash_call_args["env_vars"]
            assert "CHUCK_API_URL" in env_vars

            # Verify EMR client was called with Redshift connector and S3 JAR path
            mock_emr_client.submit_spark_redshift_job.assert_called_once()
            emr_call_args = mock_emr_client.submit_spark_redshift_job.call_args[1]
            assert emr_call_args["name"] == "Stitch Setup: stitch-redshift-test"
            assert (
                emr_call_args["jar_path"] == "s3://test-bucket/chuck/jars/job-123.jar"
            )  # S3 path from metadata
            assert emr_call_args["s3_temp_dir"] == "s3://test-bucket/temp/"
            assert (
                emr_call_args["redshift_jdbc_url"]
                == "jdbc:redshift://cluster.redshift.amazonaws.com:5439/dev"
            )
            assert (
                emr_call_args["aws_iam_role"]
                == "arn:aws:iam::123456789012:role/RedshiftRole"
            )

    def test_launch_redshift_job_cluster_not_ready(
        self, emr_provider, mock_emr_client, mock_storage_provider
    ):
        """Test launch fails when cluster is not in WAITING or RUNNING state."""
        mock_emr_client.get_cluster_status.return_value = "TERMINATED"

        preparation = {
            "success": True,
            "stitch_config": {"tables": []},
            "metadata": {
                "stitch_job_name": "test",
                "s3_config_path": "s3://bucket/config.json",
                "redshift_jdbc_url": "jdbc:redshift://cluster:5439/dev",
                "s3_temp_dir": "s3://bucket/temp/",
            },
        }

        result = emr_provider.launch_stitch_job(preparation)

        assert result["success"] is False
        assert "TERMINATED" in result["error"]
        assert "Cluster must be WAITING or RUNNING" in result["error"]

    def test_launch_redshift_job_upload_failure(
        self, emr_provider, mock_emr_client, mock_storage_provider
    ):
        """Test launch fails when config upload fails."""
        mock_storage_provider.upload_file.return_value = False

        preparation = {
            "success": True,
            "stitch_config": {"tables": []},
            "metadata": {
                "stitch_job_name": "test",
                "s3_config_path": "s3://bucket/config.json",
                "redshift_jdbc_url": "jdbc:redshift://cluster:5439/dev",
                "s3_temp_dir": "s3://bucket/temp/",
            },
        }

        result = emr_provider.launch_stitch_job(preparation)

        assert result["success"] is False
        assert "Failed to write Stitch config" in result["error"]


class TestLaunchStitchJobDatabricks:
    """Test launching Stitch jobs with Databricks Unity Catalog data source."""

    def test_launch_databricks_job_success(
        self, emr_provider, mock_emr_client, mock_storage_provider
    ):
        """Test successful launch of Stitch job with Databricks connector."""
        # Prepare test data
        stitch_config = {
            "name": "test-stitch",
            "tables": [
                {
                    "path": "customers",
                    "fields": [
                        {
                            "field-name": "email",
                            "type": "STRING",
                            "semantics": ["email"],
                        }
                    ],
                }
            ],
        }

        metadata = {
            "stitch_job_name": "stitch-databricks-test",
            "s3_config_path": "s3://test-bucket/config.json",
            "init_script_path": "s3://test-bucket/chuck/init-scripts/chuck-init-456.sh",
            "s3_jar_path": "s3://test-bucket/chuck/jars/job-456.jar",
            "main_class": "amperity.stitch_standalone.chuck_main",
            "databricks_jdbc_url": "jdbc:databricks://workspace.cloud.databricks.com:443/default;httpPath=/sql/1.0/warehouses/abc123;AuthMech=3;UID=token;PWD=dapi123",
            "databricks_catalog": "prod",
            "databricks_schema": "customers",
            "job_id": "job-789",
            "amperity_token": "token-abc",
        }

        preparation = {
            "success": True,
            "stitch_config": stitch_config,
            "metadata": metadata,
        }

        # Mock submit_bash_script to return init step ID
        mock_emr_client.submit_bash_script.return_value = "s-INIT456"

        # Mock Amperity API client
        with (
            patch("chuck_data.config.get_amperity_token") as mock_token,
            patch("chuck_data.clients.amperity.AmperityAPIClient") as mock_amp_client,
        ):
            mock_token.return_value = "token-abc"
            mock_amp_instance = Mock()
            mock_amp_client.return_value = mock_amp_instance

            # Execute
            result = emr_provider.launch_stitch_job(preparation)

            # Verify
            assert result["success"] is True
            assert result["step_id"] == "s-DATABRICKS123"
            assert result["cluster_id"] == "j-TEST123"
            assert result["job_id"] == "job-789"

            # Verify config was uploaded
            mock_storage_provider.upload_file.assert_called_once()

            # Verify init script was executed first with environment variables
            mock_emr_client.submit_bash_script.assert_called_once()
            bash_call_args = mock_emr_client.submit_bash_script.call_args[1]
            assert (
                bash_call_args["script_s3_path"]
                == "s3://test-bucket/chuck/init-scripts/chuck-init-456.sh"
            )
            # Verify environment variables are set
            assert "env_vars" in bash_call_args
            assert "CHUCK_API_URL" in bash_call_args["env_vars"]

            # Verify EMR client was called with Databricks connector and S3 JAR path
            mock_emr_client.submit_spark_databricks_job.assert_called_once()
            emr_call_args = mock_emr_client.submit_spark_databricks_job.call_args[1]
            assert emr_call_args["name"] == "Stitch Setup: stitch-databricks-test"
            assert (
                emr_call_args["jar_path"] == "s3://test-bucket/chuck/jars/job-456.jar"
            )  # S3 path from metadata
            assert (
                "jdbc:databricks://workspace.cloud.databricks.com"
                in emr_call_args["databricks_jdbc_url"]
            )
            assert emr_call_args["databricks_catalog"] == "prod"
            assert emr_call_args["databricks_schema"] == "customers"

    def test_launch_databricks_job_with_custom_spark_args(
        self, emr_provider, mock_emr_client, mock_storage_provider
    ):
        """Test launch with custom Spark arguments."""
        metadata = {
            "stitch_job_name": "test",
            "s3_config_path": "s3://bucket/config.json",
            "s3_jar_path": "s3://bucket/stitch.jar",
            "databricks_jdbc_url": "jdbc:databricks://workspace:443/default",
            "databricks_catalog": "prod",
            "databricks_schema": "customers",
            "spark_args": ["--executor-memory", "16g", "--executor-cores", "8"],
        }

        preparation = {
            "success": True,
            "stitch_config": {"tables": []},
            "metadata": metadata,
        }

        with patch("chuck_data.config.get_amperity_token"):
            result = emr_provider.launch_stitch_job(preparation)

            assert result["success"] is True

            # Verify custom spark args were passed
            emr_call_args = mock_emr_client.submit_spark_databricks_job.call_args[1]
            assert emr_call_args["spark_args"] == [
                "--executor-memory",
                "16g",
                "--executor-cores",
                "8",
            ]


class TestLaunchStitchJobGeneric:
    """Test launching Stitch jobs with generic data sources (no connector)."""

    def test_launch_generic_job(
        self, emr_provider, mock_emr_client, mock_storage_provider
    ):
        """Test launch of generic Spark job without specific connector."""
        metadata = {
            "stitch_job_name": "generic-test",
            "s3_config_path": "s3://bucket/config.json",
            "s3_jar_path": "s3://bucket/stitch.jar",
            "main_class": "amperity.stitch_standalone.chuck_main",
            # No connector-specific metadata
        }

        preparation = {
            "success": True,
            "stitch_config": {"tables": []},
            "metadata": metadata,
        }

        with patch("chuck_data.config.get_amperity_token"):
            result = emr_provider.launch_stitch_job(preparation)

            assert result["success"] is True
            assert result["step_id"] == "s-GENERIC123"

            # Verify standard submit_spark_job was called
            mock_emr_client.submit_spark_job.assert_called_once()
            mock_emr_client.submit_spark_redshift_job.assert_not_called()
            mock_emr_client.submit_spark_databricks_job.assert_not_called()


class TestLaunchStitchJobErrors:
    """Test error handling in launch_stitch_job."""

    def test_launch_with_failed_preparation(self, emr_provider):
        """Test launch fails when preparation failed."""
        preparation = {
            "success": False,
            "error": "Preparation failed",
        }

        result = emr_provider.launch_stitch_job(preparation)

        assert result["success"] is False
        assert result["error"] == "Cannot launch job: preparation failed"
        assert result["preparation_error"] == "Preparation failed"

    def test_launch_with_connection_failure(
        self, emr_provider, mock_emr_client, mock_storage_provider
    ):
        """Test launch fails when cluster connection fails."""
        mock_emr_client.validate_connection.return_value = False

        preparation = {
            "success": True,
            "stitch_config": {"tables": []},
            "metadata": {
                "stitch_job_name": "test",
                "s3_config_path": "s3://bucket/config.json",
            },
        }

        result = emr_provider.launch_stitch_job(preparation)

        assert result["success"] is False
        assert "Cannot connect to EMR cluster" in result["error"]

    def test_launch_with_emr_submission_failure(
        self, emr_provider, mock_emr_client, mock_storage_provider
    ):
        """Test launch fails when EMR step submission returns None."""
        mock_emr_client.submit_spark_job.return_value = None

        preparation = {
            "success": True,
            "stitch_config": {"tables": []},
            "metadata": {
                "stitch_job_name": "test",
                "s3_config_path": "s3://bucket/config.json",
                "s3_jar_path": "s3://bucket/stitch.jar",
            },
        }

        with patch("chuck_data.config.get_amperity_token"):
            result = emr_provider.launch_stitch_job(preparation)

            assert result["success"] is False
            assert "no step_id returned" in result["error"]


class TestGetJobStatus:
    """Test get_job_status method."""

    def test_get_job_status_success(self, emr_provider, mock_emr_client):
        """Test successful job status retrieval."""
        mock_emr_client.get_step_status.return_value = {
            "status": "RUNNING",
            "state_message": "Step is running",
            "start_time": "2024-01-15T10:30:00Z",
        }

        result = emr_provider.get_job_status("s-STEP123")

        assert result["success"] is True
        assert result["step_id"] == "s-STEP123"
        assert result["status"] == "RUNNING"
        assert result["emr_status"] == "RUNNING"
        assert result["start_time"] == "2024-01-15T10:30:00Z"

    def test_get_job_status_completed(self, emr_provider, mock_emr_client):
        """Test job status for completed job."""
        mock_emr_client.get_step_status.return_value = {
            "status": "COMPLETED",
            "state_message": "Step completed successfully",
            "start_time": "2024-01-15T10:30:00Z",
            "end_time": "2024-01-15T11:00:00Z",
        }

        result = emr_provider.get_job_status("s-STEP123")

        assert result["success"] is True
        assert result["status"] == "SUCCESS"  # Mapped to unified status
        assert result["emr_status"] == "COMPLETED"
        assert "end_time" in result

    def test_get_job_status_failed(self, emr_provider, mock_emr_client):
        """Test job status for failed job."""
        mock_emr_client.get_step_status.return_value = {
            "status": "FAILED",
            "state_message": "Step failed",
            "failure_reason": "Application error",
            "failure_message": "Exception in main",
            "start_time": "2024-01-15T10:30:00Z",
            "end_time": "2024-01-15T10:35:00Z",
        }

        result = emr_provider.get_job_status("s-STEP123")

        assert result["success"] is True
        assert result["status"] == "FAILED"
        assert result["failure_reason"] == "Application error"
        assert result["failure_message"] == "Exception in main"


class TestCancelJob:
    """Test cancel_job method."""

    def test_cancel_running_job(self, emr_provider, mock_emr_client):
        """Test cancelling a running job."""
        # Mock get_job_status to return RUNNING
        emr_provider.get_job_status = Mock(
            return_value={
                "success": True,
                "emr_status": "RUNNING",
            }
        )
        mock_emr_client.cancel_step.return_value = True

        result = emr_provider.cancel_job("s-STEP123")

        assert result is True
        mock_emr_client.cancel_step.assert_called_once_with("s-STEP123")

    def test_cancel_completed_job(self, emr_provider, mock_emr_client):
        """Test cancelling a completed job fails gracefully."""
        # Mock get_job_status to return COMPLETED
        emr_provider.get_job_status = Mock(
            return_value={
                "success": True,
                "emr_status": "COMPLETED",
            }
        )

        result = emr_provider.cancel_job("s-STEP123")

        assert result is False
        mock_emr_client.cancel_step.assert_not_called()

    def test_cancel_job_status_failure(self, emr_provider, mock_emr_client):
        """Test cancel fails when status check fails."""
        # Mock get_job_status to fail
        emr_provider.get_job_status = Mock(
            return_value={
                "success": False,
            }
        )

        result = emr_provider.cancel_job("s-STEP123")

        assert result is False
        mock_emr_client.cancel_step.assert_not_called()
