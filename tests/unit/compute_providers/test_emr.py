"""Unit tests for EMRComputeProvider."""

import pytest
from unittest.mock import patch, Mock
from chuck_data.compute_providers.emr import EMRComputeProvider


@pytest.fixture
def mock_storage_provider():
    """Mock storage provider for testing."""
    mock_storage = Mock()
    mock_storage.upload_manifest.return_value = "s3://bucket/path/manifest.json"
    mock_storage.upload_init_script.return_value = "s3://bucket/path/init.sh"
    return mock_storage


class TestEMRComputeProviderInit:
    """Tests for EMRComputeProvider initialization."""

    def test_requires_storage_provider(self):
        """Test that storage_provider is required."""
        with pytest.raises(ValueError) as exc_info:
            EMRComputeProvider(region="us-west-2", storage_provider=None)

        assert "storage_provider is required" in str(exc_info.value)
        assert "ProviderFactory.create_storage_provider" in str(exc_info.value)

    def test_can_instantiate_with_full_config(self, mock_storage_provider):
        """Test that EMRComputeProvider can be instantiated with full configuration."""
        provider = EMRComputeProvider(
            region="us-west-2",
            storage_provider=mock_storage_provider,
            cluster_id="j-test123",
            aws_profile="test-profile",
            s3_bucket="test-bucket",
        )
        assert provider.region == "us-west-2"
        assert provider.cluster_id == "j-test123"
        assert provider.aws_profile == "test-profile"
        assert provider.s3_bucket == "test-bucket"

    def test_can_instantiate_minimal_config(self, mock_storage_provider):
        """Test that only region and storage_provider are required."""
        provider = EMRComputeProvider(
            region="us-east-1", storage_provider=mock_storage_provider
        )
        assert provider.region == "us-east-1"
        assert provider.cluster_id is None
        assert provider.aws_profile is None
        assert provider.s3_bucket is None

    def test_can_instantiate_without_cluster_id(self, mock_storage_provider):
        """Test that cluster_id is optional for on-demand clusters."""
        provider = EMRComputeProvider(
            region="us-west-2",
            storage_provider=mock_storage_provider,
            s3_bucket="test-bucket",
            aws_profile="test",
        )
        assert provider.cluster_id is None
        assert provider.s3_bucket == "test-bucket"
        assert provider.aws_profile == "test"

    def test_accepts_additional_kwargs(self, mock_storage_provider):
        """Test that additional kwargs are stored in config."""
        provider = EMRComputeProvider(
            region="us-west-2",
            storage_provider=mock_storage_provider,
            instance_type="m5.xlarge",
            instance_count=5,
            custom_setting="value",
            spark_packages="io.github.spark-redshift-community:spark-redshift_2.12:6.2.0",
        )

        assert provider.config["instance_type"] == "m5.xlarge"
        assert provider.config["instance_count"] == 5
        assert provider.config["custom_setting"] == "value"
        assert (
            provider.config["spark_packages"]
            == "io.github.spark-redshift-community:spark-redshift_2.12:6.2.0"
        )

    def test_stores_emr_specific_config(self, mock_storage_provider):
        """Test that EMR-specific configuration is stored."""
        provider = EMRComputeProvider(
            region="us-west-2",
            storage_provider=mock_storage_provider,
            iam_role="arn:aws:iam::123456789012:role/EMRRole",
            ec2_key_name="my-key-pair",
            log_uri="s3://my-logs/emr",
            spark_version="3.5.0",
        )

        assert provider.config["iam_role"] == "arn:aws:iam::123456789012:role/EMRRole"
        assert provider.config["ec2_key_name"] == "my-key-pair"
        assert provider.config["log_uri"] == "s3://my-logs/emr"
        assert provider.config["spark_version"] == "3.5.0"


class TestPrepareStitchJob:
    """Tests for prepare_stitch_job method."""

    def test_prepare_stitch_job_raises_not_implemented(self, mock_storage_provider):
        """Test that prepare_stitch_job raises NotImplementedError."""
        provider = EMRComputeProvider(
            region="us-west-2",
            storage_provider=mock_storage_provider,
            s3_bucket="test-bucket",
        )

        with pytest.raises(NotImplementedError) as exc_info:
            provider.prepare_stitch_job(manifest={}, data_provider=None, config={})

        assert "will be implemented in a future PR" in str(exc_info.value)
        assert "PII scanning" in str(exc_info.value)

    def test_prepare_stitch_job_error_message_informative(self, mock_storage_provider):
        """Test that error message provides helpful information."""
        provider = EMRComputeProvider(
            region="us-west-2", storage_provider=mock_storage_provider
        )

        with pytest.raises(NotImplementedError) as exc_info:
            provider.prepare_stitch_job(
                manifest={"tables": []},
                data_provider=None,
                config={"llm_client": None},
            )

        error_msg = str(exc_info.value)
        assert "prepare_stitch_job" in error_msg.lower()
        assert "future pr" in error_msg.lower()


class TestLaunchStitchJob:
    """Tests for launch_stitch_job method."""

    def test_launch_stitch_job_raises_not_implemented(self, mock_storage_provider):
        """Test that launch_stitch_job raises NotImplementedError."""
        provider = EMRComputeProvider(
            region="us-west-2",
            storage_provider=mock_storage_provider,
            cluster_id="j-test123",
        )

        with pytest.raises(NotImplementedError) as exc_info:
            provider.launch_stitch_job(preparation={"success": True})

        assert "will be implemented in a future PR" in str(exc_info.value)
        assert "EMR steps" in str(exc_info.value)

    def test_launch_stitch_job_error_message_informative(self, mock_storage_provider):
        """Test that error message provides helpful information."""
        provider = EMRComputeProvider(
            region="us-west-2", storage_provider=mock_storage_provider
        )

        with pytest.raises(NotImplementedError) as exc_info:
            provider.launch_stitch_job(preparation={})

        error_msg = str(exc_info.value)
        assert "launch_stitch_job" in error_msg.lower()
        assert "future pr" in error_msg.lower()


class TestGetJobStatus:
    """Tests for get_job_status method."""

    def test_get_job_status_raises_not_implemented(self, mock_storage_provider):
        """Test that get_job_status raises NotImplementedError."""
        provider = EMRComputeProvider(
            region="us-west-2", storage_provider=mock_storage_provider
        )

        with pytest.raises(NotImplementedError) as exc_info:
            provider.get_job_status(job_id="s-test123")

        assert "will be implemented in a future PR" in str(exc_info.value)
        assert "EMR API" in str(exc_info.value)

    def test_get_job_status_accepts_step_id(self, mock_storage_provider):
        """Test that job_id parameter accepts EMR step IDs."""
        provider = EMRComputeProvider(
            region="us-west-2", storage_provider=mock_storage_provider
        )

        with pytest.raises(NotImplementedError):
            provider.get_job_status(job_id="s-XXXXXXXXXXXXX")

    def test_get_job_status_error_message_informative(self, mock_storage_provider):
        """Test that error message provides helpful information."""
        provider = EMRComputeProvider(
            region="us-west-2", storage_provider=mock_storage_provider
        )

        with pytest.raises(NotImplementedError) as exc_info:
            provider.get_job_status(job_id="test-job-id")

        error_msg = str(exc_info.value)
        assert "get_job_status" in error_msg.lower()
        assert "future pr" in error_msg.lower()


class TestCancelJob:
    """Tests for cancel_job method."""

    def test_cancel_job_raises_not_implemented(self, mock_storage_provider):
        """Test that cancel_job raises NotImplementedError."""
        provider = EMRComputeProvider(
            region="us-west-2", storage_provider=mock_storage_provider
        )

        with pytest.raises(NotImplementedError) as exc_info:
            provider.cancel_job(job_id="s-test123")

        assert "will be implemented in a future PR" in str(exc_info.value)
        assert "CancelSteps" in str(exc_info.value)

    def test_cancel_job_accepts_step_id(self, mock_storage_provider):
        """Test that job_id parameter accepts EMR step IDs."""
        provider = EMRComputeProvider(
            region="us-west-2", storage_provider=mock_storage_provider
        )

        with pytest.raises(NotImplementedError):
            provider.cancel_job(job_id="s-XXXXXXXXXXXXX")

    def test_cancel_job_error_message_informative(self, mock_storage_provider):
        """Test that error message provides helpful information."""
        provider = EMRComputeProvider(
            region="us-west-2", storage_provider=mock_storage_provider
        )

        with pytest.raises(NotImplementedError) as exc_info:
            provider.cancel_job(job_id="test-job-id")

        error_msg = str(exc_info.value)
        assert "cancel_job" in error_msg.lower()
        assert "future pr" in error_msg.lower()
        assert "CancelSteps" in error_msg or "API" in error_msg


class TestEMRComputeProviderInterface:
    """Tests for EMRComputeProvider interface compatibility."""

    def test_has_required_methods(self, mock_storage_provider):
        """Test that EMRComputeProvider has all required ComputeProvider methods."""
        provider = EMRComputeProvider(
            region="us-west-2", storage_provider=mock_storage_provider
        )
        assert hasattr(provider, "prepare_stitch_job")
        assert hasattr(provider, "launch_stitch_job")
        assert hasattr(provider, "get_job_status")
        assert hasattr(provider, "cancel_job")

    def test_method_signatures_match_interface(self, mock_storage_provider):
        """Test that method signatures match the ComputeProvider interface."""
        import inspect

        provider = EMRComputeProvider(
            region="us-west-2", storage_provider=mock_storage_provider
        )

        # Check prepare_stitch_job signature
        sig = inspect.signature(provider.prepare_stitch_job)
        params = list(sig.parameters.keys())
        assert "manifest" in params
        assert "data_provider" in params
        assert "config" in params

        # Check launch_stitch_job signature
        sig = inspect.signature(provider.launch_stitch_job)
        params = list(sig.parameters.keys())
        assert "preparation" in params

        # Check get_job_status signature
        sig = inspect.signature(provider.get_job_status)
        params = list(sig.parameters.keys())
        assert "job_id" in params

        # Check cancel_job signature
        sig = inspect.signature(provider.cancel_job)
        params = list(sig.parameters.keys())
        assert "job_id" in params

    def test_conforms_to_compute_provider_protocol(self, mock_storage_provider):
        """Test that EMRComputeProvider follows the same pattern as DatabricksComputeProvider."""
        from chuck_data.compute_providers.databricks import DatabricksComputeProvider

        emr_provider = EMRComputeProvider(
            region="us-west-2", storage_provider=mock_storage_provider
        )
        databricks_provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            storage_provider=mock_storage_provider,
        )

        # Both should have the same method names
        emr_methods = {
            name
            for name in dir(emr_provider)
            if not name.startswith("_") and callable(getattr(emr_provider, name))
        }
        databricks_methods = {
            name
            for name in dir(databricks_provider)
            if not name.startswith("_") and callable(getattr(databricks_provider, name))
        }

        # Core compute provider methods should match
        core_methods = {
            "prepare_stitch_job",
            "launch_stitch_job",
            "get_job_status",
            "cancel_job",
        }
        assert core_methods.issubset(emr_methods)
        assert core_methods.issubset(databricks_methods)


# Fixtures
@pytest.fixture(autouse=True)
def mock_boto3_session():
    """Mock boto3.Session for all EMR tests to avoid requiring real AWS credentials."""
    with patch("chuck_data.storage_providers.s3.boto3") as mock_boto3:
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session
        yield mock_boto3
