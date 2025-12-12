"""Unit tests for EMRComputeProvider."""

import pytest
from chuck_data.compute_providers.emr import EMRComputeProvider


class TestEMRComputeProvider:
    """Tests for EMRComputeProvider stub implementation."""

    def test_can_instantiate(self):
        """Test that EMRComputeProvider can be instantiated."""
        provider = EMRComputeProvider(
            region="us-west-2", cluster_id="j-test123", aws_profile="test-profile"
        )
        assert provider.region == "us-west-2"
        assert provider.cluster_id == "j-test123"
        assert provider.aws_profile == "test-profile"

    def test_can_instantiate_without_cluster_id(self):
        """Test that cluster_id is optional."""
        provider = EMRComputeProvider(region="us-west-2")
        assert provider.cluster_id is None
        assert provider.aws_profile is None

    def test_prepare_stitch_job_raises_not_implemented(self):
        """Test that prepare_stitch_job raises NotImplementedError."""
        provider = EMRComputeProvider(region="us-west-2")

        with pytest.raises(NotImplementedError) as exc_info:
            provider.prepare_stitch_job(manifest={}, data_provider=None, config={})

        assert "will be implemented in PR 4" in str(exc_info.value)

    def test_launch_stitch_job_raises_not_implemented(self):
        """Test that launch_stitch_job raises NotImplementedError."""
        provider = EMRComputeProvider(region="us-west-2")

        with pytest.raises(NotImplementedError) as exc_info:
            provider.launch_stitch_job(preparation={})

        assert "will be implemented in PR 4" in str(exc_info.value)

    def test_get_job_status_raises_not_implemented(self):
        """Test that get_job_status raises NotImplementedError."""
        provider = EMRComputeProvider(region="us-west-2")

        with pytest.raises(NotImplementedError) as exc_info:
            provider.get_job_status(job_id="test-job-id")

        assert "will be implemented in PR 4" in str(exc_info.value)

    def test_cancel_job_raises_not_implemented(self):
        """Test that cancel_job raises NotImplementedError."""
        provider = EMRComputeProvider(region="us-west-2")

        with pytest.raises(NotImplementedError) as exc_info:
            provider.cancel_job(job_id="test-job-id")

        assert "will be implemented in PR 4" in str(exc_info.value)

    def test_accepts_additional_kwargs(self):
        """Test that additional kwargs are stored in config."""
        provider = EMRComputeProvider(
            region="us-west-2", instance_type="m5.xlarge", custom_setting="value"
        )

        assert provider.config["instance_type"] == "m5.xlarge"
        assert provider.config["custom_setting"] == "value"
