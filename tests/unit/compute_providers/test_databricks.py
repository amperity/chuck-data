"""Unit tests for DatabricksComputeProvider."""

import pytest
from chuck_data.compute_providers.databricks import DatabricksComputeProvider


class TestDatabricksComputeProvider:
    """Tests for DatabricksComputeProvider stub implementation."""

    def test_can_instantiate(self):
        """Test that DatabricksComputeProvider can be instantiated."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com", token="test-token"
        )
        assert provider.workspace_url == "https://test.databricks.com"
        assert provider.token == "test-token"

    def test_prepare_stitch_job_raises_not_implemented(self):
        """Test that prepare_stitch_job raises NotImplementedError."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com", token="test-token"
        )

        with pytest.raises(NotImplementedError) as exc_info:
            provider.prepare_stitch_job(manifest={}, data_provider=None, config={})

        assert "will be implemented in PR 3" in str(exc_info.value)

    def test_launch_stitch_job_raises_not_implemented(self):
        """Test that launch_stitch_job raises NotImplementedError."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com", token="test-token"
        )

        with pytest.raises(NotImplementedError) as exc_info:
            provider.launch_stitch_job(preparation={})

        assert "will be implemented in PR 3" in str(exc_info.value)

    def test_get_job_status_raises_not_implemented(self):
        """Test that get_job_status raises NotImplementedError."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com", token="test-token"
        )

        with pytest.raises(NotImplementedError) as exc_info:
            provider.get_job_status(job_id="test-job-id")

        assert "will be implemented in PR 3" in str(exc_info.value)

    def test_cancel_job_raises_not_implemented(self):
        """Test that cancel_job raises NotImplementedError."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com", token="test-token"
        )

        with pytest.raises(NotImplementedError) as exc_info:
            provider.cancel_job(job_id="test-job-id")

        assert "will be implemented in PR 3" in str(exc_info.value)

    def test_accepts_additional_kwargs(self):
        """Test that additional kwargs are stored in config."""
        provider = DatabricksComputeProvider(
            workspace_url="https://test.databricks.com",
            token="test-token",
            cluster_size="small",
            custom_setting="value",
        )

        assert provider.config["cluster_size"] == "small"
        assert provider.config["custom_setting"] == "value"
