"""Unit tests for data provider adapters."""

import pytest
from chuck_data.data_providers.adapters import (
    DatabricksProviderAdapter,
    RedshiftProviderAdapter,
)


class TestDatabricksProviderAdapter:
    """Tests for DatabricksProviderAdapter implementation."""

    def test_can_instantiate(self):
        """Test that DatabricksProviderAdapter can be instantiated."""
        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test-token"
        )
        assert adapter.client is not None
        assert hasattr(adapter, "validate_connection")
        assert hasattr(adapter, "list_databases")
        assert hasattr(adapter, "list_schemas")
        assert hasattr(adapter, "list_tables")
        assert hasattr(adapter, "get_table")
        assert hasattr(adapter, "execute_query")

    def test_execute_query_requires_warehouse_id(self):
        """Test that execute_query raises ValueError without warehouse_id."""
        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test-token"
        )
        with pytest.raises(ValueError) as exc_info:
            adapter.execute_query("SELECT 1")

        assert "warehouse_id" in str(exc_info.value)


class TestRedshiftProviderAdapter:
    """Tests for RedshiftProviderAdapter implementation."""

    def test_can_instantiate_with_cluster(self):
        """Test that RedshiftProviderAdapter can be instantiated with cluster."""
        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            database="test_db",
            s3_bucket="test-bucket",
        )
        assert adapter.client is not None
        assert adapter.redshift_iam_role is None
        assert adapter.emr_cluster_id is None

    def test_can_instantiate_with_workgroup(self):
        """Test that RedshiftProviderAdapter can be instantiated with workgroup."""
        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            workgroup_name="test-workgroup",
            database="test_db",
        )
        assert adapter.client is not None

    def test_can_instantiate_with_iam_role(self):
        """Test that RedshiftProviderAdapter accepts IAM role."""
        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            redshift_iam_role="arn:aws:iam::123456789012:role/RedshiftRole",
        )
        assert (
            adapter.redshift_iam_role == "arn:aws:iam::123456789012:role/RedshiftRole"
        )

    def test_can_instantiate_with_emr_cluster_id(self):
        """Test that RedshiftProviderAdapter accepts EMR cluster ID."""
        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            emr_cluster_id="j-test123",
        )
        assert adapter.emr_cluster_id == "j-test123"

    def test_has_required_methods(self):
        """Test that adapter has all required DataProvider methods."""
        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )
        assert hasattr(adapter, "validate_connection")
        assert hasattr(adapter, "list_databases")
        assert hasattr(adapter, "list_schemas")
        assert hasattr(adapter, "list_tables")
        assert hasattr(adapter, "get_table")
        assert hasattr(adapter, "execute_query")
