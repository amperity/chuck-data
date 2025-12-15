"""Unit tests for data provider adapters."""

import pytest
from chuck_data.data_providers.adapters import (
    DatabricksProviderAdapter,
    RedshiftProviderAdapter,
)


class TestDatabricksProviderAdapter:
    """Tests for DatabricksProviderAdapter stub implementation."""

    def test_can_instantiate(self):
        """Test that DatabricksProviderAdapter can be instantiated."""
        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test-token"
        )
        assert adapter.workspace_url == "https://test.databricks.com"
        assert adapter.token == "test-token"

    def test_validate_connection_raises_not_implemented(self):
        """Test that validate_connection raises NotImplementedError."""
        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test-token"
        )

        with pytest.raises(NotImplementedError) as exc_info:
            adapter.validate_connection()

        assert "will be implemented in PR 2" in str(exc_info.value)

    def test_list_databases_raises_not_implemented(self):
        """Test that list_databases raises NotImplementedError."""
        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test-token"
        )

        with pytest.raises(NotImplementedError) as exc_info:
            adapter.list_databases()

        assert "will be implemented in PR 2" in str(exc_info.value)

    def test_list_schemas_raises_not_implemented(self):
        """Test that list_schemas raises NotImplementedError."""
        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test-token"
        )

        with pytest.raises(NotImplementedError) as exc_info:
            adapter.list_schemas(catalog="test_catalog")

        assert "will be implemented in PR 2" in str(exc_info.value)

    def test_list_tables_raises_not_implemented(self):
        """Test that list_tables raises NotImplementedError."""
        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test-token"
        )

        with pytest.raises(NotImplementedError) as exc_info:
            adapter.list_tables(catalog="test_catalog", schema="test_schema")

        assert "will be implemented in PR 2" in str(exc_info.value)

    def test_get_table_raises_not_implemented(self):
        """Test that get_table raises NotImplementedError."""
        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test-token"
        )

        with pytest.raises(NotImplementedError) as exc_info:
            adapter.get_table(
                catalog="test_catalog", schema="test_schema", table="test_table"
            )

        assert "will be implemented in PR 2" in str(exc_info.value)

    def test_execute_query_raises_not_implemented(self):
        """Test that execute_query raises NotImplementedError."""
        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test-token"
        )

        with pytest.raises(NotImplementedError) as exc_info:
            adapter.execute_query(query="SELECT 1", catalog="test_catalog")

        assert "will be implemented in PR 2" in str(exc_info.value)


class TestRedshiftProviderAdapter:
    """Tests for RedshiftProviderAdapter stub implementation."""

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
        assert adapter.aws_access_key_id == "test-key"
        assert adapter.aws_secret_access_key == "test-secret"
        assert adapter.region == "us-west-2"
        assert adapter.cluster_identifier == "test-cluster"
        assert adapter.database == "test_db"
        assert adapter.s3_bucket == "test-bucket"

    def test_can_instantiate_with_workgroup(self):
        """Test that RedshiftProviderAdapter can be instantiated with workgroup."""
        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            workgroup_name="test-workgroup",
            database="test_db",
        )
        assert adapter.aws_access_key_id == "test-key"
        assert adapter.aws_secret_access_key == "test-secret"
        assert adapter.region == "us-west-2"
        assert adapter.workgroup_name == "test-workgroup"
        assert adapter.database == "test_db"

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

    def test_validate_connection_raises_not_implemented(self):
        """Test that validate_connection raises NotImplementedError."""
        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        with pytest.raises(NotImplementedError) as exc_info:
            adapter.validate_connection()

        assert "will be implemented in PR 2" in str(exc_info.value)

    def test_list_databases_raises_not_implemented(self):
        """Test that list_databases raises NotImplementedError."""
        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        with pytest.raises(NotImplementedError) as exc_info:
            adapter.list_databases()

        assert "will be implemented in PR 2" in str(exc_info.value)

    def test_list_schemas_raises_not_implemented(self):
        """Test that list_schemas raises NotImplementedError."""
        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        with pytest.raises(NotImplementedError) as exc_info:
            adapter.list_schemas(catalog="test_db")

        assert "will be implemented in PR 2" in str(exc_info.value)

    def test_list_tables_raises_not_implemented(self):
        """Test that list_tables raises NotImplementedError."""
        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        with pytest.raises(NotImplementedError) as exc_info:
            adapter.list_tables(catalog="test_db", schema="test_schema")

        assert "will be implemented in PR 2" in str(exc_info.value)

    def test_get_table_raises_not_implemented(self):
        """Test that get_table raises NotImplementedError."""
        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        with pytest.raises(NotImplementedError) as exc_info:
            adapter.get_table(
                catalog="test_db", schema="test_schema", table="test_table"
            )

        assert "will be implemented in PR 2" in str(exc_info.value)

    def test_execute_query_raises_not_implemented(self):
        """Test that execute_query raises NotImplementedError."""
        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        with pytest.raises(NotImplementedError) as exc_info:
            adapter.execute_query(query="SELECT 1", catalog="test_db")

        assert "will be implemented in PR 2" in str(exc_info.value)
