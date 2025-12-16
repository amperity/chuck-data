"""Unit tests for data provider adapters."""

import pytest
from unittest.mock import Mock, MagicMock
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

    def test_tag_columns_requires_warehouse_id(self):
        """Test that tag_columns raises ValueError without warehouse_id."""
        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test-token"
        )
        tags = [
            {
                "table": "catalog.schema.table",
                "column": "email",
                "semantic_type": "pii/email",
            }
        ]

        with pytest.raises(ValueError) as exc_info:
            adapter.tag_columns(tags)

        assert "warehouse_id" in str(exc_info.value)

    def test_tag_columns_success(self):
        """Test successful tagging of columns."""
        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test-token"
        )

        # Mock the client's submit_sql_statement method
        adapter.client.submit_sql_statement = Mock(
            return_value={"status": {"state": "SUCCEEDED"}}
        )

        tags = [
            {
                "table": "catalog.schema.table1",
                "column": "email",
                "semantic_type": "pii/email",
            },
            {
                "table": "catalog.schema.table1",
                "column": "phone",
                "semantic_type": "pii/phone",
            },
        ]

        result = adapter.tag_columns(tags, warehouse_id="warehouse123")

        assert result["success"] is True
        assert result["tags_applied"] == 2
        assert len(result["errors"]) == 0

    def test_tag_columns_with_errors(self):
        """Test tagging with some failures."""
        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test-token"
        )

        # Mock the client to return success for first, error for second
        call_count = [0]

        def mock_submit(sql_text, warehouse_id, wait_timeout):
            call_count[0] += 1
            if call_count[0] == 1:
                return {"status": {"state": "SUCCEEDED"}}
            else:
                return {
                    "status": {
                        "state": "FAILED",
                        "error": {"message": "Column not found"},
                    }
                }

        adapter.client.submit_sql_statement = Mock(side_effect=mock_submit)

        tags = [
            {
                "table": "catalog.schema.table1",
                "column": "email",
                "semantic_type": "pii/email",
            },
            {
                "table": "catalog.schema.table1",
                "column": "bad_column",
                "semantic_type": "pii/phone",
            },
        ]

        result = adapter.tag_columns(tags, warehouse_id="warehouse123")

        assert result["success"] is False
        assert result["tags_applied"] == 1
        assert len(result["errors"]) == 1
        assert result["errors"][0]["column"] == "bad_column"


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
        assert hasattr(adapter, "tag_columns")

    def test_tag_columns_requires_schema(self):
        """Test that tag_columns raises ValueError without schema parameter."""
        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )
        tags = [{"table": "users", "column": "email", "semantic_type": "pii/email"}]

        with pytest.raises(ValueError) as exc_info:
            adapter.tag_columns(tags)

        assert "schema" in str(exc_info.value)

    def test_tag_columns_success(self):
        """Test successful storage of tags in metadata table."""
        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            database="testdb",
        )

        # Mock the client's execute_sql method
        adapter.client.execute_sql = Mock(return_value={})

        tags = [
            {"table": "users", "column": "email", "semantic_type": "pii/email"},
            {"table": "users", "column": "phone", "semantic_type": "pii/phone"},
            {"table": "customers", "column": "ssn", "semantic_type": "pii/ssn"},
        ]

        result = adapter.tag_columns(tags, schema="public")

        assert result["success"] is True
        assert result["tags_applied"] == 3
        assert len(result["errors"]) == 0

        # Verify SQL calls were made (create schema, create table, delete, insert)
        assert adapter.client.execute_sql.call_count == 4

    def test_tag_columns_with_empty_tags(self):
        """Test handling of empty tags list."""
        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        # Mock the client's execute_sql method
        adapter.client.execute_sql = Mock(return_value={})

        result = adapter.tag_columns([], catalog="testdb", schema="public")

        assert result["success"] is True
        assert result["tags_applied"] == 0
        assert len(result["errors"]) == 0

        # Should still create schema and table, then delete existing
        assert adapter.client.execute_sql.call_count == 3

    def test_tag_columns_with_sql_error(self):
        """Test handling of SQL execution error."""
        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            database="testdb",
        )

        # Mock the client to raise an error
        adapter.client.execute_sql = Mock(side_effect=Exception("Connection failed"))

        tags = [{"table": "users", "column": "email", "semantic_type": "pii/email"}]

        result = adapter.tag_columns(tags, schema="public")

        assert result["success"] is False
        assert result["tags_applied"] == 0
        assert len(result["errors"]) == 1
        assert "Connection failed" in result["errors"][0]["error"]
