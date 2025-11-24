"""Tests for DataProvider adapters."""

import pytest
from unittest.mock import MagicMock, patch
from chuck_data.data_providers.adapters import (
    DatabricksProviderAdapter,
    RedshiftProviderAdapter,
)


class TestDatabricksProviderAdapter:
    """Test Databricks provider adapter."""

    @patch("chuck_data.data_providers.adapters.DatabricksAPIClient")
    def test_init(self, mock_client_class):
        """Adapter initializes with workspace URL and token."""
        mock_client_class.return_value = MagicMock()

        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test_token"
        )

        assert adapter is not None
        mock_client_class.assert_called_once_with(
            workspace_url="https://test.databricks.com", token="test_token"
        )

    @patch("chuck_data.data_providers.adapters.DatabricksAPIClient")
    def test_validate_connection_success(self, mock_client_class):
        """Validate connection returns True when list_catalogs succeeds."""
        mock_client = MagicMock()
        mock_client.list_catalogs.return_value = {"catalogs": []}
        mock_client_class.return_value = mock_client

        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test_token"
        )

        assert adapter.validate_connection() is True
        mock_client.list_catalogs.assert_called_once()

    @patch("chuck_data.data_providers.adapters.DatabricksAPIClient")
    def test_validate_connection_failure(self, mock_client_class):
        """Validate connection returns False when list_catalogs fails."""
        mock_client = MagicMock()
        mock_client.list_catalogs.side_effect = Exception("Connection failed")
        mock_client_class.return_value = mock_client

        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test_token"
        )

        assert adapter.validate_connection() is False

    @patch("chuck_data.data_providers.adapters.DatabricksAPIClient")
    def test_list_catalogs(self, mock_client_class):
        """List catalogs returns catalog names."""
        mock_client = MagicMock()
        mock_client.list_catalogs.return_value = {
            "catalogs": [
                {"name": "catalog1", "owner": "user1"},
                {"name": "catalog2", "owner": "user2"},
            ]
        }
        mock_client_class.return_value = mock_client

        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test_token"
        )

        catalogs = adapter.list_catalogs()
        assert catalogs == ["catalog1", "catalog2"]

    @patch("chuck_data.data_providers.adapters.DatabricksAPIClient")
    def test_list_catalogs_empty(self, mock_client_class):
        """List catalogs returns empty list when no catalogs."""
        mock_client = MagicMock()
        mock_client.list_catalogs.return_value = {"catalogs": []}
        mock_client_class.return_value = mock_client

        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test_token"
        )

        catalogs = adapter.list_catalogs()
        assert catalogs == []

    @patch("chuck_data.data_providers.adapters.DatabricksAPIClient")
    def test_list_schemas(self, mock_client_class):
        """List schemas returns schema names for catalog."""
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = {
            "schemas": [
                {"name": "schema1", "catalog_name": "catalog1"},
                {"name": "schema2", "catalog_name": "catalog1"},
            ]
        }
        mock_client_class.return_value = mock_client

        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test_token"
        )

        schemas = adapter.list_schemas(catalog="catalog1")
        assert schemas == ["schema1", "schema2"]
        mock_client.list_schemas.assert_called_once_with(catalog_name="catalog1")

    @patch("chuck_data.data_providers.adapters.DatabricksAPIClient")
    def test_list_schemas_requires_catalog(self, mock_client_class):
        """List schemas raises ValueError when catalog not provided."""
        mock_client_class.return_value = MagicMock()

        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test_token"
        )

        with pytest.raises(ValueError, match="catalog"):
            adapter.list_schemas()

    @patch("chuck_data.data_providers.adapters.DatabricksAPIClient")
    def test_list_tables(self, mock_client_class):
        """List tables returns table metadata."""
        mock_client = MagicMock()
        mock_client.list_tables.return_value = {
            "tables": [
                {
                    "name": "table1",
                    "catalog_name": "catalog1",
                    "schema_name": "schema1",
                    "table_type": "MANAGED",
                },
                {
                    "name": "table2",
                    "catalog_name": "catalog1",
                    "schema_name": "schema1",
                    "table_type": "EXTERNAL",
                },
            ]
        }
        mock_client_class.return_value = mock_client

        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test_token"
        )

        tables = adapter.list_tables(catalog="catalog1", schema="schema1")
        assert len(tables) == 2
        assert tables[0]["name"] == "table1"
        assert tables[1]["name"] == "table2"
        mock_client.list_tables.assert_called_once_with(
            catalog_name="catalog1", schema_name="schema1"
        )

    @patch("chuck_data.data_providers.adapters.DatabricksAPIClient")
    def test_list_tables_requires_catalog_and_schema(self, mock_client_class):
        """List tables raises ValueError when catalog or schema not provided."""
        mock_client_class.return_value = MagicMock()

        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test_token"
        )

        with pytest.raises(ValueError, match="catalog"):
            adapter.list_tables()

        with pytest.raises(ValueError, match="schema"):
            adapter.list_tables(catalog="catalog1")

    @patch("chuck_data.data_providers.adapters.DatabricksAPIClient")
    def test_get_table(self, mock_client_class):
        """Get table returns table metadata."""
        mock_client = MagicMock()
        mock_client.get_table.return_value = {
            "name": "table1",
            "catalog_name": "catalog1",
            "schema_name": "schema1",
            "columns": [
                {"name": "col1", "type_text": "STRING"},
                {"name": "col2", "type_text": "INT"},
            ],
        }
        mock_client_class.return_value = mock_client

        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test_token"
        )

        table = adapter.get_table(catalog="catalog1", schema="schema1", table="table1")
        assert table["name"] == "table1"
        assert len(table["columns"]) == 2
        mock_client.get_table.assert_called_once_with(
            full_name="catalog1.schema1.table1"
        )

    @patch("chuck_data.data_providers.adapters.DatabricksAPIClient")
    def test_get_table_requires_all_parameters(self, mock_client_class):
        """Get table raises ValueError when parameters missing."""
        mock_client_class.return_value = MagicMock()

        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test_token"
        )

        with pytest.raises(ValueError, match="catalog"):
            adapter.get_table()

        with pytest.raises(ValueError, match="schema"):
            adapter.get_table(catalog="catalog1")

        with pytest.raises(ValueError, match="table"):
            adapter.get_table(catalog="catalog1", schema="schema1")

    @patch("chuck_data.data_providers.adapters.DatabricksAPIClient")
    def test_execute_query_not_implemented(self, mock_client_class):
        """Execute query raises NotImplementedError."""
        mock_client_class.return_value = MagicMock()

        adapter = DatabricksProviderAdapter(
            workspace_url="https://test.databricks.com", token="test_token"
        )

        with pytest.raises(NotImplementedError):
            adapter.execute_query("SELECT * FROM table")


class TestRedshiftProviderAdapter:
    """Test Redshift provider adapter."""

    @patch("chuck_data.data_providers.adapters.RedshiftAPIClient")
    def test_init_with_cluster(self, mock_client_class):
        """Adapter initializes with cluster identifier."""
        mock_client_class.return_value = MagicMock()

        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region="us-east-1",
            cluster_identifier="my-cluster",
        )

        assert adapter is not None
        mock_client_class.assert_called_once()

    @patch("chuck_data.data_providers.adapters.RedshiftAPIClient")
    def test_init_with_workgroup(self, mock_client_class):
        """Adapter initializes with workgroup name."""
        mock_client_class.return_value = MagicMock()

        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region="us-east-1",
            workgroup_name="my-workgroup",
        )

        assert adapter is not None
        mock_client_class.assert_called_once()

    @patch("chuck_data.data_providers.adapters.RedshiftAPIClient")
    def test_init_with_optional_parameters(self, mock_client_class):
        """Adapter initializes with optional parameters."""
        mock_client_class.return_value = MagicMock()

        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region="us-east-1",
            cluster_identifier="my-cluster",
            database="prod",
            s3_bucket="my-bucket",
            emr_cluster_id="j-123456",
        )

        assert adapter is not None

    @patch("chuck_data.data_providers.adapters.RedshiftAPIClient")
    def test_validate_connection(self, mock_client_class):
        """Validate connection delegates to client."""
        mock_client = MagicMock()
        mock_client.validate_connection.return_value = True
        mock_client_class.return_value = mock_client

        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region="us-east-1",
            cluster_identifier="my-cluster",
        )

        assert adapter.validate_connection() is True
        mock_client.validate_connection.assert_called_once()

    @patch("chuck_data.data_providers.adapters.RedshiftAPIClient")
    def test_list_catalogs(self, mock_client_class):
        """List catalogs returns database names."""
        mock_client = MagicMock()
        mock_client.list_databases.return_value = ["dev", "test", "prod"]
        mock_client_class.return_value = mock_client

        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region="us-east-1",
            cluster_identifier="my-cluster",
        )

        catalogs = adapter.list_catalogs()
        assert catalogs == ["dev", "test", "prod"]
        mock_client.list_databases.assert_called_once()

    @patch("chuck_data.data_providers.adapters.RedshiftAPIClient")
    def test_list_schemas_with_catalog(self, mock_client_class):
        """List schemas returns schema names for database."""
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = ["public", "staging", "analytics"]
        mock_client_class.return_value = mock_client

        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region="us-east-1",
            cluster_identifier="my-cluster",
        )

        schemas = adapter.list_schemas(catalog="prod")
        assert schemas == ["public", "staging", "analytics"]
        mock_client.list_schemas.assert_called_once_with(database="prod")

    @patch("chuck_data.data_providers.adapters.RedshiftAPIClient")
    def test_list_schemas_without_catalog(self, mock_client_class):
        """List schemas uses default database when catalog not specified."""
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = ["public"]
        mock_client_class.return_value = mock_client

        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region="us-east-1",
            cluster_identifier="my-cluster",
        )

        schemas = adapter.list_schemas()
        assert schemas == ["public"]
        mock_client.list_schemas.assert_called_once_with(database=None)

    @patch("chuck_data.data_providers.adapters.RedshiftAPIClient")
    def test_list_tables(self, mock_client_class):
        """List tables returns table metadata."""
        mock_client = MagicMock()
        mock_client.list_tables.return_value = {
            "tables": [
                {"schema": "public", "name": "users", "type": "TABLE"},
                {"schema": "public", "name": "orders", "type": "TABLE"},
            ]
        }
        mock_client_class.return_value = mock_client

        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region="us-east-1",
            cluster_identifier="my-cluster",
        )

        tables = adapter.list_tables(catalog="prod", schema="public")
        assert len(tables) == 2
        assert tables[0]["name"] == "users"
        assert tables[1]["name"] == "orders"
        mock_client.list_tables.assert_called_once_with(
            database="prod", schema_pattern="public"
        )

    @patch("chuck_data.data_providers.adapters.RedshiftAPIClient")
    def test_list_tables_with_pattern(self, mock_client_class):
        """List tables passes through table_pattern filter."""
        mock_client = MagicMock()
        mock_client.list_tables.return_value = {"tables": []}
        mock_client_class.return_value = mock_client

        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region="us-east-1",
            cluster_identifier="my-cluster",
        )

        adapter.list_tables(catalog="prod", schema="public", table_pattern="user%")
        mock_client.list_tables.assert_called_once_with(
            database="prod", schema_pattern="public", table_pattern="user%"
        )

    @patch("chuck_data.data_providers.adapters.RedshiftAPIClient")
    def test_get_table(self, mock_client_class):
        """Get table returns table metadata."""
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "schema": "public",
            "name": "users",
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "email", "type": "varchar"},
            ],
        }
        mock_client_class.return_value = mock_client

        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region="us-east-1",
            cluster_identifier="my-cluster",
        )

        table = adapter.get_table(catalog="prod", schema="public", table="users")
        assert table["name"] == "users"
        assert len(table["columns"]) == 2
        mock_client.describe_table.assert_called_once_with(
            database="prod", schema="public", table="users"
        )

    @patch("chuck_data.data_providers.adapters.RedshiftAPIClient")
    def test_execute_query(self, mock_client_class):
        """Execute query delegates to client."""
        mock_client = MagicMock()
        mock_client.execute_sql.return_value = {
            "Id": "query-123",
            "Status": "FINISHED",
            "ResultRows": 100,
        }
        mock_client_class.return_value = mock_client

        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region="us-east-1",
            cluster_identifier="my-cluster",
        )

        result = adapter.execute_query("SELECT * FROM users", catalog="prod", wait=True)
        assert result["Id"] == "query-123"
        assert result["Status"] == "FINISHED"
        mock_client.execute_sql.assert_called_once_with(
            sql="SELECT * FROM users", database="prod", wait=True
        )

    @patch("chuck_data.data_providers.adapters.RedshiftAPIClient")
    def test_execute_query_without_catalog(self, mock_client_class):
        """Execute query uses default database when catalog not specified."""
        mock_client = MagicMock()
        mock_client.execute_sql.return_value = {"Id": "query-123"}
        mock_client_class.return_value = mock_client

        adapter = RedshiftProviderAdapter(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            region="us-east-1",
            cluster_identifier="my-cluster",
        )

        adapter.execute_query("SELECT 1")
        mock_client.execute_sql.assert_called_once_with(sql="SELECT 1", database=None)
