"""
Comprehensive unit tests for RedshiftAPIClient.

Tests cover:
- Client initialization and validation
- Connection validation
- SQL execution methods
- Database/Schema/Table metadata operations
- S3 operations
- EMR operations
- Error handling
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError, BotoCoreError

from chuck_data.clients.redshift import RedshiftAPIClient


class TestRedshiftAPIClientInitialization:
    """Test RedshiftAPIClient initialization."""

    def test_initialization_with_cluster_identifier(self):
        """Test initialization with cluster identifier."""
        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            database="test_db",
        )

        assert client.aws_access_key_id == "test-key"
        assert client.aws_secret_access_key == "test-secret"
        assert client.region == "us-west-2"
        assert client.cluster_identifier == "test-cluster"
        assert client.workgroup_name is None
        assert client.database == "test_db"

    def test_initialization_with_workgroup_name(self):
        """Test initialization with Redshift Serverless workgroup name."""
        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            workgroup_name="test-workgroup",
            database="test_db",
        )

        assert client.workgroup_name == "test-workgroup"
        assert client.cluster_identifier is None

    def test_initialization_without_cluster_or_workgroup_fails(self):
        """Test that initialization fails without cluster_identifier or workgroup_name."""
        with pytest.raises(ValueError) as exc_info:
            RedshiftAPIClient(
                aws_access_key_id="test-key",
                aws_secret_access_key="test-secret",
                region="us-west-2",
                database="test_db",
            )

        assert "Either cluster_identifier or workgroup_name must be provided" in str(
            exc_info.value
        )

    def test_initialization_with_all_optional_parameters(self):
        """Test initialization with all optional parameters."""
        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            database="test_db",
            s3_bucket="test-bucket",
        )

        assert client.s3_bucket == "test-bucket"


class TestConnectionValidation:
    """Test connection validation methods."""

    @patch("chuck_data.clients.redshift.boto3")
    def test_validate_connection_success(self, mock_boto3):
        """Test successful connection validation."""
        # Mock boto3 clients
        mock_redshift_data = Mock()
        mock_redshift_data.list_databases.return_value = {
            "Databases": ["dev", "analytics"]
        }
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        assert client.validate_connection() is True

    @patch("chuck_data.clients.redshift.boto3")
    def test_validate_connection_failure(self, mock_boto3):
        """Test connection validation failure."""
        # Mock boto3 client to raise exception
        mock_redshift_data = Mock()
        mock_redshift_data.list_databases.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
            "list_databases",
        )
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        assert client.validate_connection() is False


class TestListDatabases:
    """Test list_databases method."""

    @patch("chuck_data.clients.redshift.boto3")
    def test_list_databases_with_cluster(self, mock_boto3):
        """Test listing databases with cluster identifier."""
        mock_redshift_data = Mock()
        mock_redshift_data.list_databases.return_value = {
            "Databases": ["dev", "analytics", "test"]
        }
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        databases = client.list_databases()

        assert databases == ["dev", "analytics", "test"]
        mock_redshift_data.list_databases.assert_called_once_with(
            Database="dev", ClusterIdentifier="test-cluster"
        )

    @patch("chuck_data.clients.redshift.boto3")
    def test_list_databases_with_workgroup(self, mock_boto3):
        """Test listing databases with workgroup name."""
        mock_redshift_data = Mock()
        mock_redshift_data.list_databases.return_value = {"Databases": ["dev"]}
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            workgroup_name="test-workgroup",
        )

        databases = client.list_databases()

        assert databases == ["dev"]
        mock_redshift_data.list_databases.assert_called_once_with(
            Database="dev", WorkgroupName="test-workgroup"
        )

    @patch("chuck_data.clients.redshift.boto3")
    def test_list_databases_empty_response(self, mock_boto3):
        """Test listing databases with empty response."""
        mock_redshift_data = Mock()
        mock_redshift_data.list_databases.return_value = {}
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        databases = client.list_databases()

        assert databases == []

    @patch("chuck_data.clients.redshift.boto3")
    def test_list_databases_client_error(self, mock_boto3):
        """Test list_databases with ClientError."""
        mock_redshift_data = Mock()
        mock_redshift_data.list_databases.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
            "list_databases",
        )
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        with pytest.raises(ValueError) as exc_info:
            client.list_databases()

        assert "Error listing databases" in str(exc_info.value)

    @patch("chuck_data.clients.redshift.boto3")
    def test_list_databases_botocore_error(self, mock_boto3):
        """Test list_databases with BotoCoreError (connection error)."""
        mock_redshift_data = Mock()
        mock_redshift_data.list_databases.side_effect = BotoCoreError()
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        with pytest.raises(ConnectionError) as exc_info:
            client.list_databases()

        assert "Connection error occurred" in str(exc_info.value)


class TestListSchemas:
    """Test list_schemas method."""

    @patch("chuck_data.clients.redshift.boto3")
    def test_list_schemas_default_database(self, mock_boto3):
        """Test listing schemas using default database."""
        mock_redshift_data = Mock()
        mock_redshift_data.list_schemas.return_value = {
            "Schemas": ["public", "analytics", "staging"]
        }
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            database="analytics",
        )

        schemas = client.list_schemas()

        assert schemas == ["public", "analytics", "staging"]
        mock_redshift_data.list_schemas.assert_called_once_with(
            Database="analytics", ClusterIdentifier="test-cluster"
        )

    @patch("chuck_data.clients.redshift.boto3")
    def test_list_schemas_specified_database(self, mock_boto3):
        """Test listing schemas with specified database."""
        mock_redshift_data = Mock()
        mock_redshift_data.list_schemas.return_value = {"Schemas": ["public"]}
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            database="default_db",
        )

        schemas = client.list_schemas(database="custom_db")

        assert schemas == ["public"]
        mock_redshift_data.list_schemas.assert_called_once_with(
            Database="custom_db", ClusterIdentifier="test-cluster"
        )

    @patch("chuck_data.clients.redshift.boto3")
    def test_list_schemas_with_workgroup(self, mock_boto3):
        """Test listing schemas with workgroup name."""
        mock_redshift_data = Mock()
        mock_redshift_data.list_schemas.return_value = {"Schemas": ["public"]}
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            workgroup_name="test-workgroup",
            database="analytics",
        )

        schemas = client.list_schemas()

        mock_redshift_data.list_schemas.assert_called_once_with(
            Database="analytics", WorkgroupName="test-workgroup"
        )

    @patch("chuck_data.clients.redshift.boto3")
    def test_list_schemas_error(self, mock_boto3):
        """Test list_schemas error handling."""
        mock_redshift_data = Mock()
        mock_redshift_data.list_schemas.side_effect = ClientError(
            {"Error": {"Code": "InvalidInput", "Message": "Invalid database"}},
            "list_schemas",
        )
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        with pytest.raises(ValueError) as exc_info:
            client.list_schemas()

        assert "Error listing schemas" in str(exc_info.value)


class TestListTables:
    """Test list_tables method."""

    @patch("chuck_data.clients.redshift.boto3")
    def test_list_tables_basic(self, mock_boto3):
        """Test listing tables without filters."""
        mock_redshift_data = Mock()
        mock_redshift_data.list_tables.return_value = {
            "Tables": [
                {"name": "customers", "type": "TABLE"},
                {"name": "orders", "type": "TABLE"},
            ]
        }
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            database="analytics",
        )

        tables = client.list_tables()

        assert "tables" in tables
        assert len(tables["tables"]) == 2
        assert tables["tables"][0]["name"] == "customers"
        mock_redshift_data.list_tables.assert_called_once_with(
            Database="analytics", ClusterIdentifier="test-cluster"
        )

    @patch("chuck_data.clients.redshift.boto3")
    def test_list_tables_with_schema_filter(self, mock_boto3):
        """Test listing tables with schema filter."""
        mock_redshift_data = Mock()
        mock_redshift_data.list_tables.return_value = {
            "Tables": [{"name": "customers", "type": "TABLE"}]
        }
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            database="analytics",
        )

        tables = client.list_tables(schema_pattern="public")

        mock_redshift_data.list_tables.assert_called_once_with(
            Database="analytics",
            ClusterIdentifier="test-cluster",
            SchemaPattern="public",
        )

    @patch("chuck_data.clients.redshift.boto3")
    def test_list_tables_with_table_filter(self, mock_boto3):
        """Test listing tables with table pattern filter."""
        mock_redshift_data = Mock()
        mock_redshift_data.list_tables.return_value = {"Tables": []}
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            database="analytics",
        )

        tables = client.list_tables(table_pattern="cust%")

        mock_redshift_data.list_tables.assert_called_once_with(
            Database="analytics",
            ClusterIdentifier="test-cluster",
            TablePattern="cust%",
        )

    @patch("chuck_data.clients.redshift.boto3")
    def test_list_tables_with_multiple_filters(self, mock_boto3):
        """Test listing tables with multiple filters."""
        mock_redshift_data = Mock()
        mock_redshift_data.list_tables.return_value = {"Tables": []}
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            database="analytics",
        )

        tables = client.list_tables(
            database="custom_db", schema_pattern="staging", table_pattern="temp%"
        )

        mock_redshift_data.list_tables.assert_called_once_with(
            Database="custom_db",
            ClusterIdentifier="test-cluster",
            SchemaPattern="staging",
            TablePattern="temp%",
        )

    @patch("chuck_data.clients.redshift.boto3")
    def test_list_tables_error(self, mock_boto3):
        """Test list_tables error handling."""
        mock_redshift_data = Mock()
        mock_redshift_data.list_tables.side_effect = ClientError(
            {"Error": {"Code": "InvalidInput", "Message": "Invalid schema"}},
            "list_tables",
        )
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        with pytest.raises(ValueError) as exc_info:
            client.list_tables()

        assert "Error listing tables" in str(exc_info.value)


class TestDescribeTable:
    """Test describe_table method."""

    @patch("chuck_data.clients.redshift.boto3")
    def test_describe_table_success(self, mock_boto3):
        """Test describing table successfully."""
        mock_redshift_data = Mock()
        mock_redshift_data.describe_table.return_value = {
            "TableName": "customers",
            "ColumnList": [
                {"name": "id", "typeName": "integer"},
                {"name": "email", "typeName": "varchar"},
            ],
        }
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            database="analytics",
        )

        result = client.describe_table(schema="public", table="customers")

        assert result["TableName"] == "customers"
        assert len(result["ColumnList"]) == 2
        mock_redshift_data.describe_table.assert_called_once_with(
            Database="analytics",
            Schema="public",
            Table="customers",
            ClusterIdentifier="test-cluster",
        )

    @patch("chuck_data.clients.redshift.boto3")
    def test_describe_table_with_custom_database(self, mock_boto3):
        """Test describing table with custom database."""
        mock_redshift_data = Mock()
        mock_redshift_data.describe_table.return_value = {"TableName": "orders"}
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            database="default_db",
        )

        result = client.describe_table(
            database="custom_db", schema="public", table="orders"
        )

        mock_redshift_data.describe_table.assert_called_once_with(
            Database="custom_db",
            Schema="public",
            Table="orders",
            ClusterIdentifier="test-cluster",
        )

    @patch("chuck_data.clients.redshift.boto3")
    def test_describe_table_missing_schema(self, mock_boto3):
        """Test describe_table fails without schema."""
        mock_boto3.client.return_value = Mock()

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        with pytest.raises(ValueError) as exc_info:
            client.describe_table(table="customers")

        assert "Both schema and table must be specified" in str(exc_info.value)

    @patch("chuck_data.clients.redshift.boto3")
    def test_describe_table_missing_table(self, mock_boto3):
        """Test describe_table fails without table."""
        mock_boto3.client.return_value = Mock()

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        with pytest.raises(ValueError) as exc_info:
            client.describe_table(schema="public")

        assert "Both schema and table must be specified" in str(exc_info.value)

    @patch("chuck_data.clients.redshift.boto3")
    def test_describe_table_not_found(self, mock_boto3):
        """Test describe_table with table not found."""
        mock_redshift_data = Mock()
        mock_redshift_data.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFound", "Message": "Table not found"}},
            "describe_table",
        )
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        with pytest.raises(ValueError) as exc_info:
            client.describe_table(schema="public", table="nonexistent")

        assert "Error describing table" in str(exc_info.value)


class TestSQLExecution:
    """Test SQL execution methods."""

    @patch("chuck_data.clients.redshift.boto3")
    def test_execute_sql_without_wait(self, mock_boto3):
        """Test SQL execution without waiting for completion."""
        mock_redshift_data = Mock()
        mock_redshift_data.execute_statement.return_value = {"Id": "statement-123"}
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            database="analytics",
        )

        result = client.execute_sql("SELECT * FROM customers", wait=False)

        assert result["statement_id"] == "statement-123"
        mock_redshift_data.execute_statement.assert_called_once()

    @patch("chuck_data.clients.redshift.boto3")
    @patch("chuck_data.clients.redshift.time.sleep")
    def test_execute_sql_with_wait_success(self, mock_sleep, mock_boto3):
        """Test SQL execution with successful completion."""
        mock_redshift_data = Mock()
        mock_redshift_data.execute_statement.return_value = {"Id": "statement-123"}
        mock_redshift_data.describe_statement.return_value = {
            "Status": "FINISHED",
            "HasResultSet": True,
        }
        mock_redshift_data.get_statement_result.return_value = {
            "Records": [["value1"], ["value2"]]
        }
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            database="analytics",
        )

        result = client.execute_sql("SELECT * FROM customers", wait=True)

        assert result["status"] == "FINISHED"
        assert "result" in result
        mock_redshift_data.get_statement_result.assert_called_once_with(
            Id="statement-123"
        )

    @patch("chuck_data.clients.redshift.boto3")
    @patch("chuck_data.clients.redshift.time.sleep")
    def test_execute_sql_with_wait_failure(self, mock_sleep, mock_boto3):
        """Test SQL execution with statement failure."""
        mock_redshift_data = Mock()
        mock_redshift_data.execute_statement.return_value = {"Id": "statement-123"}
        mock_redshift_data.describe_statement.return_value = {
            "Status": "FAILED",
            "Error": "Syntax error",
        }
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            database="analytics",
        )

        with pytest.raises(ValueError) as exc_info:
            client.execute_sql("SELECT * FROM invalid_table", wait=True)

        assert "Statement failed" in str(exc_info.value)

    @patch("chuck_data.clients.redshift.boto3")
    def test_execute_sql_with_custom_database(self, mock_boto3):
        """Test SQL execution with custom database."""
        mock_redshift_data = Mock()
        mock_redshift_data.execute_statement.return_value = {"Id": "statement-123"}
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            database="default_db",
        )

        result = client.execute_sql("SELECT 1", database="custom_db", wait=False)

        call_args = mock_redshift_data.execute_statement.call_args
        assert call_args[1]["Database"] == "custom_db"

    @patch("chuck_data.clients.redshift.boto3")
    def test_execute_sql_client_error(self, mock_boto3):
        """Test SQL execution with ClientError."""
        mock_redshift_data = Mock()
        mock_redshift_data.execute_statement.side_effect = ClientError(
            {"Error": {"Code": "InvalidQuery", "Message": "Syntax error"}},
            "execute_statement",
        )
        mock_boto3.client.return_value = mock_redshift_data

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        with pytest.raises(ValueError) as exc_info:
            client.execute_sql("INVALID SQL", wait=False)

        assert "SQL execution failed" in str(exc_info.value)


class TestS3Operations:
    """Test S3 operations."""

    @patch("chuck_data.clients.redshift.boto3")
    def test_upload_to_s3_success(self, mock_boto3):
        """Test successful S3 upload."""
        mock_s3 = Mock()
        mock_boto3.client.return_value = mock_s3

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            s3_bucket="test-bucket",
        )

        result = client.upload_to_s3("/local/path/file.txt", "uploads/file.txt")

        assert result == "s3://test-bucket/uploads/file.txt"
        mock_s3.upload_file.assert_called_once_with(
            "/local/path/file.txt", "test-bucket", "uploads/file.txt"
        )

    @patch("chuck_data.clients.redshift.boto3")
    def test_upload_to_s3_without_bucket(self, mock_boto3):
        """Test S3 upload without configured bucket."""
        mock_boto3.client.return_value = Mock()

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        with pytest.raises(ValueError) as exc_info:
            client.upload_to_s3("/local/path/file.txt", "uploads/file.txt")

        assert "S3 bucket not configured" in str(exc_info.value)

    @patch("chuck_data.clients.redshift.boto3")
    def test_list_s3_objects_success(self, mock_boto3):
        """Test successful S3 object listing."""
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "uploads/file1.txt"}, {"Key": "uploads/file2.txt"}]
        }
        mock_boto3.client.return_value = mock_s3

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            s3_bucket="test-bucket",
        )

        objects = client.list_s3_objects("uploads/")

        assert objects == ["uploads/file1.txt", "uploads/file2.txt"]
        mock_s3.list_objects_v2.assert_called_once_with(
            Bucket="test-bucket", Prefix="uploads/"
        )

    @patch("chuck_data.clients.redshift.boto3")
    def test_list_s3_objects_empty(self, mock_boto3):
        """Test S3 object listing with no results."""
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {}
        mock_boto3.client.return_value = mock_s3

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            s3_bucket="test-bucket",
        )

        objects = client.list_s3_objects("empty-prefix/")

        assert objects == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
