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


def setup_mock_session(
    mock_boto3, mock_redshift_data=None, mock_redshift=None, mock_s3=None
):
    """Helper function to setup boto3 Session mock with client mocks.

    Args:
        mock_boto3: The mocked boto3 module
        mock_redshift_data: Mock for redshift-data client (optional)
        mock_redshift: Mock for redshift client (optional)
        mock_s3: Mock for s3 client (optional)

    Returns:
        tuple: (mock_session, mock_redshift_data, mock_redshift, mock_s3)
    """
    mock_session = Mock()

    # Create default mocks if not provided
    if mock_redshift_data is None:
        mock_redshift_data = Mock()
    if mock_redshift is None:
        mock_redshift = Mock()
    if mock_s3 is None:
        mock_s3 = Mock()

    # Setup session.client() to return appropriate client based on service name
    def client_side_effect(service_name):
        if service_name == "redshift-data":
            return mock_redshift_data
        elif service_name == "redshift":
            return mock_redshift
        elif service_name == "s3":
            return mock_s3
        else:
            return Mock()

    mock_session.client.side_effect = client_side_effect
    mock_boto3.Session.return_value = mock_session

    return mock_session, mock_redshift_data, mock_redshift, mock_s3


class TestRedshiftAPIClientInitialization:
    """Test RedshiftAPIClient initialization."""

    @patch("chuck_data.clients.redshift.boto3")
    def test_initialization_with_cluster_identifier(self, mock_boto3):
        """Test initialization with cluster identifier."""
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session

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
        assert client.aws_profile is None

    @patch("chuck_data.clients.redshift.boto3")
    def test_initialization_with_workgroup_name(self, mock_boto3):
        """Test initialization with Redshift Serverless workgroup name."""
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            workgroup_name="test-workgroup",
            database="test_db",
        )

        assert client.workgroup_name == "test-workgroup"
        assert client.cluster_identifier is None

    @patch("chuck_data.clients.redshift.boto3")
    def test_initialization_without_cluster_or_workgroup_fails(self, mock_boto3):
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

    @patch("chuck_data.clients.redshift.boto3")
    def test_initialization_with_all_optional_parameters(self, mock_boto3):
        """Test initialization with all optional parameters."""
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            database="test_db",
            s3_bucket="test-bucket",
        )

        assert client.s3_bucket == "test-bucket"

    @patch("chuck_data.clients.redshift.boto3")
    def test_initialization_with_aws_profile(self, mock_boto3):
        """Test initialization with AWS profile."""
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session

        client = RedshiftAPIClient(
            region="us-west-2",
            cluster_identifier="test-cluster",
            aws_profile="my-profile",
        )

        # Verify Session was created with profile_name
        mock_boto3.Session.assert_called_once_with(
            profile_name="my-profile", region_name="us-west-2"
        )
        assert client.aws_profile == "my-profile"

    @patch("chuck_data.clients.redshift.boto3")
    def test_initialization_with_explicit_credentials(self, mock_boto3):
        """Test initialization with explicit AWS credentials."""
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        # Verify Session was created with explicit credentials
        mock_boto3.Session.assert_called_once_with(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region_name="us-west-2",
        )

    @patch("chuck_data.clients.redshift.boto3")
    def test_initialization_with_default_credentials(self, mock_boto3):
        """Test initialization with default credential chain (no profile, no explicit creds)."""
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session

        client = RedshiftAPIClient(
            region="us-west-2",
            cluster_identifier="test-cluster",
        )

        # Verify Session was created with only region (uses default credential chain)
        mock_boto3.Session.assert_called_once_with(region_name="us-west-2")

    @patch("chuck_data.clients.redshift.boto3")
    def test_initialization_profile_takes_precedence_over_default(self, mock_boto3):
        """Test that aws_profile takes precedence over default credential chain."""
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session

        client = RedshiftAPIClient(
            region="us-west-2",
            workgroup_name="test-workgroup",
            aws_profile="sales-power",
        )

        # Verify Session was created with profile_name
        mock_boto3.Session.assert_called_once_with(
            profile_name="sales-power", region_name="us-west-2"
        )
        assert client.aws_profile == "sales-power"

    @patch("chuck_data.clients.redshift.boto3")
    def test_initialization_explicit_creds_take_precedence_over_profile(
        self, mock_boto3
    ):
        """Test that explicit credentials take precedence over aws_profile."""
        mock_session = Mock()
        mock_boto3.Session.return_value = mock_session

        client = RedshiftAPIClient(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region="us-west-2",
            cluster_identifier="test-cluster",
            aws_profile="my-profile",  # This should be ignored
        )

        # Verify Session was created with explicit credentials (profile ignored)
        mock_boto3.Session.assert_called_once_with(
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            region_name="us-west-2",
        )
        # Profile should still be stored for reference
        assert client.aws_profile == "my-profile"

    @patch("chuck_data.clients.redshift.boto3")
    def test_session_clients_are_created(self, mock_boto3):
        """Test that boto3 clients are created from the session."""
        mock_session = Mock()
        mock_redshift_data = Mock()
        mock_redshift = Mock()
        mock_s3 = Mock()

        mock_session.client.side_effect = [mock_redshift_data, mock_redshift, mock_s3]
        mock_boto3.Session.return_value = mock_session

        client = RedshiftAPIClient(
            region="us-west-2",
            cluster_identifier="test-cluster",
            aws_profile="my-profile",
        )

        # Verify all three clients were created from session
        assert mock_session.client.call_count == 3
        mock_session.client.assert_any_call("redshift-data")
        mock_session.client.assert_any_call("redshift")
        mock_session.client.assert_any_call("s3")

        # Verify clients are assigned
        assert client.redshift_data == mock_redshift_data
        assert client.redshift == mock_redshift
        assert client.s3 == mock_s3


class TestConnectionValidation:
    """Test connection validation methods."""

    @patch("chuck_data.clients.redshift.boto3")
    def test_validate_connection_success(self, mock_boto3):
        """Test successful connection validation."""
        # Mock boto3 session and clients
        mock_redshift_data = Mock()
        mock_redshift_data.list_databases.return_value = {
            "Databases": ["dev", "analytics"]
        }
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        # Mock boto3 session and client to raise exception
        mock_redshift_data = Mock()
        mock_redshift_data.list_databases.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
            "list_databases",
        )
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3)

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
        setup_mock_session(mock_boto3)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_redshift_data=mock_redshift_data)

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
        setup_mock_session(mock_boto3, mock_s3=mock_s3)

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
        setup_mock_session(mock_boto3)

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
        setup_mock_session(mock_boto3, mock_s3=mock_s3)

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
        setup_mock_session(mock_boto3, mock_s3=mock_s3)

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
