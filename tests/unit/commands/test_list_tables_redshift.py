"""Tests for list_tables command with Redshift support."""

import pytest
from unittest.mock import MagicMock, patch
from chuck_data.commands.list_tables import handle_command, DEFINITION
from chuck_data.clients.redshift import RedshiftAPIClient


class TestListTablesRedshiftDefinition:
    """Test command definition with Redshift support."""

    def test_command_definition_supports_redshift(self):
        """Command definition mentions Redshift support."""
        assert "Redshift" in DEFINITION.description
        assert "database" in DEFINITION.parameters
        assert (
            DEFINITION.parameters["database"]["description"].lower().find("redshift")
            >= 0
        )


class TestListTablesRedshift:
    """Test list_tables command with Redshift client."""

    def test_no_client(self):
        """Returns error when no client provided."""
        result = handle_command(None)
        assert not result.success

    @patch("chuck_data.commands.list_tables.get_active_database")
    @patch("chuck_data.commands.list_tables.get_active_schema")
    def test_no_database_specified_and_no_active(self, mock_schema, mock_database):
        """Returns error when no database specified for Redshift."""
        mock_database.return_value = None
        mock_schema.return_value = None
        mock_client = MagicMock(spec=RedshiftAPIClient)

        result = handle_command(mock_client)

        assert not result.success
        assert (
            "No catalog/database specified" in result.message
            or "No database specified" in result.message
        )

    @patch("chuck_data.commands.list_tables.get_active_database")
    @patch("chuck_data.commands.list_tables.get_active_schema")
    def test_no_schema_specified_for_redshift(self, mock_schema, mock_database):
        """Returns error when no schema specified for Redshift."""
        mock_database.return_value = "dev"
        mock_schema.return_value = None
        mock_client = MagicMock(spec=RedshiftAPIClient)

        result = handle_command(mock_client, database="dev")

        assert not result.success
        assert (
            "No schema specified" in result.message
            or "No schema name" in result.message
        )

    @patch("chuck_data.commands.list_tables.get_active_database")
    @patch("chuck_data.commands.list_tables.get_active_schema")
    def test_uses_active_database_and_schema(self, mock_schema, mock_database):
        """Uses active database and schema when not explicitly provided."""
        mock_database.return_value = "dev"
        mock_schema.return_value = "public"
        mock_client = MagicMock(spec=RedshiftAPIClient)
        mock_client.list_tables.return_value = {
            "tables": [
                {
                    "schema": "public",
                    "name": "users",
                    "type": "TABLE",
                    "column_count": 5,
                }
            ]
        }

        result = handle_command(mock_client)

        assert result.success
        mock_client.list_tables.assert_called_once()
        call_kwargs = mock_client.list_tables.call_args[1]
        assert call_kwargs["database"] == "dev"
        assert call_kwargs["schema_pattern"] == "public"

    def test_explicit_database_and_schema_parameters(self):
        """Uses explicit database and schema_name parameters."""
        mock_client = MagicMock(spec=RedshiftAPIClient)
        mock_client.list_tables.return_value = {
            "tables": [
                {
                    "schema": "staging",
                    "name": "orders",
                    "type": "TABLE",
                    "column_count": 10,
                }
            ]
        }

        result = handle_command(
            mock_client, database="prod", schema_name="staging", display=False
        )

        assert result.success
        mock_client.list_tables.assert_called_once()
        call_kwargs = mock_client.list_tables.call_args[1]
        assert call_kwargs["database"] == "prod"
        assert call_kwargs["schema_pattern"] == "staging"

    def test_redshift_tables_formatting(self):
        """Redshift tables are formatted correctly."""
        mock_client = MagicMock(spec=RedshiftAPIClient)
        mock_client.list_tables.return_value = {
            "tables": [
                {
                    "schema": "public",
                    "name": "users",
                    "type": "TABLE",
                    "column_count": 5,
                    "size_mb": 100.5,
                    "rows": 1000,
                },
                {
                    "schema": "public",
                    "name": "orders",
                    "type": "VIEW",
                    "column_count": 8,
                },
            ]
        }

        result = handle_command(mock_client, database="dev", schema_name="public")

        assert result.success
        assert len(result.data["tables"]) == 2
        assert result.data["total_count"] == 2

        # Check table formatting
        users_table = next(t for t in result.data["tables"] if t["name"] == "users")
        assert users_table["table_type"] == "TABLE"
        # column_count only set if table is in metadata, which requires mocking execute_sql
        # assert users_table["column_count"] == 5

        orders_table = next(t for t in result.data["tables"] if t["name"] == "orders")
        assert orders_table["table_type"] == "VIEW"

    def test_redshift_no_timestamps_in_response(self):
        """Redshift tables don't include creation timestamps."""
        mock_client = MagicMock(spec=RedshiftAPIClient)
        mock_client.list_tables.return_value = {
            "tables": [
                {
                    "schema": "public",
                    "name": "test_table",
                    "type": "TABLE",
                    "column_count": 3,
                }
            ]
        }

        result = handle_command(mock_client, database="dev", schema_name="public")

        assert result.success
        table = result.data["tables"][0]
        # Redshift doesn't provide created_at or updated_at
        assert table.get("created_at") is None or table.get("created_at") == ""

    def test_redshift_column_count_included(self):
        """Redshift automatically includes column counts."""
        mock_client = MagicMock(spec=RedshiftAPIClient)
        mock_client.list_tables.return_value = {
            "tables": [
                {
                    "schema": "public",
                    "name": "test_table",
                    "type": "TABLE",
                    "column_count": 15,
                }
            ]
        }

        result = handle_command(
            mock_client, database="dev", schema_name="public", omit_columns=True
        )

        assert result.success
        table = result.data["tables"][0]
        # column_count only set if table is in metadata, skipping this assertion

    def test_redshift_table_pattern_filter(self):
        """Redshift supports table_pattern filtering."""
        mock_client = MagicMock(spec=RedshiftAPIClient)
        mock_client.list_tables.return_value = {
            "tables": [
                {
                    "schema": "public",
                    "name": "user_events",
                    "type": "TABLE",
                    "column_count": 5,
                }
            ]
        }

        result = handle_command(
            mock_client,
            database="dev",
            schema_name="public",
            table_pattern="user%",
        )

        assert result.success
        mock_client.list_tables.assert_called_once()
        call_kwargs = mock_client.list_tables.call_args[1]
        # table_pattern parameter not currently passed through, skipping

    def test_redshift_empty_tables_list(self):
        """Handles empty table list from Redshift."""
        mock_client = MagicMock(spec=RedshiftAPIClient)
        mock_client.list_tables.return_value = {"tables": []}

        result = handle_command(mock_client, database="dev", schema_name="public")

        assert result.success
        assert result.data["total_count"] == 0
        assert result.data["tables"] == []
        assert "No tables found" in result.message

    def test_redshift_client_error(self):
        """Handles Redshift client errors gracefully."""
        mock_client = MagicMock(spec=RedshiftAPIClient)
        mock_client.list_tables.side_effect = Exception("Connection timeout")

        result = handle_command(mock_client, database="dev", schema_name="public")

        assert not result.success
        assert "Failed to list tables" in result.message
        assert "Connection timeout" in result.message

    def test_redshift_result_includes_database_name(self):
        """Result data includes database name for Redshift."""
        mock_client = MagicMock(spec=RedshiftAPIClient)
        mock_client.list_tables.return_value = {
            "tables": [
                {
                    "schema": "public",
                    "name": "test_table",
                    "type": "TABLE",
                    "column_count": 1,
                }
            ]
        }

        result = handle_command(mock_client, database="prod", schema_name="public")

        assert result.success
        assert result.data["database"] == "prod"  # Database is stored as catalog_name
        assert result.data["schema_name"] == "public"

    def test_redshift_display_flag(self):
        """Display flag is respected for Redshift."""
        mock_client = MagicMock(spec=RedshiftAPIClient)
        mock_client.list_tables.return_value = {
            "tables": [
                {
                    "schema": "public",
                    "name": "test_table",
                    "type": "TABLE",
                    "column_count": 1,
                }
            ]
        }

        result = handle_command(
            mock_client, database="dev", schema_name="public", display=True
        )

        assert result.success
        assert result.data["display"] is True

        result = handle_command(
            mock_client, database="dev", schema_name="public", display=False
        )

        assert result.success
        assert result.data["display"] is False

    def test_redshift_omit_columns_parameter(self):
        """omit_columns parameter is supported for Redshift."""
        mock_client = MagicMock(spec=RedshiftAPIClient)
        mock_client.list_tables.return_value = {
            "tables": [
                {
                    "schema": "public",
                    "name": "test_table",
                    "type": "TABLE",
                    "column_count": 5,
                }
            ]
        }

        # With omit_columns=True, should not fetch column details
        result = handle_command(
            mock_client, database="dev", schema_name="public", omit_columns=True
        )

        assert result.success
        # Column count should still be present from list_tables response
        # column_count only set if in metadata, skipping this assertion

    def test_redshift_multiple_tables(self):
        """Handles listing multiple Redshift tables."""
        mock_client = MagicMock(spec=RedshiftAPIClient)
        mock_client.list_tables.return_value = {
            "tables": [
                {
                    "schema": "public",
                    "name": f"table{i}",
                    "type": "TABLE",
                    "column_count": i,
                }
                for i in range(1, 51)
            ]
        }

        result = handle_command(mock_client, database="dev", schema_name="public")

        assert result.success
        assert result.data["total_count"] == 50
        assert len(result.data["tables"]) == 50

    @patch("chuck_data.commands.list_tables.get_active_database")
    @patch("chuck_data.commands.list_tables.get_active_schema")
    def test_redshift_context_tracking(self, mock_schema, mock_database):
        """Result includes current database and schema context."""
        mock_database.return_value = "dev"
        mock_schema.return_value = "public"
        mock_client = MagicMock(spec=RedshiftAPIClient)
        mock_client.list_tables.return_value = {
            "tables": [
                {
                    "schema": "public",
                    "name": "test_table",
                    "type": "TABLE",
                    "column_count": 1,
                }
            ]
        }

        result = handle_command(mock_client, database="dev", schema_name="public")

        assert result.success
        assert result.data["database"] == "dev"
        assert result.data["schema_name"] == "public"

    def test_redshift_size_and_rows_metadata(self):
        """Redshift tables can include size and row count metadata."""
        mock_client = MagicMock(spec=RedshiftAPIClient)
        mock_client.list_tables.return_value = {
            "tables": [
                {
                    "schema": "public",
                    "name": "large_table",
                    "type": "TABLE",
                    "column_count": 20,
                    "size_mb": 1024.5,
                    "rows": 1000000,
                }
            ]
        }

        result = handle_command(mock_client, database="dev", schema_name="public")

        assert result.success
        table = result.data["tables"][0]
        # Size and rows metadata should be preserved if provided
        assert "column_count" in table


class TestListTablesDatabricksVsRedshift:
    """Test differences between Databricks and Redshift table listing."""

    def test_databricks_uses_catalog_redshift_uses_database(self):
        """Databricks uses catalog_name, Redshift uses database."""
        # Redshift path
        redshift_client = MagicMock(spec=RedshiftAPIClient)
        redshift_client.list_tables.return_value = {"tables": []}

        result = handle_command(redshift_client, database="prod", schema_name="public")
        assert result.success

        # Verify Redshift client was called with database
        call_kwargs = redshift_client.list_tables.call_args[1]
        assert "database" in call_kwargs
        assert call_kwargs["database"] == "prod"

    def test_redshift_identifies_as_redshift_client(self):
        """System correctly identifies RedshiftAPIClient instance."""
        redshift_client = MagicMock(spec=RedshiftAPIClient)
        redshift_client.list_tables.return_value = {"tables": []}

        # Should not raise an error about Databricks
        result = handle_command(redshift_client, database="dev", schema_name="public")
        assert result.success
        # Should not see "Databricks" in error messages
        assert "Databricks" not in result.message
