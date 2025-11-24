"""Tests for list_redshift_schemas command."""

import pytest
from unittest.mock import MagicMock, patch
from chuck_data.commands.list_redshift_schemas import handle_command, DEFINITION


class TestListRedshiftSchemas:
    """Test list_redshift_schemas command."""

    def test_command_definition(self):
        """Command has correct definition."""
        assert DEFINITION.name == "list_redshift_schemas"
        assert DEFINITION.needs_api_client is True
        assert DEFINITION.visible_to_agent is True
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.provider == "aws_redshift"

    def test_no_client(self):
        """Handle command with no client returns error."""
        result = handle_command(None)
        assert not result.success
        assert "No Redshift client" in result.message

    @patch("chuck_data.commands.list_redshift_schemas.get_active_database")
    def test_no_database_specified_and_no_active(self, mock_get_database):
        """Returns error when no database specified and no active database."""
        mock_get_database.return_value = None
        mock_client = MagicMock()

        result = handle_command(mock_client)

        assert not result.success
        assert "No database specified" in result.message
        assert "/select-database" in result.message

    @patch("chuck_data.commands.list_redshift_schemas.get_active_database")
    def test_uses_active_database_when_not_specified(self, mock_get_database):
        """Uses active database when database parameter not provided."""
        mock_get_database.return_value = "dev"
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = ["public", "staging"]

        result = handle_command(mock_client)

        assert result.success
        mock_client.list_schemas.assert_called_once_with(database="dev")

    def test_uses_explicit_database_parameter(self):
        """Uses explicit database parameter when provided."""
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = ["public"]

        result = handle_command(mock_client, database="prod")

        assert result.success
        mock_client.list_schemas.assert_called_once_with(database="prod")

    def test_list_schemas_success(self):
        """Successfully lists schemas."""
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = ["public", "staging", "analytics"]

        result = handle_command(mock_client, database="dev")

        assert result.success
        assert result.data["total_count"] == 3
        assert len(result.data["schemas"]) == 3
        assert result.data["schemas"][0]["name"] == "public"
        assert result.data["database"] == "dev"

    def test_list_schemas_empty(self):
        """Handle empty schema list."""
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = []

        result = handle_command(mock_client, database="dev")

        assert result.success
        assert result.data["total_count"] == 0
        assert result.data["schemas"] == []
        assert "No schemas found" in result.message

    def test_display_flag(self):
        """Display flag is passed through correctly."""
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = ["public"]

        result = handle_command(mock_client, database="dev", display=True)

        assert result.success
        assert result.data["display"] is True

        result = handle_command(mock_client, database="dev", display=False)

        assert result.success
        assert result.data["display"] is False

    def test_display_defaults_to_false(self):
        """Display defaults to False when not specified."""
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = ["public"]

        result = handle_command(mock_client, database="dev")

        assert result.success
        assert result.data["display"] is False

    @patch("chuck_data.commands.list_redshift_schemas.get_active_schema")
    @patch("chuck_data.commands.list_redshift_schemas.get_active_database")
    def test_includes_current_context(self, mock_get_database, mock_get_schema):
        """Result includes current database and schema context."""
        mock_get_database.return_value = "dev"
        mock_get_schema.return_value = "public"
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = ["public", "staging"]

        result = handle_command(mock_client, database="dev")

        assert result.success
        assert result.data["current_schema"] == "public"
        assert result.data["database"] == "dev"

    def test_client_error(self):
        """Handle client error gracefully."""
        mock_client = MagicMock()
        mock_client.list_schemas.side_effect = Exception("Connection failed")

        result = handle_command(mock_client, database="dev")

        assert not result.success
        assert "Failed to list schemas" in result.message
        assert "Connection failed" in result.message

    def test_formats_schemas_correctly(self):
        """Schemas are formatted with correct structure."""
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = ["schema1", "schema2"]

        result = handle_command(mock_client, database="dev")

        assert result.success
        for schema in result.data["schemas"]:
            assert "name" in schema
            assert isinstance(schema["name"], str)

    def test_multiple_schemas(self):
        """Handles listing many schemas."""
        mock_client = MagicMock()
        schemas = [f"schema{i}" for i in range(50)]
        mock_client.list_schemas.return_value = schemas

        result = handle_command(mock_client, database="dev")

        assert result.success
        assert result.data["total_count"] == 50
        assert len(result.data["schemas"]) == 50
