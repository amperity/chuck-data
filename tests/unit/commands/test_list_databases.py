"""Tests for list_databases command."""

import pytest
from unittest.mock import MagicMock, patch
from chuck_data.commands.list_databases import handle_command, DEFINITION


class TestListDatabases:
    """Test list_databases command."""

    def test_command_definition(self):
        """Command has correct definition."""
        assert DEFINITION.name == "list_databases"
        assert DEFINITION.needs_api_client is True
        assert DEFINITION.visible_to_agent is True
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.provider == "aws_redshift"
        assert "/list-databases" in DEFINITION.tui_aliases
        assert "/databases" in DEFINITION.tui_aliases

    def test_no_client(self):
        """Handle command with no client returns error."""
        result = handle_command(None)
        assert not result.success
        assert "No Redshift client" in result.message

    def test_list_databases_success(self):
        """Successfully lists databases."""
        mock_client = MagicMock()
        mock_client.list_databases.return_value = ["dev", "test", "prod"]

        result = handle_command(mock_client, display=False)

        assert result.success
        assert result.data["total_count"] == 3
        assert len(result.data["databases"]) == 3
        assert result.data["databases"][0]["name"] == "dev"
        assert result.data["databases"][1]["name"] == "test"
        assert result.data["databases"][2]["name"] == "prod"
        assert result.data["display"] is False
        mock_client.list_databases.assert_called_once()

    def test_list_databases_with_display(self):
        """Lists databases with display flag."""
        mock_client = MagicMock()
        mock_client.list_databases.return_value = ["dev"]

        result = handle_command(mock_client, display=True)

        assert result.success
        assert result.data["display"] is True

    def test_list_databases_empty(self):
        """Handle empty database list."""
        mock_client = MagicMock()
        mock_client.list_databases.return_value = []

        result = handle_command(mock_client)

        assert result.success
        assert result.data["total_count"] == 0
        assert result.data["databases"] == []
        assert "No databases found" in result.message

    @patch("chuck_data.commands.list_databases.get_active_database")
    def test_includes_current_database(self, mock_get_database):
        """Result includes current database context."""
        mock_get_database.return_value = "dev"
        mock_client = MagicMock()
        mock_client.list_databases.return_value = ["dev", "prod"]

        result = handle_command(mock_client)

        assert result.success
        assert result.data["current_database"] == "dev"

    def test_list_databases_client_error(self):
        """Handle client error gracefully."""
        mock_client = MagicMock()
        mock_client.list_databases.side_effect = Exception("Connection failed")

        result = handle_command(mock_client)

        assert not result.success
        assert "Failed to list databases" in result.message
        assert "Connection failed" in result.message

    def test_display_defaults_to_false(self):
        """Display defaults to False when not specified."""
        mock_client = MagicMock()
        mock_client.list_databases.return_value = ["dev"]

        result = handle_command(mock_client)

        assert result.success
        assert result.data["display"] is False

    def test_formats_databases_correctly(self):
        """Databases are formatted with correct structure."""
        mock_client = MagicMock()
        mock_client.list_databases.return_value = ["database1", "database2"]

        result = handle_command(mock_client)

        assert result.success
        for db in result.data["databases"]:
            assert "name" in db
            assert isinstance(db["name"], str)

    def test_multiple_databases(self):
        """Handles listing many databases."""
        mock_client = MagicMock()
        databases = [f"db{i}" for i in range(20)]
        mock_client.list_databases.return_value = databases

        result = handle_command(mock_client)

        assert result.success
        assert result.data["total_count"] == 20
        assert len(result.data["databases"]) == 20
