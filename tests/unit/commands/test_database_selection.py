"""Tests for database_selection command."""

import pytest
from unittest.mock import MagicMock, patch
from chuck_data.commands.database_selection import (
    handle_command,
    DEFINITION,
    _similarity_score,
    _find_best_database_match,
)


class TestSimilarityScore:
    """Test similarity scoring function."""

    def test_exact_match(self):
        """Exact match has score of 1.0."""
        assert _similarity_score("dev", "dev") == 1.0

    def test_case_insensitive(self):
        """Matching is case insensitive."""
        assert _similarity_score("Dev", "dev") == 1.0
        assert _similarity_score("PROD", "prod") == 1.0

    def test_with_whitespace(self):
        """Whitespace is stripped."""
        assert _similarity_score(" dev ", "dev") == 1.0

    def test_partial_similarity(self):
        """Partial matches have scores between 0 and 1."""
        score = _similarity_score("development", "dev")
        assert 0 < score < 1

    def test_no_similarity(self):
        """Completely different strings have low scores."""
        score = _similarity_score("abc", "xyz")
        assert score < 0.5


class TestFindBestDatabaseMatch:
    """Test database matching function."""

    def test_exact_match(self):
        """Finds exact match."""
        databases = ["dev", "test", "prod"]
        result = _find_best_database_match("dev", databases)
        assert result == "dev"

    def test_case_insensitive_match(self):
        """Finds match ignoring case."""
        databases = ["Dev", "Test", "Prod"]
        result = _find_best_database_match("dev", databases)
        assert result == "Dev"

    def test_substring_match(self):
        """Finds match when target is substring."""
        databases = ["development", "testing", "production"]
        result = _find_best_database_match("dev", databases)
        assert result == "development"

    def test_prefix_match(self):
        """Finds match when target is prefix."""
        databases = ["prod_analytics", "dev_analytics", "test_analytics"]
        result = _find_best_database_match("prod", databases)
        assert result == "prod_analytics"

    def test_fuzzy_match(self):
        """Finds fuzzy match when similarity is high enough."""
        databases = ["production", "development", "staging"]
        result = _find_best_database_match("prod", databases)
        assert result == "production"

    def test_no_match_below_threshold(self):
        """Returns None when no good match exists."""
        databases = ["alpha", "beta", "gamma"]
        result = _find_best_database_match("xyz", databases)
        assert result is None

    def test_empty_database_list(self):
        """Returns None for empty database list."""
        result = _find_best_database_match("dev", [])
        assert result is None

    def test_none_in_database_list(self):
        """Handles None values in database list."""
        databases = ["dev", None, "prod"]
        result = _find_best_database_match("dev", databases)
        assert result == "dev"

    def test_whitespace_handling(self):
        """Handles whitespace in names."""
        databases = ["  dev  ", "prod"]
        result = _find_best_database_match("dev", databases)
        assert result == "  dev  "


class TestDatabaseSelection:
    """Test database selection command."""

    def test_command_definition(self):
        """Command has correct definition."""
        assert DEFINITION.name == "select_database"
        assert DEFINITION.needs_api_client is True
        assert DEFINITION.visible_to_agent is True
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.provider == "aws_redshift"
        assert "/select-database" in DEFINITION.tui_aliases
        assert "/use-database" in DEFINITION.tui_aliases
        assert "database" in DEFINITION.required_params

    def test_no_database_parameter(self):
        """Returns error when database parameter missing."""
        mock_client = MagicMock()
        result = handle_command(mock_client)
        assert not result.success
        assert "database parameter is required" in result.message

    def test_no_client(self):
        """Returns error when client is None."""
        result = handle_command(None, database="dev")
        assert not result.success
        assert "No Redshift client" in result.message

    @patch("chuck_data.commands.database_selection.get_config_manager")
    def test_exact_match_selection(self, mock_config):
        """Selects database with exact match."""
        mock_client = MagicMock()
        mock_client.list_databases.return_value = ["dev", "test", "prod"]
        mock_config.return_value.update.return_value = True

        result = handle_command(mock_client, database="dev")

        assert result.success
        assert result.data["database_name"] == "dev"
        assert "dev" in result.message
        mock_config.return_value.update.assert_called_once_with(redshift_database="dev")

    @patch("chuck_data.commands.database_selection.get_config_manager")
    def test_fuzzy_match_selection(self, mock_config):
        """Selects database with fuzzy match."""
        mock_client = MagicMock()
        mock_client.list_databases.return_value = [
            "development",
            "testing",
            "production",
        ]
        mock_config.return_value.update.return_value = True

        result = handle_command(mock_client, database="prod")

        assert result.success
        assert result.data["database_name"] == "production"
        mock_config.return_value.update.assert_called_once_with(
            redshift_database="production"
        )

    def test_no_match_found(self):
        """Returns error when no matching database found."""
        mock_client = MagicMock()
        mock_client.list_databases.return_value = ["dev", "test", "prod"]

        result = handle_command(mock_client, database="xyzabc")

        assert not result.success
        assert "No database found matching" in result.message
        assert "xyzabc" in result.message
        assert "Available databases:" in result.message

    def test_no_databases_available(self):
        """Returns error when no databases exist."""
        mock_client = MagicMock()
        mock_client.list_databases.return_value = []

        result = handle_command(mock_client, database="dev")

        assert not result.success
        assert "No databases found" in result.message

    def test_available_databases_truncated(self):
        """Truncates available databases list when more than 5."""
        mock_client = MagicMock()
        mock_client.list_databases.return_value = [
            "db1",
            "db2",
            "db3",
            "db4",
            "db5",
            "db6",
            "db7",
        ]

        result = handle_command(mock_client, database="nonexistent")

        assert not result.success
        assert "and 2 more" in result.message
        assert "db1" in result.message
        assert "db7" not in result.message  # Should be truncated

    def test_available_databases_not_truncated_when_five_or_less(self):
        """Shows all databases when 5 or fewer."""
        mock_client = MagicMock()
        mock_client.list_databases.return_value = ["db1", "db2", "db3"]

        result = handle_command(mock_client, database="nonexistent")

        assert not result.success
        assert "db1" in result.message
        assert "db2" in result.message
        assert "db3" in result.message
        assert "and" not in result.message or "more" not in result.message

    @patch("chuck_data.commands.database_selection.get_config_manager")
    def test_config_update_failure(self, mock_config):
        """Handles config update failure."""
        mock_client = MagicMock()
        mock_client.list_databases.return_value = ["dev"]
        mock_config.return_value.update.return_value = False

        result = handle_command(mock_client, database="dev")

        assert not result.success
        assert "Failed to set active database" in result.message

    def test_client_error(self):
        """Handles client error gracefully."""
        mock_client = MagicMock()
        mock_client.list_databases.side_effect = Exception("Connection error")

        result = handle_command(mock_client, database="dev")

        assert not result.success
        assert "Connection error" in result.message

    @patch("chuck_data.commands.database_selection.get_config_manager")
    def test_callback_reports_steps(self, mock_config):
        """Callback receives step updates."""
        mock_client = MagicMock()
        mock_client.list_databases.return_value = ["development"]
        mock_config.return_value.update.return_value = True

        callback = MagicMock()
        result = handle_command(
            mock_client, database="dev", tool_output_callback=callback
        )

        assert result.success
        # Verify callback was called with steps
        assert callback.call_count >= 2
        callback.assert_any_call(
            "select-database", {"step": "Looking for database matching 'dev'"}
        )

    @patch("chuck_data.commands.database_selection.get_config_manager")
    def test_case_insensitive_matching(self, mock_config):
        """Matching is case insensitive."""
        mock_client = MagicMock()
        mock_client.list_databases.return_value = ["Development"]
        mock_config.return_value.update.return_value = True

        result = handle_command(mock_client, database="DEVELOPMENT")

        assert result.success
        assert result.data["database_name"] == "Development"

    @patch("chuck_data.commands.database_selection.get_config_manager")
    def test_whitespace_trimmed(self, mock_config):
        """Whitespace is trimmed from input."""
        mock_client = MagicMock()
        mock_client.list_databases.return_value = ["dev"]
        mock_config.return_value.update.return_value = True

        result = handle_command(mock_client, database="  dev  ")

        assert result.success
        assert result.data["database_name"] == "dev"

    @patch("chuck_data.commands.database_selection.get_config_manager")
    def test_result_includes_step_data(self, mock_config):
        """Result data includes step information."""
        mock_client = MagicMock()
        mock_client.list_databases.return_value = ["dev"]
        mock_config.return_value.update.return_value = True

        result = handle_command(mock_client, database="dev")

        assert result.success
        assert "step" in result.data
        assert "Database set" in result.data["step"]
        assert "dev" in result.data["step"]
