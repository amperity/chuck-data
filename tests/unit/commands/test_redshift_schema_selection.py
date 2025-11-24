"""Tests for redshift_schema_selection command."""

import pytest
from unittest.mock import MagicMock, patch
from chuck_data.commands.redshift_schema_selection import (
    handle_command,
    DEFINITION,
    _similarity_score,
    _find_best_schema_match,
)


class TestSimilarityScore:
    """Test similarity scoring function."""

    def test_exact_match(self):
        """Exact match has score of 1.0."""
        assert _similarity_score("public", "public") == 1.0

    def test_case_insensitive(self):
        """Matching is case insensitive."""
        assert _similarity_score("Public", "public") == 1.0
        assert _similarity_score("STAGING", "staging") == 1.0

    def test_with_whitespace(self):
        """Whitespace is stripped."""
        assert _similarity_score(" public ", "public") == 1.0


class TestFindBestSchemaMatch:
    """Test schema matching function."""

    def test_exact_match(self):
        """Finds exact match."""
        schemas = ["public", "staging", "analytics"]
        result = _find_best_schema_match("public", schemas)
        assert result == "public"

    def test_case_insensitive_match(self):
        """Finds match ignoring case."""
        schemas = ["Public", "Staging", "Analytics"]
        result = _find_best_schema_match("public", schemas)
        assert result == "Public"

    def test_substring_match(self):
        """Finds match when target is substring."""
        schemas = ["public_v1", "staging_v1", "analytics_v1"]
        result = _find_best_schema_match("public", schemas)
        assert result == "public_v1"

    def test_fuzzy_match(self):
        """Finds fuzzy match when similarity is high enough."""
        schemas = ["analytics", "staging", "public"]
        result = _find_best_schema_match("analytic", schemas)
        assert result == "analytics"

    def test_no_match_below_threshold(self):
        """Returns None when no good match exists."""
        schemas = ["alpha", "beta", "gamma"]
        result = _find_best_schema_match("xyz", schemas)
        assert result is None

    def test_empty_schema_list(self):
        """Returns None for empty schema list."""
        result = _find_best_schema_match("public", [])
        assert result is None


class TestRedshiftSchemaSelection:
    """Test Redshift schema selection command."""

    def test_command_definition(self):
        """Command has correct definition."""
        assert DEFINITION.name == "select_redshift_schema"
        assert DEFINITION.needs_api_client is True
        assert DEFINITION.visible_to_agent is True
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.provider == "aws_redshift"
        assert (
            "/select-schema" in DEFINITION.tui_aliases
            or "/select-redshift-schema" in DEFINITION.tui_aliases
        )
        assert "schema" in DEFINITION.required_params

    def test_no_schema_parameter(self):
        """Returns error when schema parameter missing."""
        mock_client = MagicMock()
        result = handle_command(mock_client)
        assert not result.success
        assert "Schema name is required" in result.message

    def test_no_client(self):
        """Returns error when client is None."""
        result = handle_command(None, schema="public")
        assert not result.success
        assert "No Redshift client" in result.message

    @patch("chuck_data.commands.redshift_schema_selection.get_active_database")
    def test_no_database_selected(self, mock_get_database):
        """Returns error when no database is selected."""
        mock_get_database.return_value = None
        mock_client = MagicMock()

        result = handle_command(mock_client, schema="public")

        assert not result.success
        assert (
            "No database selected" in result.message
            or "No active database" in result.message
        )
        assert "/select-database" in result.message

    @patch("chuck_data.commands.redshift_schema_selection.get_active_database")
    @patch("chuck_data.commands.redshift_schema_selection.get_config_manager")
    def test_exact_match_selection(self, mock_config, mock_get_database):
        """Selects schema with exact match."""
        mock_get_database.return_value = "dev"
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = ["public", "staging", "analytics"]
        mock_config.return_value.update.return_value = True

        result = handle_command(mock_client, schema="public")

        assert result.success
        assert result.data["schema_name"] == "public"
        assert "public" in result.message
        mock_config.return_value.update.assert_called_once_with(active_schema="public")

    @patch("chuck_data.commands.redshift_schema_selection.get_active_database")
    @patch("chuck_data.commands.redshift_schema_selection.get_config_manager")
    def test_fuzzy_match_selection(self, mock_config, mock_get_database):
        """Selects schema with fuzzy match."""
        mock_get_database.return_value = "dev"
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = ["public", "staging", "analytics"]
        mock_config.return_value.update.return_value = True

        result = handle_command(mock_client, schema="analytic")

        assert result.success
        assert result.data["schema_name"] == "analytics"

    @patch("chuck_data.commands.redshift_schema_selection.get_active_database")
    def test_no_match_found(self, mock_get_database):
        """Returns error when no matching schema found."""
        mock_get_database.return_value = "dev"
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = ["public", "staging"]

        result = handle_command(mock_client, schema="nonexistent")

        assert not result.success
        assert "No schema found matching" in result.message
        assert "nonexistent" in result.message
        assert "Available schemas:" in result.message

    @patch("chuck_data.commands.redshift_schema_selection.get_active_database")
    def test_no_schemas_available(self, mock_get_database):
        """Returns error when no schemas exist."""
        mock_get_database.return_value = "dev"
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = []

        result = handle_command(mock_client, schema="public")

        assert not result.success
        assert "No schemas found" in result.message

    @patch("chuck_data.commands.redshift_schema_selection.get_active_database")
    def test_available_schemas_truncated(self, mock_get_database):
        """Truncates available schemas list when more than 5."""
        mock_get_database.return_value = "dev"
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = [
            "s1",
            "s2",
            "s3",
            "s4",
            "s5",
            "s6",
            "s7",
        ]

        result = handle_command(mock_client, schema="nonexistent")

        assert not result.success
        assert "and 2 more" in result.message

    @patch("chuck_data.commands.redshift_schema_selection.get_active_database")
    @patch("chuck_data.commands.redshift_schema_selection.get_config_manager")
    def test_config_update_failure(self, mock_config, mock_get_database):
        """Handles config update failure."""
        mock_get_database.return_value = "dev"
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = ["public"]
        mock_config.return_value.update.return_value = False

        result = handle_command(mock_client, schema="public")

        assert not result.success
        assert "Failed to set active schema" in result.message

    @patch("chuck_data.commands.redshift_schema_selection.get_active_database")
    def test_client_error(self, mock_get_database):
        """Handles client error gracefully."""
        mock_get_database.return_value = "dev"
        mock_client = MagicMock()
        mock_client.list_schemas.side_effect = Exception("Connection error")

        result = handle_command(mock_client, schema="public")

        assert not result.success
        assert "Connection error" in result.message

    @patch("chuck_data.commands.redshift_schema_selection.get_active_database")
    @patch("chuck_data.commands.redshift_schema_selection.get_config_manager")
    def test_callback_reports_steps(self, mock_config, mock_get_database):
        """Callback receives step updates."""
        mock_get_database.return_value = "dev"
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = ["public"]
        mock_config.return_value.update.return_value = True

        callback = MagicMock()
        result = handle_command(
            mock_client, schema="public", tool_output_callback=callback
        )

        assert result.success
        # Verify callback was called
        assert callback.call_count >= 1

    @patch("chuck_data.commands.redshift_schema_selection.get_active_database")
    @patch("chuck_data.commands.redshift_schema_selection.get_config_manager")
    def test_case_insensitive_matching(self, mock_config, mock_get_database):
        """Matching is case insensitive."""
        mock_get_database.return_value = "dev"
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = ["Public"]
        mock_config.return_value.update.return_value = True

        result = handle_command(mock_client, schema="PUBLIC")

        assert result.success
        assert result.data["schema_name"] == "Public"

    @patch("chuck_data.commands.redshift_schema_selection.get_active_database")
    @patch("chuck_data.commands.redshift_schema_selection.get_config_manager")
    def test_whitespace_trimmed(self, mock_config, mock_get_database):
        """Whitespace is trimmed from input."""
        mock_get_database.return_value = "dev"
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = ["public"]
        mock_config.return_value.update.return_value = True

        result = handle_command(mock_client, schema="  public  ")

        assert result.success
        assert result.data["schema_name"] == "public"

    @patch("chuck_data.commands.redshift_schema_selection.get_active_database")
    @patch("chuck_data.commands.redshift_schema_selection.get_config_manager")
    def test_result_includes_step_data(self, mock_config, mock_get_database):
        """Result data includes step information."""
        mock_get_database.return_value = "dev"
        mock_client = MagicMock()
        mock_client.list_schemas.return_value = ["public"]
        mock_config.return_value.update.return_value = True

        result = handle_command(mock_client, schema="public")

        assert result.success
        assert "step" in result.data
        assert "Schema set" in result.data["step"]
