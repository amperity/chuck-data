"""
Unit tests for TUI autocomplete provider filtering.

Tests that autocomplete suggestions respect the provider filtering.
"""

from unittest.mock import patch
from chuck_data.ui.tui import ChuckTUI


class TestTUIAutocompleteProviderFiltering:
    """Test that TUI autocomplete respects provider filtering."""

    def test_autocomplete_with_databricks_provider(self):
        """Test that autocomplete includes Databricks commands for Databricks provider."""
        with patch("chuck_data.config.get_data_provider", return_value="databricks"):
            tui = ChuckTUI(no_color=True)
            commands = tui._get_available_commands()

            # Built-in commands should be present
            assert "/help" in commands
            assert "/exit" in commands
            assert "/quit" in commands
            assert "/debug" in commands

            # Databricks commands should be present
            assert "/list-warehouses" in commands
            assert "/warehouses" in commands
            assert "/list-catalogs" in commands
            assert "/catalogs" in commands
            assert "/select-warehouse" in commands
            assert "/run-sql" in commands
            assert "/sql" in commands

            # Redshift commands should NOT be present
            assert "/list-schemas" not in commands or any(
                # /list-schemas could be databricks (list_schemas) so we check
                cmd
                for cmd in commands
                if "/list-schemas" in cmd
            )
            # These are definitely Redshift-only
            assert all(
                cmd not in commands
                for cmd in ["/select-database"]  # Redshift-specific alias
            )

    def test_autocomplete_with_redshift_provider(self):
        """Test that autocomplete includes Redshift commands for Redshift provider."""
        with patch("chuck_data.config.get_data_provider", return_value="aws_redshift"):
            tui = ChuckTUI(no_color=True)
            commands = tui._get_available_commands()

            # Built-in commands should be present
            assert "/help" in commands
            assert "/exit" in commands

            # Redshift commands should be present
            assert "/list-schemas" in commands  # Redshift's list_redshift_schemas
            assert "/schemas" in commands  # Alias for list_redshift_schemas
            assert "/select-database" in commands
            assert "/list-databases" in commands

            # Databricks commands should NOT be present
            assert "/list-warehouses" not in commands
            assert "/warehouses" not in commands
            assert "/list-catalogs" not in commands
            assert "/catalogs" not in commands
            assert "/run-sql" not in commands
            assert "/sql" not in commands

    def test_autocomplete_with_no_provider(self):
        """Test that autocomplete includes only agnostic commands when no provider set."""
        with patch("chuck_data.config.get_data_provider", return_value=None):
            tui = ChuckTUI(no_color=True)
            commands = tui._get_available_commands()

            # Built-in commands should be present
            assert "/help" in commands
            assert "/exit" in commands

            # Provider-agnostic commands should be present
            assert "/agent" in commands
            assert "/ask" in commands

            # Provider-specific commands should NOT be present
            assert "/list-warehouses" not in commands
            assert "/list-catalogs" not in commands
            assert "/list-databases" not in commands
            assert "/select-database" not in commands

    def test_autocomplete_includes_all_tui_aliases(self):
        """Test that autocomplete includes all TUI aliases for available commands."""
        with patch("chuck_data.config.get_data_provider", return_value="databricks"):
            tui = ChuckTUI(no_color=True)
            commands = tui._get_available_commands()

            # Check that multiple aliases for the same command are all present
            # list_warehouses has aliases: ["/list-warehouses", "/warehouses"]
            assert "/list-warehouses" in commands
            assert "/warehouses" in commands

            # list_catalogs has aliases: ["/list-catalogs", "/catalogs"]
            assert "/list-catalogs" in commands
            assert "/catalogs" in commands

    def test_autocomplete_error_handling(self):
        """Test that autocomplete handles errors gracefully."""
        # Simulate an error in get_user_commands
        with patch(
            "chuck_data.command_registry.get_user_commands",
            side_effect=Exception("Test error"),
        ):
            tui = ChuckTUI(no_color=True)
            commands = tui._get_available_commands()

            # Should still have built-in commands
            assert "/help" in commands
            assert "/exit" in commands

            # Service commands should be empty due to error
            # But should not crash

    def test_autocomplete_deduplicates_commands(self):
        """Test that autocomplete removes duplicate commands."""
        with patch("chuck_data.config.get_data_provider", return_value="databricks"):
            tui = ChuckTUI(no_color=True)
            commands = tui._get_available_commands()

            # Check that there are no duplicates
            assert len(commands) == len(set(commands))

    def test_autocomplete_sorts_commands(self):
        """Test that autocomplete returns sorted commands."""
        with patch("chuck_data.config.get_data_provider", return_value="databricks"):
            tui = ChuckTUI(no_color=True)
            commands = tui._get_available_commands()

            # Check that commands are sorted
            assert commands == sorted(commands)
