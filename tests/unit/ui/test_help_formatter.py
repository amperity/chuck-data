"""
Unit tests for help formatter.

Tests that help text is properly formatted and includes all expected commands.
"""

from chuck_data.command_registry import get_user_commands, TUI_COMMAND_MAP
from chuck_data.ui.help_formatter import format_help_text


class TestHelpFormatter:
    """Test help text formatting."""

    def test_help_includes_job_management_commands(self):
        """Test that help text includes all job management commands."""
        commands = get_user_commands(provider="databricks")
        help_text = format_help_text(commands, TUI_COMMAND_MAP, provider="databricks")

        # Should include job management section
        assert "Job Management" in help_text

        # Should include all job-related commands
        assert "/jobs" in help_text
        assert "/job-status" in help_text
        assert "/monitor-job" in help_text

    def test_help_respects_provider_filtering(self):
        """Test that help text respects provider filtering."""
        # Databricks commands
        databricks_commands = get_user_commands(provider="databricks")
        databricks_help = format_help_text(
            databricks_commands, TUI_COMMAND_MAP, provider="databricks"
        )

        assert "/list-warehouses" in databricks_help or "/warehouses" in databricks_help
        assert "/list-catalogs" in databricks_help or "/catalogs" in databricks_help

        # Redshift commands
        redshift_commands = get_user_commands(provider="aws_redshift")
        redshift_help = format_help_text(
            redshift_commands, TUI_COMMAND_MAP, provider="aws_redshift"
        )

        # Redshift help should NOT include Databricks commands
        assert "/list-warehouses" not in redshift_help
        assert "/warehouses" not in redshift_help
        assert "/list-catalogs" not in redshift_help
        assert "/catalogs" not in redshift_help

    def test_help_includes_provider_agnostic_commands(self):
        """Test that help includes provider-agnostic commands for all providers."""
        # Check Databricks
        databricks_commands = get_user_commands(provider="databricks")
        databricks_help = format_help_text(
            databricks_commands, TUI_COMMAND_MAP, provider="databricks"
        )

        assert "/help" in databricks_help
        assert "/jobs" in databricks_help
        assert "/bug" in databricks_help

        # Check Redshift
        redshift_commands = get_user_commands(provider="aws_redshift")
        redshift_help = format_help_text(
            redshift_commands, TUI_COMMAND_MAP, provider="aws_redshift"
        )

        assert "/help" in redshift_help
        assert "/jobs" in redshift_help
        assert "/bug" in redshift_help

    def test_help_includes_all_categories(self):
        """Test that help includes all expected categories."""
        commands = get_user_commands(provider="databricks")
        help_text = format_help_text(commands, TUI_COMMAND_MAP, provider="databricks")

        # Check for major categories - note: category name is "Authentication & Setup", not "Workspace"
        assert "Authentication & Setup" in help_text
        assert "Model & Endpoint Management" in help_text
        assert "Job Management" in help_text
        assert "Utilities" in help_text

    def test_help_formats_commands_consistently(self):
        """Test that commands are formatted consistently."""
        commands = get_user_commands(provider="databricks")
        help_text = format_help_text(commands, TUI_COMMAND_MAP, provider="databricks")

        # Each command line should have the format: "/command <args> - description"
        lines = help_text.split("\n")
        command_lines = [
            line for line in lines if line.strip().startswith("/") and " - " in line
        ]

        # Should have multiple command lines
        assert len(command_lines) > 10

        # Each command line should follow the pattern
        for line in command_lines:
            assert " - " in line
            assert line.strip().startswith("/")
