"""
Integration tests for provider-aware help command.

These tests verify that the help command properly integrates with
the config system and command registry to filter commands by provider.
"""

from unittest.mock import patch

from chuck_data.commands.help import handle_command


class TestHelpIntegration:
    """Integration tests for help command with provider filtering."""

    @patch("chuck_data.config._config_manager")
    def test_help_command_calls_get_data_provider(self, mock_config_manager):
        """Test that help command retrieves the current data provider."""
        # Mock config to return aws_redshift as provider
        mock_config = type("MockConfig", (), {})()
        mock_config.data_provider = "aws_redshift"
        mock_config_manager.get_config.return_value = mock_config

        # Execute help command
        result = handle_command(client=None)

        # Verify success - actual content verification is tested elsewhere
        assert result.success is True
        assert "help_text" in result.data

        # Verify config was accessed
        assert mock_config_manager.get_config.called

    @patch("chuck_data.config._config_manager")
    def test_help_works_with_no_provider_set(self, mock_config_manager):
        """Test that help command works when no provider is configured."""
        # Mock config with no provider
        mock_config = type("MockConfig", (), {})()
        mock_config.data_provider = None
        mock_config_manager.get_config.return_value = mock_config

        # Execute help command - should not error
        result = handle_command(client=None)

        # Verify success
        assert result.success is True
        assert "help_text" in result.data
