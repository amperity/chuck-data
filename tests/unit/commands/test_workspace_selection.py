"""
Tests for workspace_selection command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
Tests cover direct command execution (agent visibility is disabled for this command).
"""

import tempfile
from unittest.mock import patch

from chuck_data.commands.workspace_selection import handle_command, DEFINITION
from chuck_data.config import ConfigManager, get_workspace_url


class TestWorkspaceSelectionParameterValidation:
    """Test parameter validation for workspace_selection command."""

    def test_missing_workspace_url_parameter_returns_error(self):
        """Missing workspace_url parameter returns error."""
        result = handle_command(None)

        assert not result.success
        assert "workspace_url parameter is required" in result.message

    def test_empty_workspace_url_parameter_returns_error(self):
        """Empty workspace_url parameter returns error."""
        result = handle_command(None, workspace_url="")

        assert not result.success
        assert "workspace_url parameter is required" in result.message

    def test_none_workspace_url_parameter_returns_error(self):
        """None workspace_url parameter returns error."""
        result = handle_command(None, workspace_url=None)

        assert not result.success
        assert "workspace_url parameter is required" in result.message


class TestDirectWorkspaceSelectionCommand:
    """Test direct workspace selection command execution."""

    def test_direct_command_sets_valid_workspace_id(self):
        """Direct command successfully sets valid workspace ID."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                # Use a simple workspace ID that will pass validation
                test_workspace_id = "workspace123"

                # Execute command
                result = handle_command(None, workspace_url=test_workspace_id)

                # Verify success and message format
                assert result.success
                assert "Workspace URL is now set to" in result.message
                assert "Restart may be needed" in result.message

                # Verify result data structure
                assert result.data["workspace_url"] == test_workspace_id
                assert result.data["requires_restart"] is True
                assert "display_url" in result.data
                assert "cloud_provider" in result.data

                # Verify configuration state change
                saved_url = get_workspace_url()
                assert saved_url == test_workspace_id

    def test_direct_command_sets_full_workspace_url(self):
        """Direct command successfully sets full workspace URL."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                # Use a full workspace URL
                test_workspace_url = "https://my-workspace.cloud.databricks.com"

                # Execute command
                result = handle_command(None, workspace_url=test_workspace_url)

                # Verify success
                assert result.success
                assert "Workspace URL is now set to" in result.message
                assert result.data["workspace_url"] == test_workspace_url

                # Verify configuration state change
                saved_url = get_workspace_url()
                assert saved_url == test_workspace_url

    def test_direct_command_handles_aws_workspace_url(self):
        """Direct command handles AWS Databricks workspace URL."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                # AWS workspace URL
                aws_url = "https://dbc-12345678-9abc.cloud.databricks.com"

                # Execute command
                result = handle_command(None, workspace_url=aws_url)

                # Verify success and cloud provider detection
                assert result.success
                assert result.data["cloud_provider"] == "AWS"
                assert result.data["workspace_url"] == aws_url

    def test_direct_command_handles_azure_workspace_url(self):
        """Direct command handles Azure Databricks workspace URL."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                # Azure workspace URL
                azure_url = "https://adb-12345678.9.azuredatabricks.net"

                # Execute command
                result = handle_command(None, workspace_url=azure_url)

                # Verify success and cloud provider detection
                assert result.success
                assert result.data["cloud_provider"] == "Azure"
                assert result.data["workspace_url"] == azure_url

    def test_direct_command_handles_gcp_workspace_url(self):
        """Direct command handles GCP Databricks workspace URL."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                # GCP workspace URL
                gcp_url = "https://dbc-12345678-9abc.gcp.databricks.com"

                # Execute command
                result = handle_command(None, workspace_url=gcp_url)

                # Verify success and cloud provider detection
                assert result.success
                assert result.data["cloud_provider"] == "GCP"
                assert result.data["workspace_url"] == gcp_url

    def test_direct_command_workspace_url_with_spaces_returns_error(self):
        """Direct command with workspace URL containing spaces returns error."""
        result = handle_command(None, workspace_url="workspace with spaces")

        assert not result.success
        assert "Error:" in result.message
        assert "cannot contain spaces" in result.message

    def test_direct_command_very_long_workspace_url_returns_error(self):
        """Direct command with very long workspace URL returns error."""
        # Create a URL that exceeds the 200 character limit
        very_long_url = "a" * 250

        result = handle_command(None, workspace_url=very_long_url)

        assert not result.success
        assert "Error:" in result.message
        assert "200 characters" in result.message

    def test_direct_command_handles_processing_exceptions_gracefully(self):
        """Direct command handles processing exceptions gracefully."""
        # This test uses a mock to simulate an exception during URL processing
        with patch(
            "chuck_data.databricks.url_utils.validate_workspace_url"
        ) as mock_validate:
            mock_validate.side_effect = Exception("URL processing failed")

            result = handle_command(None, workspace_url="test-workspace")

            # Verify graceful error handling
            assert not result.success
            assert "URL processing failed" in result.message


class TestWorkspaceSelectionCommandConfiguration:
    """Test workspace selection command configuration and registry integration."""

    def test_command_definition_structure(self):
        """Command definition has correct structure."""
        assert DEFINITION.name == "select-workspace"
        assert "Set the Databricks workspace URL" in DEFINITION.description
        assert DEFINITION.handler == handle_command
        assert "workspace_url" in DEFINITION.parameters
        assert DEFINITION.required_params == ["workspace_url"]
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is False  # Agents don't select workspaces

    def test_command_parameter_specification(self):
        """Command parameters are correctly specified."""
        workspace_param = DEFINITION.parameters["workspace_url"]
        assert workspace_param["type"] == "string"
        assert "URL of the Databricks workspace" in workspace_param["description"]
        assert "my-workspace.cloud.databricks.com" in workspace_param["description"]

    def test_command_display_configuration(self):
        """Command display configuration is properly set."""
        assert DEFINITION.tui_aliases == ["/select-workspace"]
        # This command is not visible to agents but has TUI aliases
        assert DEFINITION.visible_to_agent is False


class TestWorkspaceSelectionEdgeCases:
    """Test edge cases and boundary conditions for workspace selection."""

    def test_unicode_workspace_url_handled_correctly(self):
        """Unicode characters in workspace URLs are handled correctly."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                # Workspace URL with unicode characters (should be valid since it's under 200 chars and no spaces)
                unicode_url = "测试workspace123"

                # Execute command
                result = handle_command(None, workspace_url=unicode_url)

                # Verify unicode handling
                assert result.success
                assert result.data["workspace_url"] == unicode_url

    def test_workspace_url_with_special_characters(self):
        """Workspace URLs with special characters are handled correctly."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                # Workspace URL with special characters (no spaces, under limit)
                special_url = "workspace-123_test.example"

                # Execute command
                result = handle_command(None, workspace_url=special_url)

                # Verify special character handling
                assert result.success
                assert result.data["workspace_url"] == special_url

    def test_workspace_url_exactly_200_characters_accepted(self):
        """Workspace URL exactly at 200 character limit is accepted."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                # Create exactly 200 character URL
                url_200_chars = "a" * 200

                # Execute command
                result = handle_command(None, workspace_url=url_200_chars)

                # Verify 200 character limit acceptance
                assert result.success
                assert result.data["workspace_url"] == url_200_chars

    def test_workspace_url_over_200_characters_rejected(self):
        """Workspace URL over 200 characters is rejected."""
        # Create 201 character URL
        url_201_chars = "a" * 201

        # Execute command
        result = handle_command(None, workspace_url=url_201_chars)

        # Verify rejection
        assert not result.success
        assert "200 characters" in result.message

    def test_workspace_url_with_leading_trailing_whitespace_handled(self):
        """Workspace URL with leading/trailing whitespace is handled correctly."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                # URL with whitespace that gets stripped during validation
                url_with_whitespace = "  workspace123  "

                # Execute command
                result = handle_command(None, workspace_url=url_with_whitespace)

                # Should succeed since validation strips whitespace
                assert result.success
                assert result.data["workspace_url"] == url_with_whitespace

    def test_minimum_length_workspace_url_accepted(self):
        """Minimum length workspace URL (1 character) is accepted."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                # Single character URL
                min_url = "a"

                # Execute command
                result = handle_command(None, workspace_url=min_url)

                # Verify minimum length acceptance
                assert result.success
                assert result.data["workspace_url"] == min_url

    def test_workspace_url_case_preservation(self):
        """Workspace URL case is preserved correctly."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                # Mixed case URL
                mixed_case_url = "MyWorkSpace123"

                # Execute command
                result = handle_command(None, workspace_url=mixed_case_url)

                # Verify case preservation
                assert result.success
                assert result.data["workspace_url"] == mixed_case_url

                # Verify configuration preserves case
                saved_url = get_workspace_url()
                assert saved_url == mixed_case_url

    def test_display_url_formatting_for_different_providers(self):
        """Display URL formatting works correctly for different cloud providers."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                # Test with simple workspace ID (should default to AWS)
                workspace_id = "workspace123"

                # Execute command
                result = handle_command(None, workspace_url=workspace_id)

                # Verify display URL formatting
                assert result.success
                assert result.data["cloud_provider"] == "AWS"
                assert "cloud.databricks.com" in result.data["display_url"]
                assert workspace_id in result.data["display_url"]

    def test_restart_required_flag_always_set(self):
        """Restart required flag is always set to True."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with patch("chuck_data.config._config_manager", config_manager):
                # Execute command
                result = handle_command(None, workspace_url="test-workspace")

                # Verify restart flag
                assert result.success
                assert result.data["requires_restart"] is True
                assert "Restart may be needed" in result.message
