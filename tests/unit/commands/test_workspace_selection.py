"""
Tests for workspace_selection command handler.

This module contains tests for the workspace selection command handler.
"""

from unittest.mock import patch

from chuck_data.commands.workspace_selection import handle_command


def test_missing_workspace_url():
    """Test handling when workspace_url is not provided."""
    result = handle_command(None)
    assert not result.success
    assert "workspace_url parameter is required" in result.message


def test_invalid_workspace_url():
    """Test handling when workspace_url is invalid."""
    # Test with real validation function using truly invalid input
    # Use input that will fail basic validation
    invalid_url = "workspace with spaces"  # Spaces are not allowed

    # Call function with really invalid URL
    result = handle_command(None, workspace_url=invalid_url)

    # Verify results
    assert not result.success
    assert "Error:" in result.message


def test_successful_workspace_selection():
    """Test successful workspace selection."""
    import tempfile
    from chuck_data.config import ConfigManager, get_workspace_url

    # Use real config system and real URL utilities
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            # Use a known valid workspace ID that will pass validation
            test_workspace_url = "workspace123"  # Simple workspace ID

            # Call function
            result = handle_command(None, workspace_url=test_workspace_url)

            # Verify results
            assert result.success
            assert "Workspace URL is now set" in result.message
            assert "Restart may be needed" in result.message
            assert "workspace_url" in result.data
            # The exact format depends on real utility functions
            assert result.data["requires_restart"]

            # Verify config was actually updated
            saved_url = get_workspace_url()
            assert saved_url is not None


def test_workspace_url_exception():
    """Test handling when an error occurs during processing."""
    # Use an input that causes real validation to have issues
    # Test with overly long input that might cause processing issues
    very_long_url = "a" * 500  # Exceeds reasonable URL length

    # Call function
    result = handle_command(None, workspace_url=very_long_url)

    # Verify results - should handle gracefully
    assert not result.success
    # Error handling will depend on real validation behavior
