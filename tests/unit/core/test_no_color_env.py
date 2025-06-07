"""Tests for the NO_COLOR environment variable."""

import os
from unittest.mock import patch, MagicMock

import chuck_data.__main__ as chuck


@patch("chuck_data.__main__.ChuckTUI")
@patch("chuck_data.__main__.setup_logging")
def test_default_color_mode(mock_setup_logging, mock_chuck_tui):
    """Test that default mode passes no_color=False to ChuckTUI constructor."""
    mock_tui_instance = MagicMock()
    mock_chuck_tui.return_value = mock_tui_instance

    # Call main function (without NO_COLOR env var)
    chuck.main([])

    # Verify ChuckTUI was called with no_color=False
    mock_chuck_tui.assert_called_once_with(no_color=False)
    # Verify run was called
    mock_tui_instance.run.assert_called_once()


@patch("chuck_data.__main__.ChuckTUI")
@patch("chuck_data.__main__.setup_logging")
def test_no_color_env_var_1(mock_setup_logging, mock_chuck_tui, no_color_env):
    """Test that NO_COLOR=1 enables no-color mode."""
    mock_tui_instance = MagicMock()
    mock_chuck_tui.return_value = mock_tui_instance

    # Call main function (no_color_env fixture sets NO_COLOR=1)
    chuck.main([])

    # Verify ChuckTUI was called with no_color=True due to env var
    mock_chuck_tui.assert_called_once_with(no_color=True)


@patch("chuck_data.__main__.ChuckTUI")
@patch("chuck_data.__main__.setup_logging")
def test_no_color_env_var_true(mock_setup_logging, mock_chuck_tui, no_color_true_env):
    """Test that NO_COLOR=true enables no-color mode."""
    mock_tui_instance = MagicMock()
    mock_chuck_tui.return_value = mock_tui_instance

    # Call main function (no_color_true_env fixture sets NO_COLOR=true)
    chuck.main([])

    # Verify ChuckTUI was called with no_color=True due to env var
    mock_chuck_tui.assert_called_once_with(no_color=True)


@patch("chuck_data.__main__.ChuckTUI")
@patch("chuck_data.__main__.setup_logging")
def test_no_color_flag(mock_setup_logging, mock_chuck_tui):
    """The --no-color flag forces no_color=True."""
    mock_tui_instance = MagicMock()
    mock_chuck_tui.return_value = mock_tui_instance

    chuck.main(["--no-color"])

    mock_chuck_tui.assert_called_once_with(no_color=True)
