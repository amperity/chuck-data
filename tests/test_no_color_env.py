"""Tests for the NO_COLOR environment variable."""

import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Add the project root to sys.path so we can import chuck
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import chuck


class TestNoColorEnvVar(unittest.TestCase):
    """Test cases for NO_COLOR environment variable functionality."""

    @patch("chuck.ChuckTUI")
    @patch("chuck.setup_logging")
    def test_default_color_mode(self, mock_setup_logging, mock_chuck_tui):
        """Test that default mode passes no_color=False to ChuckTUI constructor."""
        mock_tui_instance = MagicMock()
        mock_chuck_tui.return_value = mock_tui_instance

        # Call main function (without NO_COLOR env var)
        chuck.main([])

        # Verify ChuckTUI was called with no_color=False
        mock_chuck_tui.assert_called_once_with(no_color=False)
        # Verify run was called
        mock_tui_instance.run.assert_called_once()

    @patch("chuck.ChuckTUI")
    @patch("chuck.setup_logging")
    @patch.dict(os.environ, {"NO_COLOR": "1"})
    def test_no_color_env_var_1(self, mock_setup_logging, mock_chuck_tui):
        """Test that NO_COLOR=1 enables no-color mode."""
        mock_tui_instance = MagicMock()
        mock_chuck_tui.return_value = mock_tui_instance

        # Call main function
        chuck.main([])

        # Verify ChuckTUI was called with no_color=True due to env var
        mock_chuck_tui.assert_called_once_with(no_color=True)

    @patch("chuck.ChuckTUI")
    @patch("chuck.setup_logging")
    @patch.dict(os.environ, {"NO_COLOR": "true"})
    def test_no_color_env_var_true(self, mock_setup_logging, mock_chuck_tui):
        """Test that NO_COLOR=true enables no-color mode."""
        mock_tui_instance = MagicMock()
        mock_chuck_tui.return_value = mock_tui_instance

        # Call main function
        chuck.main([])

        # Verify ChuckTUI was called with no_color=True due to env var
        mock_chuck_tui.assert_called_once_with(no_color=True)

    @patch("chuck.ChuckTUI")
    @patch("chuck.setup_logging")
    def test_no_color_flag(self, mock_setup_logging, mock_chuck_tui):
        """The --no-color flag forces no_color=True."""
        mock_tui_instance = MagicMock()
        mock_chuck_tui.return_value = mock_tui_instance

        chuck.main(["--no-color"])

        mock_chuck_tui.assert_called_once_with(no_color=True)


if __name__ == "__main__":
    unittest.main()
