"""Unit tests for the Chuck TUI."""

import pytest
import io
from unittest.mock import patch, MagicMock


@patch("chuck_data.__main__.ChuckTUI")
@patch("chuck_data.__main__.setup_logging")
def test_main_runs_tui(mock_setup_logging, mock_chuck_tui):
    """Test that the main function calls ChuckTUI.run()."""
    mock_instance = MagicMock()
    mock_chuck_tui.return_value = mock_instance

    from chuck_data.__main__ import main

    main([])

    mock_chuck_tui.assert_called_once_with(no_color=False)
    mock_instance.run.assert_called_once()


def test_version_flag():
    """Running with --version should exit after printing version."""
    from chuck_data.__main__ import main
    from chuck_data.version import __version__

    with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
        with pytest.raises(SystemExit) as excinfo:
            main(["--version"])
        assert excinfo.value.code == 0
    assert f"chuck-data {__version__}" in mock_stdout.getvalue()
