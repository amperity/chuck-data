"""Tests for CatalogsTableView implementation and shim delegation."""

from unittest.mock import patch, MagicMock

import pytest
from rich.console import Console
from chuck_data.ui.tui import ChuckTUI
from chuck_data.exceptions import PaginationCancelled


PAYLOAD = {
    "catalogs": [{"name": "main", "type": "UC", "comment": "prod", "owner": "data"}],
    "current_catalog": "main",
    "display": False,  # Explicitly set display=False to ensure PaginationCancelled is raised
}


def test_catalog_view_renders_table():
    from chuck_data.ui.views.catalogs import CatalogsTableView

    console = MagicMock(spec=Console)
    view = CatalogsTableView(console)

    # Need to patch at the module level where it's imported
    with patch("chuck_data.ui.table_formatter.display_table") as spy:
        # This will raise PaginationCancelled because display=False
        with pytest.raises(PaginationCancelled):
            view.render(PAYLOAD)

        # Verify display_table was called
        spy.assert_called_once()
        kwargs = spy.call_args.kwargs
        assert kwargs["title"].startswith("Available Catalogs")
        assert kwargs["data"] == PAYLOAD["catalogs"]


def test_tui_shim_calls_new_view():
    with patch("chuck_data.ui.views.catalogs.CatalogsTableView.render") as spy:
        spy.side_effect = PaginationCancelled()  # Make render raise PaginationCancelled
        with patch("chuck_data.ui.tui.ChuckService"):
            tui = ChuckTUI()
            with pytest.raises(PaginationCancelled):
                tui._display_catalogs(PAYLOAD)
        spy.assert_called_once()
