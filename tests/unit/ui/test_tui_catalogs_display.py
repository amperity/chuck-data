"""
Display-contract & routing tests for catalog related commands.
"""

import pytest
from unittest.mock import MagicMock, patch
from rich.console import Console
from chuck_data.ui.tui import ChuckTUI
from chuck_data.exceptions import PaginationCancelled
from chuck_data.command_registry import CommandDefinition, TUI_COMMAND_MAP


@pytest.fixture
def tui_with_captured_console():
    # Create a ChuckTUI with mocked console so we do not render to real stdout
    with patch("chuck_data.ui.tui.ChuckService"):
        tui = ChuckTUI()
        tui.console = MagicMock(spec=Console)
        return tui


def register_temp_cmd(agent_display="conditional", name="list-catalogs"):
    """Register a temporary CommandDefinition for the lifetime of the test."""

    def dummy(**kwargs):
        return {"success": True}

    tmp_def = CommandDefinition(
        name=name,
        handler=dummy,
        description="temp test stub",
        agent_display=agent_display,
    )
    original = TUI_COMMAND_MAP.get(name)
    TUI_COMMAND_MAP[name] = tmp_def

    def restore():
        if original is None:
            TUI_COMMAND_MAP.pop(name, None)
        else:
            TUI_COMMAND_MAP[name] = original

    return tmp_def, restore


# -------------------- T1 -------------------- #

def test_catalog_display_data_contract(tui_with_captured_console):
    tui = tui_with_captured_console
    captured = []

    def spy_display_table(**kw):
        captured.append(kw)
        raise PaginationCancelled()

    with patch("chuck_data.ui.table_formatter.display_table", side_effect=spy_display_table):
        payload = {
            "catalogs": [
                {
                    "name": "main",
                    "type": "UC",
                    "comment": "prod catalog",
                    "owner": "data_team",
                }
            ],
            "current_catalog": "main",
        }
        with pytest.raises(PaginationCancelled):
            with patch("chuck_data.ui.views.catalogs.CatalogsTableView.render", side_effect=lambda data: spy_display_table(
                console=tui.console,
                data=payload["catalogs"],
                columns=["name", "type", "comment", "owner"],
                headers=["Name", "Type", "Comment", "Owner"],
                title="Available Catalogs",
                style_map={},
                title_style="cyan",
                show_lines=False
            )):
                tui._display_catalogs(payload)

    kw = captured[0]
    assert kw["columns"] == ["name", "type", "comment", "owner"]
    assert kw["headers"] == ["Name", "Type", "Comment", "Owner"]
    # Title should contain word "Catalogs"
    assert "Catalogs" in kw["title"]
    assert kw["data"] == payload["catalogs"]


# -------------------- T2 -------------------- #

def test_slash_catalogs_calls_full(tui_with_captured_console):
    tui = tui_with_captured_console
    _, restore = register_temp_cmd(agent_display="full", name="list-catalogs")
    try:
        with patch("chuck_data.ui.views.catalogs.CatalogsTableView.render") as spy:
            spy.side_effect = PaginationCancelled()
            with pytest.raises(PaginationCancelled):
                tui._display_full_tool_output("list-catalogs", {"catalogs": []})
            spy.assert_called_once()
    finally:
        restore()


# -------------------- T3 -------------------- #

def test_agent_catalogs_condensed_default(tui_with_captured_console):
    tui = tui_with_captured_console
    _, restore = register_temp_cmd(agent_display="conditional", name="list-catalogs")
    try:
        calls = {"n": 0}
        original = tui._display_condensed_tool_output
        tui._display_condensed_tool_output = lambda n, d: calls.update(n=calls["n"]+1)
        try:
            tui.display_tool_output("list-catalogs", {"catalogs": []})
            assert calls["n"] == 1
        finally:
            tui._display_condensed_tool_output = original
    finally:
        restore()


# -------------------- T4 -------------------- #

def test_agent_catalogs_full_when_requested(tui_with_captured_console):
    tui = tui_with_captured_console
    _, restore = register_temp_cmd(agent_display="conditional", name="list-catalogs")
    try:
        with patch("chuck_data.ui.views.catalogs.CatalogsTableView.render") as spy:
            spy.side_effect = PaginationCancelled()
            with pytest.raises(PaginationCancelled):
                tui.display_tool_output("list-catalogs", {"display": True, "catalogs": []})
            spy.assert_called_once()
    finally:
        restore()
