"""
Display-contract & routing tests for volumes related commands.
"""

import pytest
from unittest.mock import MagicMock, patch
from rich.console import Console
from chuck_data.ui.tui import ChuckTUI
from chuck_data.exceptions import PaginationCancelled
from chuck_data.command_registry import CommandDefinition, TUI_COMMAND_MAP


@pytest.fixture
def tui_with_captured_console():
    with patch("chuck_data.ui.tui.ChuckService"):
        tui = ChuckTUI()
        tui.console = MagicMock(spec=Console)
        return tui


def register_temp_cmd(agent_display="conditional", name="list-volumes"):
    def dummy(**kwargs):
        return {"success": True}

    tmp_def = CommandDefinition(
        name=name,
        handler=dummy,
        description="temp stub",
        agent_display=agent_display,
        display_condition=lambda d: d.get("display", False),
    )
    from chuck_data.command_registry import COMMAND_REGISTRY

    orig_cmd = COMMAND_REGISTRY.get(name)
    COMMAND_REGISTRY[name] = tmp_def

    orig_tui = TUI_COMMAND_MAP.get(name)
    TUI_COMMAND_MAP[name] = name

    def restore():
        if orig_cmd is None:
            COMMAND_REGISTRY.pop(name, None)
        else:
            COMMAND_REGISTRY[name] = orig_cmd
        if orig_tui is None:
            TUI_COMMAND_MAP.pop(name, None)
        else:
            TUI_COMMAND_MAP[name] = orig_tui

    return tmp_def, restore


# ---------- T1 ---------- #

def test_volumes_display_data_contract(tui_with_captured_console):
    tui = tui_with_captured_console
    captured = []

    def spy_display_table(**kw):
        captured.append(kw)
        raise PaginationCancelled()

    with patch("chuck_data.ui.table_formatter.display_table", side_effect=spy_display_table):
        payload = {
            "volumes": [
                {"name": "bronze_vol", "volume_type": "MANAGED", "comment": "bronze data"}
            ],
            "catalog_name": "prod",
            "schema_name": "bronze",
        }
        with pytest.raises(PaginationCancelled):
            tui._display_volumes(payload)

    kw = captured[0]
    assert kw["columns"] == ["name", "type", "comment"]
    assert kw["headers"] == ["Name", "Type", "Comment"]
    assert "Volumes" in kw["title"]

    row = kw["data"][0]
    assert row["type"] == "MANAGED".upper()  # display uses upper()


# ---------- T2 ---------- #

def test_slash_volumes_calls_full(tui_with_captured_console):
    tui = tui_with_captured_console
    _, restore = register_temp_cmd(agent_display="full", name="list-volumes")
    try:
        with patch.object(tui, "_display_volumes") as spy:
            spy.side_effect = PaginationCancelled()
            with pytest.raises(PaginationCancelled):
                tui._display_full_tool_output("list-volumes", {"volumes": []})
            spy.assert_called_once()
    finally:
        restore()


# ---------- T3 ---------- #

def test_agent_volumes_condensed_default(tui_with_captured_console):
    tui = tui_with_captured_console
    _, restore = register_temp_cmd(agent_display="conditional", name="list-volumes")
    try:
        calls = {"n": 0}
        original = tui._display_condensed_tool_output
        tui._display_condensed_tool_output = lambda n, d: calls.update(n=calls["n"] + 1)
        try:
            tui.display_tool_output("list-volumes", {"volumes": []})
            assert calls["n"] == 1
        finally:
            tui._display_condensed_tool_output = original
    finally:
        restore()


# ---------- T4 ---------- #

def test_agent_volumes_full_when_requested(tui_with_captured_console):
    tui = tui_with_captured_console
    _, restore = register_temp_cmd(agent_display="conditional", name="list-volumes")
    try:
        with patch.object(tui, "_display_volumes") as spy:
            spy.side_effect = PaginationCancelled()
            with pytest.raises(PaginationCancelled):
                tui.display_tool_output("list-volumes", {"display": True, "volumes": []})
            spy.assert_called_once()
    finally:
        restore()
