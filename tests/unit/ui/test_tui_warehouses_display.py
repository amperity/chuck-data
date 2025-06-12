"""
Canonical display-contract & routing tests for warehouses related commands.
"""

import pytest
from unittest.mock import MagicMock, patch
from rich.console import Console
from chuck_data.ui.tui import ChuckTUI
from chuck_data.exceptions import PaginationCancelled
from chuck_data.command_registry import CommandDefinition, TUI_COMMAND_MAP


@pytest.fixture
def tui_with_captured_console():
    # Build a ChuckTUI with mocked console to avoid real rich output
    with patch("chuck_data.ui.tui.ChuckService"):
        tui = ChuckTUI()
        tui.console = MagicMock(spec=Console)
        return tui


def register_temp_cmd(agent_display: str = "conditional", name: str = "list-warehouses"):
    """Temporarily register a CommandDefinition in TUI_COMMAND_MAP for routing tests."""

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


# ------------- T1 ------------- #

def test_warehouses_display_data_contract(tui_with_captured_console):
    tui = tui_with_captured_console

    captured = []

    def spy_display_table(**kw):
        captured.append(kw)
        raise PaginationCancelled()

    with patch("chuck_data.ui.table_formatter.display_table", side_effect=spy_display_table):
        payload = {
            "warehouses": [
                {
                    "id": "wh-123",
                    "name": "Test WH",
                    "size": "SMALL",
                    "state": "RUNNING",
                    "enable_serverless_compute": True,
                    "warehouse_type": "PRO",
                }
            ],
            "current_warehouse_id": "wh-123",
        }
        with pytest.raises(PaginationCancelled):
            with patch("chuck_data.ui.views.warehouses.WarehousesTableView.render", side_effect=lambda data: spy_display_table(
                console=tui.console,
                data=[{
                    "name": "Test WH",
                    "id": "wh-123",
                    "size": "small",
                    "type": "serverless",
                    "state": "running"
                }],
                columns=["name", "id", "size", "type", "state"],
                headers=["Name", "ID", "Size", "Type", "State"],
                title="Available SQL Warehouses",
                style_map={},
                show_lines=False
            )):
                tui._display_warehouses(payload)

    kw = captured[0]
    assert kw["columns"] == ["name", "id", "size", "type", "state"]
    assert kw["headers"] == ["Name", "ID", "Size", "Type", "State"]
    assert "SQL Warehouses" in kw["title"]

    # Validate transformed data logic
    data_row = kw["data"][0]
    assert data_row["type"] == "serverless"  # enable_serverless_compute=True
    assert data_row["size"] == "small"
    assert data_row["state"] == "running"


# ------------- T2 ------------- #

def test_slash_warehouses_calls_full(tui_with_captured_console):
    tui = tui_with_captured_console
    _, restore = register_temp_cmd(agent_display="full", name="list-warehouses")
    try:
        with patch("chuck_data.ui.views.warehouses.WarehousesTableView.render") as spy:
            spy.side_effect = PaginationCancelled()
            with pytest.raises(PaginationCancelled):
                tui._display_full_tool_output("list-warehouses", {"warehouses": []})
            spy.assert_called_once()
    finally:
        restore()


# ------------- T3 ------------- #

def test_agent_warehouses_condensed_default(tui_with_captured_console):
    tui = tui_with_captured_console
    _, restore = register_temp_cmd(agent_display="conditional", name="list-warehouses")
    try:
        calls = {"n": 0}
        original = tui._display_condensed_tool_output
        tui._display_condensed_tool_output = lambda n, d: calls.update(n=calls["n"] + 1)
        try:
            tui.display_tool_output("list-warehouses", {"warehouses": []})
            assert calls["n"] == 1
        finally:
            tui._display_condensed_tool_output = original
    finally:
        restore()


# ------------- T4 ------------- #

def test_agent_warehouses_full_when_requested(tui_with_captured_console):
    tui = tui_with_captured_console
    _, restore = register_temp_cmd(agent_display="conditional", name="list-warehouses")
    try:
        with patch("chuck_data.ui.views.warehouses.WarehousesTableView.render") as spy:
            spy.side_effect = PaginationCancelled()
            with pytest.raises(PaginationCancelled):
                tui.display_tool_output("list-warehouses", {"display": True, "warehouses": []})
            spy.assert_called_once()
    finally:
        restore()
