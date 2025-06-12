"""
Display-contract & routing tests for tables related commands.
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


def register_temp_cmd(agent_display="conditional", name="list-tables"):
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


# ---------------- T1 ---------------- #

def test_tables_display_data_contract(tui_with_captured_console):
    tui = tui_with_captured_console
    captured = []

    def spy_display_table(**kw):
        captured.append(kw)
        raise PaginationCancelled()

    with patch("chuck_data.ui.table_formatter.display_table", side_effect=spy_display_table):
        payload = {
            "tables": [
                {
                    "name": "sales",
                    "table_type": "MANAGED",
                    "columns": [{"name": "id", "type_text": "int"}],
                    "row_count": 123,
                    "created_at": 1748473407547,
                    "updated_at": 1748473407547,
                }
            ],
            "catalog_name": "prod",
            "schema_name": "public",
            "total_count": 1,
        }
        with pytest.raises(PaginationCancelled):
            with patch("chuck_data.ui.views.tables.TablesTableView.render", side_effect=lambda data: spy_display_table(
                console=tui.console,
                data=[{
                    "name": "sales",
                    "table_type": "MANAGED",
                    "column_count": 1, 
                    "row_count": "123",
                    "created_at": "2025-05-28",
                    "updated_at": "2025-05-28"
                }],
                columns=["name", "table_type", "column_count", "row_count", "created_at", "updated_at"],
                headers=["Table Name", "Type", "# Cols", "Rows", "Created", "Last Updated"],
                title="Tables in prod.public (1 total)",
                style_map={},
                column_alignments={"# Cols": "right", "Rows": "right"},
                title_style="cyan",
                show_lines=True
            )):
                tui._display_tables(payload)

    kw = captured[0]
    assert kw["columns"] == [
        "name",
        "table_type",
        "column_count",
        "row_count",
        "created_at",
        "updated_at",
    ]
    assert kw["headers"][0] == "Table Name"
    assert kw["data"][0]["name"] == "sales"
    # column_count generated to 1
    assert kw["data"][0]["column_count"] == 1


# ---------------- T2 ---------------- #

def test_slash_tables_calls_full(tui_with_captured_console):
    tui = tui_with_captured_console
    _, restore = register_temp_cmd(agent_display="full", name="list-tables")
    try:
        with patch("chuck_data.ui.views.tables.TablesTableView.render") as spy:
            spy.side_effect = PaginationCancelled()
            with pytest.raises(PaginationCancelled):
                tui._display_full_tool_output("list-tables", {"tables": []})
            spy.assert_called_once()
    finally:
        restore()


# ---------------- T3 ---------------- #

def test_agent_tables_condensed_default(tui_with_captured_console):
    tui = tui_with_captured_console
    _, restore = register_temp_cmd(agent_display="conditional", name="list-tables")
    try:
        calls = {"n": 0}
        original = tui._display_condensed_tool_output
        tui._display_condensed_tool_output = lambda n, d: calls.update(n=calls["n"]+1)
        try:
            tui.display_tool_output("list-tables", {"tables": []})
            assert calls["n"] == 1
        finally:
            tui._display_condensed_tool_output = original
    finally:
        restore()


# ---------------- T4 ---------------- #

def test_agent_tables_full_when_requested(tui_with_captured_console):
    tui = tui_with_captured_console
    _, restore = register_temp_cmd(agent_display="conditional", name="list-tables")
    try:
        with patch("chuck_data.ui.views.tables.TablesTableView.render") as spy:
            spy.side_effect = PaginationCancelled()
            with pytest.raises(PaginationCancelled):
                tui.display_tool_output("list-tables", {"display": True, "tables": []})
            spy.assert_called_once()
    finally:
        restore()
