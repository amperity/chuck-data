"""
Display-contract & routing tests for models related commands.
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


def register_temp_cmd(agent_display="conditional", name="list-models"):
    def dummy(**kwargs):
        return {"success": True}

    tmp_def = CommandDefinition(
        name=name,
        handler=dummy,
        description="temp test stub",
        agent_display=agent_display,
        display_condition=lambda d: d.get("display", False),
    )
    from chuck_data.command_registry import COMMAND_REGISTRY

    original = COMMAND_REGISTRY.get(name)
    COMMAND_REGISTRY[name] = tmp_def

    original_tui = TUI_COMMAND_MAP.get(name)
    TUI_COMMAND_MAP[name] = name  # map directly to itself for TUI paths

    def restore():
        if original is None:
            COMMAND_REGISTRY.pop(name, None)
        else:
            COMMAND_REGISTRY[name] = original
        if original_tui is None:
            TUI_COMMAND_MAP.pop(name, None)
        else:
            TUI_COMMAND_MAP[name] = original_tui

    return tmp_def, restore


# -------------- T1 -------------- #


def test_models_display_data_contract(tui_with_captured_console):
    tui = tui_with_captured_console
    captured = []

    def spy_display_table(**kw):
        captured.append(kw)
        raise PaginationCancelled()

    with patch(
        "chuck_data.ui.table_formatter.display_table", side_effect=spy_display_table
    ):
        payload = {
            "models": [
                {
                    "name": "databricks-foo-model",
                    "creator": "ml_team",
                    "state": {"ready": "READY"},
                }
            ],
            "active_model": "databricks-foo-model",
        }
        with pytest.raises(PaginationCancelled):
            with patch(
                "chuck_data.ui.views.models.ModelsTableView.render",
                side_effect=lambda data: spy_display_table(
                    console=tui.console,
                    data=[
                        {
                            "name": "databricks-foo-model",
                            "status": "READY",
                            "creator": "ml_team",
                        }
                    ],
                    columns=["name", "status", "creator"],
                    headers=["Model Name", "Status", "Creator"],
                    title="Available Models",
                    style_map={},
                    title_style="cyan",
                    show_lines=True,
                ),
            ):
                tui._display_models_consolidated(payload)

    kw = captured[0]
    assert kw["columns"] == ["name", "status", "creator"]
    assert kw["headers"] == ["Model Name", "Status", "Creator"]
    assert "Models" in kw["title"]
    assert kw["data"][0]["name"].startswith("databricks-foo-model")


# -------------- T2 -------------- #


def test_slash_models_calls_full(tui_with_captured_console):
    tui = tui_with_captured_console
    _, restore = register_temp_cmd(agent_display="full", name="list-models")
    try:
        with patch("chuck_data.ui.views.models.ModelsTableView.render") as spy:
            spy.side_effect = PaginationCancelled()
            with pytest.raises(PaginationCancelled):
                tui._display_full_tool_output("list-models", {"models": []})
            spy.assert_called_once()
    finally:
        restore()


# -------------- T3 -------------- #


def test_agent_models_condensed_default(tui_with_captured_console):
    tui = tui_with_captured_console
    _, restore = register_temp_cmd(agent_display="conditional", name="list-models")
    try:
        calls = {"n": 0}
        original = tui._display_condensed_tool_output
        tui._display_condensed_tool_output = lambda n, d: calls.update(n=calls["n"] + 1)
        try:
            tui.display_tool_output("list-models", {"models": []})
            assert calls["n"] == 1
        finally:
            tui._display_condensed_tool_output = original
    finally:
        restore()


# -------------- T4 -------------- #


def test_agent_models_full_when_requested(tui_with_captured_console):
    tui = tui_with_captured_console
    _, restore = register_temp_cmd(agent_display="conditional", name="list-models")
    try:
        with patch("chuck_data.ui.views.models.ModelsTableView.render") as spy:
            spy.side_effect = PaginationCancelled()
            with pytest.raises(PaginationCancelled):
                tui.display_tool_output("list-models", {"display": True, "models": []})
            spy.assert_called_once()
    finally:
        restore()


# -------------- T5 -------------- #


def test_models_highlighting(tui_with_captured_console):
    tui = tui_with_captured_console
    bucket = {}

    def spy(**kw):
        bucket.update(kw["style_map"])
        raise PaginationCancelled()

    with patch("chuck_data.ui.table_formatter.display_table", side_effect=spy):
        with pytest.raises(PaginationCancelled):
            with patch(
                "chuck_data.ui.views.models.ModelsTableView.render",
                side_effect=lambda data: spy(
                    console=tui.console,
                    data=[
                        {"name": "active", "status": "READY", "creator": "x"},
                        {"name": "other", "status": "READY", "creator": "y"},
                    ],
                    columns=["name", "status", "creator"],
                    headers=["Model Name", "Status", "Creator"],
                    title="Available Models",
                    style_map={
                        "name": lambda name: (
                            "bold green" if name == "active" else "cyan"
                        ),
                        "status": lambda status: "green" if status == "READY" else None,
                        "creator": lambda _: "blue",
                    },
                    show_lines=True,
                ),
            ):
                tui._display_models_consolidated(
                    {
                        "models": [
                            {
                                "name": "active",
                                "creator": "x",
                                "state": {"ready": "READY"},
                            },
                            {
                                "name": "other",
                                "creator": "y",
                                "state": {"ready": "READY"},
                            },
                        ],
                        "active_model": "active",
                    }
                )

    name_style_fn = bucket["name"]
    assert name_style_fn("active") == "bold green"
    assert name_style_fn("other") == "cyan"
