# Playbook: Adding TUI-Display Tests for Any New Chuck Command
Audience  : Junior developers
Goal  : Guarantee that **both user (/slash) and agent** paths keep rendering exactly what Product & Design expect.

---

## 0. Prep: What you need to know before coding
| Item | Where to look |
|------|---------------|
| Command name & Python file | `chuck_data/commands/<name>.py` |
| Display method in TUI | `_display_<object>()` inside `chuck_data/ui/tui.py` |
| CommandDefinition entry | `DEFINITION = CommandDefinition(... agent_display=...)` |
| Tool result payload shape | The command’s `handler()` docstring / tests / manual run |

---

## 1. Create a new test module

File path:
```
tests/unit/ui/test_tui_<object>_display.py
```
Use snake-case for `<object>` (e.g. `warehouses`, `volumes`).

```python
"""
Display-contract & routing tests for <object> related commands.
"""

import pytest
from unittest.mock import MagicMock, patch
from rich.console import Console
from chuck_data.ui.tui import ChuckTUI
from chuck_data.exceptions import PaginationCancelled
from chuck_data.command_registry import CommandDefinition, TUI_COMMAND_MAP
```

---

## 2. Shared fixtures (copy verbatim)

```python
@pytest.fixture
def tui_with_captured_console():
    with patch("chuck_data.ui.tui.ChuckService"):
        tui = ChuckTUI()
        tui.console = MagicMock(spec=Console)
        return tui
```

---

## 3. Helper to register a temporary CommandDefinition

```python
def register_temp_cmd(agent_display="conditional", name="<tool-name>"):
    def dummy(**kwargs): return {"success": True}

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
```

Replace `<tool-name>` with the real tool (e.g. `"list-warehouses"`).

---

## 4. Test #1 – Data-contract (full display method)

1. Decide which key list the method expects (`"warehouses"`, `"volumes"`, …).
2. Craft a minimal but realistic payload.
3. Patch `display_table`, capture kwargs, raise `PaginationCancelled`.
4. Assert title, columns, headers, data.

```python
def test_<object>_display_data_contract(tui_with_captured_console):
    tui = tui_with_captured_console
    captured = []

    def spy_display_table(**kw):
        captured.append(kw)
        raise PaginationCancelled()

    with patch("chuck_data.ui.table_formatter.display_table", side_effect=spy_display_table):
        payload = {
            "<list-key>": [ { ... sample row ... } ],
            "current_<object>": "<active-name>"        # only if that field exists
        }
        with pytest.raises(PaginationCancelled):
            tui._display_<object>(payload)

    kw = captured[0]
    assert kw["columns"] == [...]
    assert kw["headers"] == [...]
    assert kw["title"].startswith("Available")        # or other rule
    assert kw["data"] == payload["<list-key>"]
```

---

## 5. Test #2 – Slash command triggers full display

```python
def test_slash_<object>_calls_full(tui_with_captured_console):
    tui = tui_with_captured_console
    _, restore = register_temp_cmd(agent_display="full", name="<tool-name>")
    try:
        with patch.object(tui, "_display_<object>") as spy:
            spy.side_effect = PaginationCancelled()
            with pytest.raises(PaginationCancelled):
                tui._display_full_tool_output("<tool-name>", {"<list-key>": []})
            spy.assert_called_once()
    finally:
        restore()
```

---

## 6. Test #3 – Agent condensed by default

```python
def test_agent_<object>_condensed_default(tui_with_captured_console):
    tui = tui_with_captured_console
    _, restore = register_temp_cmd(agent_display="conditional", name="<tool-name>")
    try:
        calls = {"n": 0}
        original = tui._display_condensed_tool_output
        tui._display_condensed_tool_output = lambda n, d: calls.update(n=calls["n"]+1)
        try:
            tui.display_tool_output("<tool-name>", {"<list-key>": []})
            assert calls["n"] == 1
        finally:
            tui._display_condensed_tool_output = original
    finally:
        restore()
```

---

## 7. Test #4 – Agent full when `display=True`

```python
def test_agent_<object>_full_when_requested(tui_with_captured_console):
    tui = tui_with_captured_console
    _, restore = register_temp_cmd(agent_display="conditional", name="<tool-name>")
    try:
        with patch.object(tui, "_display_<object>") as spy:
            spy.side_effect = PaginationCancelled()
            with pytest.raises(PaginationCancelled):
                tui.display_tool_output("<tool-name>", {"display": True, "<list-key>": []})
            spy.assert_called_once()
    finally:
        restore()
```

---

## 8. (Optional) Test highlighting logic

If the display method uses a `style_map` for an “active” item, capture the map:

```python
def test_<object>_highlighting(tui_with_captured_console):
    tui = tui_with_captured_console
    bucket = {}
    def spy(**kw):
        bucket.update(kw["style_map"])
        raise PaginationCancelled()

    with patch("chuck_data.ui.table_formatter.display_table", side_effect=spy):
        with pytest.raises(PaginationCancelled):
            tui._display_<object>({
                "<list-key>": [{"name": "prod"}, {"name": "dev"}],
                "current_<object>": "prod"
            })
    style_fn = bucket["name"]
    assert style_fn("prod") == "bold green"
    assert style_fn("dev")  is None
```

---

## 9. Run only your new tests

```
pytest tests/unit/ui/test_tui_<object>_display.py
```

They should all pass.

---

## 10. Pull request checklist

- [ ] Data-contract test verifies columns / headers / title / raw data.
- [ ] Routing tests cover **direct** and **agent** flows (condensed & full).
- [ ] Optional highlighting test included if applicable.
- [ ] File and function names follow existing naming conventions.
- [ ] `pytest` green locally.
