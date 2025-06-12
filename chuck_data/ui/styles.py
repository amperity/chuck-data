"""Common Rich style helper functions reused by multiple views."""

from __future__ import annotations

__all__ = [
    "table_type_style",
    "warehouse_state_style",
]


def table_type_style(type_val: str | None):
    if not type_val:
        return None
    return "bright_blue" if type_val.lower() == "view" else None


def warehouse_state_style(state: str | None):
    if not state:
        return "dim"
    state = state.lower()
    if state == "running":
        return "green"
    if state == "stopped":
        return "red"
    if state in {"starting", "stopping", "deleting", "resizing"}:
        return "yellow"
    return "dim"
