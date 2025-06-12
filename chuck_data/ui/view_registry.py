"""Global registry for mapping command/tool names to View classes."""

from __future__ import annotations

from typing import Dict, Type

from chuck_data.ui.view_base import BaseView

_VIEW_REGISTRY: Dict[str, Type[BaseView]] = {}


def register_view(command_name: str, view_cls: Type[BaseView]):
    """Register or overwrite the View class for a command/tool name."""
    # Overwrite silently; caller may warn if desired.
    _VIEW_REGISTRY[command_name] = view_cls


def get_view(command_name: str) -> Type[BaseView] | None:
    return _VIEW_REGISTRY.get(command_name)
