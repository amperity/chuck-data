"""Minimal base classes for new TUI view layer."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any
from rich.console import Console

__all__ = ["BaseView", "TableViewMixin"]


class BaseView(ABC):
    """Abstract base class for all views."""

    def __init__(self, console: Console):
        self.console = console

    @abstractmethod
    def render(self, data: dict[str, Any]):
        """Render data to the console and (optionally) raise PaginationCancelled."""
        raise NotImplementedError


class TableViewMixin:
    """Mixin that provides a convenience `render_table` method."""

    columns: list[str]
    headers: list[str]
    title: str

    def _render_table(self, data_rows, **extra):
        from chuck_data.ui.table_formatter import display_table

        display_table(
            console=self.console,
            data=data_rows,
            columns=self.columns,
            headers=self.headers,
            title=self.title,
            **extra,
        )
