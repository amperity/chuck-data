"""Tests for the generic view registry helper."""

from unittest.mock import MagicMock

import pytest

from rich.console import Console

from chuck_data.ui.view_base import BaseView
from chuck_data.ui import view_registry


class _DummyView(BaseView):
    def render(self, data):
        pass


def test_register_and_get_view():
    view_registry.register_view("foo-cmd", _DummyView)
    assert view_registry.get_view("foo-cmd") is _DummyView


def test_overwrite_view_registration(caplog):
    class _NewView(BaseView):
        def render(self, data):
            pass

    view_registry.register_view("foo-cmd", _DummyView)
    view_registry.register_view("foo-cmd", _NewView)
    assert view_registry.get_view("foo-cmd") is _NewView
