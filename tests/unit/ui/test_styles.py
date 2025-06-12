"""Unit tests for common style helper functions in chuck_data.ui.styles."""

import importlib
import pytest

importlib.invalidate_caches()


def test_warehouse_state_style():
    from chuck_data.ui import styles

    style_fn = styles.warehouse_state_style
    assert style_fn("running") == "green"
    assert style_fn("stopped") == "red"
    assert style_fn("starting") == "yellow"
    assert style_fn("deleting") == "yellow"
    assert style_fn("unknown") == "dim"


def test_table_type_style():
    from chuck_data.ui import styles

    tts = styles.table_type_style
    assert tts("VIEW") == "bright_blue"
    assert tts("view") == "bright_blue"
    assert tts("MANAGED") is None
