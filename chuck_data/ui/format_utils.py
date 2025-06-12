"""Reusable formatting helpers for TUI display code.

This module is *pure* (no Rich / prompt-toolkit imports) so it can be unit-
tested independently and reused both in application code and tests.
"""
from __future__ import annotations

from datetime import datetime
from typing import Union

__all__ = ["format_timestamp", "humanize_row_count"]


def format_timestamp(value: Union[int, str]) -> str:
    """Return YYYY-MM-DD string for various timestamp representations.

    Accepts:
    • Unix epoch *milliseconds*  (int >= 1e12)
    • ISO-8601 strings (e.g. "2025-05-28T14:03:00Z")
    • Already-formatted short strings (YYYY-MM-DD)
    If parsing fails, returns the original value unchanged.
    """

    # Already looks like YYYY-MM-DD → keep
    if isinstance(value, str) and len(value) == 10 and value[4] == "-":
        return value

    # Milliseconds epoch
    if isinstance(value, int):
        try:
            return datetime.fromtimestamp(value / 1000).strftime("%Y-%m-%d")
        except Exception:
            return str(value)

    if isinstance(value, str):
        try:
            # Split before "T" if present and length > 10
            if "T" in value and len(value) > 10:
                return value.split("T", 1)[0]
            # Otherwise attempt parse
            ts = datetime.fromisoformat(value.rstrip("Z"))
            return ts.strftime("%Y-%m-%d")
        except Exception:
            return value

    # Fallback
    return str(value)


def humanize_row_count(value):
    """Return a human-friendly K/M/B suffix for integers; passthrough otherwise."""
    if isinstance(value, str):
        # unknown / "-" etc.
        if not value.isdigit():
            return value
        try:
            value = int(value)
        except ValueError:
            return value

    if not isinstance(value, int):
        return str(value)

    n = value
    if n >= 1_000_000_000:
        return f"{n/1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)
