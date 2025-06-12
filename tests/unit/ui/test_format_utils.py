"""Unit tests for chuck_data.ui.format_utils helper functions."""

import pytest

import datetime

import importlib

# The module may not exist yet; import error is expected in red phase.
importlib.invalidate_caches()

import pytest

@pytest.mark.parametrize(
    "input_ts, expected",
    [
        (1748473407547, "2025-05-28"),  # ms int
        ("2025-05-28T14:03:00Z", "2025-05-28"),  # ISO string with T
        ("2020-01-01", "2020-01-01"),  # already short string
    ],
)
def test_format_timestamp(input_ts, expected):
    from chuck_data.ui import format_utils  # import inside test to allow creation later

    assert format_utils.format_timestamp(input_ts) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        (123, "123"),
        (1234, "1.2K"),
        (1_000_000, "1.0M"),
        (2_500_000_000, "2.5B"),
        ("-", "-"),
    ],
)
def test_humanize_row_count(value, expected):
    from chuck_data.ui import format_utils

    assert format_utils.humanize_row_count(value) == expected
