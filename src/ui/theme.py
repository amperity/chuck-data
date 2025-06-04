"""
Theme constants for the Chuck TUI application.

This module provides standardized color and style constants to ensure
consistent visual presentation across the application.
"""

from typing import Dict

# Primary color scheme
CHUCK_LOGO = "#0bb9c9"
DIALOG_BORDER = "#ABB222"
MESSAGE_LIGHT = "#54d3de"
MESSAGE_STANDARD = "#0bb9c9"
INFO = "#75c163"
ERROR = "#FF504A"
SUCCESS = "#75c163"
TABLE_HEADER = "#54d3de"
TABLE_BORDER = "#00a0b2"

# Additional semantic colors
WARNING = "#75c163"
NEUTRAL = "white"
INACTIVE = "#C2CBD1"
HIGHLIGHT = "bold white"

# Style combinations
TITLE_STYLE = f"bold {TABLE_HEADER}"
HEADER_STYLE = "bold"
ERROR_STYLE = f"bold {ERROR}"
SUCCESS_STYLE = f"bold {SUCCESS}"
INFO_STYLE = f"bold {INFO}"
WARNING_STYLE = f"bold {WARNING}"

# Compound styles for specific UI elements
TABLE_TITLE_STYLE = TITLE_STYLE
TABLE_BORDER_STYLE = TABLE_BORDER

# Status styling map - for consistent status indicators across the application
STATUS_STYLES: Dict[str, str] = {
    "active": SUCCESS,
    "running": SUCCESS,
    "available": SUCCESS,
    "ready": SUCCESS,
    "stopped": NEUTRAL,
    "inactive": NEUTRAL,
    "paused": WARNING,
    "error": ERROR,
    "failed": ERROR,
    "terminated": ERROR,
    "unknown": WARNING,
}


def get_status_style(status: str) -> str:
    """
    Get the appropriate style for a status value.

    Args:
        status: The status string to style

    Returns:
        A Rich-compatible style string
    """
    status_lower = status.lower()

    # Check exact matches first
    if status_lower in STATUS_STYLES:
        return STATUS_STYLES[status_lower]

    # Check for partial matches
    for key, style in STATUS_STYLES.items():
        if key in status_lower:
            return style

    # Default style if no match
    return NEUTRAL
