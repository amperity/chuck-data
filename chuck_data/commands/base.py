"""
Base classes for the command system.

This module contains shared base classes and utilities used by command handlers.
"""

from typing import Any


class CommandResult:
    """Class to represent the result of a command execution."""

    def __init__(
        self,
        success: bool,
        data: Any = None,
        message: str | None = None,
        error: Exception | None = None,
    ):
        self.success = success
        self.data = data
        self.message = message
        self.error = error
