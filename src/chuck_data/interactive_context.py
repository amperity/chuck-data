"""
Interactive context management for Chuck command handlers.

This module provides a centralized system for tracking and managing
interactive command contexts, allowing commands to prompt for and
process user input across multiple steps.
"""

from typing import Dict, Any, Optional


class InteractiveContext:
    """
    Global manager for interactive context states.
    Helps coordinate between command handlers and the TUI.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(InteractiveContext, cls).__new__(cls)
            cls._instance._active_contexts = {}
            cls._instance._current_command = None
        return cls._instance

    @property
    def current_command(self) -> Optional[str]:
        """Get the currently active command, if any."""
        return self._current_command

    def set_active_context(self, command: str) -> None:
        """
        Set a command as active in interactive mode.

        Args:
            command: The command name (e.g., "setup_wizard" or "/setup_wizard")
                    Slash prefix will be removed internally for consistency.
        """
        # Normalize command name by removing slash prefix if present
        normalized_cmd = command.lstrip("/")

        # Store original command for UI reference but normalize for internal storage
        self._current_command = command
        if normalized_cmd not in self._active_contexts:
            self._active_contexts[normalized_cmd] = {}

    def clear_active_context(self, command: str) -> None:
        """
        Clear the interactive context for a command.

        Args:
            command: The command name (e.g., "setup_wizard" or "/setup_wizard")
                    Slash prefix will be removed internally for consistency.
        """
        # Normalize command name by removing slash prefix if present
        normalized_cmd = command.lstrip("/")

        # Compare normalized versions of commands to avoid mismatches
        if (
            self._current_command
            and self._current_command.lstrip("/") == normalized_cmd
        ):
            self._current_command = None

        if normalized_cmd in self._active_contexts:
            del self._active_contexts[normalized_cmd]

    def is_in_interactive_mode(self) -> bool:
        """Check if any command is in interactive mode."""
        return self._current_command is not None

    def get_context_data(self, command: str) -> Dict[str, Any]:
        """
        Get stored context data for a command.

        Args:
            command: The command name (e.g., "setup_wizard" or "/setup_wizard")
                    Slash prefix will be removed internally for consistency.

        Returns:
            Dictionary of context data for the command
        """
        # Normalize command name by removing slash prefix if present
        normalized_cmd = command.lstrip("/")

        return self._active_contexts.get(normalized_cmd, {})

    def store_context_data(self, command: str, key: str, value: Any) -> None:
        """
        Store data in the context for a command.

        Args:
            command: The command name (e.g., "setup_wizard" or "/setup_wizard")
                    Slash prefix will be removed internally for consistency.
            key: The key to store the data under
            value: The value to store
        """
        # Normalize command name by removing slash prefix if present
        normalized_cmd = command.lstrip("/")

        if normalized_cmd not in self._active_contexts:
            self._active_contexts[normalized_cmd] = {}

        self._active_contexts[normalized_cmd][key] = value
