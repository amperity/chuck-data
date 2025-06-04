"""
Interactive prompt utilities for Chuck command handlers.

This module provides utilities for command handlers to interact with users,
collect input, and manage interactive workflows.
"""

from typing import List, Optional
from getpass import getpass
from rich.console import Console

from src.ui.theme import MESSAGE_STANDARD


class InteractivePrompt:
    """
    Utility for handling interactive user input within command handlers.

    This allows command handlers to prompt for and process user input
    before returning their final CommandResult.
    """

    def __init__(self, console: Optional[Console] = None):
        """Initialize with an optional console instance."""
        self.console = console or Console()
        self.context = {}  # Store context data across interactions

    def prompt(
        self,
        message: str,
        valid_responses: Optional[List[str]] = None,
        case_sensitive: bool = False,
        default: Optional[str] = None,
        hidden: bool = False,
    ) -> str:
        """
        Display a prompt and get validated user input.

        Args:
            message: The message to display to the user
            valid_responses: Optional list of valid responses
            case_sensitive: Whether validation should be case-sensitive
            default: Default value if user enters nothing
            hidden: If True, hide user input (useful for secrets)

        Returns:
            The user's response as a string
        """
        # Display prompt
        self.console.print(f"\n[{MESSAGE_STANDARD}]{message}[/{MESSAGE_STANDARD}]")

        if valid_responses:
            options = "/".join(valid_responses)
            self.console.print(f"[dim]Options: {options}[/dim]")

        if default:
            self.console.print(f"[dim]Default: {default}[/dim]")

        # Display the prompt and get user input directly
        # This works with the TUI interface
        self.console.print(f"[{MESSAGE_STANDARD}]> [/{MESSAGE_STANDARD}]", end="")
        if hidden:
            response = getpass("")
        else:
            response = input()

        response = response.strip()

        # Handle empty response with default
        if not response and default:
            return default

        # Validate response if needed
        if valid_responses:
            valid_check = response
            if not case_sensitive:
                valid_check = response.lower()
                valid_options = [opt.lower() for opt in valid_responses]
            else:
                valid_options = valid_responses

            if valid_check in valid_options:
                return response
            else:
                self.console.print(
                    f"[yellow]Invalid response. Please enter one of: {'/'.join(valid_responses)}[/yellow]"
                )
                # Recursive call to prompt again
                return self.prompt(
                    message,
                    valid_responses,
                    case_sensitive,
                    default,
                    hidden=hidden,
                )
        else:
            # No validation needed
            return response

    def prompt_yes_no(self, message: str, default: Optional[str] = None) -> bool:
        """
        Convenience method for yes/no prompts.

        Args:
            message: The question to ask
            default: Default value ("yes", "no", or None)

        Returns:
            True for yes, False for no
        """
        valid_default = None
        if default:
            valid_default = (
                default.lower() if default.lower() in ["yes", "no", "y", "n"] else None
            )

        response = self.prompt(
            message,
            valid_responses=["yes", "no", "y", "n"],
            case_sensitive=False,
            default=valid_default,
        ).lower()

        return response in ["yes", "y"]
