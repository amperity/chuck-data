"""
UI rendering for setup wizard.
"""

import platform
import subprocess
import logging
from typing import List, Dict, Any
from rich.console import Console
from rich.table import Table
from rich import box

from .state import WizardState, WizardStep
from .steps import SetupStep

from src.ui.theme import (
    INFO_STYLE,
    TABLE_TITLE_STYLE,
    SUCCESS_STYLE,
    ERROR_STYLE,
    WARNING_STYLE,
)


class WizardRenderer:
    """Handles UI rendering for the setup wizard."""

    def __init__(self, console: Console):
        self.console = console

    def clear_terminal(self):
        """Clear the terminal screen."""
        system = platform.system().lower()

        try:
            if system == "windows":
                subprocess.run("cls", shell=True, check=False)
            else:
                subprocess.run("clear", shell=True, check=False)
        except Exception as e:
            logging.debug(f"Failed to clear terminal: {e}")

    def render_step_header(
        self, step_number: int, title: str, clear_screen: bool = True
    ):
        """Display a header for the current step."""
        if clear_screen:
            self.clear_terminal()
        self.console.print(f"\n[bold]Step {step_number}: {title}[/bold]")
        self.console.print("=" * 50)

    def render_error(self, message: str):
        """Render an error message."""
        self.console.print(f"[{ERROR_STYLE}]Error: {message}[/{ERROR_STYLE}]")

    def render_warning(self, message: str):
        """Render a warning message."""
        self.console.print(f"[{WARNING_STYLE}]{message}[/{WARNING_STYLE}]")

    def render_success(self, message: str):
        """Render a success message."""
        self.console.print(f"[{SUCCESS_STYLE}]{message}[/{SUCCESS_STYLE}]")

    def render_info(self, message: str):
        """Render an info message."""
        self.console.print(f"[{INFO_STYLE}]{message}[/{INFO_STYLE}]")

    def render_prompt(self, message: str):
        """Render a prompt message."""
        self.console.print(message)

    def render_step(
        self,
        step: "SetupStep",
        state: WizardState,
        step_number: int,
        clear_screen: bool = False,
    ):
        """Render a complete step including header and prompt."""
        # Clear screen first if needed
        if clear_screen:
            self.clear_terminal()

        # Render any error message BEFORE the step header so it's clear the error is from previous step
        if state.error_message:
            self.render_error(state.error_message)
            self.console.print()  # Add blank line after error

        # Render step header (but don't clear screen again since we already did it)
        self.render_step_header(step_number, step.get_step_title(), clear_screen=False)

        # Handle special rendering for specific steps
        if state.current_step == WizardStep.MODEL_SELECTION:
            self._render_models_list(state.models)
        elif state.current_step == WizardStep.USAGE_CONSENT:
            self._render_usage_consent_info()

        # Render the prompt
        prompt_message = step.get_prompt_message(state)
        self.render_prompt(prompt_message)

    def render_completion(self):
        """Render wizard completion message."""
        self.console.print("\n[bold]Setup wizard completed successfully![/bold]")
        self.console.print("You are now ready to use Chuck with all features enabled.")
        self.console.print("Type /help to see available commands.")

    def _render_models_list(self, models: List[Dict[str, Any]]):
        """Render the list of available models."""
        if not models:
            self.render_warning("No models available.")
            return

        self.console.print("\nAvailable models:")

        # Define recommended models
        recommended_models = [
            "databricks-meta-llama-3-3-70b-instruct",
            "databricks-claude-3-7-sonnet",
        ]

        # Sort models - recommended first
        sorted_models = []

        # Add recommended models first
        for rec_model in recommended_models:
            for model in models:
                if model["name"] == rec_model:
                    sorted_models.append(model)
                    break

        # Add remaining models
        for model in models:
            if model["name"] not in recommended_models:
                sorted_models.append(model)

        # Display the models
        for i, model in enumerate(sorted_models, 1):
            model_name = model["name"]
            if model_name in recommended_models:
                self.console.print(f"{i}. {model_name} [green](recommended)[/green]")
            else:
                self.console.print(f"{i}. {model_name}")

    def _render_usage_consent_info(self):
        """Render usage consent information."""
        self.console.print(
            "\nChuck is a research preview application meant to showcase a new wave of data engineering tooling powered by AI.\n"
        )
        self.console.print(
            "Our goal is to learn as much about what is working and not working as possible, and your usage is key to that!\n"
        )
        self.console.print(
            "Chuck can log usage to Amperity so that we can see how users are using the application. "
            "This is a key piece of information that we will use to inform our roadmap, prioritize bug fixes, and refine existing features.\n"
        )
        self.console.print(
            "Chuck runs locally and Amperity will never have access to your data.\n"
        )

        # Create table showing what Chuck shares vs never shares
        table = Table(show_header=True, header_style=TABLE_TITLE_STYLE, box=box.SIMPLE)
        table.add_column("Chuck shares", style="green")
        table.add_column("Chuck NEVER shares", style="red bold")

        table.add_row(
            "1. Prompts you type",
            "1. Your data or the values in the tables you interact with",
        )
        table.add_row("2. Tools/context the LLM uses", "2. Credentials of any form")
        table.add_row(
            "3. Errors you encounter",
            "3. Security details about your Databricks account",
        )

        self.console.print(table)

        self.console.print(
            "\nChuck is an Open Source CLI and you can always review the code for security at https://github.com/amperity/chuck-data\n"
        )

    def get_step_number(self, step: WizardStep) -> int:
        """Get display step number for a wizard step."""
        step_numbers = {
            WizardStep.AMPERITY_AUTH: 1,
            WizardStep.WORKSPACE_URL: 2,
            WizardStep.TOKEN_INPUT: 3,
            WizardStep.MODEL_SELECTION: 4,
            WizardStep.USAGE_CONSENT: 5,
            WizardStep.COMPLETE: 6,
        }
        return step_numbers.get(step, 1)
