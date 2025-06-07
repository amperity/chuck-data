"""
Main TUI interface for CHUCK AI.
"""

import os
import shlex
from typing import List, Dict, Any
import logging

from rich.console import Console
import traceback

# Rich imports for TUI rendering
from rich.panel import Panel

# Prompt Toolkit imports for enhanced CLI experience
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.styles import Style
from prompt_toolkit.key_binding import KeyBindings

from chuck_data.ui.ascii_art import display_welcome_screen

from chuck_data.ui.theme import (
    WARNING,
    INFO,
    INFO_STYLE,
    SUCCESS_STYLE,
    ERROR_STYLE,
    WARNING_STYLE,
    DIALOG_BORDER,
    TABLE_TITLE_STYLE,
)

from chuck_data.service import ChuckService
from chuck_data.commands.base import CommandResult
from chuck_data.command_registry import get_command
from chuck_data.config import get_active_model

# Import the interactive context manager
from chuck_data.interactive_context import InteractiveContext

# Global reference to TUI instance for service access
_tui_instance = None


def get_chuck_service():
    """Get the global ChuckService instance."""
    if _tui_instance is None:
        return None
    return _tui_instance.get_service()


def set_chuck_service(service):
    """Set a new global ChuckService instance."""
    if _tui_instance is None:
        return False
    return _tui_instance.set_service(service)


def get_console():
    """Get the global TUI console instance."""
    if _tui_instance is None:
        # Fallback to a default console if TUI not available
        from rich.console import Console

        return Console()
    return _tui_instance.console


class ChuckTUI:
    """
    Main TUI interface for CHUCK AI.
    Handles user interaction, execution via ChuckService, and result display.
    """

    def __init__(self, no_color=False):
        """Initialize the CHUCK AI TUI."""
        self.console = Console(force_terminal=not no_color, no_color=no_color)
        self.service = ChuckService()
        self.running = True
        self.debug = False  # Debug state
        self.no_color = no_color

        # Register this instance as the global TUI instance
        # This allows other modules to access the service instance
        global _tui_instance
        _tui_instance = self

    def get_service(self):
        """Get the current ChuckService instance."""
        return self.service

    def set_service(self, service):
        """Set a new ChuckService instance."""
        self.service = service
        return True

    def _get_available_commands(self) -> List[str]:
        """
        Get a list of available commands for autocompletion.
        """
        # Built-in commands always available
        builtin_commands = ["/exit", "/quit", "/help", "/debug"]

        # Add service commands from the command registry
        service_commands = []
        try:
            # Use the command registry instead of hardcoding
            from chuck_data.command_registry import TUI_COMMAND_MAP

            service_commands = list(TUI_COMMAND_MAP.keys())
        except Exception as e:
            if self.debug:
                self.console.print(f"[dim]Error getting commands: {str(e)}[/dim]")

        # Combine all commands and remove duplicates
        all_commands = builtin_commands + service_commands
        return sorted(list(set(all_commands)))

    def _check_first_run(self) -> bool:
        """
        Check if this is the first run of the application and configuration is needed.

        Returns:
            True if setup wizard should be launched, False otherwise
        """
        from chuck_data.config import get_config_manager
        from pathlib import Path

        config_manager = get_config_manager()

        if config_manager.needs_setup():
            if not Path(config_manager.config_path).exists():
                self.console.print(
                    f"[{WARNING_STYLE}]First time running Chuck! Starting setup wizard...[/{WARNING_STYLE}]"
                )
                return True
            else:
                self.console.print(
                    f"[{WARNING_STYLE}]Some configuration settings are missing. Starting setup wizard...[/{WARNING_STYLE}]"
                )
                return True

        return False

    def run(self) -> None:
        """
        Run the CHUCK AI TUI interface with enhanced prompt-toolkit interface.
        """
        # Clear the screen (skip in no-color mode for better integration test compatibility)
        if not self.no_color:
            os.system("cls" if os.name == "nt" else "clear")

        # Display welcome screen
        display_welcome_screen(self.console)

        # Initialize the interactive context
        interactive_context = InteractiveContext()
        history_file = os.path.expanduser("~/.chuck_history")

        # Get available commands from service for autocomplete
        commands = self._get_available_commands()

        # Set up command completer with slash prefix
        command_completer = WordCompleter(
            commands, ignore_case=True, match_middle=True, sentence=True
        )

        # Custom prompt styles to match our Rich formatting
        # Use colors that are consistent with our theme, but respect no_color setting
        # Note: prompt-toolkit requires specific ANSI color names
        if self.no_color:
            style = Style.from_dict(
                {
                    "prompt": "",  # No styling in no-color mode
                    "interactive-prompt": "",  # No styling in no-color mode
                }
            )
        else:
            style = Style.from_dict(
                {
                    "prompt": "ansicyan bold",  # Matches TABLE_TITLE_STYLE
                    "interactive-prompt": "ansiblue bold",  # Matches DIALOG_BORDER
                }
            )

        # Key bindings to accept history suggestions with Tab
        bindings = KeyBindings()

        @bindings.add("tab")
        def _(event):
            buff = event.current_buffer
            if buff.suggestion:
                buff.insert_text(buff.suggestion.text)
            else:
                if buff.complete_state:
                    buff.complete_next()
                else:
                    buff.start_completion(select_first=True)

        # Create the prompt session with history and completion
        # Disable syntax highlighting in no-color mode
        session = PromptSession(
            history=FileHistory(history_file),
            auto_suggest=AutoSuggestFromHistory(),
            completer=command_completer,
            complete_while_typing=True,
            style=style,
            lexer=None,
            include_default_pygments_style=False,
            key_bindings=bindings,
        )

        # Check if this is the first run and we need to set up
        if self._check_first_run():
            # Start the setup wizard in interactive mode
            self.console.print("[bold]Starting Chuck setup wizard...[/bold]")
            # Just call the setup command - it will handle its own context
            result = self.service.execute_command("/setup")

        # Main TUI application loop
        while self.running:
            try:
                # Check if we're in interactive mode
                if interactive_context.is_in_interactive_mode():
                    current_cmd = interactive_context.current_command

                    # Use prompt toolkit with interactive styling
                    prompt_message = HTML(
                        "<interactive-prompt>chuck (interactive) ></interactive-prompt> "
                    )

                    # Determine if we should hide input (e.g., when entering tokens)
                    hide_input = False
                    if current_cmd == "/setup":
                        ctx = interactive_context.get_context_data("/setup")
                        # We're on the token input step of the setup wizard
                        step = ctx.get("current_step")
                        # Only hide input if we're specifically on the token input step AND have a workspace URL
                        if step == "token_input" and ctx.get("workspace_url"):
                            hide_input = True

                    user_input = session.prompt(
                        prompt_message,
                        is_password=hide_input,
                        enable_history_search=not hide_input,
                    ).strip()

                    # Allow escaping from interactive mode
                    if user_input.lower() in ["cancel", "exit", "quit"]:
                        interactive_context.clear_active_context(current_cmd)
                        self.console.print(
                            f"[{WARNING_STYLE}]Exited interactive mode[/{WARNING_STYLE}]"
                        )
                        continue

                    # Process the interactive input
                    result = self.service.execute_command(
                        current_cmd, interactive_input=user_input
                    )

                    # Process the result even in interactive mode
                    if result:
                        # Check for special exit_interactive message
                        if (
                            not result.success
                            and result.message
                            and result.message.startswith("exit_interactive:")
                        ):
                            # Extract the actual message after the prefix
                            actual_message = (
                                result.message.split(":", 1)[1]
                                if ":" in result.message
                                else result.message
                            )
                            interactive_context.clear_active_context(current_cmd)
                            self.console.print(
                                f"[{INFO_STYLE}]{actual_message}[/{INFO_STYLE}]"
                            )
                            continue

                        # Check if the command rendered content and we should not show a message
                        if (
                            result.success
                            and isinstance(result.data, dict)
                            and result.data.get("rendered_next_step")
                        ):
                            # Content was rendered, just continue to next prompt without additional output
                            continue

                        # If the command is no longer in interactive mode, it means it's complete
                        if (
                            not interactive_context.is_in_interactive_mode()
                            or interactive_context.current_command != current_cmd
                        ):
                            # Command completed its interactive flow, show final result
                            if result.success and result.message:
                                self.console.print(
                                    f"[bold green]{result.message}[/bold green]"
                                )
                            elif not result.success:
                                self._display_error(result)
                else:
                    # Regular command mode - use prompt toolkit
                    prompt_message = HTML("<prompt>chuck ></prompt> ")
                    command = session.prompt(prompt_message).strip()

                    # Skip empty commands
                    if not command:
                        continue

                    # Process user command
                    self._process_command(command)

            except KeyboardInterrupt:
                # Handle Ctrl+C
                self.console.print(
                    f"\n[{WARNING_STYLE}]Interrupted by user. Type 'exit' to quit.[/{WARNING_STYLE}]"
                )
            except Exception as e:
                # Import at the top of the method to avoid scoping issues
                from chuck_data.exceptions import PaginationCancelled

                if isinstance(e, PaginationCancelled):
                    # Handle pagination cancellation silently - just return to prompt
                    pass
                else:
                    raise  # Re-raise other exceptions
            except EOFError:
                # Handle Ctrl+D
                self.console.print(
                    f"\n[{WARNING_STYLE}]Thank you for using chuck![/{WARNING_STYLE}]"
                )
                break
            except Exception as e:
                # Handle other exceptions
                self.console.print(
                    f"[{ERROR_STYLE}]Unexpected Error: {str(e)}[/{ERROR_STYLE}]"
                )
                # Print stack trace in debug mode
                if self.debug:
                    self.console.print("[dim]" + traceback.format_exc() + "[/dim]")

    def _needs_shlex_parsing(self, command: str) -> bool:
        """Determine if command needs shlex parsing (has quotes or flags)."""
        # Never use shlex for agent commands - they need simple splitting
        # to preserve natural language as-is
        if command.startswith("/agent "):
            return False

        # Use shlex parsing for commands that clearly need it:
        # 1. Flag-style arguments (--flag)
        # 2. Intentional quoted strings (balanced quotes)

        # Always use shlex for flag-style arguments
        if "--" in command:
            return True

        # For quoted strings, only use shlex if quotes appear balanced
        if '"' in command:
            double_quotes = command.count('"')
            if double_quotes >= 2 and double_quotes % 2 == 0:
                return True

        # For single quotes, be more careful - avoid contractions like "let's"
        if "'" in command:
            single_quotes = command.count("'")
            # Only use shlex if we have multiple balanced single quotes
            # and they don't appear to be contractions
            if single_quotes >= 2 and single_quotes % 2 == 0:
                # Additional check: make sure quotes aren't part of contractions
                # This is a simple heuristic - look for patterns like "'s " or "'t "
                if not (
                    "'s " in command
                    or "'t " in command
                    or "'ll " in command
                    or "'re " in command
                    or "'ve " in command
                    or "'d " in command
                ):
                    return True

        return False

    def _process_command(self, command):
        """Process a user command in the TUI interface."""
        interactive_context = InteractiveContext()

        # Handle built-in TUI commands
        if command.lower() in ["/exit", "/quit", "exit", "quit"]:
            self.running = False
            self.console.print(
                f"[{WARNING_STYLE}]Exiting Chuck AI...[/{WARNING_STYLE}]"
            )
            return

        # Process command with agent if no slash prefix
        if not command.startswith("/"):
            self.console.print("[teal]Thinking...[/teal]")
            command = f"/agent {command}"  # Default to agent if no slash
        elif command.startswith("/ask "):
            # Show thinking message for /ask commands too
            self.console.print("[teal]Thinking...[/teal]")

        # Split command into parts for service layer
        # Use shlex for commands with quotes or flags, simple split for natural language
        if self._needs_shlex_parsing(command):
            try:
                parts = shlex.split(command)
            except ValueError as e:
                self.console.print(f"[red]Error parsing command: {e}[/red]")
                return
        else:
            parts = command.split()

        cmd = parts[0].lower()
        args = parts[1:]

        # Process special commands
        if cmd == "/debug":
            self._handle_debug(args)
            return

        # Execute command via service layer
        # Pass display callback for agent commands to show tool outputs immediately
        if cmd in ["/agent", "/ask"]:
            result = self.service.execute_command(
                cmd, *args, tool_output_callback=self.display_tool_output
            )
        elif cmd in ["/warehouses", "/list-warehouses"]:
            # For TUI warehouse commands, always show the full table
            result = self.service.execute_command(cmd, *args, display=True)
        else:
            result = self.service.execute_command(cmd, *args)

        if not result:
            return

        # Skip result processing if entering interactive mode
        # Ensure valid commands initiated outside of agent tools are not skipped
        if interactive_context.is_in_interactive_mode() and cmd not in [
            "/agent",
            "/ask",
        ]:
            return

        # Process command result
        self._process_command_result(cmd, result)

    def _process_command_result(self, cmd, result):
        """Process a command result and display it appropriately in the TUI."""
        if result.success:
            # Display success message if available
            if result.message:
                # Check for specific data types to format messages differently
                if (
                    cmd in ["/agent", "/ask"]
                    and isinstance(result.data, dict)
                    and "response" in result.data
                ):
                    # Agent response is handled below, just print success message if any
                    if (
                        result.message != result.data["response"]
                    ):  # Avoid duplicate printing
                        self.console.print(f"[bold green]{result.message}[/bold green]")
                else:
                    self.console.print(f"[bold green]{result.message}[/bold green]")

            # Skip if no data to display
            if not result.data:
                return

            # Specialized display for different commands
            if cmd in ["/catalogs", "/search_catalogs", "/list-catalogs"]:
                self._display_catalogs(result.data)
            elif cmd in ["/schemas", "/search_schemas", "/list-schemas"]:
                self._display_schemas(result.data)
            elif cmd in ["/tables", "/search_tables", "/list-tables"]:
                self._display_tables(result.data)
            elif cmd in ["/catalog", "/catalog-details"]:
                self._display_catalog_details(result.data)
            elif cmd in ["/schema", "/schema-details"]:
                self._display_schema_details(result.data)
            elif cmd == "/models":
                self._display_models(result.data)
            elif cmd == "/list-models":
                self._display_detailed_models(result.data)
            elif cmd in ["/warehouses", "/list-warehouses"]:
                self._display_warehouses(result.data)
            elif cmd in ["/volumes", "/list-volumes"]:
                self._display_volumes(result.data)
            elif cmd in ["/table", "/show_table", "/table-details"]:
                self._display_table_details(result.data)
            elif cmd in ["/run-sql", "/sql"]:
                self._display_sql_results(result.data)
            elif cmd == "/scan-pii":
                self._display_pii_scan_results(result.data)
            elif cmd == "/status":
                self._display_status(result.data)
            elif cmd == "/auth" and "permissions" in result.data:
                self._display_permissions(result.data["permissions"])
            elif cmd == "/usage":
                # For the usage command, we just display the message
                if result.message:
                    self._display_usage(result.message)
            elif (
                cmd == "/help"
                and isinstance(result.data, dict)
                and "help_text" in result.data
            ):
                self.console.print(
                    Panel(
                        result.data["help_text"],
                        title="CHUCK AI Help",
                        border_style="cyan",
                    )
                )
            elif (
                cmd in ["/agent", "/ask"]
                and isinstance(result.data, dict)
                and "response" in result.data
                and result.data[
                    "response"
                ].strip()  # Only show if response is not empty
            ):
                # Agent response - print directly or format nicely
                self.console.print(
                    Panel(
                        result.data["response"],
                        title="Agent Response",
                        border_style=DIALOG_BORDER,
                    )
                )
        else:
            # Display error
            self._display_error(result)

    def _handle_debug(self, args: List[str]) -> None:
        """Toggle debug mode."""
        if not args:
            self.debug = not self.debug
        elif args[0].lower() in ("on", "true", "1", "yes"):
            self.debug = True
        elif args[0].lower() in ("off", "false", "0", "no"):
            self.debug = False
        else:
            self.console.print(f"[{ERROR_STYLE}]Invalid debug option.[/{ERROR_STYLE}]")
            self.console.print("Usage: /debug [on|off]")
            return

        status = "ON" if self.debug else "OFF"
        self.console.print(
            f"[{SUCCESS_STYLE}]Debug mode is now {status}[/{SUCCESS_STYLE}]"
        )

    def display_tool_output(self, tool_name: str, tool_result: Dict[str, Any]) -> None:
        """Display tool output immediately during agent execution."""
        try:
            # Get command definition to check display type
            command_def = get_command(tool_name)

            # Use the command's agent_display setting, defaulting to "condensed"
            display_type = "condensed"
            if command_def:
                agent_display = getattr(command_def, "agent_display", "condensed")

                if agent_display == "conditional":
                    # Use display_condition function to determine display type
                    display_condition = getattr(command_def, "display_condition", None)
                    if display_condition and isinstance(tool_result, dict):
                        display_type = (
                            "full" if display_condition(tool_result) else "condensed"
                        )
                    else:
                        display_type = "condensed"  # Fallback if no condition function
                else:
                    display_type = agent_display

            # Route based on display type
            if display_type == "condensed":
                self._display_condensed_tool_output(tool_name, tool_result)
            else:
                # Full display - use existing detailed display methods
                self._display_full_tool_output(tool_name, tool_result)

        except Exception as e:
            # Handle pagination cancellation specially - let it bubble up
            from chuck_data.exceptions import PaginationCancelled

            if isinstance(e, PaginationCancelled):
                raise  # Re-raise to bubble up to main TUI loop

            # Don't let other display errors break agent execution
            logging.warning(f"Failed to display tool output for {tool_name}: {e}")
            # Show a simple notification that output was attempted
            self.console.print(f"[dim][Tool: {tool_name} executed][/dim]")

    def _display_full_tool_output(
        self, tool_name: str, tool_result: Dict[str, Any]
    ) -> None:
        """Display full detailed tool output (existing behavior)."""
        # Map tool names to their display methods
        # This reuses existing display logic to maintain consistency
        if tool_name in ["list-catalogs", "list_catalogs", "catalogs"]:
            self._display_catalogs(tool_result)
        elif tool_name in ["list-schemas", "list_schemas", "schemas"]:
            self._display_schemas(tool_result)
        elif tool_name in ["list-tables", "list_tables", "tables"]:
            self._display_tables(tool_result)
        elif tool_name in ["get_catalog_details", "catalog"]:
            self._display_catalog_details(tool_result)
        elif tool_name in ["get_schema_details", "schema"]:
            self._display_schema_details(tool_result)
        elif tool_name in ["detailed-models", "list-models", "list_models", "models"]:
            if "models" in tool_result:
                self._display_detailed_models(tool_result)
            else:
                self._display_models(tool_result)
        elif tool_name in ["list-warehouses", "list_warehouses", "warehouses"]:
            self._display_warehouses(tool_result)
        elif tool_name in ["list-volumes", "list_volumes", "volumes"]:
            self._display_volumes(tool_result)
        elif tool_name in ["get_table_info", "table", "show_table"]:
            self._display_table_details(tool_result)
        elif tool_name in ["scan_schema_for_pii", "scan_pii"]:
            self._display_pii_scan_results(tool_result)
        elif tool_name == "get_status":
            self._display_status(tool_result)
        elif tool_name == "run-sql":
            self._display_sql_results_formatted(tool_result)
        else:
            # For unknown tools, display a generic panel with the data
            from rich.panel import Panel
            import json

            # Try to format as JSON for readability
            try:
                formatted_data = json.dumps(tool_result, indent=2)
                self.console.print(
                    Panel(
                        formatted_data,
                        title=f"Tool Output: {tool_name}",
                        border_style=DIALOG_BORDER,
                    )
                )
            except (TypeError, ValueError):
                # If JSON serialization fails, display as string
                self.console.print(
                    Panel(
                        str(tool_result),
                        title=f"Tool Output: {tool_name}",
                        border_style=DIALOG_BORDER,
                    )
                )

    def _display_condensed_tool_output(
        self, tool_name: str, tool_result: Dict[str, Any]
    ) -> None:
        """Display condensed tool output with status and key metrics."""
        # Get friendly action name if available

        command_def = get_command(tool_name)
        friendly_name = tool_name
        if command_def and getattr(command_def, "condensed_action", None):
            friendly_name = command_def.condensed_action

        # Extract key information based on tool type
        status_line = f"[dim cyan]→[/dim cyan] {friendly_name}"
        metrics = []

        # Extract meaningful metrics from common result patterns
        if isinstance(tool_result, dict):
            # Look for common success indicators
            if tool_result.get("success") is True or "message" in tool_result:
                status_line += " [green]✓[/green]"
            elif tool_result.get("success") is False:
                status_line += " [red]✗[/red]"

            # Extract key metrics based on common patterns
            if "total_count" in tool_result:
                metrics.append(f"{tool_result['total_count']} items")
            elif "count" in tool_result:
                metrics.append(f"{tool_result['count']} items")

            # PII-specific metrics
            if "tables_with_pii" in tool_result:
                metrics.append(f"{tool_result['tables_with_pii']} tables with PII")
            if "total_pii_columns" in tool_result:
                metrics.append(f"{tool_result['total_pii_columns']} PII columns")

            # Tag-specific metrics
            if "tagged_columns" in tool_result:
                metrics.append(f"{len(tool_result['tagged_columns'])} columns tagged")

            # Status-specific info
            if tool_name == "status" and "workspace" in tool_result:
                workspace = tool_result.get("workspace", {})
                if "name" in workspace:
                    metrics.append(f"workspace: {workspace['name']}")

            # Step-based progress reporting (used by warehouse selection, etc.)
            if "step" in tool_result:
                metrics.append(tool_result["step"])
            # Schema/Catalog selection specific info - keep it simple (fallback if no step)
            elif (
                tool_name in ["set_schema", "select-schema"]
                and "schema_name" in tool_result
            ):
                metrics.append(f"Schema set (Name: {tool_result['schema_name']})")
            elif (
                tool_name in ["set_catalog", "select-catalog"]
                and "catalog_name" in tool_result
            ):
                metrics.append(f"Catalog set (Name: {tool_result['catalog_name']})")

            # Generic message fallback
            if not metrics and "message" in tool_result:
                metrics.append(tool_result["message"])

        # Format the condensed display
        if metrics:
            status_line += f" ({', '.join(metrics)})"

        self.console.print(status_line)

    def _display_error(self, result: CommandResult) -> None:
        """Display an error from a command result."""
        error_message = result.message or "Unknown error occurred"
        self.console.print(f"[{ERROR_STYLE}]Error: {error_message}[/{ERROR_STYLE}]")

        if result.error and self.debug:
            # Use Rich's traceback rendering for better formatting
            self.console.print_exception(show_locals=True)

    def _display_catalogs(self, data: Dict[str, Any]) -> None:
        """Display catalogs in a nicely formatted way."""
        from chuck_data.ui.table_formatter import display_table
        from chuck_data.exceptions import PaginationCancelled

        catalogs = data.get("catalogs", [])
        current_catalog = data.get("current_catalog")

        if not catalogs:
            self.console.print(f"[{WARNING_STYLE}]No catalogs found.[/{WARNING_STYLE}]")
            # Raise PaginationCancelled to return to chuck > prompt immediately
            raise PaginationCancelled()

        # Define column styling based on the active catalog
        def name_style(name):
            return "bold green" if name == current_catalog else None

        style_map = {"name": name_style}

        # Prepare catalog data - ensure lowercase types
        for catalog in catalogs:
            if "type" in catalog and catalog["type"]:
                catalog["type"] = catalog["type"].lower()

        # Display the table
        display_table(
            console=self.console,
            data=catalogs,
            columns=["name", "type", "comment", "owner"],
            headers=["Name", "Type", "Comment", "Owner"],
            title="Available Catalogs",
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=False,
        )

        # Display current catalog if set
        if current_catalog:
            self.console.print(
                f"\nCurrent catalog: [bold green]{current_catalog}[/bold green]"
            )

        # Raise PaginationCancelled to return to chuck > prompt immediately
        # This prevents agent from continuing processing after catalog display is complete
        raise PaginationCancelled()

    def _display_schemas(self, data: Dict[str, Any]) -> None:
        """Display schemas in a nicely formatted way."""
        from chuck_data.ui.table_formatter import display_table
        from chuck_data.exceptions import PaginationCancelled

        schemas = data.get("schemas", [])
        catalog_name = data.get("catalog_name", "")
        current_schema = data.get("current_schema")

        if not schemas:
            self.console.print(
                f"[{WARNING_STYLE}]No schemas found in catalog '{catalog_name}'.[/{WARNING_STYLE}]"
            )
            # Raise PaginationCancelled to return to chuck > prompt immediately
            raise PaginationCancelled()

        # Define column styling based on the active schema
        def name_style(name):
            return "bold green" if name == current_schema else None

        style_map = {"name": name_style}

        # Display the table
        display_table(
            console=self.console,
            data=schemas,
            columns=["name", "comment"],
            headers=["Name", "Comment"],
            title=f"Schemas in catalog '{catalog_name}'",
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=False,
        )

        # Display current schema if available
        if current_schema:
            self.console.print(
                f"\nCurrent schema: [{SUCCESS_STYLE}]{current_schema}[/{SUCCESS_STYLE}]"
            )

        # Raise PaginationCancelled to return to chuck > prompt immediately
        # This prevents agent from continuing processing after schema display is complete
        raise PaginationCancelled()

    def _display_tables(self, data: Dict[str, Any]) -> None:
        """Display tables in a nicely formatted way."""
        from chuck_data.ui.table_formatter import display_table
        from chuck_data.exceptions import PaginationCancelled

        tables = data.get("tables", [])
        catalog_name = data.get("catalog_name", "")
        schema_name = data.get("schema_name", "")
        total_count = data.get("total_count", len(tables))

        if not tables:
            self.console.print(
                f"[{WARNING_STYLE}]No tables found in {catalog_name}.{schema_name}[/{WARNING_STYLE}]"
            )
            # Raise PaginationCancelled to return to chuck > prompt immediately
            raise PaginationCancelled()

        # Process the table data for display
        for table in tables:
            # Convert columns list to count if present
            if "columns" in table and isinstance(table["columns"], list):
                table["column_count"] = len(table["columns"])
            else:
                table["column_count"] = 0

            # Format timestamps if present
            for ts_field in ["created_at", "updated_at"]:
                if ts_field in table and table[ts_field]:
                    try:
                        # Convert timestamp to more readable format if needed
                        # Handle Unix timestamps (integers) and ISO strings
                        timestamp = table[ts_field]
                        if isinstance(timestamp, int):
                            # Convert Unix timestamp (milliseconds) to readable date
                            from datetime import datetime

                            date_obj = datetime.fromtimestamp(timestamp / 1000)
                            table[ts_field] = date_obj.strftime("%Y-%m-%d")
                        elif isinstance(timestamp, str) and len(timestamp) > 10:
                            table[ts_field] = timestamp.split("T")[0]
                    except Exception:
                        pass  # Keep the original format if conversion fails

            # Format row count if present
            if "row_count" in table and table["row_count"] not in ["-", "Unknown"]:
                try:
                    row_count = table["row_count"]
                    if isinstance(row_count, str) and row_count.isdigit():
                        row_count = int(row_count)

                    if isinstance(row_count, int):
                        # Format large numbers with appropriate suffixes
                        if row_count >= 1_000_000_000:
                            table["row_count"] = f"{row_count / 1_000_000_000:.1f}B"
                        elif row_count >= 1_000_000:
                            table["row_count"] = f"{row_count / 1_000_000:.1f}M"
                        elif row_count >= 1_000:
                            table["row_count"] = f"{row_count / 1_000:.1f}K"
                        else:
                            table["row_count"] = str(row_count)
                except Exception:
                    pass  # Keep the original format if conversion fails

        # Define column styling functions
        def table_type_style(type_val):
            if type_val == "VIEW" or type_val == "view":
                return "bright_blue"
            return None

        # Set up style map
        style_map = {
            "table_type": table_type_style,
            "column_count": lambda val: "dim" if val == 0 else None,
        }

        # Adjust title based on method
        method = data.get("method", "")
        title = (
            f"Tables in {catalog_name}.{schema_name} ({total_count} total)"
            if method == "unity_catalog"
            else "Available Tables"
        )

        # Set up column alignments for numerical columns
        column_alignments = {
            "# Cols": "right",
            "Rows": "right",
        }

        # Display the table using our formatter
        display_table(
            console=self.console,
            data=tables,
            columns=[
                "name",
                "table_type",
                "column_count",
                "row_count",
                "created_at",
                "updated_at",
            ],
            headers=["Table Name", "Type", "# Cols", "Rows", "Created", "Last Updated"],
            title=title,
            style_map=style_map,
            column_alignments=column_alignments,
            title_style=TABLE_TITLE_STYLE,
            show_lines=True,
        )

        # Raise PaginationCancelled to return to chuck > prompt immediately
        # This prevents agent from continuing processing after table display is complete
        raise PaginationCancelled()

    def _display_models(self, models: List[Dict[str, Any]]) -> None:
        """Display models in a nicely formatted way."""
        from chuck_data.ui.table_formatter import display_table
        from chuck_data.exceptions import PaginationCancelled

        # Use imported function to get the active model for highlighting
        active_model = get_active_model()

        if not models:
            self.console.print(
                f"[{WARNING_STYLE}]No models found or returned.[/{WARNING_STYLE}]"
            )
            # Raise PaginationCancelled to return to chuck > prompt immediately
            raise PaginationCancelled()

        # Process model data for display
        processed_models = []
        for model in models:
            # Create a processed model with clear fields
            processed = {
                "name": model.get("name", "N/A"),
                "creator": model.get("creator", "N/A"),
            }

            # Extract and process state information
            state = model.get("state", {})
            ready_status = state.get("ready", "UNKNOWN").upper()
            processed["status"] = ready_status

            # Add to our list
            processed_models.append(processed)

        # Define styling function for status
        def status_style(status):
            if status == "READY":
                return "green"
            elif status == "NOT_READY":
                return "yellow"
            elif "ERROR" in status:
                return "red"
            return None

        # Define styling function for the name to highlight active model
        def name_style(name):
            return "bold green" if name == active_model else None

        # Process model names to add recommended tag
        for model in processed_models:
            if model["name"] in [
                "databricks-meta-llama-3-3-70b-instruct",
                "databricks-claude-3-7-sonnet",
            ]:
                model["name"] = f"{model['name']} (recommended)"

        # Set up style map
        style_map = {"name": name_style, "status": status_style}

        # Display the table using our formatter
        display_table(
            console=self.console,
            data=processed_models,
            columns=["name", "creator", "status"],
            headers=["Endpoint Name", "Creator", "State"],
            title="Available Model Serving Endpoints",
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=False,
        )

        # Display active model if set
        if active_model:
            self.console.print(
                f"\nCurrent active model: [{SUCCESS_STYLE}]{active_model}[/{SUCCESS_STYLE}]"
            )

        # Raise PaginationCancelled to return to chuck > prompt immediately
        # This prevents agent from continuing processing after model display is complete
        raise PaginationCancelled()

    def _display_detailed_models(self, data: Dict[str, Any]) -> None:
        """Display models with detailed information and filtering."""
        from chuck_data.ui.table_formatter import display_table
        from chuck_data.exceptions import PaginationCancelled

        models = data.get("models", [])
        active_model = data.get("active_model")
        detailed = data.get("detailed", False)
        filter_text = data.get("filter")

        # If no models, display the help message
        if not models:
            self.console.print(
                f"[{WARNING_STYLE}]No models found in workspace.[/{WARNING_STYLE}]"
            )
            if data.get("message"):
                self.console.print("\n" + data.get("message"))
            # Raise PaginationCancelled to return to chuck > prompt immediately
            raise PaginationCancelled()

        # Display header with filter information if applicable
        title = "Available Models"
        if filter_text:
            title += f" matching '{filter_text}'"

        # Process model data for display
        processed_models = []
        for model in models:
            # Create a processed model entry
            processed = {
                "name": model.get("name", "N/A"),
                "creator": model.get("creator", "N/A"),
            }

            # Get state information
            state = model.get("state", {})
            ready_status = state.get("ready", "UNKNOWN").upper()
            processed["status"] = ready_status

            # Add detailed fields if requested
            if detailed:
                processed["endpoint_type"] = model.get("endpoint_type", "Unknown")
                processed["last_modified"] = model.get("last_updated", "Unknown")

                # Add any additional details from the details field
                details = model.get("details", {})
                if details:
                    for key, value in details.items():
                        # Only add if not already present and meaningful
                        if key not in processed and value is not None and key != "name":
                            processed[key] = value

            # Add to our list
            processed_models.append(processed)

        # Define column styling functions
        def status_style(status):
            if status == "READY":
                return "green"
            elif status == "NOT_READY" or status == "UNKNOWN":
                return "yellow"
            elif "ERROR" in status:
                return "red"
            return None

        # Define styling function for the name to highlight active model
        def name_style(name):
            if name == active_model:
                return "bold green"
            return "cyan"

        # Set up style map with appropriate styles for each column
        style_map = {
            "name": name_style,
            "status": status_style,
            "creator": lambda _: f"{INFO}",
            "endpoint_type": lambda _: f"{WARNING}",
            "last_modified": lambda _: f"{DIALOG_BORDER}",
        }

        # Define columns and headers based on detail level
        if detailed:
            columns = ["name", "status", "creator", "endpoint_type", "last_modified"]
            headers = ["Name", "Status", "Creator", "Type", "Last Modified"]
        else:
            columns = ["name", "status", "creator"]
            headers = ["Model Name", "Status", "Creator"]

        # Display the table using our formatter
        display_table(
            console=self.console,
            data=processed_models,
            columns=columns,
            headers=headers,
            title=title,
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=False,
            box_style="SIMPLE",
        )

        # Display help instructions
        self.console.print(
            "\n[dim]Use [bold]/select_model <name>[/bold] to set the active model for agent operations.[/dim]"
        )
        if not detailed:
            self.console.print(
                "[dim]Use [bold]/list-models --detailed[/bold] to see more details about available models.[/dim]"
            )
        if not filter_text:
            self.console.print(
                "[dim]Use [bold]/list-models --filter <text>[/bold] to filter models by name.[/dim]"
            )
        self.console.print(
            "[dim]Use [bold]/models[/bold] for a simpler view of available models.[/dim]"
        )

        # Raise PaginationCancelled to return to chuck > prompt immediately
        # This prevents agent from continuing processing after detailed model display is complete
        raise PaginationCancelled()

    def _display_warehouses(self, data: Dict[str, Any]) -> None:
        """Display SQL warehouses in a nicely formatted way."""
        from chuck_data.ui.table_formatter import display_table
        from chuck_data.exceptions import PaginationCancelled

        # This method is only called when we actually want to display the table
        # The conditional display logic handles the display/no-display decision

        warehouses = data.get("warehouses", [])
        current_warehouse_id = data.get("current_warehouse_id")

        if not warehouses:
            self.console.print(
                f"[{WARNING_STYLE}]No SQL warehouses found.[/{WARNING_STYLE}]"
            )
            # Raise PaginationCancelled to return to chuck > prompt immediately
            raise PaginationCancelled()

        # Process warehouse data for display
        processed_warehouses = []
        for warehouse in warehouses:
            # Determine warehouse type: show 'serverless' if serverless is enabled, otherwise show warehouse_type
            warehouse_type = (
                "serverless"
                if warehouse.get("enable_serverless_compute", False)
                else warehouse.get("warehouse_type", "").lower()
            )

            # Create a processed warehouse with formatted fields
            processed = {
                "name": warehouse.get("name", ""),
                "id": warehouse.get("id", ""),
                "size": warehouse.get("size", "").lower(),  # Lowercase size field
                "type": warehouse_type,
                "state": warehouse.get("state", "").lower(),  # Lowercase state field
            }
            processed_warehouses.append(processed)

        # Define styling function for name based on current warehouse
        def name_style(name, row):
            if row.get("id") == current_warehouse_id:
                return "bold green"
            return None

        # Define styling function for ID based on current warehouse
        def id_style(id_val):
            if id_val == current_warehouse_id:
                return "bold green"
            return None

        # Define styling function for state
        def state_style(state):
            if state == "running":
                return "green"
            elif state == "stopped":
                return "red"
            elif state in ["starting", "stopping", "deleting", "resizing"]:
                return "yellow"
            return "dim"

        # Set up style map
        style_map = {
            "name": lambda name, row=None: name_style(name, row),
            "id": id_style,
            "state": state_style,
        }

        # Set maximum lengths for fields

        # Display the table
        display_table(
            console=self.console,
            data=processed_warehouses,
            columns=["name", "id", "size", "type", "state"],
            headers=["Name", "ID", "Size", "Type", "State"],
            title="Available SQL Warehouses",
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=False,
        )

        # Display current warehouse ID if set
        if current_warehouse_id:
            self.console.print(
                f"\nCurrent SQL warehouse ID: [{SUCCESS_STYLE}]{current_warehouse_id}[/{SUCCESS_STYLE}]"
            )

        # Always raise PaginationCancelled when we actually display the table
        # (we already returned early if display=false)
        raise PaginationCancelled()

    def _display_volumes(self, data: Dict[str, Any]) -> None:
        """Display volumes in a nicely formatted way."""
        from chuck_data.ui.table_formatter import display_table
        from chuck_data.exceptions import PaginationCancelled

        volumes = data.get("volumes", [])
        catalog_name = data.get("catalog_name", "")
        schema_name = data.get("schema_name", "")

        if not volumes:
            self.console.print(
                f"[{WARNING_STYLE}]No volumes found in {catalog_name}.{schema_name}.[/{WARNING_STYLE}]"
            )
            # Raise PaginationCancelled to return to chuck > prompt immediately
            raise PaginationCancelled()

        # Process volume data for display
        processed_volumes = []
        for volume in volumes:
            # Create a processed volume with normalized fields
            processed = {
                "name": volume.get("name", ""),
                "type": volume.get(
                    "volume_type", ""
                ).upper(),  # Use upper for consistency
                "comment": volume.get("comment", ""),
            }
            processed_volumes.append(processed)

        # Define styling for volume types
        def type_style(volume_type):
            if volume_type == "EXTERNAL":  # Example conditional styling
                return "yellow"
            elif volume_type == "MANAGED":  # Example conditional styling
                return "blue"
            return None

        # Set up style map
        style_map = {"type": type_style}

        # Display the table
        display_table(
            console=self.console,
            data=processed_volumes,
            columns=["name", "type", "comment"],
            headers=["Name", "Type", "Comment"],
            title=f"Volumes in {catalog_name}.{schema_name}",
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=False,
        )

        # Raise PaginationCancelled to return to chuck > prompt immediately
        # This prevents agent from continuing processing after volume display is complete
        raise PaginationCancelled()

    def _display_status(self, data: Dict[str, Any]) -> None:
        """Display current status information including connection status and permissions."""
        from chuck_data.ui.table_formatter import display_table

        workspace_url = data.get("workspace_url", "Not set")
        active_catalog = data.get("active_catalog", "Not set")
        active_schema = data.get("active_schema", "Not set")
        active_model = data.get("active_model", "Not set")
        warehouse_id = data.get("warehouse_id", "Not set")
        connection_status = data.get("connection_status", "Unknown")

        # Prepare settings for display
        status_items = [
            {"setting": "Workspace URL", "value": workspace_url},
            {"setting": "Active Catalog", "value": active_catalog},
            {"setting": "Active Schema", "value": active_schema},
            {"setting": "Active Model", "value": active_model},
            {"setting": "Active Warehouse", "value": warehouse_id},
            {"setting": "Connection Status", "value": connection_status},
        ]

        # Define styling functions
        def value_style(value, row):
            setting = row.get("setting", "")

            # Special handling for connection status
            if setting == "Connection Status":
                if value == "Connected - token is valid":
                    return "green"
                elif "Invalid" in value or "Not connected" in value:
                    return "red"
                else:
                    return "yellow"
            # General styling for values
            elif value != "Not set":
                return "green"
            else:
                return "yellow"

        # Set up style map
        style_map = {"value": lambda value, row: value_style(value, row)}

        # Display the status table
        display_table(
            console=self.console,
            data=status_items,
            columns=["setting", "value"],
            headers=["Setting", "Value"],
            title="Current Configuration",
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=False,
        )

        # If permissions data is available, display it
        permissions_data = data.get("permissions")
        if permissions_data:
            self._display_permissions(permissions_data)

    def _display_permissions(self, permissions_data: Dict[str, Any]) -> None:
        """
        Display detailed permission check results.

        Args:
            permissions_data: Dictionary of permission check results
        """
        from chuck_data.ui.table_formatter import display_table

        if not permissions_data:
            self.console.print(
                f"[{WARNING_STYLE}]No permission data available.[/{WARNING_STYLE}]"
            )
            return

        # Format permission data for display
        formatted_permissions = []
        for resource, data in permissions_data.items():
            authorized = data.get("authorized", False)
            details = (
                data.get("details")
                if authorized
                else data.get("error", "Access denied")
            )
            api_path = data.get("api_path", "Unknown")

            # Create a dictionary for this permission
            resource_name = resource.replace("_", " ").title()
            permission_entry = {
                "resource": resource_name,
                "status": "Authorized" if authorized else "Denied",
                "details": details,
                "api_path": api_path,  # Store for reference in the endpoints section
                "authorized": authorized,  # Store for conditional styling
            }
            formatted_permissions.append(permission_entry)

        # Define styling function for status column
        def status_style(status, row):
            return "green" if row.get("authorized") else "red"

        # Set up style map
        style_map = {"status": status_style}

        # Display the permissions table
        display_table(
            console=self.console,
            data=formatted_permissions,
            columns=["resource", "status", "details"],
            headers=["Resource", "Status", "Details"],
            title="Databricks API Token Permissions",
            style_map=style_map,
            title_style=TABLE_TITLE_STYLE,
            show_lines=True,
        )

        # Additional note about API endpoints
        self.console.print("\n[dim]API endpoints checked:[/dim]")
        for item in formatted_permissions:
            resource_name = item["resource"]
            api_path = item["api_path"]
            self.console.print(f"[dim]- {resource_name}: {api_path}[/dim]")

    def _display_table_details(self, data: Dict[str, Any]) -> None:
        """Display detailed information for a single table."""
        from chuck_data.ui.table_formatter import display_table

        table = data.get("table", {})
        full_name = data.get("full_name", "")
        has_delta_metadata = data.get("has_delta_metadata", False)

        if not table:
            self.console.print(
                f"[{WARNING_STYLE}]No table details available.[/{WARNING_STYLE}]"
            )
            return

        # Display table header
        self.console.print(
            f"\n[{TABLE_TITLE_STYLE}]Table Details: {full_name}[/{TABLE_TITLE_STYLE}]"
        )

        # Prepare basic information data
        basic_info = []
        properties = [
            ("Name", table.get("name", "")),
            ("Full Name", full_name),
            ("Type", table.get("table_type", "")),
            ("Format", table.get("data_source_format", "")),
            ("Storage Location", table.get("storage_location", "")),
            ("Owner", table.get("owner", "")),
            ("Created", table.get("created_at", "")),
            ("Created By", table.get("created_by", "")),
            ("Updated", table.get("updated_at", "")),
            ("Updated By", table.get("updated_by", "")),
            ("Comment", table.get("comment", "")),
        ]

        for prop, value in properties:
            if value:  # Only include non-empty values
                basic_info.append({"property": prop, "value": value})

        # Display basic information table
        self.console.print("\n[bold]Basic Information:[/bold]")
        display_table(
            console=self.console,
            data=basic_info,
            columns=["property", "value"],
            headers=["Property", "Value"],
            show_lines=False,
        )

        # Display columns if available
        columns_data = table.get("columns", [])
        if columns_data:
            # Prepare column data
            columns_for_display = []
            for column in columns_data:
                columns_for_display.append(
                    {
                        "name": column.get("name", ""),
                        "type": column.get("type_text", column.get("type", "")),
                        "nullable": "Yes" if column.get("nullable", False) else "No",
                        "comment": column.get("comment", ""),
                    }
                )

            # Display columns table
            self.console.print("\n[bold]Columns:[/bold]")
            display_table(
                console=self.console,
                data=columns_for_display,
                columns=["name", "type", "nullable", "comment"],
                headers=["Name", "Type", "Nullable", "Comment"],
                show_lines=False,
            )

        # Display properties if available
        properties_data = table.get("properties", {})
        if properties_data:
            # Prepare properties data
            props_for_display = []
            for prop, value in properties_data.items():
                # Skip empty values
                if value is None or value == "":
                    continue

                props_for_display.append({"property": prop, "value": value})

            # Display properties table
            self.console.print("\n[bold]Table Properties:[/bold]")
            display_table(
                console=self.console,
                data=props_for_display,
                columns=["property", "value"],
                headers=["Property", "Value"],
                show_lines=False,
            )

        # Display Delta metadata if available
        if has_delta_metadata and "delta" in table:
            delta_info = table.get("delta", {})

            # Prepare Delta metadata data
            delta_for_display = []
            delta_properties = [
                ("Format", delta_info.get("format", "")),
                ("ID", delta_info.get("id", "")),
                ("Last Updated", delta_info.get("last_updated", "")),
                ("Min Reader Version", delta_info.get("min_reader_version", "")),
                ("Min Writer Version", delta_info.get("min_writer_version", "")),
                ("Num Files", delta_info.get("num_files", "")),
                ("Size (Bytes)", delta_info.get("size_in_bytes", "")),
            ]

            for prop, value in delta_properties:
                if value:  # Only include non-empty values
                    delta_for_display.append({"property": prop, "value": value})

            # Display Delta metadata table
            if delta_for_display:  # Only if we have data to show
                self.console.print("\n[bold]Delta Metadata:[/bold]")
                display_table(
                    console=self.console,
                    data=delta_for_display,
                    columns=["property", "value"],
                    headers=["Property", "Value"],
                    show_lines=False,
                )

    def _display_catalog_details(self, data: Dict[str, Any]) -> None:
        """Display detailed information for a specific catalog."""
        from chuck_data.ui.table_formatter import display_table

        catalog = data

        if not catalog:
            self.console.print(
                f"[{WARNING_STYLE}]No catalog details available.[/{WARNING_STYLE}]"
            )
            return

        # Display catalog header
        catalog_name = catalog.get("name", "Unknown")
        self.console.print(
            f"\n[{TABLE_TITLE_STYLE}]Catalog Details: {catalog_name}[/{TABLE_TITLE_STYLE}]"
        )

        # Prepare basic information data
        basic_info = []
        properties = [
            ("Name", catalog.get("name", "")),
            ("Type", catalog.get("type", "")),
            ("Comment", catalog.get("comment", "")),
            ("Provider", catalog.get("provider", {}).get("name", "")),
            ("Storage Root", catalog.get("storage_root", "")),
            ("Storage Location", catalog.get("storage_location", "")),
            ("Owner", catalog.get("owner", "")),
            ("Created At", catalog.get("created_at", "")),
            ("Created By", catalog.get("created_by", "")),
            ("Options", str(catalog.get("options", {}))),
        ]

        for prop, value in properties:
            if value:  # Only include non-empty values
                basic_info.append({"property": prop, "value": value})

        # Display basic information table
        display_table(
            console=self.console,
            data=basic_info,
            columns=["property", "value"],
            headers=["Property", "Value"],
            show_lines=False,
        )

    def _display_schema_details(self, data: Dict[str, Any]) -> None:
        """Display detailed information for a specific schema."""
        from chuck_data.ui.table_formatter import display_table

        schema = data

        if not schema:
            self.console.print(
                f"[{WARNING_STYLE}]No schema details available.[/{WARNING_STYLE}]"
            )
            return

        # Display schema header
        schema_name = schema.get("name", "Unknown")
        catalog_name = schema.get("catalog_name", "Unknown")
        full_name = f"{catalog_name}.{schema_name}"
        self.console.print(
            f"\n[{TABLE_TITLE_STYLE}]Schema Details: {full_name}[/{TABLE_TITLE_STYLE}]"
        )

        # Prepare basic information data
        basic_info = []
        properties = [
            ("Name", schema.get("name", "")),
            ("Full Name", schema.get("full_name", "")),
            ("Catalog Name", schema.get("catalog_name", "")),
            ("Comment", schema.get("comment", "")),
            ("Storage Root", schema.get("storage_root", "")),
            ("Storage Location", schema.get("storage_location", "")),
            ("Owner", schema.get("owner", "")),
            ("Created At", schema.get("created_at", "")),
            ("Created By", schema.get("created_by", "")),
        ]

        for prop, value in properties:
            if value:  # Only include non-empty values
                basic_info.append({"property": prop, "value": value})

        # Display basic information table
        display_table(
            console=self.console,
            data=basic_info,
            columns=["property", "value"],
            headers=["Property", "Value"],
            show_lines=False,
        )

    def _display_pii_scan_results(self, data: Dict[str, Any]) -> None:
        """Display PII scan results for tables in a schema."""
        from chuck_data.ui.table_formatter import display_table

        if not data:
            self.console.print(
                f"[{WARNING_STYLE}]No PII scan results available.[/{WARNING_STYLE}]"
            )
            return

        catalog_name = data.get("catalog", "Unknown")
        schema_name = data.get("schema", "Unknown")
        results_detail = data.get("results_detail", [])
        tables_with_pii = data.get("tables_with_pii", 0)
        total_pii_columns = data.get("total_pii_columns", 0)

        # Display summary header
        self.console.print(
            f"\n[{TABLE_TITLE_STYLE}]PII Scan Results: {catalog_name}.{schema_name}[/{TABLE_TITLE_STYLE}]"
        )
        self.console.print(
            f"Found {tables_with_pii} tables with a total of {total_pii_columns} PII columns."
        )

        # If no details, exit early
        if not results_detail:
            self.console.print(
                f"[{WARNING_STYLE}]No detailed scan results available.[/{WARNING_STYLE}]"
            )
            return

        # Prepare table data for display - just tables with PII
        tables_with_pii_data = []
        for table_result in results_detail:
            if not table_result.get("skipped", False) and table_result.get(
                "has_pii", False
            ):
                # Format data for display
                table_data = {
                    "name": table_result.get("table_name", ""),
                    "full_name": table_result.get("full_name", ""),
                    "pii_columns_count": table_result.get("pii_column_count", 0),
                    "total_columns": table_result.get("column_count", 0),
                }
                tables_with_pii_data.append(table_data)

        # Sort by PII column count, most PII columns first
        tables_with_pii_data.sort(
            key=lambda x: x.get("pii_columns_count", 0), reverse=True
        )

        # Display tables with PII
        if tables_with_pii_data:
            self.console.print("\n[bold]Tables with PII:[/bold]")
            display_table(
                console=self.console,
                data=tables_with_pii_data,
                columns=["name", "full_name", "pii_columns_count", "total_columns"],
                headers=["Table Name", "Full Name", "PII Columns", "Total Columns"],
                title="Tables with PII",
                title_style=TABLE_TITLE_STYLE,
                show_lines=True,
            )

            # For each table with PII, display the PII columns
            for table_result in results_detail:
                if not table_result.get("skipped", False) and table_result.get(
                    "has_pii", False
                ):
                    table_name = table_result.get("table_name", "")
                    pii_columns = table_result.get("pii_columns", [])

                    if pii_columns:
                        self.console.print(
                            f"\n[bold]PII Columns in {table_name}:[/bold]"
                        )

                        # Prepare column data
                        column_data = []
                        for col in pii_columns:
                            column_data.append(
                                {
                                    "name": col.get("name", ""),
                                    "type": col.get("type", ""),
                                    "semantic": col.get("semantic", ""),
                                }
                            )

                        # Display column data
                        display_table(
                            console=self.console,
                            data=column_data,
                            columns=["name", "type", "semantic"],
                            headers=["Column Name", "Data Type", "PII Type"],
                            show_lines=False,
                        )
        else:
            self.console.print(
                f"[{WARNING_STYLE}]No tables with PII columns found.[/{WARNING_STYLE}]"
            )

    def _display_sql_results(self, data: Dict[str, Any]) -> None:
        """Display SQL query results in a formatted table."""
        from chuck_data.ui.table_formatter import display_table
        from chuck_data.exceptions import PaginationCancelled

        if not data:
            self.console.print(
                f"[{WARNING_STYLE}]No SQL results available.[/{WARNING_STYLE}]"
            )
            return

        columns = data.get("columns", [])
        rows = data.get("rows", [])
        row_count = data.get("row_count", 0)
        execution_time = data.get("execution_time_ms")

        if not rows:
            self.console.print(
                f"[{WARNING_STYLE}]Query returned no results.[/{WARNING_STYLE}]"
            )
            return

        # Check if we should paginate - either external links OR > 50 rows
        should_paginate = (
            data.get("is_paginated", False)  # External links case
            or len(rows) > 50  # Large result set in data_array
        )

        if should_paginate:
            self._display_paginated_sql_results_local(data)
            return

        # Small result set - display normally
        # Convert rows (list of lists) to list of dictionaries for display_table
        formatted_data = []
        for row in rows:
            row_dict = {}
            for i, value in enumerate(row):
                if i < len(columns):
                    row_dict[columns[i]] = value if value is not None else ""
            formatted_data.append(row_dict)

        # Create title with execution info
        title = f"SQL Query Results ({row_count} rows"
        if execution_time is not None:
            title += f", {execution_time}ms"
        title += ")"

        # Display the results table
        display_table(
            console=self.console,
            data=formatted_data,
            columns=columns,
            headers=columns,
            title=title,
            title_style=TABLE_TITLE_STYLE,
            show_lines=True,
        )

        # Raise PaginationCancelled to return to chuck > prompt immediately
        # This prevents agent from continuing processing after SQL display is complete
        raise PaginationCancelled()

    def _display_sql_results_formatted(self, data: Dict[str, Any]) -> None:
        """Display SQL query results from the original command result data."""
        # Since we now pass original data to TUI, we can use the regular display method
        self._display_sql_results(data)

    def _display_paginated_sql_results(self, data: Dict[str, Any]) -> None:
        """Display paginated SQL query results with interactive navigation."""
        import sys
        from chuck_data.commands.sql_external_data import PaginatedSQLResult
        from chuck_data.ui.table_formatter import display_table
        from chuck_data.exceptions import PaginationCancelled

        columns = data.get("columns", [])
        external_links = data.get("external_links", [])
        total_row_count = data.get("total_row_count", 0)
        chunks = data.get("chunks", [])
        execution_time = data.get("execution_time_ms")

        if not external_links:
            self.console.print(
                f"[{WARNING_STYLE}]No external data links available.[/{WARNING_STYLE}]"
            )
            return

        # Initialize paginated result handler
        paginated_result = PaginatedSQLResult(
            columns=columns,
            external_links=external_links,
            total_row_count=total_row_count,
            chunks=chunks,
        )

        rows_displayed = 0
        page_num = 1

        try:
            while True:
                # Get the next page of data
                try:
                    rows, has_more = paginated_result.get_next_page()
                except Exception as e:
                    self.console.print(
                        f"[{ERROR_STYLE}]Error fetching data: {str(e)}[/{ERROR_STYLE}]"
                    )
                    break

                if not rows and rows_displayed == 0:
                    self.console.print(
                        f"[{WARNING_STYLE}]Query returned no results.[/{WARNING_STYLE}]"
                    )
                    break

                if rows:
                    # Convert rows to the format expected by display_table
                    formatted_data = []
                    for row in rows:
                        row_dict = {}
                        for i, value in enumerate(row):
                            if i < len(columns):
                                row_dict[columns[i]] = (
                                    value if value is not None else ""
                                )
                        formatted_data.append(row_dict)

                    # Create title with pagination info
                    start_row = rows_displayed + 1
                    end_row = rows_displayed + len(rows)
                    title = f"SQL Query Results (Rows {start_row}-{end_row} of {total_row_count}"
                    if execution_time is not None and page_num == 1:
                        title += f", {execution_time}ms"
                    title += ")"

                    # Display the current page
                    display_table(
                        console=self.console,
                        data=formatted_data,
                        columns=columns,
                        headers=columns,
                        title=title,
                        title_style=TABLE_TITLE_STYLE,
                        show_lines=True,
                    )

                    rows_displayed += len(rows)
                    page_num += 1

                # Check if there are more pages
                if not has_more:
                    self.console.print(
                        f"\n[{INFO_STYLE}]End of results ({total_row_count} total rows)[/{INFO_STYLE}]"
                    )
                    # Raise PaginationCancelled to return to chuck > prompt immediately
                    raise PaginationCancelled()

                # Show pagination prompt
                self.console.print(
                    f"\n[dim]Press [bold]SPACE[/bold] for next page, or [bold]q[/bold] to quit... ({rows_displayed}/{total_row_count} rows shown)[/dim]"
                )

                # Get user input
                try:
                    if sys.stdin.isatty():
                        import readchar

                        char = readchar.readchar()

                        # Handle user input
                        if char.lower() == "q":
                            raise PaginationCancelled()
                        elif char == " ":
                            # Continue to next page
                            self.console.print()  # Add spacing
                            continue
                        else:
                            # Invalid input, show help
                            self.console.print(
                                f"[{WARNING_STYLE}]Press SPACE for next page or 'q' to quit[/{WARNING_STYLE}]"
                            )
                            continue
                    else:
                        # Not a TTY (e.g., running in a script), auto-continue
                        self.console.print(
                            "[dim]Auto-continuing (not in interactive terminal)...[/dim]"
                        )
                        continue

                except (KeyboardInterrupt, EOFError):
                    raise  # Re-raise to bubble up to main TUI loop
                except Exception as e:
                    if isinstance(e, PaginationCancelled):
                        raise  # Re-raise PaginationCancelled to bubble up
                    self.console.print(
                        f"\n[{ERROR_STYLE}]Input error: {str(e)}[/{ERROR_STYLE}]"
                    )
                    # Fall back to regular input
                    try:
                        response = (
                            input(
                                "[dim]Type 'q' to quit or press ENTER to continue: [/dim]"
                            )
                            .strip()
                            .lower()
                        )
                        if response == "q":
                            raise PaginationCancelled()
                        else:
                            continue
                    except (KeyboardInterrupt, EOFError):
                        raise  # Re-raise to bubble up to main TUI loop

        except Exception as e:
            if isinstance(e, PaginationCancelled):
                raise  # Re-raise PaginationCancelled to bubble up
            self.console.print(
                f"[{ERROR_STYLE}]Error during pagination: {str(e)}[/{ERROR_STYLE}]"
            )

    def _display_paginated_sql_results_local(self, data: Dict[str, Any]) -> None:
        """Display paginated SQL query results with interactive navigation for local data."""
        import sys
        from chuck_data.ui.table_formatter import display_table
        from chuck_data.exceptions import PaginationCancelled

        columns = data.get("columns", [])
        rows = data.get("rows", [])
        execution_time = data.get("execution_time_ms")
        total_rows = len(rows)

        # Check if this has external links (true pagination) or local rows (chunked display)
        if data.get("is_paginated", False) and data.get("external_links"):
            # Use the existing external links pagination
            self._display_paginated_sql_results(data)
            return

        # Local pagination for large row sets
        page_size = 50
        current_position = 0
        page_num = 1

        try:
            while current_position < total_rows:
                # Get current page of rows
                end_position = min(current_position + page_size, total_rows)
                page_rows = rows[current_position:end_position]

                if page_rows:
                    # Convert rows to the format expected by display_table
                    formatted_data = []
                    for row in page_rows:
                        row_dict = {}
                        for i, value in enumerate(row):
                            if i < len(columns):
                                row_dict[columns[i]] = (
                                    value if value is not None else ""
                                )
                        formatted_data.append(row_dict)

                    # Create title with pagination info
                    start_row = current_position + 1
                    title = f"SQL Query Results (Rows {start_row}-{end_position} of {total_rows}"
                    if execution_time is not None and page_num == 1:
                        title += f", {execution_time}ms"
                    title += ")"

                    # Display the current page
                    display_table(
                        console=self.console,
                        data=formatted_data,
                        columns=columns,
                        headers=columns,
                        title=title,
                        title_style=TABLE_TITLE_STYLE,
                        show_lines=True,
                    )

                    current_position = end_position
                    page_num += 1

                # Check if there are more pages
                if current_position >= total_rows:
                    self.console.print(
                        f"\n[{INFO_STYLE}]End of results ({total_rows} total rows)[/{INFO_STYLE}]"
                    )
                    # Raise PaginationCancelled to return to chuck > prompt immediately
                    raise PaginationCancelled()

                # Show pagination prompt
                self.console.print(
                    f"\n[dim]Press [bold]SPACE[/bold] for next page, or [bold]q[/bold] to quit... ({current_position}/{total_rows} rows shown)[/dim]"
                )

                # Get user input
                try:
                    if sys.stdin.isatty():
                        import readchar

                        char = readchar.readchar()

                        # Handle user input
                        if char.lower() == "q":
                            raise PaginationCancelled()
                        elif char == " ":
                            # Continue to next page
                            self.console.print()  # Add spacing
                            continue
                        else:
                            # Invalid input, show help
                            self.console.print(
                                f"[{WARNING_STYLE}]Press SPACE for next page or 'q' to quit[/{WARNING_STYLE}]"
                            )
                            continue
                    else:
                        # Not a TTY (e.g., running in a script), auto-continue
                        self.console.print(
                            "[dim]Auto-continuing (not in interactive terminal)...[/dim]"
                        )
                        continue

                except (KeyboardInterrupt, EOFError):
                    raise  # Re-raise to bubble up to main TUI loop
                except Exception as e:
                    if isinstance(e, PaginationCancelled):
                        raise  # Re-raise PaginationCancelled to bubble up
                    self.console.print(
                        f"\n[{ERROR_STYLE}]Input error: {str(e)}[/{ERROR_STYLE}]"
                    )
                    # Fall back to regular input
                    try:
                        response = (
                            input(
                                "[dim]Type 'q' to quit or press ENTER to continue: [/dim]"
                            )
                            .strip()
                            .lower()
                        )
                        if response == "q":
                            raise PaginationCancelled()
                        else:
                            continue
                    except (KeyboardInterrupt, EOFError):
                        raise  # Re-raise to bubble up to main TUI loop

        except Exception as e:
            if isinstance(e, PaginationCancelled):
                raise  # Re-raise PaginationCancelled to bubble up
            self.console.print(
                f"[{ERROR_STYLE}]Error during pagination: {str(e)}[/{ERROR_STYLE}]"
            )
