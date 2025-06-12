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

            # Use the view registry to route display requests
            from chuck_data.ui.view_registry import get_view
            
            # Try to get the appropriate view from the registry
            cmd_without_slash = cmd[1:] if cmd.startswith("/") else cmd  # Remove leading slash
            
            # Command name mappings to standardize the lookup
            cmd_mapping = {
                "catalogs": "list-catalogs",
                "search_catalogs": "list-catalogs",
                "schemas": "list-schemas",
                "search_schemas": "list-schemas",
                "tables": "list-tables",
                "search_tables": "list-tables",
                "catalog": "catalog-details",
                "schema": "schema-details",
                "models": "list-models",
                "warehouses": "list-warehouses",
                "volumes": "list-volumes", 
                "table": "table-details",
                "show_table": "table-details",
                "run-sql": "sql",
            }
            
            # Standardize command name for lookup
            standard_cmd = cmd_mapping.get(cmd_without_slash, cmd_without_slash)
            view_cls = get_view(standard_cmd)
            
            if view_cls:
                # Use the view class to render the data
                try:
                    view_cls(self.console).render(result.data)
                    return  # View handled the display
                except Exception as e:
                    logging.warning(f"Failed to render view for {cmd}: {e}")
            
            # Fallback for commands without views - handle special cases
            if cmd == "/auth" and "permissions" in result.data:
                self._display_permissions(result.data["permissions"])
            elif cmd in ["/run-sql", "/sql"]:
                self._display_sql_results(result.data)
            elif cmd == "/scan-pii":
                self._display_pii_scan_results(result.data)
            elif cmd == "/usage":
                # For the usage command, we just display the message
                if result.message:
                    self._display_usage(result.message)
            elif (
                cmd.startswith("/help")
                and isinstance(result.data, dict)
                and "help_text" in result.data
            ):
                # Custom display for help text with special formatting
                self.console.print(
                    Panel(
                        result.data["help_text"],
                        title="CHUCK AI Help",
                        border_style="cyan",
                    )
                )
            elif (
                cmd.startswith("/getting-started")
                or cmd.startswith("/examples")
                or cmd.startswith("/tips")
            ):
                # Custom display for getting started tips with special formatting
                self.console.print(
                    Panel(
                        result.data["getting_started_text"],
                        title="Getting Started with Chuck",
                        border_style="cyan",
                    )
                )
            elif (
                (cmd.startswith("/support") or cmd.startswith("/help-me"))
                and isinstance(result.data, dict)
                and "support_text" in result.data
            ):
                # Custom display for support options with special formatting
                self.console.print(
                    Panel(
                        result.data["support_text"],
                        title="Chuck Support Options",
                        border_style="cyan",
                    )
                )
            elif (
                cmd.startswith("/discord")
                and isinstance(result.data, dict)
                and "discord_message" in result.data
                and "discord_url" in result.data
                and result.data.get("prompt_open_browser", False)
            ):
                # Custom display for Discord invitation
                self.console.print(
                    Panel(
                        result.data["discord_message"],
                        title="Join Chuck on Discord",
                        border_style="cyan",
                    )
                )

                # Prompt for browser opening
                try:
                    response = input("> ").strip().lower()
                    if response in ["y", "yes"]:
                        discord_url = result.data["discord_url"]
                        self.console.print(
                            f"[{INFO_STYLE}]Opening Discord invite link in browser...[/{INFO_STYLE}]"
                        )
                        import webbrowser

                        webbrowser.open(discord_url)
                except Exception as e:
                    self.console.print(
                        f"[{ERROR_STYLE}]Error opening browser: {str(e)}[/{ERROR_STYLE}]"
                    )
                    logging.error(
                        f"Error opening browser for Discord: {e}", exc_info=True
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
        """Display full detailed tool output using view registry when available."""
        from chuck_data.ui.view_registry import get_view
        
        # Map legacy tool names to their updated equivalents for routing
        tool_name_mapping = {
            "get_catalog_details": "catalog",
            "show_table": "table",
            "scan_schema_for_pii": "scan-pii",
            "scan_pii": "scan-pii",
        }
        
        # Apply tool name mapping if needed
        mapped_tool_name = tool_name_mapping.get(tool_name, tool_name)
        
        # Try to get the view class from registry
        view_cls = get_view(mapped_tool_name)
        
        if view_cls:
            # Use registered view class
            view_cls(self.console).render(tool_result)
            return
        
        # Fallback for tools without registered views - display as JSON panel
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
            if tool_name in ["status", "get_status"]:
                if "workspace_url" in tool_result and tool_result["workspace_url"]:
                    # Extract just the hostname from workspace URL for brevity
                    workspace_url = tool_result["workspace_url"]
                    try:
                        from urllib.parse import urlparse

                        hostname = urlparse(workspace_url).hostname or workspace_url
                        metrics.append(f"workspace: {hostname}")
                    except Exception:
                        metrics.append(f"workspace: {workspace_url}")

                # Show connection status if it indicates an issue
                connection_status = tool_result.get("connection_status", "")
                if (
                    "error" in connection_status.lower()
                    or "not" in connection_status.lower()
                ):
                    metrics.append("connection issue")

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

    # >>> NEW SHIM using view layer
    def _display_catalogs(self, data: Dict[str, Any]) -> None:
        """Shim: delegate to CatalogsTableView for backwards compatibility."""
        from chuck_data.ui.views.catalogs import CatalogsTableView
        CatalogsTableView(self.console).render(data)


    # >>> NEW SHIM for schemas view
    def _display_schemas(self, data: Dict[str, Any]) -> None:
        """Shim: delegate to SchemasTableView for backwards compatibility."""
        from chuck_data.ui.views.schemas import SchemasTableView
        SchemasTableView(self.console).render(data)


    # >>> NEW SHIM for tables view
    def _display_tables(self, data: Dict[str, Any]) -> None:
        from chuck_data.ui.views.tables import TablesTableView
        TablesTableView(self.console).render(data)



    def _display_models_consolidated(self, data: Dict[str, Any]) -> None:
        """Shim delegate to ModelsTableView."""
        from chuck_data.ui.views.models import ModelsTableView
        ModelsTableView(self.console).render(data)

    def _display_warehouses(self, data: Dict[str, Any]) -> None:
        """Shim delegate to WarehousesTableView."""
        from chuck_data.ui.views.warehouses import WarehousesTableView
        WarehousesTableView(self.console).render(data)

    def _display_volumes(self, data: Dict[str, Any]) -> None:
        """Shim delegate to VolumesTableView."""
        from chuck_data.ui.views.volumes import VolumesTableView
        VolumesTableView(self.console).render(data)

    def _display_status(self, data: Dict[str, Any]) -> None:
        """Shim delegate to StatusTableView."""
        from chuck_data.ui.views.status import StatusTableView
        StatusTableView(self.console).render(data)

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

            # For each table with PII, display all columns with PII indicators
            for table_result in results_detail:
                if not table_result.get("skipped", False) and table_result.get(
                    "has_pii", False
                ):
                    table_name = table_result.get("table_name", "")
                    all_columns = table_result.get("columns", [])

                    if all_columns:
                        self.console.print(f"\n[bold]Columns in {table_name}:[/bold]")

                        # Prepare column data - show all columns with PII indicators
                        column_data = []
                        for col in all_columns:
                            pii_type = col.get("semantic", "")
                            column_data.append(
                                {
                                    "name": col.get("name", ""),
                                    "type": col.get("type", ""),
                                    "semantic": pii_type if pii_type else "",
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
