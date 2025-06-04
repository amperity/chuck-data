"""
Service layer for Chuck application.
Provides a facade for all business operations needed by the UI.
"""

import json
import logging
import jsonschema
import traceback
from typing import Dict, Optional, Any, Tuple

from src.clients.databricks import DatabricksAPIClient
from src.commands.base import CommandResult
from src.command_registry import get_command, CommandDefinition
from src.config import (
    get_workspace_url,
    get_databricks_token,
)
from src.metrics_collector import get_metrics_collector


class ChuckService:
    """Service layer that provides a clean API for the UI to interact with business logic."""

    def __init__(self, client: Optional[DatabricksAPIClient] = None):
        """
        Initialize the service with an optional client.
        If no client is provided, it attempts to initialize one from config.
        """
        self.client = client
        self.init_error: Optional[str] = None  # Store initialization error message
        if not self.client:
            try:
                token = get_databricks_token()
                workspace_url = get_workspace_url()
                if token and workspace_url:  # Only initialize if both are present
                    self.client = DatabricksAPIClient(workspace_url, token)
                elif not workspace_url:
                    self.init_error = "Workspace URL not configured."
                elif not token:
                    self.init_error = "Databricks token not configured."
                # else: both are missing, client remains None, init_error remains None (or set explicitly)

            except Exception as e:
                logging.error(
                    f"Error initializing DatabricksAPIClient in ChuckService: {e}",
                    exc_info=True,
                )
                self.client = None
                self.init_error = f"Client initialization failed: {str(e)}"

    def _parse_and_validate_tui_args(
        self,
        command_def: CommandDefinition,
        raw_args: Tuple[str, ...],
        raw_kwargs: Dict[
            str, Any
        ],  # For potential future use if TUI supports named args
    ) -> Tuple[Optional[Dict[str, Any]], Optional[CommandResult]]:
        """
        Parses raw string arguments from the TUI, converts types based on JSON schema,
        applies defaults, and validates against the command's schema.

        Args:
            command_def: The CommandDefinition for the command.
            raw_args: A tuple of raw string arguments from the TUI.
            raw_kwargs: A dictionary of raw keyword arguments (currently unused by TUI).

        Returns:
            A tuple: (parsed_args_dict, None) if successful,
                     (None, error_command_result) if parsing/validation fails.
        """
        parsed_kwargs: Dict[str, Any] = {}
        param_definitions = command_def.parameters or {}
        param_names_ordered = list(
            param_definitions.keys()
        )  # Assumes order is defined or consistent

        # 1. Handle special TUI argument packing (e.g., for agent query, upload content)
        #    This might need more sophisticated logic or command-specific metadata if complex.
        #    For now, assuming direct mapping or simple concatenation for specific commands.

        if (command_def.name == "agent_query" or command_def.name == "agent") and len(
            raw_args
        ) > 0:
            # Join all raw_args into a single 'query' string
            # This assumes the command has a parameter named 'query' in its schema.
            if "query" in param_definitions:
                query_text = " ".join(raw_args)
                parsed_kwargs["query"] = query_text
                # Store the joined text as a "rest" parameter too
                parsed_kwargs["rest"] = query_text
                # We don't set raw_args explicitly here, as the command handler will get
                # the original raw_args directly from the function call
                raw_args = ()  # Consumed all raw_args
            else:
                return None, CommandResult(
                    False,
                    message=f"Command '{command_def.name}' is misconfigured: expected 'query' parameter.",
                )

        elif command_def.name == "bug" and len(raw_args) > 0:
            # Join all raw_args into a single 'description' string
            if "description" in param_definitions:
                description_text = " ".join(raw_args)
                parsed_kwargs["description"] = description_text
                # Store the joined text as a "rest" parameter too
                parsed_kwargs["rest"] = description_text
                raw_args = ()  # Consumed all raw_args
            else:
                # Fallback to rest parameter
                parsed_kwargs["rest"] = " ".join(raw_args)
                raw_args = ()

        elif command_def.name == "add_stitch_report" and len(raw_args) > 0:
            # First arg is table_path, rest is notebook name
            if len(raw_args) >= 1:
                parsed_kwargs["table_path"] = raw_args[0]
                if len(raw_args) > 1:
                    # Join remaining arguments as notebook name
                    name_text = " ".join(raw_args[1:])
                    parsed_kwargs["name"] = name_text
                    # Store joined text as rest parameter too
                    parsed_kwargs["rest"] = name_text
                raw_args = ()  # Consumed all raw_args

        elif command_def.name == "upload_file" and len(raw_args) >= 1:
            # First arg is filename, rest is content (joined)
            # Assumes parameters "filename" and "content"
            if "filename" in param_definitions and "content" in param_definitions:
                parsed_kwargs["filename"] = raw_args[0]
                if len(raw_args) > 1:
                    parsed_kwargs["content"] = " ".join(raw_args[1:])
                else:
                    # Content might be optional or handled by schema default/validation
                    # If content is required and not provided, schema validation should catch it.
                    # Or, if it can be truly empty: parsed_kwargs["content"] = ""
                    pass  # Let schema validation handle if content is missing but required
                raw_args = ()  # Consumed all raw_args
            else:
                return None, CommandResult(
                    False,
                    message=f"Command '{command_def.name}' is misconfigured: expected 'filename' and 'content' parameters.",
                )

        # 2. Parse arguments - support both positional and flag-style arguments
        remaining_args = list(raw_args)

        # First, parse flag-style arguments (--flag value)
        i = 0
        while i < len(remaining_args):
            arg = remaining_args[i]
            if arg.startswith("--"):
                # Flag-style argument
                flag_name = arg[2:]  # Remove '--' prefix
                if flag_name in param_definitions:
                    # Check if we have a value for this flag
                    if i + 1 < len(remaining_args) and not remaining_args[
                        i + 1
                    ].startswith("--"):
                        flag_value = remaining_args[i + 1]
                        parsed_kwargs[flag_name] = flag_value
                        # Remove both flag and value from remaining args
                        remaining_args.pop(i)  # Remove flag
                        remaining_args.pop(
                            i
                        )  # Remove value (index shifts after first pop)
                        continue
                    else:
                        # Flag without value - could be boolean flag
                        if (
                            param_definitions.get(flag_name, {}).get("type")
                            == "boolean"
                        ):
                            parsed_kwargs[flag_name] = True
                            remaining_args.pop(i)
                            continue
                        else:
                            usage = (
                                command_def.usage_hint
                                or f"Correct usage for '{command_def.name}' not available."
                            )
                            return None, CommandResult(
                                False,
                                message=f"Flag '--{flag_name}' requires a value. {usage}",
                            )
                else:
                    usage = (
                        command_def.usage_hint
                        or f"Correct usage for '{command_def.name}' not available."
                    )
                    return None, CommandResult(
                        False,
                        message=f"Unknown flag '--{flag_name}' for command '{command_def.name}'. {usage}",
                    )
            i += 1

        # Then, map remaining positional arguments to parameter names
        for i, arg_val_str in enumerate(remaining_args):
            if i < len(param_names_ordered):
                param_name = param_names_ordered[i]
                # If special handling above or flag parsing already populated this, skip to avoid overwrite
                if param_name not in parsed_kwargs:
                    parsed_kwargs[param_name] = (
                        arg_val_str  # Store as string for now, type conversion next
                    )
            else:
                # Too many positional arguments provided
                usage = (
                    command_def.usage_hint
                    or f"Correct usage for '{command_def.name}' not available."
                )
                return None, CommandResult(
                    False,
                    message=f"Too many arguments for command '{command_def.name}'. {usage}",
                )

        # 3. Type conversion (string from TUI to schema-defined type) & apply defaults
        final_args_for_validation: Dict[str, Any] = {}
        for param_name, schema_prop in param_definitions.items():
            param_type = schema_prop.get("type")
            default_value = schema_prop.get("default")

            if param_name in parsed_kwargs:
                raw_value = parsed_kwargs[param_name]
                try:
                    if param_type == "integer":
                        final_args_for_validation[param_name] = int(raw_value)
                    elif param_type == "number":  # JSON schema 'number' can be float
                        final_args_for_validation[param_name] = float(raw_value)
                    elif param_type == "boolean":
                        # Handle common boolean strings, jsonschema might do this too
                        if isinstance(raw_value, str):
                            val_lower = raw_value.lower()
                            if val_lower in ["true", "t", "yes", "y", "1"]:
                                final_args_for_validation[param_name] = True
                            elif val_lower in ["false", "f", "no", "n", "0"]:
                                final_args_for_validation[param_name] = False
                            else:
                                # Let jsonschema validation catch this if type is strict
                                final_args_for_validation[param_name] = raw_value
                        else:  # Already a bool (e.g. from default or previous step)
                            final_args_for_validation[param_name] = bool(raw_value)
                    elif param_type == "array" or (
                        isinstance(param_type, list) and "array" in param_type
                    ):
                        # Support both JSON array format and comma-separated strings
                        if isinstance(raw_value, str):
                            # Try to parse as JSON first
                            if raw_value.strip().startswith(
                                "["
                            ) and raw_value.strip().endswith("]"):
                                try:
                                    final_args_for_validation[param_name] = json.loads(
                                        raw_value
                                    )
                                except json.JSONDecodeError as je:
                                    usage = (
                                        command_def.usage_hint
                                        or f"Check help for '{command_def.name}'."
                                    )
                                    return None, CommandResult(
                                        False,
                                        message=f"Invalid JSON array format for '{param_name}': {str(je)}. {usage}",
                                    )
                            else:
                                # Fall back to comma-separated strings
                                final_args_for_validation[param_name] = [
                                    s.strip() for s in raw_value.split(",") if s.strip()
                                ]
                        elif isinstance(
                            raw_value, (list, tuple)
                        ):  # Already a list or tuple
                            final_args_for_validation[param_name] = list(raw_value)
                        else:
                            return None, CommandResult(
                                False,
                                message=f"Invalid array format for '{param_name}'. Expected JSON array or comma-separated string.",
                            )
                    elif param_type == "string":
                        final_args_for_validation[param_name] = str(raw_value)
                    else:  # Unknown type or type not requiring conversion from string (e.g. already processed)
                        final_args_for_validation[param_name] = raw_value
                except ValueError:
                    usage = (
                        command_def.usage_hint
                        or f"Check help for '{command_def.name}'."
                    )
                    return None, CommandResult(
                        False,
                        message=f"Invalid value for '{param_name}': '{raw_value}'. Expected type '{param_type}'. {usage}",
                    )
            elif default_value is not None:
                final_args_for_validation[param_name] = default_value
            # If not in parsed_kwargs and no default, it's either optional or will be caught by 'required' validation

        # 4. Incorporate raw_kwargs if TUI ever supports named args directly (e.g. /cmd --option val)
        #    For now, this is a placeholder.
        # final_args_for_validation.update(raw_kwargs) # If raw_kwargs were typed and validated

        # 5. Validate against the full JSON schema
        full_schema_for_validation = {
            "type": "object",
            "properties": param_definitions,
            "required": command_def.required_params or [],
        }
        try:
            jsonschema.validate(
                instance=final_args_for_validation, schema=full_schema_for_validation
            )
        except jsonschema.exceptions.ValidationError as ve:
            usage = (
                command_def.usage_hint or f"Use /help {command_def.name} for details."
            )
            # More detailed error: ve.message, ve.path, ve.schema_path
            error_path = " -> ".join(map(str, ve.path)) if ve.path else "argument"
            return None, CommandResult(
                False, message=f"Invalid argument '{error_path}': {ve.message}. {usage}"
            )

        return final_args_for_validation, None

    def execute_command(
        self,
        command_name_from_ui: str,
        *raw_args: str,
        interactive_input: Optional[str] = None,
        tool_output_callback: Optional[callable] = None,
        **raw_kwargs: Any,  # For future TUI use, e.g. /cmd --named_arg value
    ) -> CommandResult:
        """
        Execute a command looked up from the registry, with argument parsing and validation.
        """
        command_def = get_command(
            command_name_from_ui
        )  # Handles TUI aliases via registry

        if not command_def:
            return CommandResult(
                False,
                message=f"Unknown command: '{command_name_from_ui}'. Type /help for list.",
            )

        if not command_def.visible_to_user:
            return CommandResult(
                False,
                message=f"Command '{command_name_from_ui}' is not available for direct use.",
            )

        # Authentication Check (before argument parsing)
        effective_client = self.client
        if command_def.needs_api_client:
            # Special case for setup_wizard - always allow it to run even without client
            if command_def.name == "setup_wizard" or command_name_from_ui == "/setup":
                # Allow setup wizard to run without a client
                effective_client = None
            elif not self.client:
                error_msg = (
                    self.init_error
                    or "Client not initialized. Please set workspace URL and token (e.g. /select-workspace, /set-token, then /status or /connect)."
                )
                return CommandResult(False, message=f"Not authenticated. {error_msg}")

        parsed_args_dict: Dict[str, Any]
        args_for_handler: Dict[str, Any]

        # Interactive Mode Handling
        if command_def.supports_interactive_input:
            # For interactive commands, the `interactive_input` is the primary payload.
            # Always include the interactive_input parameter, even if None, for interactive commands
            # This prevents dropping inputs in complex interactive flows
            args_for_handler = {"interactive_input": interactive_input}
        else:
            # Standard Argument Parsing & Validation
            parsed_args_dict, error_result = self._parse_and_validate_tui_args(
                command_def, raw_args, raw_kwargs
            )
            if error_result:
                return error_result
            if (
                parsed_args_dict is None
            ):  # Should be caught by error_result, but as a safeguard
                return CommandResult(
                    False, message="Internal error during argument parsing."
                )
            args_for_handler = parsed_args_dict

        # Pass tool output callback for agent commands
        if command_def.name == "agent" and tool_output_callback:
            args_for_handler["tool_output_callback"] = tool_output_callback

        # Handler Execution
        try:
            # All handlers now expect (client, **kwargs)
            result: CommandResult = command_def.handler(
                effective_client, **args_for_handler
            )

            # Special Command Post-Processing (Example: set-token)
            if command_def.name == "databricks-login" and result.success:
                # The handler for set_token now returns data={"reinitialize_client": True} on success
                if isinstance(result.data, dict) and result.data.get(
                    "reinitialize_client"
                ):
                    logging.info("Reinitializing client after successful set-token.")
                    self.reinitialize_client()

            return result
        except Exception as e_handler:
            # Handle pagination cancellation specially - let it bubble up
            from src.exceptions import PaginationCancelled

            if isinstance(e_handler, PaginationCancelled):
                raise  # Re-raise to bubble up to main TUI loop

            logging.error(
                f"Error executing handler for command '{command_def.name}': {e_handler}",
                exc_info=True,
            )

            # Track error event
            try:
                metrics_collector = get_metrics_collector()
                # Create a context string from the command and args
                command_context = f"command: {command_name_from_ui}"
                if args_for_handler:
                    # Convert args to a simple string representation for the error report
                    args_str = ", ".join(
                        [
                            f"{k}={v}"
                            for k, v in args_for_handler.items()
                            if k != "interactive_input"
                        ]
                    )
                    if args_str:
                        command_context += f", args: {args_str}"

                metrics_collector.track_event(
                    prompt=command_context,
                    error=traceback.format_exc(),
                    tools=[{"name": command_def.name, "arguments": args_for_handler}],
                    additional_data={"event_context": "error_report"},
                )
            except Exception as metrics_error:
                # Don't let metrics collection errors affect the primary error handling
                logging.error(
                    f"Failed to track error metrics: {metrics_error}", exc_info=True
                )

            return CommandResult(
                False,
                message=f"Error during command execution: {str(e_handler)}",
                error=e_handler,
            )

    def reinitialize_client(self) -> bool:
        """
        Reinitialize the API client with current configuration.
        This should be called after settings like token or workspace URL change.
        """
        logging.info("Attempting to reinitialize DatabricksAPIClient...")
        try:
            token = get_databricks_token()
            workspace_url = get_workspace_url()
            if token and workspace_url:
                self.client = DatabricksAPIClient(workspace_url, token)
                self.init_error = None  # Clear previous init error
                logging.info("DatabricksAPIClient reinitialized successfully.")
                return True
            else:
                self.client = None
                self.init_error = "Cannot reinitialize client: Workspace URL or token missing from config."
                logging.warning(f"{self.init_error}")
                return False
        except Exception as e:
            self.client = None
            self.init_error = f"Failed to reinitialize client: {str(e)}"
            logging.error(self.init_error, exc_info=True)
            return False
