"""
Agent tool schema provider and execution logic.

This module is responsible for:
1. Providing tool schemas to the LLM agent, sourced from the command_registry.
2. Executing tools (commands) requested by the LLM agent, including:
   - Looking up the command definition from the command_registry.
   - Validating arguments provided by the LLM against the command's JSON schema.
   - Calling the appropriate command handler.
   - Returning the result (or error) in a JSON-serializable dictionary format.
"""

import logging
import jsonschema  # Requires jsonschema to be installed

from src.command_registry import get_command
from src.command_registry import (
    get_agent_tool_schemas as get_command_registry_tool_schemas,
)
from src.commands.base import (
    CommandResult,
)  # For type hinting and checking handler result
from src.clients.databricks import DatabricksAPIClient  # For type hinting api_client
from typing import Dict, Any, Optional, List


# The display_to_user utility and individual tool implementation functions
# (like list_models, set_warehouse, tag_pii_columns, scan_schema_for_pii, etc.)
# that were previously in this file have been removed.
# Their logic now resides within the modular command handlers in src.commands package.

# --- PII Scanning and Stitch Setup Functions ---
# The original agent_tools.py contained `tag_pii_columns`, `scan_schema_for_pii`, and `setup_stitch`
# which were complex and also used by command handlers.
# These functions have now been moved to their respective command modules in the src.commands package.
# For example, tag_pii_columns logic is now in src.commands.tag_pii,
# scan_schema_for_pii is in src.commands.scan_pii, and setup_stitch is in src.commands.setup_stitch.


def get_tool_schemas() -> List[Dict[str, Any]]:
    """Get all command schemas in agent tool format from the command registry."""
    return get_command_registry_tool_schemas()


def execute_tool(
    api_client: Optional[DatabricksAPIClient],
    tool_name: str,
    tool_args: Dict[str, Any],
    output_callback: Optional[callable] = None,
) -> Dict[str, Any]:
    """Execute a tool (command) by its name with the provided arguments.

    Args:
        api_client: The Databricks API client, passed to the handler if needed.
        tool_name: The name of the tool (command) to execute.
        tool_args: A dictionary of arguments for the tool, parsed from LLM JSON.

    Returns:
        A dictionary containing the result of the tool execution, suitable for
        JSON serialization and consumption by the LLM agent.
        Includes {"error": "..."} if any issues occur.
    """
    logging.debug(
        f"Agent attempting to execute tool: {tool_name} with args: {tool_args}"
    )

    command_def = get_command(tool_name)

    if not command_def:
        logging.error(f"Agent tool '{tool_name}' not found in command registry.")
        return {"error": f"Tool '{tool_name}' not found."}

    if not command_def.visible_to_agent:
        logging.warning(f"Agent attempted to call non-agent-visible tool: {tool_name}")
        return {"error": f"Tool '{tool_name}' is not available to the agent."}

    # --- Argument Validation using JSON Schema ---
    # Construct the full schema for validation, including required fields
    schema_to_validate = {
        "type": "object",
        "properties": command_def.parameters or {},
        "required": command_def.required_params or [],
    }

    try:
        jsonschema.validate(instance=tool_args, schema=schema_to_validate)
        logging.debug(f"Tool arguments for '{tool_name}' validated successfully.")
    except jsonschema.exceptions.ValidationError as ve:
        logging.error(
            f"Validation error for tool '{tool_name}' args {tool_args}: {ve.message}"
        )
        # Provide a more user-friendly error message if possible, or just the validation message
        return {
            "error": f"Invalid arguments for tool '{tool_name}': {ve.message}. Schema: {ve.schema}"
        }
    except Exception as e_schema:  # Catch other schema-related errors if any
        logging.error(
            f"Schema validation unexpected error for '{tool_name}': {e_schema}"
        )
        return {
            "error": f"Internal error during argument validation for '{tool_name}': {str(e_schema)}"
        }

    # --- Client Provisioning and Handler Execution ---
    effective_client = None
    if command_def.needs_api_client:
        if not api_client:
            logging.error(
                f"Tool '{tool_name}' requires an API client, but none was provided or initialized."
            )
            return {
                "error": f"Tool '{tool_name}' cannot be executed: API client not available."
            }
        effective_client = api_client

    try:
        # Handlers are now standardized to (client, **kwargs)
        # The command_def.handler should point to a function from command_handlers.py
        logging.debug(
            f"Executing agent tool '{tool_name}' with handler: {command_def.handler.__name__}"
        )
        result_obj: CommandResult = command_def.handler(effective_client, **tool_args)

        if not isinstance(result_obj, CommandResult):
            logging.error(
                f"Handler for '{tool_name}' did not return a CommandResult object. Got: {type(result_obj)}"
            )
            return {
                "error": f"Tool '{tool_name}' did not execute correctly (internal error: unexpected result type)."
            }

        if result_obj.success:
            logging.debug(
                f"Tool '{tool_name}' executed successfully. Data: {result_obj.data}"
            )

            # Use custom output formatter if available, otherwise return data directly
            formatted_output = None
            if command_def.output_formatter:
                try:
                    formatted_output = command_def.output_formatter(result_obj)
                except Exception as e:
                    logging.warning(
                        f"Output formatter failed for '{tool_name}': {e}, falling back to default"
                    )

            # Call output callback to display results immediately if provided
            # Always use original data for TUI display to preserve pagination capability
            if output_callback and result_obj.data is not None:
                try:
                    output_callback(tool_name, result_obj.data)
                except Exception as e:
                    # Handle pagination cancellation specially - let it bubble up
                    from src.exceptions import PaginationCancelled

                    if isinstance(e, PaginationCancelled):
                        raise  # Re-raise to bubble up to agent manager

                    logging.warning(
                        f"Tool output callback failed for '{tool_name}': {e}"
                    )

            # Return the appropriate output
            if formatted_output is not None:
                return formatted_output

            # Ensure data is JSON serializable. If not, this will be an issue for the agent.
            # The plan is for CommandResult.data to be JSON serializable.
            if result_obj.data is not None:
                return result_obj.data  # Return the data directly as per plan
            else:
                # If data is None but success is true, return a success message or an empty dict
                # The LLM might expect some JSON output.
                return {
                    "success": True,
                    "message": result_obj.message or f"Tool '{tool_name}' completed.",
                }
        else:
            logging.error(
                f"Tool '{tool_name}' execution failed. Message: {result_obj.message}, Error: {result_obj.error}"
            )
            error_payload = {
                "error": result_obj.message or f"Tool '{tool_name}' failed."
            }
            if result_obj.error and hasattr(result_obj.error, "__str__"):
                # Add more specific error details if available and simple
                error_payload["details"] = str(result_obj.error)
            return error_payload

    except Exception as e_exec:
        # Handle pagination cancellation specially - let it bubble up
        from src.exceptions import PaginationCancelled

        if isinstance(e_exec, PaginationCancelled):
            raise  # Re-raise to bubble up to agent manager

        logging.warning(
            f"Critical error executing tool '{tool_name}' handler '{command_def.handler.__name__}': {e_exec}",
            exc_info=True,
        )
        return {
            "error": f"Unexpected error during execution of tool '{tool_name}': {str(e_exec)}"
        }
