"""
Command for processing natural language queries using an LLM agent.

This module contains the handler for the agent command which processes
natural language queries and interacts with Databricks resources.
"""

import logging
from typing import Optional, Any

from src.clients.databricks import DatabricksAPIClient
from src.command_registry import CommandDefinition
from src.commands.base import CommandResult
from src.metrics_collector import get_metrics_collector


def handle_command(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """
    Process a natural language query using the LLM agent.

    Args:
        client: DatabricksAPIClient instance for API calls (optional)
        **kwargs: Command parameters
            - query: The natural language query from the user
            - mode: Optional agent mode (general, pii, bulk_pii, stitch)
            - rest: Any additional text input provided after the command
            - raw_args: Unparsed arguments (fallback when command parser fails)
            - catalog_name: Optional catalog name for context
            - schema_name: Optional schema name for context

    Returns:
        CommandResult with agent response
    """
    # First check for different ways the query might be provided
    # Priority: 1. query parameter, 2. rest parameter, 3. raw_args
    query = kwargs.get("query")
    rest = kwargs.get("rest")
    raw_args = kwargs.get("raw_args")

    # If query wasn't provided but we have rest or raw_args, use that as the query
    if not query:  # This checks if the initial kwargs.get("query") was empty/None
        if rest:
            query = rest
        elif raw_args:
            if isinstance(raw_args, (list, tuple)):
                query = " ".join(str(arg) for arg in raw_args)
            else:
                # Handle case where raw_args is a single string
                query = str(raw_args)

    # At this point, query might be a string from 'query', 'rest', 'raw_args', or still None.
    # Strip whitespace if query is a string. This handles cases like "   ".
    # If query is None, .strip() would error, so we check isinstance.
    if isinstance(query, str):
        query = query.strip()

    # Now, check if the (potentially stripped) query is truly empty or None.
    if not query:
        return CommandResult(
            False, message="Please provide a query. Usage: /ask Your question here"
        )

    # Get optional parameters
    mode = kwargs.get("mode", "general").lower()
    catalog_name = kwargs.get("catalog_name")
    schema_name = kwargs.get("schema_name")
    tool_output_callback = kwargs.get("tool_output_callback")

    try:
        from src.agent import AgentManager
        from src.config import get_agent_history, set_agent_history

        # Get metrics collector
        metrics_collector = get_metrics_collector()

        # Create agent manager with the API client and tool output callback
        agent = AgentManager(client, tool_output_callback=tool_output_callback)

        # Load conversation history
        try:
            history = get_agent_history()
        except Exception:
            history = []

        if history:
            agent.conversation_history = history

        # Process the query based on the selected mode
        if mode == "pii":
            # PII detection mode for a single table
            response = agent.process_pii_detection(
                table_name=query, catalog_name=catalog_name, schema_name=schema_name
            )
        elif mode == "bulk_pii":
            # Bulk PII scanning mode for a schema
            response = agent.process_bulk_pii_scan(
                catalog_name=catalog_name, schema_name=schema_name
            )
        elif mode == "stitch":
            # Stitch setup mode
            response = agent.process_setup_stitch(
                catalog_name=catalog_name, schema_name=schema_name
            )
        else:
            # Default general query mode
            response = agent.process_query(query)

        # Save conversation history
        set_agent_history(agent.conversation_history)

        # Track the agent interaction event
        if mode == "pii":
            # For PII detection mode
            processed_tools = [{"name": "pii_detection", "arguments": {"table": query}}]
            event_context = "agent_interaction"
            additional_data = {"event_context": event_context, "agent_mode": mode}
        elif mode == "bulk_pii":
            # For bulk PII scanning mode
            processed_tools = [
                {
                    "name": "bulk_pii_scan",
                    "arguments": {"catalog": catalog_name, "schema": schema_name},
                }
            ]
            event_context = "agent_interaction"
            additional_data = {"event_context": event_context, "agent_mode": mode}
        elif mode == "stitch":
            # For Stitch setup mode
            processed_tools = [
                {
                    "name": "setup_stitch",
                    "arguments": {"catalog": catalog_name, "schema": schema_name},
                }
            ]
            event_context = "agent_interaction"
            additional_data = {"event_context": event_context, "agent_mode": mode}
        else:
            # For general query mode
            processed_tools = [{"name": "general_query", "arguments": {"query": query}}]
            event_context = "agent_interaction"
            additional_data = {"event_context": event_context, "agent_mode": mode}

        # Get the last AI response from the conversation history
        last_ai_response = None
        if agent.conversation_history and len(agent.conversation_history) > 0:
            for msg in reversed(agent.conversation_history):
                # Handle both dict messages and ChatCompletionMessage objects
                role = (
                    msg.get("role")
                    if hasattr(msg, "get")
                    else getattr(msg, "role", None)
                )
                if role == "assistant":
                    last_ai_response = msg
                    break

        # Track the event
        metrics_collector.track_event(
            prompt=query,
            tools=processed_tools,
            conversation_history=[last_ai_response] if last_ai_response else None,
            additional_data=additional_data,
        )

        return CommandResult(
            True,
            data={"response": response, "conversation": agent.conversation_history},
        )

    except Exception as e:
        # Handle pagination cancellation specially - let it bubble up
        from src.exceptions import PaginationCancelled

        if isinstance(e, PaginationCancelled):
            raise  # Re-raise to bubble up to main TUI loop

        logging.error(f"Agent error: {e}", exc_info=True)
        return CommandResult(
            False, message=f"Failed to process query: {str(e)}", error=e
        )


DEFINITION = CommandDefinition(
    name="agent",
    description="Process natural language queries using an LLM agent",
    handler=handle_command,
    parameters={
        "query": {
            "type": "string",
            "description": "Natural language query to process",
        },
        "mode": {
            "type": "string",
            "description": "Agent mode (general, pii, bulk_pii, stitch)",
            "default": "general",
        },
        "catalog_name": {
            "type": "string",
            "description": "Optional catalog name for context (uses active catalog if not provided)",
        },
        "schema_name": {
            "type": "string",
            "description": "Optional schema name for context (uses active schema if not provided)",
        },
        "rest": {
            "type": "string",
            "description": "Additional text after the command to use as query",
        },
        "raw_args": {
            "type": ["array", "string"],
            "description": "Raw unparsed arguments in case parsing fails",
        },
    },
    # Not requiring query since we handle combining raw_args and rest in the handler
    required_params=[],
    tui_aliases=["/agent", "/ask"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=False,  # Don't let the agent use itself
    usage_hint='Usage: /ask Your natural language question here\n       /agent --query "Your question here" [--mode general|pii|bulk_pii|stitch]',
)
