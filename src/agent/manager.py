import json
import logging
from copy import deepcopy
from src.llm.client import LLMClient
from .tool_executor import get_tool_schemas, execute_tool
from src.config import (
    get_active_catalog,
    get_active_schema,
    get_warehouse_id,
    get_workspace_url,
)

from .prompts import (
    DEFAULT_SYSTEM_MESSAGE,
    PII_AGENT_SYSTEM_MESSAGE,
    BULK_PII_AGENT_SYSTEM_MESSAGE,
    STITCH_AGENT_SYSTEM_MESSAGE,
)


class AgentManager:
    def __init__(self, client, model=None, tool_output_callback=None):
        self.api_client = client
        self.llm_client = LLMClient()
        self.model = model
        self.tool_output_callback = tool_output_callback
        self.conversation_history = [
            {"role": "system", "content": DEFAULT_SYSTEM_MESSAGE}
        ]

    def add_user_message(self, content):
        self.conversation_history.append({"role": "user", "content": content})

    def add_assistant_message(self, content):
        self.conversation_history.append({"role": "assistant", "content": content})

    def add_system_message(self, content):
        # If there's already a system message, replace it; otherwise prepend
        for i, msg in enumerate(self.conversation_history):
            if msg["role"] == "system":
                self.conversation_history[i] = {"role": "system", "content": content}
                return
        self.conversation_history.insert(0, {"role": "system", "content": content})

    def process_pii_detection(self, table_name):
        """Process a PII detection request for a specific table

        Args:
            table_name: Name of the table to analyze
        Returns:
            Final response from the LLM
        """
        # Start with a clean conversation specifically for PII detection
        self.conversation_history = []

        # Add system message for PII detection
        self.add_system_message(PII_AGENT_SYSTEM_MESSAGE)

        # Add user message requesting PII analysis
        self.add_user_message(f"Analyze the table '{table_name}' for PII data.")

        # Get available tools - specifically need the tag_pii_columns and get_table_info tools
        tools = get_tool_schemas()

        # Process using the LLM
        return self.process_with_tools(tools)

    def process_bulk_pii_scan(self, catalog_name=None, schema_name=None):
        """Process a bulk PII scan for all tables in the current catalog and schema

        Args:
            catalog_name: Optional name of the catalog to scan (uses active catalog if None)
            schema_name: Optional name of the schema to scan (uses active schema if None)

        Returns:
            Final response from the LLM with consolidated PII analysis
        """
        # Start with a clean conversation specifically for bulk PII scanning
        self.conversation_history = []

        # Add system message for bulk PII detection
        self.add_system_message(BULK_PII_AGENT_SYSTEM_MESSAGE)

        # Add user message requesting bulk PII analysis
        if catalog_name and schema_name:
            self.add_user_message(
                f"Scan all tables in catalog '{catalog_name}' and schema '{schema_name}' for PII data."
            )
        else:
            self.add_user_message(
                "Scan all tables in the current catalog and schema for PII data."
            )

        # Get available tools
        tools = get_tool_schemas()

        # Process using the LLM
        return self.process_with_tools(tools)

    def process_setup_stitch(self, catalog_name=None, schema_name=None):
        """Process a Stitch setup request

        Args:
            catalog_name: Optional name of the catalog to use
            schema_name: Optional name of the schema to use

        Returns:
            Final response from the LLM with setup instructions
        """
        # Start with a clean conversation
        self.conversation_history = []

        # Add system message for stitch setup
        self.add_system_message(STITCH_AGENT_SYSTEM_MESSAGE)

        # Add user message requesting stitch setup
        if catalog_name and schema_name:
            self.add_user_message(
                f"Set up a Stitch integration for catalog '{catalog_name}' and schema '{schema_name}'."
            )
        else:
            self.add_user_message(
                "Set up a Stitch integration for the current catalog and schema."
            )

        # Get available tools
        tools = get_tool_schemas()

        # Process using the LLM
        return self.process_with_tools(tools)

    def process_with_tools(self, tools, max_iterations: int = 20):
        """Process the current conversation with tools until a final response is received.

        Args:
            tools: Tool schemas to use
            max_iterations: Maximum number of LLM calls to make before aborting

        Returns:
            Final text response from the LLM, or an error message if the limit is reached
        """
        original_system_message_content = None
        system_message_index = -1
        iteration_count = 0

        # Find the system message and store its original content
        # We do this once before the loop starts
        for i, msg in enumerate(self.conversation_history):
            if msg["role"] == "system":
                system_message_index = i
                # Store the content as it was when the process started
                original_system_message_content = msg["content"]
                break

        if system_message_index == -1:
            # This should ideally not happen if the history is initialized correctly
            logging.error("System message not found in conversation history.")
            # Handle error appropriately, maybe raise exception or return error message

        while iteration_count < max_iterations:
            # Prepare a temporary history copy for this specific LLM call
            current_history = deepcopy(self.conversation_history)

            # Get current configuration state
            active_catalog = get_active_catalog() or "Not set"
            active_schema = get_active_schema() or "Not set"
            warehouse_id = get_warehouse_id() or "Not set"
            workspace_url = (
                get_workspace_url() or "Not set"
            )  # Assuming get_workspace_url exists

            config_state_info = (
                f"\n\n--- CURRENT CONTEXT ---\n"
                f"Workspace URL: {workspace_url}\n"
                f"Active Catalog: {active_catalog}\n"
                f"Active Schema: {active_schema}\n"
                f"Active Warehouse ID: {warehouse_id}\n"
                f"-----------------------"
            )

            # Update the system message *in the temporary copy*
            # Append the current config state to the *original* system message content
            if (
                system_message_index != -1
                and original_system_message_content is not None
            ):
                current_history[system_message_index]["content"] = (
                    original_system_message_content + config_state_info
                )
            # else: log warning or handle case where system message wasn't found initially

            # Get the LLM response using the temporary, updated history
            response = self.llm_client.chat(
                messages=current_history,  # Use the modified temporary history for the call
                model=self.model,
                tools=tools,
                stream=False,  # Important: No streaming within the loop
            )

            response_message = response.choices[0].message
            iteration_count += 1

            # --- IMPORTANT ---
            # All modifications to the conversation history (appending assistant messages, tool calls, tool results)
            # MUST be done on the original self.conversation_history, NOT the temporary current_history.
            # current_history is only used for the LLM call itself.

            # Check if the response contains tool calls
            if response_message.tool_calls:
                # Add the assistant's response (requesting tool calls) to history
                self.conversation_history.append(response_message)

                # Execute each tool call
                for tool_call in response_message.tool_calls:
                    tool_name = tool_call.function.name
                    tool_id = tool_call.id
                    try:
                        tool_args = json.loads(tool_call.function.arguments)
                        tool_result = execute_tool(
                            self.api_client,
                            tool_name,
                            tool_args,
                            output_callback=self.tool_output_callback,
                        )
                    except json.JSONDecodeError as e:
                        tool_result = {"error": f"Invalid JSON arguments: {e}"}
                    except Exception as e:
                        # Handle pagination cancellation specially - let it bubble up
                        from src.exceptions import PaginationCancelled

                        if isinstance(e, PaginationCancelled):
                            raise  # Re-raise to bubble up to main TUI loop

                        tool_result = {"error": f"Tool execution failed: {e}"}

                    # Add the tool execution result to the *original* history
                    self.conversation_history.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_id,
                            "name": tool_name,
                            "content": json.dumps(tool_result),
                        }
                    )

                # Check if any tool initiated interactive mode - if so, stop processing
                from src.interactive_context import InteractiveContext

                interactive_context = InteractiveContext()
                if interactive_context.is_in_interactive_mode():
                    # A tool has initiated interactive mode, stop agent processing
                    # Return empty response to let TUI handle the interaction
                    logging.debug(
                        "Tool initiated interactive mode, stopping agent processing"
                    )
                    return ""

                # Continue the loop to get the next LLM response based on tool results
                continue
            else:
                # No tool calls, this is the final response
                final_content = response_message.content
                # remove all lines with any <function> tags
                final_content = "\n".join(
                    line
                    for line in final_content.splitlines()
                    if "<function" not in line
                )
                self.add_assistant_message(final_content)
                return final_content

        logging.error(
            "process_with_tools reached maximum iterations without final response"
        )
        error_msg = "Error: maximum iterations reached."
        self.add_assistant_message(error_msg)
        return error_msg

    def process_query(self, query):
        """Process a general query using available tools

        Args:
            query: User's query text

        Returns:
            Final response from the LLM
        """
        # If no system message exists, add the default one
        has_system = False
        for msg in self.conversation_history:
            if msg["role"] == "system":
                has_system = True
                break

        if not has_system:
            self.add_system_message(DEFAULT_SYSTEM_MESSAGE)

        # Add user message to history
        self.add_user_message(query)

        # Get available tools
        tools = get_tool_schemas()

        # Process using the LLM
        return self.process_with_tools(tools)
