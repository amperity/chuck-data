"""
Unified command registry for both user commands and agent tools.

This module provides a central registry for all commands that can be executed
by both the user interface and LLM agent tools, reducing code duplication.
"""

from typing import Dict, Any, Callable, List, Optional
from dataclasses import dataclass, field


@dataclass
class CommandDefinition:
    """
    Definition for a command that can be used by both TUI and agent.

    Attributes:
        name: Command name (without leading slash)
        description: Human-readable description of the command
        handler: Function that implements the command logic
        parameters: JSON Schema compatible parameter definitions
        required_params: List of required parameter names
        needs_api_client: Whether the command needs an API client to execute
        visible_to_user: Whether the command should be visible in user interface
        visible_to_agent: Whether the command should be available to the agent
        tui_aliases: Alternative command names in the TUI (with leading slash)
        output_formatter: Optional custom output formatter function
        usage_hint: Optional[str] = None (for better TUI error messages)
        supports_interactive_input: bool = False (to make interactive mode explicit)
        agent_display: str = "condensed" (how to display when called by agent: "condensed" or "full")
        condensed_action: Optional[str] = None (friendly action name for condensed display, e.g. "Setting catalog")
    """

    name: str
    description: str
    handler: Callable
    parameters: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    required_params: List[str] = field(default_factory=list)
    needs_api_client: bool = True
    visible_to_user: bool = True  # Some commands may be agent-only
    visible_to_agent: bool = True  # Some commands may be user-only
    tui_aliases: List[str] = field(
        default_factory=list
    )  # TUI alternatives like "/models" for "list_models"
    output_formatter: Optional[Callable] = None  # Custom output formatter if needed
    usage_hint: Optional[str] = None  # For better TUI error messages
    supports_interactive_input: bool = False  # To make interactive mode explicit
    agent_display: str = (
        "condensed"  # How to display when called by agent: "condensed" or "full"
    )
    condensed_action: Optional[str] = None  # Friendly action name for condensed display


# Command registry - populated with all available commands
COMMAND_REGISTRY: Dict[str, CommandDefinition] = {}
TUI_COMMAND_MAP: Dict[str, str] = (
    {}
)  # Maps TUI command names (with slash) to registry names


def register_command(command_def: CommandDefinition) -> None:
    """
    Register a command in the unified registry.

    Args:
        command_def: Command definition to register
    """
    COMMAND_REGISTRY[command_def.name] = command_def

    # Also register TUI command aliases if any
    for alias in command_def.tui_aliases:
        if alias.startswith("/"):
            TUI_COMMAND_MAP[alias] = command_def.name
        else:
            TUI_COMMAND_MAP[f"/{alias}"] = command_def.name


def get_command(name: str) -> Optional[CommandDefinition]:
    """
    Get a command by name.

    Args:
        name: Command name to look up (without leading slash)

    Returns:
        CommandDefinition if found, None otherwise
    """
    # Direct lookup in registry
    if name in COMMAND_REGISTRY:
        return COMMAND_REGISTRY[name]

    # Handle TUI command names (with leading slash)
    if name.startswith("/"):
        tui_name = name
    else:
        tui_name = f"/{name}"

    # Check TUI command map
    if tui_name in TUI_COMMAND_MAP:
        registry_name = TUI_COMMAND_MAP[tui_name]
        return COMMAND_REGISTRY.get(registry_name)

    return None


def get_user_commands() -> Dict[str, CommandDefinition]:
    """
    Get all commands available to the user.

    Returns:
        Dict mapping command names to definitions for user-visible commands
    """
    return {name: cmd for name, cmd in COMMAND_REGISTRY.items() if cmd.visible_to_user}


def get_agent_commands() -> Dict[str, CommandDefinition]:
    """
    Get all commands available to the agent.

    Returns:
        Dict mapping command names to definitions for agent-visible commands
    """
    return {name: cmd for name, cmd in COMMAND_REGISTRY.items() if cmd.visible_to_agent}


def get_agent_tool_schemas() -> List[Dict[str, Any]]:
    """
    Get all command schemas in agent tool format.

    Returns:
        List of tool schema definitions compatible with the LLM agent API
    """
    agent_tools = []
    for name, cmd in COMMAND_REGISTRY.items():
        if not cmd.visible_to_agent:
            continue

        tool = {
            "type": "function",
            "function": {
                "name": name,
                "description": cmd.description,
                "parameters": {
                    "type": "object",
                    "properties": cmd.parameters or {},
                    "required": cmd.required_params or [],
                },
            },
        }
        agent_tools.append(tool)
    return agent_tools


def resolve_tui_command(command: str) -> Optional[str]:
    """
    Resolve a TUI command (with slash) to its registry name.

    Args:
        command: TUI command (with leading slash)

    Returns:
        Registry command name if found, None otherwise
    """
    return TUI_COMMAND_MAP.get(command)
