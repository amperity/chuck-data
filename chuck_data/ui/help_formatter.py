"""
Help text formatter for the TUI interface.

This module provides functions to format help text for the TUI interface
based on the registered commands in the command registry.
"""

from typing import Dict
from chuck_data.command_registry import (
    CommandDefinition,
    _is_command_available_for_provider,
)
from chuck_data.config import get_data_provider


def format_help_text(
    commands: Dict[str, CommandDefinition],
    tui_map: Dict[str, str],
    provider: str = None,
) -> str:
    """
    Format help text for TUI display based on available commands.

    Args:
        commands: Dictionary of command definitions from the registry
        tui_map: Mapping of TUI commands to registry commands
        provider: Optional provider to use for filtering (defaults to current config provider)

    Returns:
        Formatted help text as a string
    """
    # Get current data provider to filter commands (use provided or from config)
    current_provider = provider if provider is not None else get_data_provider()
    # Create reverse mapping of registry names to TUI aliases for easy lookup
    registry_to_tui = {}
    for tui_cmd, registry_name in tui_map.items():
        if registry_name in registry_to_tui:
            registry_to_tui[registry_name].append(tui_cmd)
        else:
            registry_to_tui[registry_name] = [tui_cmd]

    # Create a mapping for TUI alias lookup (without slash prefix)
    # This allows us to look up commands by their TUI name without the slash
    tui_to_registry = {}
    for tui_cmd, registry_name in tui_map.items():
        # Store both with and without slash prefix for flexibility
        clean_name = tui_cmd[1:] if tui_cmd.startswith("/") else tui_cmd
        tui_to_registry[clean_name] = registry_name

    # Group commands by category with provider-specific commands
    # Commands will be filtered based on current_provider
    categories = {
        "Authentication & Setup": [
            "workspace-selection",  # Databricks
            "select-workspace",  # Databricks
            "databricks-login",  # Databricks
            "amperity-login",  # Amperity (provider-agnostic)
            "logout",  # Provider-agnostic
            "status",  # Provider-agnostic (shows Databricks status)
            "redshift_status",  # Redshift (note: uses underscores in registry)
            "setup-wizard",  # Provider-agnostic
        ],
        "Database & Schema Management": [
            # Databricks
            "list-catalogs",
            "catalog",
            "catalog-selection",
            "select-catalog",
            # Redshift (note: Redshift commands use underscores in registry)
            "list_databases",
            "select_database",
            # Both
            "list-schemas",  # Databricks
            "list_redshift_schemas",  # Redshift
            "schema",
            "schema-selection",
            "select-schema",
            "select_redshift_schema",  # Redshift
            "list-tables",
            "table",
        ],
        "Model & Endpoint Management": [
            "list-models",
            "model-selection",
            "select-model",
        ],
        "SQL Warehouse Management": [
            "list_warehouses",
            "warehouse",
            "warehouse-selection",
            "select-warehouse",
            "create_warehouse",
            "run-sql",
        ],
        "Volume Management": [
            "list_volumes",
            "create-volume",
            "upload-file",
        ],
        "PII & Data Management": [
            "scan-schema-for-pii",
            "tag-pii",  # Redshift shorthand for bulk-tag-pii
            "bulk-tag-pii",
            "tag-pii-columns",
            "setup-stitch",
            "add-stitch-report",
        ],
        "Job Management": [
            "launch-job",
            "jobs",
            "job-status",
            "monitor-job",
        ],
        "Utilities": [
            "help",
            "getting-started",
            "discord",
            "support",
            "bug",
            "exit",
        ],
    }

    # Format each command with proper spacing
    def format_command(cmd_name: str) -> str:
        # First, try to look up the command by its name directly
        cmd = commands.get(cmd_name)

        # If not found, try to look it up by TUI alias (without slash)
        if not cmd and cmd_name in tui_to_registry:
            registry_name = tui_to_registry[cmd_name]
            cmd = commands.get(registry_name)

        # If still not found, return empty string
        if not cmd:
            return ""

        # Filter by provider - only show commands for current provider
        if not _is_command_available_for_provider(cmd, current_provider):
            return ""

        # Get TUI aliases for this command
        tui_aliases = registry_to_tui.get(cmd.name, [])
        if not tui_aliases:
            # Use the command name with a slash if no aliases defined
            tui_aliases = [f"/{cmd.name}"]

        # Select the first alias (primary command)
        alias = tui_aliases[0]

        # Add parameter placeholder if applicable
        if cmd_name == "workspace-selection" or cmd.name == "workspace-selection":
            alias = f"{alias} <url>"
        elif cmd_name == "select-workspace" or cmd.name == "select-workspace":
            alias = f"{alias} <workspace_name>"
        elif cmd_name == "databricks-login" or cmd.name == "databricks-login":
            alias = f"{alias} <token>"
        elif cmd_name == "amperity-login" or cmd.name == "amperity-login":
            alias = f"{alias} <token>"
        elif cmd_name == "catalog" or cmd.name == "catalog":
            alias = f"{alias} <catalog_name>"
        elif cmd_name == "schema" or cmd.name == "schema":
            alias = f"{alias} <schema_name>"
        elif cmd_name == "table" or cmd.name == "table":
            alias = f"{alias} <table_name>"
        elif cmd_name == "catalog-selection" or cmd.name == "catalog-selection":
            alias = f"{alias} <catalog_name>"
        elif cmd_name == "set-catalog" or cmd.name == "set-catalog":
            alias = f"{alias} <catalog_name>"
        elif cmd_name == "select_database" or cmd.name == "select_database":
            alias = f"{alias} <database_name>"
        elif cmd_name == "select-database" or cmd.name == "select-database":
            alias = f"{alias} <database_name>"
        elif (
            cmd_name == "select_redshift_schema" or cmd.name == "select_redshift_schema"
        ):
            alias = f"{alias} <schema_name>"
        elif cmd_name == "schema-selection" or cmd.name == "schema-selection":
            alias = f"{alias} <schema_name>"
        elif cmd_name == "set-schema" or cmd.name == "set-schema":
            alias = f"{alias} <schema_name>"
        elif cmd_name == "select-schema" or cmd.name == "select-schema":
            alias = f"{alias} <schema_name>"
        elif cmd_name == "model-selection" or cmd.name == "model-selection":
            alias = f"{alias} <model_name>"
        elif cmd_name == "select-model" or cmd.name == "select-model":
            alias = f"{alias} <model_name>"
        elif cmd_name == "warehouse-selection" or cmd.name == "warehouse-selection":
            alias = f"{alias} <warehouse_id>"
        elif cmd_name == "select-warehouse" or cmd.name == "select-warehouse":
            alias = f"{alias} <warehouse_id>"
        elif cmd_name == "warehouse" or cmd.name == "warehouse":
            alias = f"{alias} <warehouse_id>"
        elif cmd_name == "agent" or cmd.name == "agent":
            alias = f"{alias} <query>"
        elif cmd_name == "scan-schema-for-pii" or cmd.name == "scan-schema-for-pii":
            alias = f"{alias}"
        elif cmd_name == "tag-pii-columns" or cmd.name == "tag-pii-columns":
            alias = f"{alias}"
        elif cmd_name == "create-volume" or cmd.name == "create-volume":
            alias = f"{alias}"
        elif cmd_name == "upload-file" or cmd.name == "upload-file":
            alias = f"{alias} <file_path> <volume_path>"
        elif cmd_name == "launch-job" or cmd.name == "launch-job":
            alias = f"{alias}"
        elif cmd_name == "job-status" or cmd.name == "job-status":
            alias = f"{alias} <run_id>"
        elif cmd_name == "run-sql" or cmd.name == "run-sql":
            alias = f"{alias} <query>"
        elif cmd_name == "bug" or cmd.name == "bug":
            alias = f"{alias} <description>"

        # Format the command with its description
        return f"{alias:<40} - {cmd.description}"

    # Start with Chuck Data CLI Commands header
    help_lines = ["", "Chuck Data CLI Commands:"]

    # Build help text with sections
    for category, cmd_list in categories.items():
        category_commands = []
        for cmd_name in cmd_list:
            cmd_help = format_command(cmd_name)
            if cmd_help:
                category_commands.append(cmd_help)

        if category_commands:
            help_lines.append("")  # Add spacing
            help_lines.append(f" [bold]{category}:[/bold]")
            for cmd in category_commands:
                help_lines.append(f" {cmd}")

    # Add extra spacing for readability
    help_lines.append("")

    # Join all lines
    return "\n".join(help_lines)
