"""
Command registration for Chuck.

This module registers all commands with the unified command registry,
making them available for both CLI and agent usage.
"""

from chuck_data.command_registry import register_command

# Import command definitions
from .list_models import DEFINITION as list_models_definition
from .model_selection import DEFINITION as model_selection_definition
from .catalog_selection import DEFINITION as catalog_selection_definition
from .schema_selection import DEFINITION as schema_selection_definition
from .tag_pii import DEFINITION as tag_pii_definition
from .scan_pii import DEFINITION as scan_pii_definition
from .bulk_tag_pii import DEFINITION as bulk_tag_pii_definition
from .setup_stitch import DEFINITION as setup_stitch_definition
from .workspace_selection import DEFINITION as workspace_selection_definition
from .help import DEFINITION as help_definition
from .status import DEFINITION as status_definition
from .jobs import DEFINITION as jobs_definition
from .setup_wizard import DEFINITION as setup_wizard_definition
from .agent import DEFINITION as agent_definition
from .auth import DEFINITION as auth_definition

# Import new Databricks commands
from .list_warehouses import DEFINITION as list_warehouses_definition
from .warehouse import DEFINITION as warehouse_definition
from .warehouse_selection import DEFINITION as warehouse_selection_definition
from .create_warehouse import DEFINITION as create_warehouse_definition
from .run_sql import DEFINITION as run_sql_definition
from .list_volumes import DEFINITION as list_volumes_definition
from .create_volume import DEFINITION as create_volume_definition
from .upload_file import DEFINITION as upload_file_definition
from .job_status import DEFINITION as job_status_definition
from .list_tables import DEFINITION as list_tables_definition
from .add_stitch_report import DEFINITION as add_stitch_report_definition

# Import catalog/schema/table commands
from .list_catalogs import DEFINITION as list_catalogs_definition
from .catalog import DEFINITION as catalog_definition
from .list_schemas import DEFINITION as list_schemas_definition
from .schema import DEFINITION as schema_definition
from .table import DEFINITION as table_definition

# Import bug report command
from .bug import DEFINITION as bug_definition

# Import getting started command
from .getting_started import DEFINITION as getting_started_definition

# Import discord community command
from .discord import DEFINITION as discord_definition

# Import support command
from .support import DEFINITION as support_definition

# List of all command definitions to register
ALL_COMMAND_DEFINITIONS = [
    # Authentication & Workspace commands
    *auth_definition,
    workspace_selection_definition,
    setup_wizard_definition,
    # Model related commands
    list_models_definition,
    model_selection_definition,
    # Catalog & Schema commands
    catalog_selection_definition,
    schema_selection_definition,
    list_catalogs_definition,
    catalog_definition,
    list_schemas_definition,
    schema_definition,
    list_tables_definition,
    table_definition,
    # PII and Stitch related commands
    tag_pii_definition,
    scan_pii_definition,
    bulk_tag_pii_definition,
    setup_stitch_definition,
    add_stitch_report_definition,
    # Job commands
    *jobs_definition,
    job_status_definition,
    # Warehouse commands
    list_warehouses_definition,
    warehouse_definition,
    warehouse_selection_definition,
    create_warehouse_definition,
    run_sql_definition,
    # Volume commands
    list_volumes_definition,
    create_volume_definition,
    upload_file_definition,
    # Utility commands
    help_definition,
    status_definition,
    bug_definition,
    getting_started_definition,
    discord_definition,
    support_definition,
    # Agent command
    agent_definition,
]


def register_all_commands():
    """Register all commands with the unified registry."""
    for definition in ALL_COMMAND_DEFINITIONS:
        register_command(definition)
