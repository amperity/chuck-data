"""
Unit tests for command registry provider filtering.

Tests the provider-based filtering of commands to ensure:
- Databricks-specific commands only appear for Databricks provider
- Redshift-specific commands only appear for Redshift provider
- Provider-agnostic commands appear for all providers
"""

from chuck_data.command_registry import (
    get_command,
    get_user_commands,
    get_agent_commands,
    get_agent_tool_schemas,
    COMMAND_REGISTRY,
)


class TestCommandRegistryProviderFiltering:
    """Test provider-based command filtering in the command registry."""

    # Known Databricks-specific commands
    DATABRICKS_COMMANDS = [
        "list_warehouses",
        "select_warehouse",
        "create_warehouse",
        "warehouse",
        "list_catalogs",
        "select_catalog",
        "catalog",
        "list_schemas",
        "select_schema",
        "schema",
        "list_volumes",
        "create_volume",
        "upload_file",
        "run_sql",
        "launch_job",
        "add_stitch_report",
        "table",
        "tag_pii_columns",
        "status",
    ]

    # Known Redshift-specific commands
    REDSHIFT_COMMANDS = [
        "list_databases",
        "select_database",
        "list_redshift_schemas",
        "select_redshift_schema",
        "redshift_status",
    ]

    # Known provider-agnostic commands
    PROVIDER_AGNOSTIC_COMMANDS = [
        "help",
        "setup_wizard",
        "agent",
        "amperity-login",  # Note: databricks-login and select_workspace are provider-specific (databricks only)
        "logout",
        "select_model",
        "list_models",
        "bug",
        "support",
        "discord",
        "getting_started",
        "scan_schema_for_pii",
        "bulk_tag_pii",
        "setup_stitch",
        "job_status",
        "jobs",
        "monitor_job",
        "list_tables",
    ]

    def test_databricks_provider_includes_databricks_commands(self):
        """Test that Databricks provider includes all Databricks-specific commands."""
        user_commands = get_user_commands(provider="databricks")

        for cmd_name in self.DATABRICKS_COMMANDS:
            assert (
                cmd_name in user_commands
            ), f"Databricks command '{cmd_name}' should be available for Databricks provider"

    def test_databricks_provider_excludes_redshift_commands(self):
        """Test that Databricks provider excludes all Redshift-specific commands."""
        user_commands = get_user_commands(provider="databricks")

        for cmd_name in self.REDSHIFT_COMMANDS:
            assert (
                cmd_name not in user_commands
            ), f"Redshift command '{cmd_name}' should NOT be available for Databricks provider"

    def test_databricks_provider_includes_agnostic_commands(self):
        """Test that Databricks provider includes provider-agnostic commands."""
        user_commands = get_user_commands(provider="databricks")

        for cmd_name in self.PROVIDER_AGNOSTIC_COMMANDS:
            assert (
                cmd_name in user_commands
            ), f"Provider-agnostic command '{cmd_name}' should be available for Databricks provider"

    def test_redshift_provider_includes_redshift_commands(self):
        """Test that Redshift provider includes all Redshift-specific commands."""
        user_commands = get_user_commands(provider="aws_redshift")

        for cmd_name in self.REDSHIFT_COMMANDS:
            assert (
                cmd_name in user_commands
            ), f"Redshift command '{cmd_name}' should be available for Redshift provider"

    def test_redshift_provider_excludes_databricks_commands(self):
        """Test that Redshift provider excludes all Databricks-specific commands."""
        user_commands = get_user_commands(provider="aws_redshift")

        for cmd_name in self.DATABRICKS_COMMANDS:
            assert (
                cmd_name not in user_commands
            ), f"Databricks command '{cmd_name}' should NOT be available for Redshift provider"

    def test_redshift_provider_includes_agnostic_commands(self):
        """Test that Redshift provider includes provider-agnostic commands."""
        user_commands = get_user_commands(provider="aws_redshift")

        for cmd_name in self.PROVIDER_AGNOSTIC_COMMANDS:
            assert (
                cmd_name in user_commands
            ), f"Provider-agnostic command '{cmd_name}' should be available for Redshift provider"

    def test_none_provider_includes_only_agnostic_commands(self):
        """Test that None provider only includes provider-agnostic commands."""
        user_commands = get_user_commands(provider=None)

        # Should include provider-agnostic commands
        for cmd_name in self.PROVIDER_AGNOSTIC_COMMANDS:
            assert (
                cmd_name in user_commands
            ), f"Provider-agnostic command '{cmd_name}' should be available when provider is None"

        # Should NOT include provider-specific commands
        for cmd_name in self.DATABRICKS_COMMANDS:
            assert (
                cmd_name not in user_commands
            ), f"Databricks command '{cmd_name}' should NOT be available when provider is None"

        for cmd_name in self.REDSHIFT_COMMANDS:
            assert (
                cmd_name not in user_commands
            ), f"Redshift command '{cmd_name}' should NOT be available when provider is None"

    def test_agent_commands_filtered_by_databricks_provider(self):
        """Test that agent commands are filtered by Databricks provider."""
        agent_commands = get_agent_commands(provider="databricks")

        # Check a few Databricks commands
        assert "list_warehouses" in agent_commands
        assert "select_warehouse" in agent_commands
        assert "run_sql" in agent_commands

        # Check Redshift commands are excluded
        assert "list_redshift_schemas" not in agent_commands
        assert "select_database" not in agent_commands

    def test_agent_commands_filtered_by_redshift_provider(self):
        """Test that agent commands are filtered by Redshift provider."""
        agent_commands = get_agent_commands(provider="aws_redshift")

        # Check Redshift commands
        assert "list_redshift_schemas" in agent_commands
        assert "select_database" in agent_commands

        # Check Databricks commands are excluded
        assert "list_warehouses" not in agent_commands
        assert "select_warehouse" not in agent_commands
        assert "run_sql" not in agent_commands

    def test_agent_tool_schemas_filtered_by_provider(self):
        """Test that agent tool schemas are filtered by provider."""
        # Databricks provider
        databricks_schemas = get_agent_tool_schemas(provider="databricks")
        databricks_tool_names = [
            tool["function"]["name"] for tool in databricks_schemas
        ]

        assert "list_warehouses" in databricks_tool_names
        assert "list_redshift_schemas" not in databricks_tool_names

        # Redshift provider
        redshift_schemas = get_agent_tool_schemas(provider="aws_redshift")
        redshift_tool_names = [tool["function"]["name"] for tool in redshift_schemas]

        assert "list_redshift_schemas" in redshift_tool_names
        assert "list_warehouses" not in redshift_tool_names

    def test_get_command_with_databricks_provider(self):
        """Test get_command with Databricks provider filtering."""
        # Should return Databricks command
        cmd = get_command("list_warehouses", provider="databricks")
        assert cmd is not None
        assert cmd.name == "list_warehouses"

        # Should NOT return Redshift command
        cmd = get_command("list_redshift_schemas", provider="databricks")
        assert cmd is None

        # Should return provider-agnostic command
        cmd = get_command("help", provider="databricks")
        assert cmd is not None
        assert cmd.name == "help"

    def test_get_command_with_redshift_provider(self):
        """Test get_command with Redshift provider filtering."""
        # Should return Redshift command
        cmd = get_command("list_redshift_schemas", provider="aws_redshift")
        assert cmd is not None
        assert cmd.name == "list_redshift_schemas"

        # Should NOT return Databricks command
        cmd = get_command("list_warehouses", provider="aws_redshift")
        assert cmd is None

        # Should return provider-agnostic command
        cmd = get_command("help", provider="aws_redshift")
        assert cmd is not None
        assert cmd.name == "help"

    def test_get_command_without_provider(self):
        """Test get_command without provider returns any command."""
        # Should return any command when provider is not specified
        cmd = get_command("list_warehouses")
        assert cmd is not None
        assert cmd.name == "list_warehouses"

        cmd = get_command("list_redshift_schemas")
        assert cmd is not None
        assert cmd.name == "list_redshift_schemas"

    def test_tui_alias_resolution_with_databricks_provider(self):
        """Test TUI alias resolution with Databricks provider."""
        # Should resolve Databricks TUI alias
        cmd = get_command("/warehouses", provider="databricks")
        assert cmd is not None
        assert cmd.name == "list_warehouses"

        # Should NOT resolve Redshift TUI alias
        cmd = get_command("/list-schemas", provider="databricks")
        # This is tricky - /list-schemas is used by both providers
        # For Databricks it should resolve to list_schemas (Unity Catalog)
        # For Redshift it should resolve to list_redshift_schemas
        if cmd is not None:
            # If it resolves, it should be the Databricks version
            assert cmd.provider == "databricks" or cmd.provider is None

    def test_tui_alias_resolution_with_redshift_provider(self):
        """Test TUI alias resolution with Redshift provider."""
        # Should resolve Redshift TUI alias
        cmd = get_command("/list-schemas", provider="aws_redshift")
        assert cmd is not None
        # For Redshift, /list-schemas should resolve to list_redshift_schemas
        assert cmd.name == "list_redshift_schemas"

        # Should NOT resolve Databricks-only TUI alias
        cmd = get_command("/warehouses", provider="aws_redshift")
        assert cmd is None

    def test_command_count_consistency(self):
        """Test that command counts are consistent across filters."""
        databricks_user = len(get_user_commands(provider="databricks"))
        databricks_agent = len(get_agent_commands(provider="databricks"))

        # Agent commands should be fewer than or equal to user commands
        assert databricks_agent <= databricks_user

        redshift_user = len(get_user_commands(provider="aws_redshift"))
        redshift_agent = len(get_agent_commands(provider="aws_redshift"))

        # Agent commands should be fewer than or equal to user commands
        assert redshift_agent <= redshift_user

        # Databricks should have more commands than Redshift (more features)
        assert databricks_user > redshift_user

    def test_all_registered_commands_have_provider_or_are_agnostic(self):
        """Test that all commands are either provider-specific or agnostic."""
        for cmd_name, cmd_def in COMMAND_REGISTRY.items():
            # Provider should be None, "databricks", or "aws_redshift"
            assert cmd_def.provider in [
                None,
                "databricks",
                "aws_redshift",
            ], f"Command '{cmd_name}' has invalid provider: {cmd_def.provider}"

    def test_databricks_commands_are_tagged_correctly(self):
        """Test that Databricks-specific commands have correct provider tag."""
        for cmd_name in self.DATABRICKS_COMMANDS:
            cmd = COMMAND_REGISTRY.get(cmd_name)
            assert cmd is not None, f"Command '{cmd_name}' not found in registry"
            assert (
                cmd.provider == "databricks"
            ), f"Command '{cmd_name}' should have provider='databricks', got {cmd.provider}"

    def test_redshift_commands_are_tagged_correctly(self):
        """Test that Redshift-specific commands have correct provider tag."""
        for cmd_name in self.REDSHIFT_COMMANDS:
            cmd = COMMAND_REGISTRY.get(cmd_name)
            assert cmd is not None, f"Command '{cmd_name}' not found in registry"
            assert (
                cmd.provider == "aws_redshift"
            ), f"Command '{cmd_name}' should have provider='aws_redshift', got {cmd.provider}"

    def test_agnostic_commands_have_no_provider_tag(self):
        """Test that provider-agnostic commands have no provider tag."""
        for cmd_name in self.PROVIDER_AGNOSTIC_COMMANDS:
            cmd = COMMAND_REGISTRY.get(cmd_name)
            if cmd is not None:  # Some commands might not be registered yet
                assert (
                    cmd.provider is None
                ), f"Provider-agnostic command '{cmd_name}' should have provider=None, got {cmd.provider}"

    def test_invalid_provider_returns_empty_or_agnostic_only(self):
        """Test that invalid provider returns only agnostic commands."""
        user_commands = get_user_commands(provider="invalid_provider")

        # Should only include provider-agnostic commands
        for cmd_name in self.PROVIDER_AGNOSTIC_COMMANDS:
            assert (
                cmd_name in user_commands
            ), f"Provider-agnostic command '{cmd_name}' should be available for invalid provider"

        # Should NOT include provider-specific commands
        for cmd_name in self.DATABRICKS_COMMANDS:
            assert (
                cmd_name not in user_commands
            ), f"Databricks command '{cmd_name}' should NOT be available for invalid provider"

        for cmd_name in self.REDSHIFT_COMMANDS:
            assert (
                cmd_name not in user_commands
            ), f"Redshift command '{cmd_name}' should NOT be available for invalid provider"
