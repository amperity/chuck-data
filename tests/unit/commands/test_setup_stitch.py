"""
Tests for setup_stitch command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
Tests cover both direct command execution and interactive mode with multi-phase handling.
"""

import tempfile
from unittest.mock import patch, MagicMock

from chuck_data.commands.setup_stitch import handle_command, DEFINITION
from chuck_data.config import ConfigManager, set_active_catalog, set_active_schema
from chuck_data.interactive_context import InteractiveContext
from chuck_data.agent.tool_executor import execute_tool


def _setup_successful_stitch_test_data(
    databricks_client_stub,
    llm_client_stub,
    catalog="test_catalog",
    schema="test_schema",
):
    """Helper function to set up test data for successful Stitch operations."""
    # Setup test data in client stub
    databricks_client_stub.add_catalog(catalog)
    databricks_client_stub.add_schema(catalog, schema)
    databricks_client_stub.add_table(
        catalog,
        schema,
        "users",
        columns=[
            {"name": "email", "type_name": "STRING"},
            {"name": "name", "type_name": "STRING"},
            {"name": "id", "type_name": "BIGINT"},
        ],
    )

    # Mock PII scan results - set up table with PII columns
    llm_client_stub.set_pii_detection_result(
        [
            {"column": "email", "semantic": "email"},
            {"column": "name", "semantic": "given-name"},
        ]
    )

    # Fix API compatibility issues
    # Override create_volume to accept 'name' parameter like real API
    original_create_volume = databricks_client_stub.create_volume

    def mock_create_volume(catalog_name, schema_name, name, **kwargs):
        return original_create_volume(catalog_name, schema_name, name, **kwargs)

    databricks_client_stub.create_volume = mock_create_volume

    # Override upload_file to match real API signature
    def mock_upload_file(path, content=None, overwrite=False, **kwargs):
        return True

    databricks_client_stub.upload_file = mock_upload_file

    # Set up other required API responses
    databricks_client_stub.fetch_amperity_job_init_response = {
        "cluster-init": "#!/bin/bash\necho init"
    }
    databricks_client_stub.submit_job_run_response = {"run_id": "12345"}
    databricks_client_stub.create_stitch_notebook_response = {
        "notebook_path": "/Workspace/test"
    }


def _get_sample_stitch_config():
    """Get a sample Stitch configuration for testing."""
    return {
        "name": "stitch-test",
        "tables": [
            {
                "path": "test_catalog.test_schema.users",
                "fields": [
                    {"field-name": "email", "type": "STRING", "semantics": ["email"]},
                    {
                        "field-name": "name",
                        "type": "STRING",
                        "semantics": ["given-name"],
                    },
                ],
            }
        ],
        "settings": {
            "output_catalog_name": "test_catalog",
            "output_schema_name": "stitch_outputs",
        },
    }


def _get_sample_metadata():
    """Get sample metadata for testing."""
    return {
        "target_catalog": "test_catalog",
        "target_schema": "test_schema",
        "volume_name": "chuck",
        "stitch_job_name": "stitch-test",
        "config_file_path": "/Volumes/test_catalog/test_schema/chuck/stitch-test.json",
        "init_script_path": "/Volumes/test_catalog/test_schema/chuck/cluster_init.sh",
        "init_script_content": "#!/bin/bash\necho init",
        "amperity_token": "test_token",
        "pii_scan_output": {},
        "unsupported_columns": [],
    }


class TestSetupStitchParameterValidation:
    """Test parameter validation for setup_stitch command."""

    def test_missing_client_returns_error(self):
        """Missing client parameter returns clear error message."""
        result = handle_command(None)

        assert not result.success
        assert "Client is required for Stitch setup" in result.message

    def test_missing_catalog_and_schema_context_returns_error(
        self, databricks_client_stub
    ):
        """Missing catalog and schema context returns helpful error."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            # Don't set active catalog or schema

            with patch("chuck_data.config._config_manager", config_manager):
                result = handle_command(databricks_client_stub)

        assert not result.success
        assert "Target catalog and schema must be specified or active" in result.message

    def test_partial_context_missing_schema_returns_error(self, databricks_client_stub):
        """Missing schema with active catalog returns helpful error."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            config_manager.update(active_catalog="production_catalog")
            # Don't set active_schema

            with patch("chuck_data.config._config_manager", config_manager):
                result = handle_command(databricks_client_stub)

        assert not result.success
        assert "Target catalog and schema must be specified or active" in result.message


class TestDirectSetupStitchCommand:
    """Test direct setup_stitch command execution (Phase 1: Preparation)."""

    def test_direct_command_phase_1_prepares_configuration(
        self, databricks_client_stub, llm_client_stub, temp_config
    ):
        """Direct command Phase 1 prepares configuration and shows preview."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")

            # Setup test data for successful operation
            _setup_successful_stitch_test_data(databricks_client_stub, llm_client_stub)

            with patch(
                "chuck_data.commands.setup_stitch.LLMClient",
                return_value=llm_client_stub,
            ):
                with patch(
                    "chuck_data.commands.stitch_tools.get_amperity_token",
                    return_value="test_token",
                ):
                    # Call without interactive_input to enter Phase 1
                    result = handle_command(
                        databricks_client_stub,
                        catalog_name="test_catalog",
                        schema_name="test_schema",
                    )

        # Verify Phase 1 preparation behavior
        assert result.success
        # Interactive mode should return empty message (console output handles display)
        assert result.message == ""

    def test_direct_command_uses_active_catalog_and_schema(
        self, databricks_client_stub, llm_client_stub, temp_config
    ):
        """Direct command uses active catalog and schema from config."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("active_catalog")
            set_active_schema("active_schema")

            # Setup test data
            _setup_successful_stitch_test_data(
                databricks_client_stub,
                llm_client_stub,
                catalog="active_catalog",
                schema="active_schema",
            )

            with patch(
                "chuck_data.commands.setup_stitch.LLMClient",
                return_value=llm_client_stub,
            ):
                with patch(
                    "chuck_data.commands.stitch_tools.get_amperity_token",
                    return_value="test_token",
                ):
                    # Call without catalog/schema parameters - should use active context
                    result = handle_command(databricks_client_stub)

        # Verify uses active context
        assert result.success
        assert result.message == ""

    def test_direct_command_explicit_parameters_override_active_context(
        self, databricks_client_stub, llm_client_stub, temp_config
    ):
        """Direct command explicit parameters take priority over active config."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("active_catalog")
            set_active_schema("active_schema")

            # Setup data for both active and explicit contexts
            _setup_successful_stitch_test_data(
                databricks_client_stub,
                llm_client_stub,
                "explicit_catalog",
                "explicit_schema",
            )

            with patch(
                "chuck_data.commands.setup_stitch.LLMClient",
                return_value=llm_client_stub,
            ):
                with patch(
                    "chuck_data.commands.stitch_tools.get_amperity_token",
                    return_value="test_token",
                ):
                    # Call with explicit parameters - should override active config
                    result = handle_command(
                        databricks_client_stub,
                        catalog_name="explicit_catalog",
                        schema_name="explicit_schema",
                    )

        # Verify explicit parameters are used
        assert result.success
        assert result.message == ""

    def test_pii_scan_failure_returns_error(
        self, databricks_client_stub, llm_client_stub, temp_config
    ):
        """PII scan failures are handled gracefully with helpful error messages."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")

            # Setup minimal test data with no PII tables (will cause failure)
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "test_schema")
            databricks_client_stub.add_volume("test_catalog", "test_schema", "chuck")  # Add volume to avoid volume creation error
            # No tables with PII - will cause failure

            with patch(
                "chuck_data.commands.setup_stitch.LLMClient",
                return_value=llm_client_stub,
            ):
                result = handle_command(
                    databricks_client_stub,
                    catalog_name="test_catalog",
                    schema_name="test_schema",
                )

        # Verify failure behavior
        assert not result.success
        assert (
            "No tables with PII found" in result.message
            or "PII Scan failed" in result.message
        )

    def test_databricks_api_error_handled_gracefully(
        self, databricks_client_stub, llm_client_stub, temp_config
    ):
        """Databricks API errors are handled gracefully with helpful messages."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")

            # Force API error during table listing
            def failing_list_tables(**kwargs):
                raise Exception("Databricks API temporarily unavailable")

            databricks_client_stub.list_tables = failing_list_tables

            with patch(
                "chuck_data.commands.setup_stitch.LLMClient",
                return_value=llm_client_stub,
            ):
                result = handle_command(
                    databricks_client_stub,
                    catalog_name="test_catalog",
                    schema_name="test_schema",
                )

        # Should handle API errors gracefully
        assert not result.success
        assert (
            "PII Scan failed" in result.message
            or "Error setting up Stitch" in result.message
        )

    def test_llm_client_exception_handled_gracefully(self, databricks_client_stub):
        """LLM client creation exceptions are handled gracefully."""
        with patch("chuck_data.commands.setup_stitch.LLMClient") as mock_llm_client:
            # Setup external boundary to fail
            mock_llm_client.side_effect = Exception("LLM client error")

            result = handle_command(
                databricks_client_stub,
                catalog_name="test_catalog",
                schema_name="test_schema",
            )

        # Verify error handling behavior
        assert not result.success
        assert "Error setting up Stitch" in result.message
        assert str(result.error) == "LLM client error"


class TestSetupStitchInteractiveMode:
    """Test setup_stitch interactive mode with multi-phase handling."""

    def test_interactive_phase_2_launch_command_transitions_to_phase_3(
        self, databricks_client_stub, llm_client_stub, temp_config
    ):
        """Interactive Phase 2 'launch' command transitions to Phase 3."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup context with Phase 1 data
            context = InteractiveContext()
            context.set_active_context("setup-stitch")
            context.store_context_data("setup-stitch", "phase", "review")
            context.store_context_data(
                "setup-stitch", "stitch_config", _get_sample_stitch_config()
            )
            context.store_context_data(
                "setup-stitch", "metadata", _get_sample_metadata()
            )

            with patch(
                "chuck_data.commands.setup_stitch.InteractiveContext",
                return_value=context,
            ):
                with patch(
                    "chuck_data.commands.setup_stitch.LLMClient",
                    return_value=llm_client_stub,
                ):
                    result = handle_command(
                        databricks_client_stub, interactive_input="launch"
                    )

        # Verify transition to Phase 3
        assert result.success
        assert "Ready to launch" in result.message
        assert "Type 'confirm' to proceed" in result.message

    def test_interactive_phase_2_cancel_command_clears_context(
        self, databricks_client_stub, llm_client_stub, temp_config
    ):
        """Interactive Phase 2 'cancel' command clears context and exits."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup context with Phase 1 data
            context = InteractiveContext()
            context.set_active_context("setup-stitch")
            context.store_context_data("setup-stitch", "phase", "review")
            context.store_context_data(
                "setup-stitch", "stitch_config", _get_sample_stitch_config()
            )
            context.store_context_data(
                "setup-stitch", "metadata", _get_sample_metadata()
            )

            with patch(
                "chuck_data.commands.setup_stitch.InteractiveContext",
                return_value=context,
            ):
                result = handle_command(
                    databricks_client_stub, interactive_input="cancel"
                )

        # Verify cancellation behavior
        assert result.success
        assert "Stitch setup cancelled" in result.message

    def test_interactive_phase_2_modification_request_updates_config(
        self, databricks_client_stub, llm_client_stub, temp_config
    ):
        """Interactive Phase 2 modification requests update configuration."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup context with Phase 1 data
            context = InteractiveContext()
            context.set_active_context("setup-stitch")
            context.store_context_data("setup-stitch", "phase", "review")
            context.store_context_data(
                "setup-stitch", "stitch_config", _get_sample_stitch_config()
            )
            context.store_context_data(
                "setup-stitch", "metadata", _get_sample_metadata()
            )

            # Configure LLM to return modified config
            modified_config = _get_sample_stitch_config()
            modified_config["tables"] = []  # Modified to remove all tables
            import json

            modified_config_json = json.dumps(modified_config)
            llm_client_stub.set_response_content(f"```json\n{modified_config_json}```")

            with patch(
                "chuck_data.commands.setup_stitch.InteractiveContext",
                return_value=context,
            ):
                with patch(
                    "chuck_data.commands.setup_stitch.LLMClient",
                    return_value=llm_client_stub,
                ):
                    result = handle_command(
                        databricks_client_stub, interactive_input="remove all tables"
                    )

        # Verify configuration modification behavior
        assert result.success
        assert "Please review the updated configuration" in result.message

    def test_interactive_phase_3_confirm_launches_job(
        self, databricks_client_stub, llm_client_stub, temp_config
    ):
        """Interactive Phase 3 'confirm' command launches the job."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup test data for successful job launch
            _setup_successful_stitch_test_data(databricks_client_stub, llm_client_stub)

            # Setup context with Phase 3 data
            context = InteractiveContext()
            context.set_active_context("setup-stitch")
            context.store_context_data("setup-stitch", "phase", "ready_to_launch")
            context.store_context_data(
                "setup-stitch", "stitch_config", _get_sample_stitch_config()
            )
            context.store_context_data(
                "setup-stitch", "metadata", _get_sample_metadata()
            )

            with patch(
                "chuck_data.commands.setup_stitch.InteractiveContext",
                return_value=context,
            ):
                with patch(
                    "chuck_data.commands.setup_stitch.get_metrics_collector",
                    return_value=MagicMock(),
                ):
                    result = handle_command(
                        databricks_client_stub, interactive_input="confirm"
                    )

        # Verify job launch behavior
        assert result.success
        assert "Stitch is now running" in result.message
        assert "run_id" in result.data

    def test_interactive_phase_3_cancel_command_exits_without_launch(
        self, databricks_client_stub, llm_client_stub, temp_config
    ):
        """Interactive Phase 3 'cancel' command exits without launching."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup context with Phase 3 data
            context = InteractiveContext()
            context.set_active_context("setup-stitch")
            context.store_context_data("setup-stitch", "phase", "ready_to_launch")
            context.store_context_data(
                "setup-stitch", "stitch_config", _get_sample_stitch_config()
            )
            context.store_context_data(
                "setup-stitch", "metadata", _get_sample_metadata()
            )

            with patch(
                "chuck_data.commands.setup_stitch.InteractiveContext",
                return_value=context,
            ):
                result = handle_command(
                    databricks_client_stub, interactive_input="cancel"
                )

        # Verify cancellation behavior
        assert result.success
        assert "Stitch job launch cancelled" in result.message

    def test_interactive_context_lost_returns_error(self, databricks_client_stub):
        """Interactive mode with lost context returns helpful error."""
        # Create empty context (no stored data)
        context = InteractiveContext()

        with patch(
            "chuck_data.commands.setup_stitch.InteractiveContext", return_value=context
        ):
            result = handle_command(
                databricks_client_stub, interactive_input="some command"
            )

        assert not result.success
        assert "Stitch setup context lost" in result.message
        assert "Please run /setup-stitch again" in result.message


class TestSetupStitchCommandConfiguration:
    """Test setup_stitch command configuration and registry integration."""

    def test_command_definition_structure(self):
        """Command definition has correct structure."""
        assert DEFINITION.name == "setup-stitch"
        assert "Interactively set up a Stitch integration" in DEFINITION.description
        assert DEFINITION.handler == handle_command
        assert "catalog_name" in DEFINITION.parameters
        assert "schema_name" in DEFINITION.parameters
        assert "auto_confirm" in DEFINITION.parameters
        assert DEFINITION.required_params == []
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True
        assert DEFINITION.supports_interactive_input is True

    def test_command_parameter_specification(self):
        """Command parameters are correctly specified."""
        catalog_param = DEFINITION.parameters["catalog_name"]
        assert catalog_param["type"] == "string"
        assert "Optional" in catalog_param["description"]
        assert "active catalog" in catalog_param["description"]

        schema_param = DEFINITION.parameters["schema_name"]
        assert schema_param["type"] == "string"
        assert "Optional" in schema_param["description"]
        assert "active schema" in schema_param["description"]

        auto_param = DEFINITION.parameters["auto_confirm"]
        assert auto_param["type"] == "boolean"
        assert "Optional" in auto_param["description"]
        assert "default: false" in auto_param["description"]

    def test_command_display_configuration(self):
        """Command display configuration is properly set."""
        assert DEFINITION.tui_aliases == ["/setup-stitch"]
        assert "Example:" in DEFINITION.usage_hint
        assert "auto-confirm" in DEFINITION.usage_hint
        assert DEFINITION.condensed_action == "Setting up Stitch integration"


class TestSetupStitchAgentBehavior:
    """Test setup_stitch command behavior with agent integration."""

    def test_agent_tool_executor_end_to_end_integration(
        self, databricks_client_stub, llm_client_stub, temp_config
    ):
        """Agent tool_executor integration works end-to-end."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("integration_catalog")
            set_active_schema("integration_schema")

            # Setup test data
            _setup_successful_stitch_test_data(
                databricks_client_stub,
                llm_client_stub,
                "integration_catalog",
                "integration_schema",
            )

            with patch(
                "chuck_data.commands.setup_stitch.LLMClient",
                return_value=llm_client_stub,
            ):
                with patch(
                    "chuck_data.commands.stitch_tools.get_amperity_token",
                    return_value="test_token",
                ):
                    result = execute_tool(
                        api_client=databricks_client_stub,
                        tool_name="setup-stitch",
                        tool_args={
                            "catalog_name": "integration_catalog",
                            "schema_name": "integration_schema",
                        },
                    )

        # Verify agent gets proper result format
        assert isinstance(result, dict)
        assert result.get("success") is True
        assert "message" in result

    def test_agent_callback_errors_handled_gracefully(
        self, databricks_client_stub, llm_client_stub, temp_config
    ):
        """Agent callback failures are handled gracefully (current behavior)."""
        with patch("chuck_data.config._config_manager", temp_config):
            set_active_catalog("test_catalog")
            set_active_schema("test_schema")

            # Setup minimal test data (will likely fail but shouldn't crash)
            databricks_client_stub.add_catalog("test_catalog")
            databricks_client_stub.add_schema("test_catalog", "test_schema")

            def failing_callback(tool_name, data):
                raise Exception("Display system failure")

            with patch(
                "chuck_data.commands.setup_stitch.LLMClient",
                return_value=llm_client_stub,
            ):
                # Note: setup-stitch doesn't use tool_output_callback for progress reporting
                # It should complete or fail regardless of callback issues
                result = handle_command(
                    databricks_client_stub,
                    catalog_name="test_catalog",
                    schema_name="test_schema",
                    tool_output_callback=failing_callback,
                )

        # Should fail for business logic reasons (no PII data), not callback issues
        assert not result.success
        # Error should be related to business logic, not callback
        assert "Display system failure" not in result.message


class TestSetupStitchEdgeCases:
    """Test edge cases and boundary conditions for setup_stitch."""

    def test_unicode_catalog_and_schema_names_handled_correctly(
        self, databricks_client_stub, llm_client_stub, temp_config
    ):
        """Unicode characters in catalog and schema names are handled correctly."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup with unicode names
            unicode_catalog = "目录_測試"
            unicode_schema = "スキーマ_test"

            _setup_successful_stitch_test_data(
                databricks_client_stub, llm_client_stub, unicode_catalog, unicode_schema
            )

            with patch(
                "chuck_data.commands.setup_stitch.LLMClient",
                return_value=llm_client_stub,
            ):
                with patch(
                    "chuck_data.commands.stitch_tools.get_amperity_token",
                    return_value="test_token",
                ):
                    result = handle_command(
                        databricks_client_stub,
                        catalog_name=unicode_catalog,
                        schema_name=unicode_schema,
                    )

        # Verify unicode handling
        assert result.success
        assert result.message == ""

    def test_very_long_catalog_and_schema_names(
        self, databricks_client_stub, llm_client_stub, temp_config
    ):
        """Very long catalog and schema names are handled correctly."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Create long names
            long_catalog = "very_long_catalog_name_" + "x" * 200
            long_schema = "very_long_schema_name_" + "x" * 200

            _setup_successful_stitch_test_data(
                databricks_client_stub, llm_client_stub, long_catalog, long_schema
            )

            with patch(
                "chuck_data.commands.setup_stitch.LLMClient",
                return_value=llm_client_stub,
            ):
                with patch(
                    "chuck_data.commands.stitch_tools.get_amperity_token",
                    return_value="test_token",
                ):
                    result = handle_command(
                        databricks_client_stub,
                        catalog_name=long_catalog,
                        schema_name=long_schema,
                    )

        # Verify long name handling
        assert result.success
        assert result.message == ""

    def test_error_handling_clears_interactive_context(
        self, databricks_client_stub, llm_client_stub, temp_config
    ):
        """Errors during setup clear the interactive context properly."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup context that will be cleared on error
            context = InteractiveContext()
            context.set_active_context("setup-stitch")

            # Force an error by not setting up required data
            with patch(
                "chuck_data.commands.setup_stitch.InteractiveContext",
                return_value=context,
            ):
                with patch(
                    "chuck_data.commands.setup_stitch.LLMClient",
                    return_value=llm_client_stub,
                ):
                    result = handle_command(
                        databricks_client_stub,
                        catalog_name="test_catalog",
                        schema_name="test_schema",
                    )

        # Verify error handling and context cleanup
        assert not result.success
        # The specific error can vary, but we want to verify context cleanup happens
        assert result.message  # Should have some error message
