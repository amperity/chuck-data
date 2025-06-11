"""
Tests for list_models command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
These tests verify what users see when they interact with the list_models command,
both directly and when an agent uses the list-models tool.
"""

import pytest
from unittest.mock import patch

from chuck_data.commands.list_models import handle_command
from chuck_data.config import set_active_model


class TestListModelsParameterValidation:
    """Test parameter validation for list_models command."""

    def test_none_client_returns_error(self):
        """None client returns error."""
        result = handle_command(None)

        assert not result.success
        assert isinstance(result.error, Exception)
        assert (
            "NoneType" in str(result.error)
            or "None" in str(result.error)
            or "client" in str(result.error).lower()
        )


class TestDirectListModelsCommand:
    """Test direct list_models command execution."""

    def test_direct_command_lists_models_basic_format(
        self, databricks_client_stub, temp_config
    ):
        """Direct command lists models in basic format by default."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup test data
            databricks_client_stub.add_model("claude-v1", created_timestamp=123456789)
            databricks_client_stub.add_model("gpt-4", created_timestamp=987654321)
            set_active_model("claude-v1")

            # Execute command
            result = handle_command(databricks_client_stub)

            # Verify behavioral outcome
            assert result.success
            assert len(result.data["models"]) == 2
            assert result.data["active_model"] == "claude-v1"
            assert not result.data["detailed"]
            assert result.data["filter"] is None
            assert result.message is None

    def test_direct_command_shows_detailed_information_when_requested(
        self, databricks_client_stub, temp_config
    ):
        """Direct command shows detailed model information when requested."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup test data with details
            databricks_client_stub.add_model(
                "claude-v1", created_timestamp=123456789, details="claude details"
            )
            databricks_client_stub.add_model(
                "gpt-4", created_timestamp=987654321, details="gpt details"
            )
            set_active_model("claude-v1")

            # Execute command with detailed flag
            result = handle_command(databricks_client_stub, detailed=True)

            # Verify detailed behavioral outcome
            assert result.success
            assert len(result.data["models"]) == 2
            assert result.data["detailed"]
            assert "details" in result.data["models"][0]
            assert "details" in result.data["models"][1]

    def test_direct_command_filters_models_by_name_pattern(
        self, databricks_client_stub, temp_config
    ):
        """Direct command filters models by name pattern."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup test data with mixed model names
            databricks_client_stub.add_model("claude-v1", created_timestamp=123456789)
            databricks_client_stub.add_model("gpt-4", created_timestamp=987654321)
            databricks_client_stub.add_model(
                "claude-instant", created_timestamp=456789123
            )
            set_active_model("claude-v1")

            # Execute command with filter
            result = handle_command(databricks_client_stub, filter="claude")

            # Verify filtering behavior
            assert result.success
            assert len(result.data["models"]) == 2
            assert all("claude" in model["name"] for model in result.data["models"])
            assert result.data["filter"] == "claude"

    def test_direct_command_handles_empty_model_list(
        self, databricks_client_stub, temp_config
    ):
        """Direct command handles empty model list gracefully."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Don't add any models to stub

            # Execute command
            result = handle_command(databricks_client_stub)

            # Verify graceful handling of empty list
            assert result.success
            assert len(result.data["models"]) == 0
            assert "No models found" in result.message
            assert "set up a model" in result.message.lower()

    def test_direct_command_with_detailed_empty_list(
        self, databricks_client_stub, temp_config
    ):
        """Direct command handles empty model list with detailed flag."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Execute command with detailed parameter
            result = handle_command(databricks_client_stub, detailed=True)

            # Verify graceful handling
            assert result.success
            assert len(result.data["models"]) == 0
            assert result.data["detailed"]
            assert "No models found" in result.message

    def test_direct_command_filter_with_no_matches(
        self, databricks_client_stub, temp_config
    ):
        """Direct command handles filter with no matches."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add models that won't match the filter
            databricks_client_stub.add_model("claude-v1")
            databricks_client_stub.add_model("gpt-4")

            # Execute command with non-matching filter
            result = handle_command(databricks_client_stub, filter="nonexistent")

            # Verify empty result with filter
            assert result.success
            assert len(result.data["models"]) == 0
            assert result.data["filter"] == "nonexistent"
            assert "No models found" in result.message

    def test_direct_command_case_insensitive_filter(
        self, databricks_client_stub, temp_config
    ):
        """Direct command applies case-insensitive filtering."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add models with mixed case
            databricks_client_stub.add_model("Claude-v1")
            databricks_client_stub.add_model("GPT-4")

            # Execute with lowercase filter
            result = handle_command(databricks_client_stub, filter="claude")

            # Verify case-insensitive matching
            assert result.success
            assert len(result.data["models"]) == 1
            assert result.data["models"][0]["name"] == "Claude-v1"

    def test_direct_command_without_active_model(
        self, databricks_client_stub, temp_config
    ):
        """Direct command works with no active model set."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Setup test data but don't set active model
            databricks_client_stub.add_model("claude-v1")
            databricks_client_stub.add_model("gpt-4")

            # Execute command
            result = handle_command(databricks_client_stub)

            # Verify result with no active model
            assert result.success
            assert result.data["active_model"] is None

    def test_databricks_api_errors_handled_gracefully(
        self, databricks_client_stub, temp_config
    ):
        """Databricks API errors are handled gracefully."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Configure stub to raise API exception
            databricks_client_stub.set_list_models_error(Exception("API error"))

            # Execute command
            result = handle_command(databricks_client_stub)

            # Verify graceful error handling
            assert not result.success
            assert str(result.error) == "API error"
            # The error message is passed through directly from the API
            assert "API error" in result.message


class TestListModelsCommandConfiguration:
    """Test list_models command configuration and registry integration."""

    def test_command_definition_properties(self):
        """List_models command definition has correct configuration."""
        from chuck_data.commands.list_models import DEFINITION

        assert DEFINITION.name == "list-models"
        assert "models" in DEFINITION.description.lower()
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True
        assert DEFINITION.agent_display == "full"

    def test_command_parameter_definitions(self):
        """List_models command has correct parameter definitions."""
        from chuck_data.commands.list_models import DEFINITION

        parameters = DEFINITION.parameters
        assert "detailed" in parameters
        assert parameters["detailed"]["type"] == "boolean"
        assert "filter" in parameters
        assert parameters["filter"]["type"] == "string"

    def test_command_aliases(self):
        """List_models command has expected aliases."""
        from chuck_data.commands.list_models import DEFINITION

        assert "/models" in DEFINITION.tui_aliases
        assert "/list-models" in DEFINITION.tui_aliases


class TestListModelsDisplayIntegration:
    """Test list_models command integration with display system."""

    def test_command_result_contains_display_ready_data(
        self, databricks_client_stub, temp_config
    ):
        """List_models command returns display-ready data structure."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add test models with display properties
            databricks_client_stub.add_model(
                "claude-v1",
                status="READY",
                creator_name="test@example.com",
                created_timestamp=123456789,
            )

            result = handle_command(databricks_client_stub)

            # Verify data structure is display-ready
            assert result.success
            assert isinstance(result.data, dict)
            assert "models" in result.data
            assert isinstance(result.data["models"], list)

            # Ensure model data has expected display fields
            model = result.data["models"][0]
            assert "name" in model
            assert "status" in model


class TestListModelsAgentBehavior:
    """Test list_models command agent-specific behavior."""

    def test_agent_tool_executor_end_to_end_integration(
        self, databricks_client_stub, temp_config
    ):
        """Agent tool_executor integration works end-to-end."""
        from chuck_data.agent.tool_executor import execute_tool

        with patch("chuck_data.config._config_manager", temp_config):
            # Setup test data
            databricks_client_stub.add_model("claude-v1", created_timestamp=123456789)
            databricks_client_stub.add_model("gpt-4", created_timestamp=987654321)
            set_active_model("claude-v1")

            # Execute via agent tool_executor
            result = execute_tool(
                api_client=databricks_client_stub, tool_name="list-models", tool_args={}
            )

            # Verify agent gets proper result format (list_models returns data dict)
            assert "models" in result
            assert "active_model" in result
            assert len(result["models"]) == 2
            assert result["active_model"] == "claude-v1"

    def test_agent_list_models_with_detailed_flag(
        self, databricks_client_stub, temp_config
    ):
        """Agent can request detailed model information."""
        from chuck_data.agent.tool_executor import execute_tool

        with patch("chuck_data.config._config_manager", temp_config):
            # Setup test data
            databricks_client_stub.add_model("claude-v1")

            # Execute via agent with detailed flag
            result = execute_tool(
                api_client=databricks_client_stub,
                tool_name="list-models",
                tool_args={"detailed": True},
            )

            # Verify detailed flag is respected
            assert result["detailed"] is True

    def test_agent_list_models_with_filter(self, databricks_client_stub, temp_config):
        """Agent can filter models by name pattern."""
        from chuck_data.agent.tool_executor import execute_tool

        with patch("chuck_data.config._config_manager", temp_config):
            # Setup test data with multiple models
            databricks_client_stub.add_model("claude-v1")
            databricks_client_stub.add_model("gpt-4")
            databricks_client_stub.add_model("claude-instant")

            # Execute via agent with filter
            result = execute_tool(
                api_client=databricks_client_stub,
                tool_name="list-models",
                tool_args={"filter": "claude"},
            )

            # Verify filtering works
            assert result["filter"] == "claude"
            assert len(result["models"]) == 2
            assert all("claude" in model["name"].lower() for model in result["models"])


class TestListModelsEdgeCases:
    """Test edge cases and boundary conditions for list_models command."""

    def test_command_handles_unicode_in_model_names(
        self, databricks_client_stub, temp_config
    ):
        """List_models command handles Unicode characters in model names."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add model with Unicode in name
            databricks_client_stub.add_model("模型-v1")
            databricks_client_stub.add_model("üñîçødé-model")

            # Filter for Unicode model
            result = handle_command(databricks_client_stub, filter="模型")

            # Verify Unicode handling
            assert result.success
            assert len(result.data["models"]) == 1
            assert result.data["models"][0]["name"] == "模型-v1"

    def test_command_with_model_having_complex_metadata(
        self, databricks_client_stub, temp_config
    ):
        """List_models command handles models with complex metadata."""
        with patch("chuck_data.config._config_manager", temp_config):
            # Add model with nested metadata
            complex_metadata = {
                "parameters": {"temperature": 0.7, "top_p": 0.95},
                "endpoints": ["us-west", "eu-central"],
                "tags": {"purpose": "testing", "version": "beta"},
            }

            databricks_client_stub.add_model("complex-model", **complex_metadata)

            # Get detailed model info
            result = handle_command(databricks_client_stub, detailed=True)

            # Verify complex metadata handling
            assert result.success
            model = result.data["models"][0]
            assert model["name"] == "complex-model"
            assert "parameters" in model
            assert "endpoints" in model
            assert "tags" in model
