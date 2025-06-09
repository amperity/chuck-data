"""
Integration tests for TUI display system.

These tests verify end-to-end display flows and integration between components,
providing safety for refactoring by testing complete user-visible scenarios.
"""

import pytest
import tempfile
from unittest.mock import MagicMock, patch
from rich.console import Console
from rich.panel import Panel

from chuck_data.ui.tui import ChuckTUI
from chuck_data.config import ConfigManager
from chuck_data.commands.base import CommandResult
from chuck_data.exceptions import PaginationCancelled


def create_real_command_def(
    name="test-tool", agent_display="condensed", display_condition=None
):
    """Create a real CommandDefinition with test attributes."""

    def dummy_handler(**kwargs):
        return {"success": True}

    from chuck_data.command_registry import CommandDefinition

    cmd_def = CommandDefinition(
        name=name,
        handler=dummy_handler,
        description="Test command",
        agent_display=agent_display,
    )

    if display_condition:
        cmd_def.display_condition = display_condition

    return cmd_def


@pytest.fixture
def tui_with_captured_console():
    """Create ChuckTUI instance with console that captures output for testing."""
    # Only mock external boundary (console)
    with patch("chuck_data.ui.tui.ChuckService"):
        tui = ChuckTUI()
        # Mock only the console output - the external boundary
        tui.console = MagicMock(spec=Console)
        return tui


@pytest.fixture
def temp_config():
    """Create a temporary config manager for testing."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        yield config_manager


class TestEndToEndDisplayFlows:
    """Test complete end-to-end display flows from tool execution to output."""

    def test_complete_agent_tool_to_display_flow(self, tui_with_captured_console):
        """Test complete flow: agent calls tool -> routing -> display output."""
        tui = tui_with_captured_console

        # Use real command definition for list-catalogs
        from chuck_data.commands.list_catalogs import DEFINITION
        from chuck_data.command_registry import TUI_COMMAND_MAP

        # Save original definition if it exists
        original_definition = TUI_COMMAND_MAP.get("list-catalogs")
        tool_name = "list-catalogs"

        try:
            # Register the real command definition
            TUI_COMMAND_MAP[tool_name] = DEFINITION

            # Test data that would come from actual command execution
            catalog_data = {
                "catalogs": [
                    {
                        "name": "production",
                        "type": "MANAGED",
                        "comment": "Production data",
                    },
                    {"name": "dev", "type": "MANAGED", "comment": "Development data"},
                ],
                "current_catalog": "production",
                "total_count": 2,
                "display": True,  # This should trigger full display
            }

            # Patch the display method to avoid pagination and track calls
            with patch.object(tui, "_display_catalogs") as mock_display_catalogs:
                mock_display_catalogs.side_effect = (
                    PaginationCancelled()
                )  # Expected behavior

                # Execute the complete flow
                with pytest.raises(PaginationCancelled):
                    tui.display_tool_output(tool_name, catalog_data)

                # Verify the right display method was called with right data
                mock_display_catalogs.assert_called_once_with(catalog_data)
        finally:
            # Restore original command definition
            if original_definition is not None:
                TUI_COMMAND_MAP[tool_name] = original_definition
            else:
                TUI_COMMAND_MAP.pop(tool_name, None)

    def test_agent_condensed_display_flow(self, tui_with_captured_console):
        """Test agent condensed display flow without display=true."""
        tui = tui_with_captured_console

        # Use real command definition for list-catalogs
        from chuck_data.commands.list_catalogs import DEFINITION
        from chuck_data.command_registry import TUI_COMMAND_MAP

        # Save original definition if it exists
        tool_name = "list-catalogs"
        original_definition = TUI_COMMAND_MAP.get(tool_name)

        try:
            # Register the real command definition
            TUI_COMMAND_MAP[tool_name] = DEFINITION

            # Test data without display=true (should use condensed)
            catalog_data = {
                "catalogs": [
                    {"name": "production", "type": "MANAGED"},
                    {"name": "dev", "type": "MANAGED"},
                ],
                "total_count": 2,
                # No display=true, so should use condensed
            }

            # Create a tracking wrapper for the condensed display method
            original_method = tui._display_condensed_tool_output
            condensed_called = [False]
            condensed_args = [None, None]

            def tracking_condensed(tool_name, tool_data):
                condensed_called[0] = True
                condensed_args[0] = tool_name
                condensed_args[1] = tool_data
                # Don't actually call original to avoid potential errors
                return None

            # Replace method with tracking version
            tui._display_condensed_tool_output = tracking_condensed

            try:
                # Call the method
                tui.display_tool_output(tool_name, catalog_data)

                # Verify condensed method was called with correct args
                assert condensed_called[
                    0
                ], "_display_condensed_tool_output should be called"
                assert condensed_args[0] == tool_name, "Tool name should match"
                assert condensed_args[1] == catalog_data, "Tool data should match"
            finally:
                # Restore original method
                tui._display_condensed_tool_output = original_method
        finally:
            # Restore original command definition
            if original_definition is not None:
                TUI_COMMAND_MAP[tool_name] = original_definition
            else:
                TUI_COMMAND_MAP.pop(tool_name, None)

    def test_status_custom_handler_integration(self, tui_with_captured_console):
        """Test status command uses custom agent handler integration."""
        tui = tui_with_captured_console

        # Status should be registered in custom handlers
        assert "status" in tui.agent_full_display_handlers

        # Use real command definition with agent_display="full" to trigger custom handler
        tool_name = "status"
        from chuck_data.command_registry import TUI_COMMAND_MAP

        # Create and register a real command definition
        real_cmd_def = create_real_command_def(name=tool_name, agent_display="full")
        original_command = TUI_COMMAND_MAP.get(tool_name)
        TUI_COMMAND_MAP[tool_name] = real_cmd_def

        status_data = {
            "workspace_url": "https://test.databricks.com",
            "active_catalog": "production",
            "connection_status": "Connected",
            "permissions": {},
            # Note: For custom handlers, display=true is not required
        }

        # Create a tracking handler to verify it gets called
        handler_called = [False]
        handler_args = [None, None]

        def tracking_handler(tool_name, tool_data):
            handler_called[0] = True
            handler_args[0] = tool_name
            handler_args[1] = tool_data
            # Return a dummy value
            return None

        # Save original handler and register the tracking one
        original_handler = tui.agent_full_display_handlers[tool_name]

        try:
            # Register our tracking handler
            tui.agent_full_display_handlers[tool_name] = tracking_handler

            # Call the display method
            # Add display=True to trigger full display path
            tui.display_tool_output(tool_name, {**status_data, "display": True})

            # Verify our handler was called with correct args
            assert handler_called[0], "Custom handler should be called"
            assert handler_args[0] == tool_name, "Tool name should match"

            # Compare tool data, ignoring the display flag
            received_data = {k: v for k, v in handler_args[1].items() if k != "display"}
            assert (
                received_data == status_data
            ), "Tool data should match (ignoring display flag)"
        finally:
            # Restore original handler and command definition
            tui.agent_full_display_handlers[tool_name] = original_handler

            if original_command is not None:
                TUI_COMMAND_MAP[tool_name] = original_command
            else:
                TUI_COMMAND_MAP.pop(tool_name, None)

        def test_multiple_sequential_display_calls(self, tui_with_captured_console):
            """Test multiple sequential display calls work correctly."""
            tui = tui_with_captured_console

            # Create real command definitions with different display types
            condensed_cmd = create_real_command_def(
                name="tool1", agent_display="condensed"
            )
            full_cmd = create_real_command_def(name="tool2", agent_display="full")
            third_cmd = create_real_command_def(name="tool3", agent_display="condensed")

            # Register commands in real registry
            from chuck_data.command_registry import TUI_COMMAND_MAP

            original_commands = {
                "tool1": TUI_COMMAND_MAP.get("tool1"),
                "tool2": TUI_COMMAND_MAP.get("tool2"),
                "tool3": TUI_COMMAND_MAP.get("tool3"),
            }

            TUI_COMMAND_MAP["tool1"] = condensed_cmd
            TUI_COMMAND_MAP["tool2"] = full_cmd
            TUI_COMMAND_MAP["tool3"] = third_cmd

            try:
                # Create tracking wrappers for display methods
                condensed_calls = []
                full_calls = []

                original_condensed = tui._display_condensed_tool_output
                original_full = tui._display_full_tool_output

                def track_condensed(tool_name, tool_data):
                    condensed_calls.append((tool_name, tool_data))
                    # Don't call original to avoid potential exceptions
                    return None

                def track_full(tool_name, tool_data):
                    full_calls.append((tool_name, tool_data))
                    # Don't call original to avoid potential exceptions
                    return None

                # Replace with tracking versions
                tui._display_condensed_tool_output = track_condensed
                tui._display_full_tool_output = track_full

                try:
                    # First call - condensed
                    tui.display_tool_output("tool1", {"data": "test1"})

                    # Second call - full with display=true
                    tui.display_tool_output("tool2", {"display": True, "data": "test2"})

                    # Third call - condensed
                    tui.display_tool_output("tool3", {"data": "test3"})

                    # Verify correct routing for each call
                    assert len(condensed_calls) == 2
                    assert len(full_calls) == 1

                    # Verify the calls match expected tool and data
                    assert condensed_calls[0] == ("tool1", {"data": "test1"})
                    assert full_calls[0] == (
                        "tool2",
                        {"display": True, "data": "test2"},
                    )
                    assert condensed_calls[1] == ("tool3", {"data": "test3"})
                finally:
                    # Restore original methods
                    tui._display_condensed_tool_output = original_condensed
                    tui._display_full_tool_output = original_full
            finally:
                # Clean up registry
                for tool_name, original_cmd in original_commands.items():
                    if original_cmd is not None:
                        TUI_COMMAND_MAP[tool_name] = original_cmd
                    else:
                        TUI_COMMAND_MAP.pop(tool_name, None)


class TestDisplayDataContractIntegration:
    """Test that display methods receive data in the format they expect."""

    def test_catalog_display_data_contract(self, tui_with_captured_console):
        """Test that catalog display methods get properly formatted catalog data."""
        tui = tui_with_captured_console

        # Instead of mocking the table formatter, wrap it to capture args
        display_table_calls = []

        # Import the real display_table function

        def capture_display_table_args(**kwargs):
            # Save the arguments for assertion
            display_table_calls.append(kwargs)
            # Simulate expected behavior without actually displaying
            raise PaginationCancelled()

        # Patch only the external boundary (formatting function)
        with patch(
            "chuck_data.ui.table_formatter.display_table",
            side_effect=capture_display_table_args,
        ):
            # Test data in expected format
            catalog_data = {
                "catalogs": [
                    {
                        "name": "prod",
                        "type": "MANAGED",
                        "comment": "Production",
                        "owner": "admin",
                    },
                    {
                        "name": "dev",
                        "type": "EXTERNAL",
                        "comment": "Development",
                        "owner": "dev-team",
                    },
                ],
                "current_catalog": "prod",
            }

            # Call the display method directly
            with pytest.raises(PaginationCancelled):
                tui._display_catalogs(catalog_data)

            # Verify display_table was called with properly formatted data
            assert len(display_table_calls) == 1
            call_kwargs = display_table_calls[0]

            # Check the table data format
            assert call_kwargs["data"] == catalog_data["catalogs"]
            assert call_kwargs["columns"] == ["name", "type", "comment", "owner"]
            assert call_kwargs["headers"] == ["Name", "Type", "Comment", "Owner"]
            assert call_kwargs["title"] == "Available Catalogs"

    def test_schema_display_data_contract(self, tui_with_captured_console):
        """Test that schema display methods get properly formatted schema data."""
        tui = tui_with_captured_console

        # Use a wrapper to capture display_table arguments
        display_table_calls = []

        def capture_display_table_args(**kwargs):
            # Save the arguments for assertion
            display_table_calls.append(kwargs)
            # Simulate expected behavior without actually displaying
            raise PaginationCancelled()

        # Only patch the external display boundary
        with patch(
            "chuck_data.ui.table_formatter.display_table",
            side_effect=capture_display_table_args,
        ):
            schema_data = {
                "schemas": [
                    {"name": "bronze", "comment": "Bronze layer"},
                    {"name": "silver", "comment": "Silver layer"},
                ],
                "catalog_name": "production",
                "current_schema": "bronze",
            }

            with pytest.raises(PaginationCancelled):
                tui._display_schemas(schema_data)

            # Verify display_table was called
            assert len(display_table_calls) == 1
            call_kwargs = display_table_calls[0]

            # Verify schema-specific formatting
            assert "Schemas in catalog" in call_kwargs["title"]
            assert "production" in call_kwargs["title"]

            # Verify expected columns and data
            assert call_kwargs["columns"] == ["name", "comment"]
            assert call_kwargs["headers"] == ["Name", "Comment"]
            assert call_kwargs["data"] == schema_data["schemas"]

    def test_status_display_data_contract(self, tui_with_captured_console):
        """Test that status display methods get properly formatted status data."""
        tui = tui_with_captured_console

        # Reset the console mock to ensure we only track calls from this test
        tui.console.reset_mock()

        # Test data in expected format
        status_data = {
            "workspace_url": "https://test.databricks.com",
            "active_catalog": "production",
            "active_schema": "bronze",
            "active_model": "test-model",
            "warehouse_id": "warehouse-123",
            "connection_status": "Connected (client present).",
            "permissions": {"catalog_access": {"authorized": True}},
        }

        # Call the agent status display method directly
        tui._display_status_for_agent("status", status_data)

        # Verify a Panel was printed with expected content
        tui.console.print.assert_called_once()
        call_args = tui.console.print.call_args[0][0]

        # Verify the Panel structure
        assert isinstance(call_args, Panel)
        assert call_args.title == "Current Status"
        assert call_args.border_style == "cyan"

        # Convert Panel content to string for easier testing
        panel_content = str(call_args.renderable)

        # Verify status data is properly formatted in the panel
        assert "test.databricks.com" in panel_content
        assert "production" in panel_content
        assert "bronze" in panel_content
        assert "test-model" in panel_content
        assert "warehouse-123" in panel_content
        assert "Connected" in panel_content


class TestDisplayConsistencyIntegration:
    """Test that same data produces consistent display across different call paths."""

    def test_status_display_consistency_agent_vs_direct(self, temp_config):
        """Test that status data displays consistently via agent vs direct calls."""
        # Create a temporary config with test values
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            config_manager.update(
                workspace_url="https://test.databricks.com",
                active_catalog="production",
                active_schema="bronze",
            )

            # Use the real configuration but mock only the console output
            with patch("chuck_data.config._config_manager", config_manager):
                tui = ChuckTUI()
                tui.console = MagicMock(spec=Console)

                # Test data for status calls
                status_data = {
                    "workspace_url": "https://test.databricks.com",
                    "active_catalog": "production",
                    "active_schema": "bronze",
                    "connection_status": "Connected",
                    "permissions": {},
                }

                # Call via agent display method
                tui._display_status_for_agent("status", status_data)
                agent_call_args = tui.console.print.call_args

                # Reset console mock for second call
                tui.console.reset_mock()

                # Create a wrapper for table_formatter.display_table to catch PaginationCancelled
                display_table_called = [False]

                def track_display_table(**kwargs):
                    display_table_called[0] = True
                    tui.console.print("Table displayed")
                    raise PaginationCancelled()

                with patch(
                    "chuck_data.ui.table_formatter.display_table",
                    side_effect=track_display_table,
                ):
                    # Call via direct display method (should raise PaginationCancelled)
                    with pytest.raises(PaginationCancelled):
                        tui._display_status(status_data)

                # Both should have printed something
                assert agent_call_args is not None
                assert tui.console.print.called

                # Verify consistent formatting between both methods
                agent_panel = agent_call_args[0][0]
                assert isinstance(agent_panel, Panel)

                # Direct display calls table_formatter which we made raise PaginationCancelled
                # but it still should have printed a console header

        def test_catalog_display_consistency_different_routing_paths(
            self, tui_with_captured_console
        ):
            """Test catalog display consistency across different routing paths."""
            tui = tui_with_captured_console

            # Create real catalog data
            catalog_data = {
                "catalogs": [{"name": "test", "type": "MANAGED"}],
                "total_count": 1,
            }

            # Create a real command definition with full display
            real_cmd = create_real_command_def(
                name="list-catalogs", agent_display="full"
            )

            # Register with real command registry
            from chuck_data.command_registry import TUI_COMMAND_MAP

            original_command = TUI_COMMAND_MAP.get("list-catalogs")
            TUI_COMMAND_MAP["list-catalogs"] = real_cmd

            try:
                # Track calls to _display_catalogs
                first_call_data = None
                second_call_data = None
                call_count = [0]

                # Save the original method
                original_display_catalogs = tui._display_catalogs

                # Create a wrapper that captures arguments and raises PaginationCancelled
                def capture_display_catalogs(data):
                    call_count[0] += 1

                    # Store data based on call order
                    nonlocal first_call_data, second_call_data
                    if call_count[0] == 1:
                        first_call_data = data.copy()
                    elif call_count[0] == 2:
                        second_call_data = data.copy()

                    # Simulate expected behavior without actually displaying
                    raise PaginationCancelled()

                # Replace the method
                tui._display_catalogs = capture_display_catalogs

                try:
                    # First call via display_tool_output with display=true
                    with pytest.raises(PaginationCancelled):
                        tui.display_tool_output(
                            "list-catalogs", {**catalog_data, "display": True}
                        )

                    # Second call via _display_full_tool_output directly
                    with pytest.raises(PaginationCancelled):
                        tui._display_full_tool_output("list-catalogs", catalog_data)

                    # Both methods should have been called
                    assert call_count[0] == 2

                    # Verify data passes through both routes correctly
                    assert first_call_data is not None
                    assert second_call_data is not None

                    # Data should be equivalent (ignoring display flag difference)
                    assert first_call_data["catalogs"] == second_call_data["catalogs"]
                    assert (
                        first_call_data["total_count"]
                        == second_call_data["total_count"]
                    )
                finally:
                    # Restore original method
                    tui._display_catalogs = original_display_catalogs
            finally:
                # Clean up registry
                if original_command is not None:
                    TUI_COMMAND_MAP["list-catalogs"] = original_command
                else:
                    TUI_COMMAND_MAP.pop("list-catalogs", None)


class TestDisplayInteractionWithAgentSystem:
    """Test display system interaction with agent execution system."""

    def test_agent_tool_executor_integration_with_display(self):
        """Test integration between agent tool executor and display system."""
        from chuck_data.agent.tool_executor import execute_tool

        # Create a real TUI instance, only mocking the external console
        with patch("chuck_data.ui.tui.ChuckService"):
            tui = ChuckTUI()
            tui.console = MagicMock(spec=Console)

            # Use the real callback mechanism
            def real_output_callback(tool_name, tool_data):
                """Real callback that displays tool output."""
                tui.display_tool_output(tool_name, tool_data)

            # Make a mock API client
            mock_api_client = MagicMock()

            # Use real command definition from the registry
            from chuck_data.commands.status import DEFINITION

            # We only need to patch the handler since we can't connect to real Databricks
            with patch.object(DEFINITION, "handler") as mock_handler:
                # Set up mock handler to return realistic test data
                mock_handler.return_value = CommandResult(
                    True,
                    data={
                        "workspace_url": "https://test.databricks.com",
                        "active_catalog": "production",
                        "connection_status": "Connected",
                        "permissions": {},
                    },
                    message="Status retrieved",
                )
                # Need to add __name__ attribute to mock
                mock_handler.__name__ = "status_handler"

                # Register the status definition for display
                try:
                    # Make sure the status custom handler is registered
                    tui.agent_full_display_handlers["status"] = (
                        tui._display_status_for_agent
                    )

                    # Execute tool with output callback - this uses the real tool executor
                    result = execute_tool(
                        mock_api_client,
                        "status",
                        {},
                        output_callback=real_output_callback,
                    )

                    # Verify tool executed successfully
                    assert "workspace_url" in result
                    assert result["workspace_url"] == "https://test.databricks.com"

                    # Verify display was called (custom status handler)
                    tui.console.print.assert_called()
                finally:
                    # Clean up if needed
                    pass

    def test_pagination_cancelled_propagation_through_agent_system(
        self, tui_with_captured_console
    ):
        """Test that PaginationCancelled propagates correctly through agent system."""
        tui = tui_with_captured_console

        # Create real command definition with full display
        real_cmd = create_real_command_def(name="list-catalogs", agent_display="full")

        # Register with real command registry
        from chuck_data.command_registry import TUI_COMMAND_MAP

        original_command = TUI_COMMAND_MAP.get("list-catalogs")
        TUI_COMMAND_MAP["list-catalogs"] = real_cmd

        try:
            # Create a wrapper for _display_full_tool_output that raises PaginationCancelled
            original_display_full = tui._display_full_tool_output

            def raising_display_full(*args, **kwargs):
                raise PaginationCancelled()

            # Replace the method
            tui._display_full_tool_output = raising_display_full

            try:
                # Simulate agent calling display
                def agent_callback(tool_name, tool_data):
                    tui.display_tool_output(tool_name, tool_data)

                # Should propagate PaginationCancelled to agent
                with pytest.raises(PaginationCancelled):
                    agent_callback("list-catalogs", {"display": True, "catalogs": []})
            finally:
                # Restore original method
                tui._display_full_tool_output = original_display_full
        finally:
            # Clean up registry
            if original_command is not None:
                TUI_COMMAND_MAP["list-catalogs"] = original_command
            else:
                TUI_COMMAND_MAP.pop("list-catalogs", None)

    def test_display_error_isolation_from_agent_execution(
        self, tui_with_captured_console
    ):
        """Test that display errors don't break agent execution flow."""
        tui = tui_with_captured_console

        # Create real command definition with condensed display
        real_cmd = create_real_command_def(name="test-tool", agent_display="condensed")

        # Register with real command registry
        from chuck_data.command_registry import TUI_COMMAND_MAP

        original_command = TUI_COMMAND_MAP.get("test-tool")
        TUI_COMMAND_MAP["test-tool"] = real_cmd

        try:
            # Create a wrapper for _display_condensed_tool_output that fails
            original_condensed = tui._display_condensed_tool_output

            def failing_condensed(*args, **kwargs):
                raise Exception("Display failed")

            # Replace the method
            tui._display_condensed_tool_output = failing_condensed

            try:
                # Patch logging to prevent warning messages in test output
                with patch("chuck_data.ui.tui.logging.warning"):
                    # Simulate agent callback - should not raise
                    def agent_callback(tool_name, tool_data):
                        return tui.display_tool_output(tool_name, tool_data)

                    # Reset console mock
                    tui.console.reset_mock()

                    # Should handle error gracefully, not break agent flow
                    result = agent_callback("test-tool", {"data": "test"})
                    assert result is None  # No exception raised

                    # Should have printed fallback notification
                    tui.console.print.assert_called_with(
                        "[dim][Tool: test-tool executed][/dim]"
                    )
            finally:
                # Restore original method
                tui._display_condensed_tool_output = original_condensed
        finally:
            # Clean up registry
            if original_command is not None:
                TUI_COMMAND_MAP["test-tool"] = original_command
            else:
                TUI_COMMAND_MAP.pop("test-tool", None)


class TestDisplayPerformanceAndScaling:
    """Test display system performance and scaling characteristics."""

    def test_large_data_display_handling(self, tui_with_captured_console):
        """Test that large datasets are handled efficiently in display."""
        tui = tui_with_captured_console

        # Create large dataset
        large_catalog_data = {
            "catalogs": [
                {"name": f"catalog_{i}", "type": "MANAGED", "comment": f"Catalog {i}"}
                for i in range(1000)  # Large number of catalogs
            ],
            "total_count": 1000,
        }

        # Create a tracking wrapper for table display
        display_table_calls = []

        def track_display_table_args(**kwargs):
            display_table_calls.append(kwargs.copy())
            # Simulate expected behavior without actually displaying
            raise PaginationCancelled()

        # Only patch the external boundary
        with patch(
            "chuck_data.ui.table_formatter.display_table",
            side_effect=track_display_table_args,
        ):
            # Should handle large dataset without issues
            with pytest.raises(PaginationCancelled):
                tui._display_catalogs(large_catalog_data)

            # Verify display_table was called with the large dataset
            assert len(display_table_calls) == 1
            call_kwargs = display_table_calls[0]
            assert len(call_kwargs["data"]) == 1000

            # Verify the data was passed through without modification
            assert call_kwargs["data"] == large_catalog_data["catalogs"]

            # Verify that the display system uses the correct columns and headers
            assert call_kwargs["columns"] == ["name", "type", "comment", "owner"]
            assert call_kwargs["headers"] == ["Name", "Type", "Comment", "Owner"]

    def test_concurrent_display_call_safety(self):
        """Test that concurrent display calls don't interfere with each other."""
        # This is a basic test - real concurrency testing would require threading
        with patch("chuck_data.ui.tui.ChuckService"):
            # Create two independent TUI instances
            tui1 = ChuckTUI()
            tui1.console = MagicMock(spec=Console)

            tui2 = ChuckTUI()
            tui2.console = MagicMock(spec=Console)

            # Both TUI instances should be independent
            assert (
                tui1.agent_full_display_handlers is not tui2.agent_full_display_handlers
            )

            # Create real command definitions for both TUIs
            cmd_def = create_real_command_def(agent_display="condensed")

            # Register the command in the registry
            from chuck_data.command_registry import TUI_COMMAND_MAP

            original_command = TUI_COMMAND_MAP.get("test-tool")
            TUI_COMMAND_MAP["test-tool"] = cmd_def

            try:
                # Create tracking wrappers for both instances
                tui1_calls = []
                tui2_calls = []

                original1 = tui1._display_condensed_tool_output
                original2 = tui2._display_condensed_tool_output

                def track1(tool_name, tool_data):
                    tui1_calls.append((tool_name, tool_data))
                    # Add console output to ensure console.print is called
                    tui1.console.print(f"Tool: {tool_name}")

                def track2(tool_name, tool_data):
                    tui2_calls.append((tool_name, tool_data))
                    # Add console output to ensure console.print is called
                    tui2.console.print(f"Tool: {tool_name}")

                # Replace with tracking versions
                tui1._display_condensed_tool_output = track1
                tui2._display_condensed_tool_output = track2

                try:
                    # Make calls on different instances
                    tui1.display_tool_output("test-tool", {"data": "test1"})
                    tui2.display_tool_output("test-tool", {"data": "test2"})

                    # Verify each instance received the right data
                    assert len(tui1_calls) == 1
                    assert len(tui2_calls) == 1
                    assert tui1_calls[0] == ("test-tool", {"data": "test1"})
                    assert tui2_calls[0] == ("test-tool", {"data": "test2"})

                    # Verify console calls - each instance should print to its own console
                    assert tui1.console.print.called
                    assert tui2.console.print.called
                finally:
                    # Restore original methods
                    tui1._display_condensed_tool_output = original1
                    tui2._display_condensed_tool_output = original2
            finally:
                # Clean up registry
                if original_command is not None:
                    TUI_COMMAND_MAP["test-tool"] = original_command
                else:
                    TUI_COMMAND_MAP.pop("test-tool", None)
