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


@pytest.fixture
def mock_chuck_service_init():
    """Mock ChuckService to avoid complex dependencies in TUI tests."""
    with patch("chuck_data.ui.tui.ChuckService") as mock:
        mock.return_value = MagicMock()
        yield mock


@pytest.fixture
def tui_with_mocked_console(mock_chuck_service_init):
    """Create ChuckTUI instance with mocked console for testing."""
    tui = ChuckTUI()
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

    @patch("chuck_data.ui.tui.get_command")
    def test_complete_agent_tool_to_display_flow(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Test complete flow: agent calls tool -> routing -> display output."""
        tui = tui_with_mocked_console
        
        # Mock command definition for list-catalogs
        from chuck_data.commands.list_catalogs import DEFINITION
        mock_get_cmd.return_value = DEFINITION
        
        # Test data that would come from actual command execution
        catalog_data = {
            "catalogs": [
                {"name": "production", "type": "MANAGED", "comment": "Production data"},
                {"name": "dev", "type": "MANAGED", "comment": "Development data"},
            ],
            "current_catalog": "production",
            "total_count": 2,
            "display": True,  # This should trigger full display
        }
        
        # Mock the display method that should be called
        with patch.object(tui, '_display_catalogs') as mock_display_catalogs:
            mock_display_catalogs.side_effect = PaginationCancelled()  # Expected behavior
            
            # Execute the complete flow
            with pytest.raises(PaginationCancelled):
                tui.display_tool_output("list-catalogs", catalog_data)
            
            # Verify the right display method was called with right data
            mock_display_catalogs.assert_called_once_with(catalog_data)

    @patch("chuck_data.ui.tui.get_command")
    def test_agent_condensed_display_flow(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Test agent condensed display flow without display=true."""
        tui = tui_with_mocked_console
        
        # Mock command definition for list-catalogs
        from chuck_data.commands.list_catalogs import DEFINITION
        mock_get_cmd.return_value = DEFINITION
        
        # Test data without display=true (should use condensed)
        catalog_data = {
            "catalogs": [
                {"name": "production", "type": "MANAGED"},
                {"name": "dev", "type": "MANAGED"},
            ],
            "total_count": 2,
            # No display=true, so should use condensed
        }
        
        # Mock the condensed display method
        with patch.object(tui, '_display_condensed_tool_output') as mock_condensed:
            tui.display_tool_output("list-catalogs", catalog_data)
            
            # Should call condensed display
            mock_condensed.assert_called_once_with("list-catalogs", catalog_data)

    def test_status_custom_handler_integration(self, tui_with_mocked_console):
        """Test status command uses custom agent handler integration."""
        tui = tui_with_mocked_console
        
        # Status should be registered in custom handlers
        assert "status" in tui.agent_full_display_handlers
        
        # Mock command definition for status - use agent_display="full" to trigger custom handler
        with patch("chuck_data.ui.tui.get_command") as mock_get_cmd:
            mock_cmd = MagicMock()
            mock_cmd.agent_display = "full"  # This will trigger custom handler check
            mock_get_cmd.return_value = mock_cmd
            
            status_data = {
                "workspace_url": "https://test.databricks.com", 
                "active_catalog": "production",
                "connection_status": "Connected",
                "permissions": {}
                # Note: For custom handlers, display=true is not required
            }
            
            # Create a mock handler and register it (the registry is what gets called)
            mock_status_handler = MagicMock()
            original_handler = tui.agent_full_display_handlers["status"]
            tui.agent_full_display_handlers["status"] = mock_status_handler
            
            try:
                tui.display_tool_output("status", status_data)
                
                # Should call the custom handler from the registry
                mock_status_handler.assert_called_once_with("status", status_data)
            finally:
                # Restore original handler
                tui.agent_full_display_handlers["status"] = original_handler

    @patch("chuck_data.ui.tui.get_command")
    def test_multiple_sequential_display_calls(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Test multiple sequential display calls work correctly."""
        tui = tui_with_mocked_console
        
        # Mock different command definitions
        mock_condensed_cmd = MagicMock()
        mock_condensed_cmd.agent_display = "condensed"
        
        mock_full_cmd = MagicMock()
        mock_full_cmd.agent_display = "full"
        
        mock_get_cmd.side_effect = [mock_condensed_cmd, mock_full_cmd, mock_condensed_cmd]
        
        with patch.object(tui, '_display_condensed_tool_output') as mock_condensed, \
             patch.object(tui, '_display_full_tool_output') as mock_full:
            
            # First call - condensed
            tui.display_tool_output("tool1", {"data": "test1"})
            
            # Second call - full with display=true
            tui.display_tool_output("tool2", {"display": True, "data": "test2"})
            
            # Third call - condensed
            tui.display_tool_output("tool3", {"data": "test3"})
            
            # Verify correct routing for each call
            assert mock_condensed.call_count == 2
            assert mock_full.call_count == 1
            
            mock_condensed.assert_any_call("tool1", {"data": "test1"})
            mock_full.assert_called_once_with("tool2", {"display": True, "data": "test2"})
            mock_condensed.assert_any_call("tool3", {"data": "test3"})


class TestDisplayDataContractIntegration:
    """Test that display methods receive data in the format they expect."""

    def test_catalog_display_data_contract(self, tui_with_mocked_console):
        """Test that catalog display methods get properly formatted catalog data."""
        tui = tui_with_mocked_console
        
        # Mock the table display function to verify data format
        with patch("chuck_data.ui.table_formatter.display_table") as mock_display_table:
            mock_display_table.side_effect = PaginationCancelled()  # Expected behavior
            
            # Test data in expected format
            catalog_data = {
                "catalogs": [
                    {"name": "prod", "type": "MANAGED", "comment": "Production", "owner": "admin"},
                    {"name": "dev", "type": "EXTERNAL", "comment": "Development", "owner": "dev-team"},
                ],
                "current_catalog": "prod"
            }
            
            # Call the display method directly
            with pytest.raises(PaginationCancelled):
                tui._display_catalogs(catalog_data)
            
            # Verify display_table was called with properly formatted data
            mock_display_table.assert_called_once()
            call_args = mock_display_table.call_args
            
            # Check the table data format
            assert call_args[1]["data"] == catalog_data["catalogs"]
            assert call_args[1]["columns"] == ["name", "type", "comment", "owner"]
            assert call_args[1]["headers"] == ["Name", "Type", "Comment", "Owner"]
            assert call_args[1]["title"] == "Available Catalogs"

    def test_schema_display_data_contract(self, tui_with_mocked_console):
        """Test that schema display methods get properly formatted schema data."""
        tui = tui_with_mocked_console
        
        with patch("chuck_data.ui.table_formatter.display_table") as mock_display_table:
            mock_display_table.side_effect = PaginationCancelled()
            
            schema_data = {
                "schemas": [
                    {"name": "bronze", "comment": "Bronze layer"},
                    {"name": "silver", "comment": "Silver layer"},
                ],
                "catalog_name": "production",
                "current_schema": "bronze"
            }
            
            with pytest.raises(PaginationCancelled):
                tui._display_schemas(schema_data)
            
            mock_display_table.assert_called_once()
            call_args = mock_display_table.call_args
            
            # Verify schema-specific formatting
            assert "Schemas in catalog" in call_args[1]["title"]
            assert "production" in call_args[1]["title"]

    def test_status_display_data_contract(self, tui_with_mocked_console):
        """Test that status display methods get properly formatted status data."""
        tui = tui_with_mocked_console
        
        status_data = {
            "workspace_url": "https://test.databricks.com",
            "active_catalog": "production",
            "active_schema": "bronze",
            "active_model": "test-model",
            "warehouse_id": "warehouse-123",
            "connection_status": "Connected (client present).",
            "permissions": {"catalog_access": {"authorized": True}}
        }
        
        # Call the agent status display method
        tui._display_status_for_agent("status", status_data)
        
        # Verify a Panel was printed with expected content
        tui.console.print.assert_called_once()
        call_args = tui.console.print.call_args[0][0]
        
        assert isinstance(call_args, Panel)
        panel_content = str(call_args.renderable)
        
        # Verify status data is properly formatted in the panel
        assert "test.databricks.com" in panel_content
        assert "production" in panel_content
        assert "bronze" in panel_content
        assert "Connected" in panel_content


class TestDisplayConsistencyIntegration:
    """Test that same data produces consistent display across different call paths."""

    def test_status_display_consistency_agent_vs_direct(self, temp_config):
        """Test that status data displays consistently via agent vs direct calls."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            config_manager.update(
                workspace_url="https://test.databricks.com",
                active_catalog="production",
                active_schema="bronze"
            )
            
            with patch("chuck_data.config._config_manager", config_manager):
                tui = ChuckTUI()
                tui.console = MagicMock(spec=Console)
                
                status_data = {
                    "workspace_url": "https://test.databricks.com",
                    "active_catalog": "production",
                    "active_schema": "bronze",
                    "connection_status": "Connected",
                    "permissions": {}
                }
                
                # Call via agent display method
                tui._display_status_for_agent("status", status_data)
                agent_call_args = tui.console.print.call_args
                
                # Reset console mock
                tui.console.reset_mock()
                
                # Call via direct display method (should raise PaginationCancelled)
                with pytest.raises(PaginationCancelled):
                    tui._display_status(status_data)
                
                # Both should have printed something
                assert agent_call_args is not None
                assert tui.console.print.called

    @patch("chuck_data.ui.tui.get_command")
    def test_catalog_display_consistency_different_routing_paths(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Test catalog display consistency across different routing paths."""
        tui = tui_with_mocked_console
        
        catalog_data = {
            "catalogs": [{"name": "test", "type": "MANAGED"}],
            "total_count": 1
        }
        
        # Mock command definition
        mock_cmd = MagicMock()
        mock_cmd.agent_display = "full"
        mock_get_cmd.return_value = mock_cmd
        
        with patch.object(tui, '_display_catalogs') as mock_display:
            mock_display.side_effect = PaginationCancelled()
            
            # Call via display_tool_output with display=true
            with pytest.raises(PaginationCancelled):
                tui.display_tool_output("list-catalogs", {**catalog_data, "display": True})
            
            # Verify display method was called
            mock_display.assert_called_once()
            first_call_data = mock_display.call_args[0][0]
            
            # Reset mock
            mock_display.reset_mock()
            mock_display.side_effect = PaginationCancelled()
            
            # Call via _display_full_tool_output directly
            with pytest.raises(PaginationCancelled):
                tui._display_full_tool_output("list-catalogs", catalog_data)
            
            # Should produce the same display call
            mock_display.assert_called_once()
            second_call_data = mock_display.call_args[0][0]
            
            # Data should be equivalent (ignoring display flag difference)
            assert first_call_data["catalogs"] == second_call_data["catalogs"]
            assert first_call_data["total_count"] == second_call_data["total_count"]


class TestDisplayInteractionWithAgentSystem:
    """Test display system interaction with agent execution system."""

    def test_agent_tool_executor_integration_with_display(self, mock_chuck_service_init):
        """Test integration between agent tool executor and display system."""
        from chuck_data.agent.tool_executor import execute_tool
        
        # Mock the tool execution to return status data
        mock_api_client = MagicMock()
        
        # Create TUI instance
        tui = ChuckTUI()
        tui.console = MagicMock(spec=Console)
        
        def mock_output_callback(tool_name, tool_data):
            """Mock callback that mimics agent display behavior."""
            tui.display_tool_output(tool_name, tool_data)
        
        # Mock the command execution
        with patch("chuck_data.agent.tool_executor.get_command") as mock_get_cmd, \
             patch("chuck_data.agent.tool_executor.jsonschema.validate"):
            
            # Mock status command
            from chuck_data.commands.status import DEFINITION
            mock_get_cmd.return_value = DEFINITION
            
            # Mock the handler to return status data
            with patch.object(DEFINITION, "handler") as mock_handler:
                mock_handler.__name__ = "mock_status_handler"  # Add __name__ attribute
                mock_handler.return_value = CommandResult(
                    True,
                    data={
                        "workspace_url": "https://test.databricks.com",
                        "active_catalog": "production",
                        "connection_status": "Connected",
                        "permissions": {}
                    },
                    message="Status retrieved"
                )
                
                # Execute tool with output callback
                result = execute_tool(
                    mock_api_client,
                    "status",
                    {},
                    output_callback=mock_output_callback
                )
                
                # Verify tool executed successfully
                assert "workspace_url" in result
                assert result["workspace_url"] == "https://test.databricks.com"
                
                # Verify display was called (custom status handler)
                tui.console.print.assert_called()

    def test_pagination_cancelled_propagation_through_agent_system(self, tui_with_mocked_console):
        """Test that PaginationCancelled propagates correctly through agent system."""
        tui = tui_with_mocked_console
        
        # Mock a command that should raise PaginationCancelled
        with patch("chuck_data.ui.tui.get_command") as mock_get_cmd:
            mock_cmd = MagicMock()
            mock_cmd.agent_display = "full"
            mock_get_cmd.return_value = mock_cmd
            
            with patch.object(tui, '_display_full_tool_output') as mock_full:
                mock_full.side_effect = PaginationCancelled()
                
                # Simulate agent calling display
                def agent_callback(tool_name, tool_data):
                    tui.display_tool_output(tool_name, tool_data)
                
                # Should propagate PaginationCancelled to agent
                with pytest.raises(PaginationCancelled):
                    agent_callback("list-catalogs", {"display": True, "catalogs": []})

    def test_display_error_isolation_from_agent_execution(self, tui_with_mocked_console):
        """Test that display errors don't break agent execution flow."""
        tui = tui_with_mocked_console
        
        # Mock a display method that fails
        with patch("chuck_data.ui.tui.get_command") as mock_get_cmd:
            mock_cmd = MagicMock()
            mock_cmd.agent_display = "condensed"
            mock_get_cmd.return_value = mock_cmd
            
            with patch.object(tui, '_display_condensed_tool_output') as mock_condensed:
                mock_condensed.side_effect = Exception("Display failed")
                
                with patch("chuck_data.ui.tui.logging.warning"):
                    # Simulate agent callback - should not raise
                    def agent_callback(tool_name, tool_data):
                        return tui.display_tool_output(tool_name, tool_data)
                    
                    # Should handle error gracefully, not break agent flow
                    result = agent_callback("test-tool", {"data": "test"})
                    assert result is None  # No exception raised
                    
                    # Should have printed fallback notification
                    tui.console.print.assert_called_with("[dim][Tool: test-tool executed][/dim]")


class TestDisplayPerformanceAndScaling:
    """Test display system performance and scaling characteristics."""

    def test_large_data_display_handling(self, tui_with_mocked_console):
        """Test that large datasets are handled efficiently in display."""
        tui = tui_with_mocked_console
        
        # Create large dataset
        large_catalog_data = {
            "catalogs": [
                {"name": f"catalog_{i}", "type": "MANAGED", "comment": f"Catalog {i}"}
                for i in range(1000)  # Large number of catalogs
            ],
            "total_count": 1000
        }
        
        with patch("chuck_data.ui.table_formatter.display_table") as mock_display_table:
            mock_display_table.side_effect = PaginationCancelled()
            
            # Should handle large dataset without issues
            with pytest.raises(PaginationCancelled):
                tui._display_catalogs(large_catalog_data)
            
            # Verify display_table was called with the large dataset
            mock_display_table.assert_called_once()
            call_args = mock_display_table.call_args
            assert len(call_args[1]["data"]) == 1000

    def test_concurrent_display_call_safety(self, mock_chuck_service_init):
        """Test that concurrent display calls don't interfere with each other."""
        # This is a basic test - real concurrency testing would require threading
        tui1 = ChuckTUI()
        tui1.console = MagicMock(spec=Console)
        
        tui2 = ChuckTUI()
        tui2.console = MagicMock(spec=Console)
        
        # Both TUI instances should be independent
        assert tui1.agent_full_display_handlers is not tui2.agent_full_display_handlers
        
        # Mock calls on both
        with patch("chuck_data.ui.tui.get_command") as mock_get_cmd:
            mock_cmd = MagicMock()
            mock_cmd.agent_display = "condensed"
            mock_get_cmd.return_value = mock_cmd
            
            with patch.object(tui1, '_display_condensed_tool_output') as mock1, \
                 patch.object(tui2, '_display_condensed_tool_output') as mock2:
                
                # Calls on different instances should be independent
                tui1.display_tool_output("tool1", {"data": "test1"})
                tui2.display_tool_output("tool2", {"data": "test2"})
                
                mock1.assert_called_once_with("tool1", {"data": "test1"})
                mock2.assert_called_once_with("tool2", {"data": "test2"})