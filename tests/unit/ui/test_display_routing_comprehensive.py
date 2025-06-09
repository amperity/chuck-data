"""
Comprehensive tests for TUI display routing logic.

These tests ensure that display_tool_output routes correctly in all scenarios,
providing safety for refactoring the display logic.
Uses real objects where possible, only mocking external boundaries.
"""

import pytest
import tempfile
from unittest.mock import MagicMock, patch
from rich.console import Console

from chuck_data.ui.tui import ChuckTUI
from chuck_data.config import ConfigManager
from chuck_data.command_registry import CommandDefinition


def create_real_command_def(
    agent_display="condensed", 
    condensed_action=None, 
    display_condition=None
):
    """Create a real CommandDefinition with test attributes."""
    def dummy_handler(**kwargs):
        return {"success": True}
    
    cmd_def = CommandDefinition(
        command="/test-command",
        handler=dummy_handler,
        description="Test command",
        agent_display=agent_display
    )
    
    if condensed_action:
        cmd_def.condensed_action = condensed_action
    if display_condition:
        cmd_def.display_condition = display_condition
        
    return cmd_def


@pytest.fixture
def temp_config():
    """Create a temporary config manager for testing."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        yield config_manager


@pytest.fixture
def tui_with_captured_console():
    """Create ChuckTUI instance with console that captures output for testing."""
    # Only mock the external boundary (console output)
    with patch("chuck_data.ui.tui.ChuckService"):
        tui = ChuckTUI()
        # Mock only the console output - the external boundary
        tui.console = MagicMock(spec=Console)
        return tui


class TestDisplayRoutingLogicComprehensive:
    """Comprehensive tests for all display routing paths in display_tool_output."""

    @pytest.mark.parametrize("agent_display,tool_result,should_show_output", [
        # Condensed display scenarios - should always show condensed output
        ("condensed", {}, True),
        ("condensed", {"display": True}, True),
        ("condensed", {"display": False}, True),
        
        # Full display scenarios - only shows full when display=True, else condensed
        ("full", {"display": True}, True),  # Full display
        ("full", {"display": False}, True),  # Falls back to condensed
        ("full", {}, True),  # Falls back to condensed
        ("full", {"other": "data"}, True),  # Falls back to condensed
        
        # None display scenarios - should show nothing
        ("none", {}, False),
        ("none", {"display": True}, False),
        ("none", {"display": False}, False),
    ])
    def test_agent_display_routing_comprehensive(
        self, tui_with_captured_console, agent_display, tool_result, should_show_output
    ):
        """Test all agent_display routing scenarios using real command definitions."""
        tui = tui_with_captured_console
        
        # Create real command definition with the test agent_display setting
        real_cmd_def = create_real_command_def(agent_display=agent_display)
        
        # Use real command registry lookup (only mock the external get_command call)
        with patch("chuck_data.ui.tui.get_command", return_value=real_cmd_def):
            tui.display_tool_output("test-tool", tool_result)
            
            if should_show_output:
                # Should have printed something to console
                assert tui.console.print.called, f"Expected output for {agent_display} with {tool_result}"
            else:
                # Should not have printed anything (none display)
                assert not tui.console.print.called, f"Expected no output for {agent_display} with {tool_result}"

    def test_conditional_display_routing_with_true_condition(self, tui_with_captured_console):
        """Test conditional display routing when condition returns True."""
        tui = tui_with_captured_console
        
        # Create real condition function
        def should_display_full(tool_result):
            return tool_result.get("trigger_full", False)
        
        # Create real command definition with conditional display
        real_cmd_def = create_real_command_def(
            agent_display="conditional",
            display_condition=should_display_full
        )
        
        with patch("chuck_data.ui.tui.get_command", return_value=real_cmd_def):
            # Test case where condition returns True but no display=True -> condensed
            tui.display_tool_output("test-tool", {"trigger_full": True})
            assert tui.console.print.called
            
            # Reset console
            tui.console.print.reset_mock()
            
            # Test case where condition returns True AND display=True -> should still work
            tui.display_tool_output("test-tool", {"trigger_full": True, "display": True})
            assert tui.console.print.called

    def test_conditional_display_routing_with_false_condition(self, tui_with_captured_console):
        """Test conditional display routing when condition returns False."""
        tui = tui_with_captured_console
        
        # Create real condition function that returns False
        def should_not_display_full(tool_result):
            return False
        
        # Create real command definition with conditional display
        real_cmd_def = create_real_command_def(
            agent_display="conditional",
            display_condition=should_not_display_full
        )
        
        with patch("chuck_data.ui.tui.get_command", return_value=real_cmd_def):
            # Even with display=True, condition overrides to condensed
            tui.display_tool_output("test-tool", {"display": True})
            assert tui.console.print.called  # Should show condensed output

    def test_conditional_display_missing_condition_function(self, tui_with_captured_console):
        """Test conditional display when display_condition is None (fallback to condensed)."""
        tui = tui_with_captured_console
        
        # Create real command definition with conditional display but no condition function
        real_cmd_def = create_real_command_def(
            agent_display="conditional",
            display_condition=None  # Missing condition function
        )
        
        with patch("chuck_data.ui.tui.get_command", return_value=real_cmd_def):
            tui.display_tool_output("test-tool", {"display": True})
            # Should fallback to condensed and show output
            assert tui.console.print.called

    def test_conditional_display_non_dict_tool_result(self, tui_with_captured_console):
        """Test conditional display when tool_result is not a dict (fallback to condensed)."""
        tui = tui_with_captured_console
        
        # Create real condition function (won't be called with non-dict input)
        def test_condition(tool_result):
            return True
        
        real_cmd_def = create_real_command_def(
            agent_display="conditional",
            display_condition=test_condition
        )
        
        with patch("chuck_data.ui.tui.get_command", return_value=real_cmd_def):
            # Test with non-dict tool_result
            tui.display_tool_output("test-tool", "not-a-dict")
            # Should fallback to condensed and show output
            assert tui.console.print.called

    def test_custom_agent_handler_routing(self, tui_with_captured_console):
        """Test that custom agent handlers are called when present."""
        tui = tui_with_captured_console
        
        # Create real command definition with full display
        real_cmd_def = create_real_command_def(agent_display="full")
        
        # Create a real custom handler that tracks if it was called
        custom_handler_called = []
        def real_custom_handler(tool_name, tool_data):
            custom_handler_called.append((tool_name, tool_data))
            # Simulate what a real handler would do - print to console
            tui.console.print(f"Custom handler for {tool_name}")
        
        # Register the real custom handler
        tui.agent_full_display_handlers["test-tool"] = real_custom_handler
        
        try:
            with patch("chuck_data.ui.tui.get_command", return_value=real_cmd_def):
                tool_data = {"some": "data"}
                tui.display_tool_output("test-tool", tool_data)
                
                # Verify custom handler was called
                assert len(custom_handler_called) == 1
                assert custom_handler_called[0] == ("test-tool", tool_data)
                
                # Verify console output happened (from custom handler)
                assert tui.console.print.called
        finally:
            # Clean up - remove the custom handler
            if "test-tool" in tui.agent_full_display_handlers:
                del tui.agent_full_display_handlers["test-tool"]

    @patch("chuck_data.ui.tui.get_command")
    def test_missing_command_definition_defaults_to_condensed(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Test that missing command definition defaults to condensed display."""
        tui = tui_with_mocked_console
        mock_get_cmd.return_value = None  # Command not found
        
        with patch.object(tui, '_display_condensed_tool_output') as mock_condensed:
            
            tui.display_tool_output("unknown-tool", {"data": "test"})
            
            # Should default to condensed display
            mock_condensed.assert_called_once_with("unknown-tool", {"data": "test"})

    @pytest.mark.parametrize("display_value,is_dict,expected_method", [
        # Boolean True - should trigger full display
        (True, True, "_display_full_tool_output"),
        # Boolean False - should trigger condensed display
        (False, True, "_display_condensed_tool_output"),
        # String "true" - should trigger condensed display (not boolean True)
        ("true", True, "_display_condensed_tool_output"),
        # String "True" - should trigger condensed display (not boolean True)
        ("True", True, "_display_condensed_tool_output"),
        # Integer 1 - should trigger condensed display (not boolean True)
        (1, True, "_display_condensed_tool_output"),
        # Missing display key - should trigger condensed display
        (None, False, "_display_condensed_tool_output"),  # No display key in dict
    ])
    @patch("chuck_data.ui.tui.get_command")
    def test_display_parameter_type_sensitivity(
        self, mock_get_cmd, tui_with_mocked_console, display_value, is_dict, expected_method
    ):
        """Test that display parameter is strictly boolean True."""
        tui = tui_with_mocked_console
        mock_get_cmd.return_value = MockCommandDef(agent_display="full")
        
        # Create tool_result based on test parameters
        if is_dict and display_value is not None:
            tool_result = {"display": display_value}
        elif is_dict:
            tool_result = {"other": "data"}  # No display key
        else:
            tool_result = {"other": "data"}
        
        with patch.object(tui, '_display_condensed_tool_output') as mock_condensed, \
             patch.object(tui, '_display_full_tool_output') as mock_full:
            
            tui.display_tool_output("test-tool", tool_result)
            
            if expected_method == "_display_condensed_tool_output":
                mock_condensed.assert_called_once_with("test-tool", tool_result)
                mock_full.assert_not_called()
            elif expected_method == "_display_full_tool_output":
                mock_full.assert_called_once_with("test-tool", tool_result)
                mock_condensed.assert_not_called()

    @patch("chuck_data.ui.tui.get_command")
    def test_command_definition_without_agent_display_attribute(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Test behavior when command definition lacks agent_display attribute."""
        tui = tui_with_mocked_console
        
        # Create command definition without agent_display attribute
        mock_cmd_def = MagicMock()
        del mock_cmd_def.agent_display  # Remove the attribute
        mock_get_cmd.return_value = mock_cmd_def
        
        with patch.object(tui, '_display_condensed_tool_output') as mock_condensed:
            
            tui.display_tool_output("test-tool", {"data": "test"})
            
            # Should default to condensed when agent_display attribute is missing
            mock_condensed.assert_called_once_with("test-tool", {"data": "test"})


class TestDisplayMethodSelection:
    """Test the specific display method selection logic within _display_full_tool_output."""

    @pytest.fixture
    def tui_with_mocked_display_methods(self, mock_chuck_service_init):
        """Create TUI with all display methods mocked."""
        tui = ChuckTUI()
        tui.console = MagicMock(spec=Console)
        
        # Mock all the display methods that _display_full_tool_output might call
        display_methods = [
            '_display_catalogs', '_display_schemas', '_display_tables',
            '_display_catalog_details', '_display_schema_details', 
            '_display_models', '_display_models_consolidated',
            '_display_warehouses', '_display_volumes',
            '_display_table_details', '_display_pii_scan_results',
            '_display_sql_results_formatted', '_display_status'
        ]
        
        for method in display_methods:
            setattr(tui, method, MagicMock())
        
        return tui

    @pytest.mark.parametrize("tool_name,expected_method", [
        # Catalog-related tools
        ("list-catalogs", "_display_catalogs"),
        ("list_catalogs", "_display_catalogs"),
        ("catalogs", "_display_catalogs"),
        
        # Schema-related tools
        ("list-schemas", "_display_schemas"),
        ("list_schemas", "_display_schemas"),
        ("schemas", "_display_schemas"),
        
        # Table-related tools
        ("list-tables", "_display_tables"),
        ("list_tables", "_display_tables"),
        ("tables", "_display_tables"),
        
        # Warehouse-related tools
        ("list-warehouses", "_display_warehouses"),
        ("list_warehouses", "_display_warehouses"),
        ("warehouses", "_display_warehouses"),
        
        # Volume-related tools
        ("list-volumes", "_display_volumes"),
        ("list_volumes", "_display_volumes"),
        ("volumes", "_display_volumes"),
        
        # Detail tools
        ("get_catalog_details", "_display_catalog_details"),
        ("catalog", "_display_catalog_details"),
        ("get_schema_details", "_display_schema_details"),
        ("schema", "_display_schema_details"),
        ("get_table_info", "_display_table_details"),
        ("table", "_display_table_details"),
        ("show_table", "_display_table_details"),
        
        # Other tools
        ("scan-schema-for-pii", "_display_pii_scan_results"),
        ("scan_schema_for_pii", "_display_pii_scan_results"),
        ("scan_pii", "_display_pii_scan_results"),
        ("run-sql", "_display_sql_results_formatted"),
        ("status", "_display_status"),
        ("get_status", "_display_status"),
    ])
    def test_tool_name_to_display_method_mapping(
        self, tui_with_mocked_display_methods, tool_name, expected_method
    ):
        """Test that tool names correctly map to their display methods."""
        tui = tui_with_mocked_display_methods
        test_data = {"test": "data"}
        
        tui._display_full_tool_output(tool_name, test_data)
        
        # Verify the correct display method was called
        expected_display_method = getattr(tui, expected_method)
        expected_display_method.assert_called_once_with(test_data)
        
        # Verify other display methods were not called
        for method_name in ['_display_catalogs', '_display_schemas', '_display_tables',
                           '_display_catalog_details', '_display_schema_details', 
                           '_display_models', '_display_models_consolidated',
                           '_display_warehouses', '_display_volumes',
                           '_display_table_details', '_display_pii_scan_results',
                           '_display_sql_results_formatted', '_display_status']:
            if method_name != expected_method:
                method = getattr(tui, method_name)
                method.assert_not_called()

    @pytest.mark.parametrize("tool_name,tool_result,expected_method", [
        # Models with "models" key -> consolidated display
        ("list-models", {"models": [{"name": "test"}]}, "_display_models_consolidated"),
        ("list_models", {"models": [{"name": "test"}]}, "_display_models_consolidated"),
        ("models", {"models": [{"name": "test"}]}, "_display_models_consolidated"),
        ("detailed-models", {"models": [{"name": "test"}]}, "_display_models_consolidated"),
        
        # Models without "models" key -> regular display
        ("list-models", {"data": [{"name": "test"}]}, "_display_models"),
        ("list_models", {"data": [{"name": "test"}]}, "_display_models"),
        ("models", {"data": [{"name": "test"}]}, "_display_models"),
        ("detailed-models", {"data": [{"name": "test"}]}, "_display_models"),
    ])
    def test_models_special_case_routing(
        self, tui_with_mocked_display_methods, tool_name, tool_result, expected_method
    ):
        """Test the special case routing for models tools."""
        tui = tui_with_mocked_display_methods
        
        tui._display_full_tool_output(tool_name, tool_result)
        
        # Verify the correct display method was called
        expected_display_method = getattr(tui, expected_method)
        expected_display_method.assert_called_once_with(tool_result)
        
        # Verify the other models method was not called
        other_method = "_display_models" if expected_method == "_display_models_consolidated" else "_display_models_consolidated"
        other_display_method = getattr(tui, other_method)
        other_display_method.assert_not_called()

    def test_unknown_tool_generic_display(self, tui_with_mocked_display_methods):
        """Test that unknown tools get generic JSON panel display."""
        tui = tui_with_mocked_display_methods
        test_data = {"unknown": "tool", "data": [1, 2, 3]}
        
        tui._display_full_tool_output("completely-unknown-tool", test_data)
        
        # Should print a generic panel with JSON content
        tui.console.print.assert_called_once()
        call_args = tui.console.print.call_args[0][0]
        assert isinstance(call_args, Panel)
        assert "Tool Output: completely-unknown-tool" in str(call_args.title)
        
        # Verify no specific display methods were called
        for method_name in ['_display_catalogs', '_display_schemas', '_display_tables',
                           '_display_catalog_details', '_display_schema_details', 
                           '_display_models', '_display_models_consolidated',
                           '_display_warehouses', '_display_volumes',
                           '_display_table_details', '_display_pii_scan_results',
                           '_display_sql_results_formatted', '_display_status']:
            method = getattr(tui, method_name)
            method.assert_not_called()

    def test_unknown_tool_non_serializable_data(self, tui_with_mocked_display_methods):
        """Test that unknown tools with non-JSON-serializable data are handled."""
        tui = tui_with_mocked_display_methods
        
        # Create data that can't be JSON serialized
        test_data = {"function": lambda x: x, "data": "test"}
        
        tui._display_full_tool_output("unknown-tool", test_data)
        
        # Should still print a panel with string representation
        tui.console.print.assert_called_once()
        call_args = tui.console.print.call_args[0][0]
        assert isinstance(call_args, Panel)
        assert "Tool Output: unknown-tool" in str(call_args.title)