"""
Comprehensive tests for TUI display routing logic.

These tests ensure that display_tool_output routes correctly in all scenarios,
providing safety for refactoring the display logic.
"""

import pytest
from unittest.mock import MagicMock, patch
from rich.console import Console
from rich.panel import Panel

from chuck_data.ui.tui import ChuckTUI


class MockCommandDef:
    """Helper for creating mock CommandDefinition objects with all needed attributes."""
    
    def __init__(
        self, 
        agent_display="condensed", 
        condensed_action=None, 
        display_condition=None
    ):
        self.agent_display = agent_display
        self.condensed_action = condensed_action
        self.display_condition = display_condition


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


class TestDisplayRoutingLogicComprehensive:
    """Comprehensive tests for all display routing paths in display_tool_output."""

    @pytest.mark.parametrize("agent_display,tool_result,expected_method", [
        # Condensed display scenarios
        ("condensed", {}, "_display_condensed_tool_output"),
        ("condensed", {"display": True}, "_display_condensed_tool_output"),
        ("condensed", {"display": False}, "_display_condensed_tool_output"),
        
        # Full display scenarios
        ("full", {"display": True}, "_display_full_tool_output"),
        ("full", {"display": False}, "_display_condensed_tool_output"),
        ("full", {}, "_display_condensed_tool_output"),  # No display param
        ("full", {"other": "data"}, "_display_condensed_tool_output"),  # No display param
        
        # None display scenarios  
        ("none", {}, None),  # Should return early, no method called
        ("none", {"display": True}, None),
        ("none", {"display": False}, None),
    ])
    @patch("chuck_data.ui.tui.get_command")
    def test_agent_display_routing_comprehensive(
        self, mock_get_cmd, tui_with_mocked_console, agent_display, tool_result, expected_method
    ):
        """Test all agent_display routing scenarios."""
        tui = tui_with_mocked_console
        mock_get_cmd.return_value = MockCommandDef(agent_display=agent_display)
        
        # Mock the display methods to track calls
        with patch.object(tui, '_display_condensed_tool_output') as mock_condensed, \
             patch.object(tui, '_display_full_tool_output') as mock_full:
            
            tui.display_tool_output("test-tool", tool_result)
            
            if expected_method == "_display_condensed_tool_output":
                mock_condensed.assert_called_once_with("test-tool", tool_result)
                mock_full.assert_not_called()
            elif expected_method == "_display_full_tool_output":
                mock_full.assert_called_once_with("test-tool", tool_result)
                mock_condensed.assert_not_called() 
            elif expected_method is None:
                # None display - no methods should be called
                mock_condensed.assert_not_called()
                mock_full.assert_not_called()

    @pytest.mark.parametrize("condition_result,tool_result,expected_method", [
        # Conditional display with True condition - still needs display=true in result for full display
        (True, {}, "_display_condensed_tool_output"),  # No display=true, falls back to condensed
        (True, {"display": True}, "_display_full_tool_output"),  # Has display=true, uses full
        (True, {"display": False}, "_display_condensed_tool_output"),  # display=false, uses condensed
        
        # Conditional display with False condition - always condensed regardless of display param
        (False, {}, "_display_condensed_tool_output"),
        (False, {"display": True}, "_display_condensed_tool_output"),  # Condition overrides
        (False, {"display": False}, "_display_condensed_tool_output"),
    ])
    @patch("chuck_data.ui.tui.get_command")
    def test_conditional_display_routing(
        self, mock_get_cmd, tui_with_mocked_console, condition_result, tool_result, expected_method
    ):
        """Test conditional display routing logic."""
        tui = tui_with_mocked_console
        
        # Create mock condition function that returns the test result
        mock_condition = MagicMock(return_value=condition_result)
        mock_get_cmd.return_value = MockCommandDef(
            agent_display="conditional", 
            display_condition=mock_condition
        )
        
        # Mock the display methods to track calls
        with patch.object(tui, '_display_condensed_tool_output') as mock_condensed, \
             patch.object(tui, '_display_full_tool_output') as mock_full:
            
            tui.display_tool_output("test-tool", tool_result)
            
            # Verify condition function was called with tool_result
            mock_condition.assert_called_once_with(tool_result)
            
            if expected_method == "_display_condensed_tool_output":
                mock_condensed.assert_called_once_with("test-tool", tool_result)
                mock_full.assert_not_called()
            elif expected_method == "_display_full_tool_output":
                mock_full.assert_called_once_with("test-tool", tool_result)
                mock_condensed.assert_not_called()

    @patch("chuck_data.ui.tui.get_command")
    def test_conditional_display_missing_condition_function(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Test conditional display when display_condition is None (fallback to condensed)."""
        tui = tui_with_mocked_console
        mock_get_cmd.return_value = MockCommandDef(
            agent_display="conditional",
            display_condition=None  # Missing condition function
        )
        
        with patch.object(tui, '_display_condensed_tool_output') as mock_condensed, \
             patch.object(tui, '_display_full_tool_output') as mock_full:
            
            tui.display_tool_output("test-tool", {"display": True})
            
            # Should fallback to condensed when no condition function
            mock_condensed.assert_called_once_with("test-tool", {"display": True})
            mock_full.assert_not_called()

    @patch("chuck_data.ui.tui.get_command")
    def test_conditional_display_non_dict_tool_result(
        self, mock_get_cmd, tui_with_mocked_console
    ):
        """Test conditional display when tool_result is not a dict (fallback to condensed)."""
        tui = tui_with_mocked_console
        mock_condition = MagicMock(return_value=True)
        mock_get_cmd.return_value = MockCommandDef(
            agent_display="conditional",
            display_condition=mock_condition
        )
        
        with patch.object(tui, '_display_condensed_tool_output') as mock_condensed, \
             patch.object(tui, '_display_full_tool_output') as mock_full:
            
            # Test with non-dict tool_result
            tui.display_tool_output("test-tool", "not-a-dict")
            
            # Should fallback to condensed without calling condition function
            mock_condition.assert_not_called()
            mock_condensed.assert_called_once_with("test-tool", "not-a-dict")
            mock_full.assert_not_called()

    @patch("chuck_data.ui.tui.get_command")
    def test_custom_agent_handler_routing(self, mock_get_cmd, tui_with_mocked_console):
        """Test that custom agent handlers are called when present."""
        tui = tui_with_mocked_console
        mock_get_cmd.return_value = MockCommandDef(agent_display="full")
        
        # Register a custom handler for test-tool
        mock_custom_handler = MagicMock()
        tui.agent_full_display_handlers["test-tool"] = mock_custom_handler
        
        with patch.object(tui, '_display_condensed_tool_output') as mock_condensed, \
             patch.object(tui, '_display_full_tool_output') as mock_full:
            
            tool_data = {"some": "data"}
            tui.display_tool_output("test-tool", tool_data)
            
            # Should call custom handler, not the fallback methods
            mock_custom_handler.assert_called_once_with("test-tool", tool_data)
            mock_condensed.assert_not_called()
            mock_full.assert_not_called()

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