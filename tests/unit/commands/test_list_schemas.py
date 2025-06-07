"""
Tests for schema commands including list-schemas and select-schema.
"""

from unittest.mock import patch

from chuck_data.commands.list_schemas import handle_command as list_schemas_handler
from chuck_data.commands.schema_selection import handle_command as select_schema_handler
from chuck_data.config import get_active_schema, set_active_catalog


# Tests for list-schemas command
def test_list_schemas_with_display_true(databricks_client_stub, temp_config):
    """Test list schemas with display=true shows table."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Set up test data
        set_active_catalog("test_catalog")
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "test_schema")

        result = list_schemas_handler(databricks_client_stub, display=True)

        assert result.success
        assert result.data.get("display")
        assert len(result.data.get("schemas", [])) == 1
        assert result.data["schemas"][0]["name"] == "test_schema"


def test_list_schemas_with_display_false(databricks_client_stub, temp_config):
    """Test list schemas with display=false returns data without display."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Set up test data
        set_active_catalog("test_catalog")
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "test_schema")

        result = list_schemas_handler(databricks_client_stub, display=False)

        assert result.success
        assert not result.data.get("display")
        assert len(result.data.get("schemas", [])) == 1


def test_list_schemas_no_active_catalog(databricks_client_stub, temp_config):
    """Test list schemas when no active catalog is set."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = list_schemas_handler(databricks_client_stub)

        assert not result.success
        assert "No catalog specified and no active catalog selected" in result.message


def test_list_schemas_empty_catalog(databricks_client_stub, temp_config):
    """Test list schemas with empty catalog."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_active_catalog("empty_catalog")
        databricks_client_stub.add_catalog("empty_catalog")

        result = list_schemas_handler(databricks_client_stub, display=True)

        assert result.success
        assert len(result.data.get("schemas", [])) == 0
        assert result.data.get("display")


# Tests for select-schema command
def test_select_schema_by_name(databricks_client_stub, temp_config):
    """Test schema selection by name."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_active_catalog("test_catalog")
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "test_schema")

        result = select_schema_handler(databricks_client_stub, schema="test_schema")

        assert result.success
        assert "Active schema is now set to 'test_schema'" in result.message
        assert get_active_schema() == "test_schema"


def test_select_schema_fuzzy_matching(databricks_client_stub, temp_config):
    """Test schema selection with fuzzy matching."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_active_catalog("test_catalog")
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "test_schema_long_name")

        result = select_schema_handler(databricks_client_stub, schema="test")

        assert result.success
        assert "test_schema_long_name" in result.message
        assert get_active_schema() == "test_schema_long_name"


def test_select_schema_no_match(databricks_client_stub, temp_config):
    """Test schema selection with no matching schema."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_active_catalog("test_catalog")
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "different_schema")

        result = select_schema_handler(databricks_client_stub, schema="nonexistent")

        assert not result.success
        assert "No schema found matching 'nonexistent'" in result.message
        assert "Available schemas:" in result.message


def test_select_schema_missing_parameter(databricks_client_stub, temp_config):
    """Test schema selection with missing schema parameter."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = select_schema_handler(databricks_client_stub)

        assert not result.success
        assert "schema parameter is required" in result.message


def test_select_schema_no_active_catalog(databricks_client_stub, temp_config):
    """Test schema selection with no active catalog."""
    with patch("chuck_data.config._config_manager", temp_config):
        result = select_schema_handler(databricks_client_stub, schema="test_schema")

        assert not result.success
        assert "No active catalog selected" in result.message


def test_select_schema_tool_output_callback(databricks_client_stub, temp_config):
    """Test schema selection with tool output callback."""
    with patch("chuck_data.config._config_manager", temp_config):
        set_active_catalog("test_catalog")
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "test_schema_with_callback")

        # Mock callback to capture output
        callback_calls = []

        def mock_callback(tool_name, data):
            callback_calls.append((tool_name, data))

        result = select_schema_handler(
            databricks_client_stub,
            schema="callback",
            tool_output_callback=mock_callback,
        )

        assert result.success
        # Should have called the callback with step information
        assert len(callback_calls) > 0
        assert callback_calls[0][0] == "select-schema"
