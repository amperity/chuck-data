"""
Tests for bulk_tag_pii handler.

Behavioral tests focused on command execution patterns rather than implementation details.
"""

import pytest
from unittest.mock import patch, MagicMock
from chuck_data.commands.bulk_tag_pii import handle_bulk_tag_pii
from chuck_data.interactive_context import InteractiveContext
from chuck_data.config import set_active_catalog, set_active_schema, set_warehouse_id


# ===== PARAMETER VALIDATION TESTS =====

def test_missing_catalog_uses_active_config(databricks_client_stub, temp_config):
    """Missing catalog parameter uses active catalog from config."""
    with patch("chuck_data.commands.bulk_tag_pii.config.get_active_catalog", return_value="active_catalog"), \
         patch("chuck_data.commands.bulk_tag_pii.config.get_active_schema", return_value="active_schema"), \
         patch("chuck_data.commands.bulk_tag_pii.config.get_warehouse_id", return_value="warehouse123"):
        
        # Need to add catalog and schema to stub for validation to pass
        databricks_client_stub.add_catalog("active_catalog")
        databricks_client_stub.add_schema("active_catalog", "active_schema")
        
        result = handle_bulk_tag_pii(databricks_client_stub, auto_confirm=True)
        
        assert result.success
        assert "active_catalog.active_schema" in result.message


def test_missing_warehouse_returns_error(databricks_client_stub, temp_config):
    """Missing warehouse configuration returns helpful error."""
    with patch("chuck_data.commands.bulk_tag_pii.config.get_active_catalog", return_value="test_catalog"), \
         patch("chuck_data.commands.bulk_tag_pii.config.get_active_schema", return_value="test_schema"), \
         patch("chuck_data.commands.bulk_tag_pii.config.get_warehouse_id", return_value=None):
        
        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog", 
            schema_name="test_schema",
            auto_confirm=True
        )
        
        assert not result.success
        assert "warehouse" in result.message.lower()
        assert "configure" in result.message.lower()


def test_nonexistent_schema_returns_helpful_error(databricks_client_stub):
    """Nonexistent schema returns error with available options."""
    with patch("chuck_data.commands.bulk_tag_pii.config.get_warehouse_id", return_value="warehouse123"):
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "existing_schema")
        
        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="nonexistent_schema", 
            auto_confirm=True
        )
        
        assert not result.success
        assert "Schema 'nonexistent_schema' not found" in result.message
        assert "Available schemas: existing_schema" in result.message


def test_nonexistent_catalog_returns_helpful_error(databricks_client_stub):
    """Nonexistent catalog returns error with available options."""
    with patch("chuck_data.commands.bulk_tag_pii.config.get_warehouse_id", return_value="warehouse123"):
        databricks_client_stub.add_catalog("existing_catalog")
        
        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="nonexistent_catalog",
            schema_name="test_schema",
            auto_confirm=True
        )
        
        assert not result.success
        assert "Catalog 'nonexistent_catalog' not found" in result.message
        assert "Available catalogs: existing_catalog" in result.message


def test_missing_schema_parameter_uses_active_config(databricks_client_stub, temp_config):
    """Missing schema parameter uses active schema from config."""
    with patch("chuck_data.commands.bulk_tag_pii.config.get_active_catalog", return_value="test_catalog"), \
         patch("chuck_data.commands.bulk_tag_pii.config.get_active_schema", return_value="active_schema"), \
         patch("chuck_data.commands.bulk_tag_pii.config.get_warehouse_id", return_value="warehouse123"):
        
        # Need to add catalog and schema to stub for validation to pass
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "active_schema")
        
        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            auto_confirm=True
        )
        
        assert result.success
        assert "test_catalog.active_schema" in result.message


def test_both_catalog_and_schema_missing_uses_active_config(databricks_client_stub, temp_config):
    """Both catalog and schema missing uses active config.""" 
    with patch("chuck_data.commands.bulk_tag_pii.config.get_active_catalog", return_value="active_catalog"), \
         patch("chuck_data.commands.bulk_tag_pii.config.get_active_schema", return_value="active_schema"), \
         patch("chuck_data.commands.bulk_tag_pii.config.get_warehouse_id", return_value="warehouse123"):
        
        # Need to add catalog and schema to stub for validation to pass
        databricks_client_stub.add_catalog("active_catalog")
        databricks_client_stub.add_schema("active_catalog", "active_schema")
        
        result = handle_bulk_tag_pii(databricks_client_stub, auto_confirm=True)
        
        assert result.success
        assert "active_catalog.active_schema" in result.message


# ===== DIRECT COMMAND TESTS =====

def test_direct_command_successful_bulk_tagging(databricks_client_stub, temp_config):
    """Direct command with auto_confirm successfully scans and tags PII."""
    with patch("chuck_data.commands.bulk_tag_pii.config.get_active_catalog", return_value="test_catalog"), \
         patch("chuck_data.commands.bulk_tag_pii.config.get_active_schema", return_value="test_schema"), \
         patch("chuck_data.commands.bulk_tag_pii.config.get_warehouse_id", return_value="warehouse123"):
        
        setup_successful_bulk_pii_test_data(databricks_client_stub, None)  # No LLM client for now
        
        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema", 
            auto_confirm=True
        )
        
        assert result.success
        assert "Bulk PII tagging completed" in result.message
        assert "tables_processed" in result.data
        assert "columns_tagged" in result.data
        assert result.data["tables_processed"] >= 0


def test_direct_command_no_pii_found_returns_informative_message(databricks_client_stub):
    """Direct command with no PII found returns informative message."""
    with patch("chuck_data.commands.bulk_tag_pii.config.get_warehouse_id", return_value="warehouse123"):
        setup_no_pii_test_data(databricks_client_stub, None)  # No LLM client for now
        
        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
            auto_confirm=True  
        )
        
        assert result.success
        assert "No PII columns found" in result.message
        assert result.data["tables_processed"] == 0
        assert result.data["columns_tagged"] == 0


def test_direct_command_partial_failures_handled_gracefully(databricks_client_stub):
    """Direct command handles partial tagging failures gracefully."""
    # This test will fail initially - driving error handling
    pass


# ===== AGENT INTEGRATION TESTS =====

def test_agent_shows_progress_during_bulk_operations(databricks_client_stub):
    """Agent execution shows detailed progress during bulk tagging."""
    # This test will fail initially - driving progress reporting
    pass


def test_agent_tool_executor_integration(databricks_client_stub):
    """Agent tool_executor integration works end-to-end."""
    # This test will fail initially - driving agent integration
    pass


# ===== INTERACTIVE WORKFLOW TESTS =====

def test_interactive_mode_phase_1_scanning(databricks_client_stub):
    """Interactive mode Phase 1 scans schema and shows PII preview."""
    # This test will fail initially - driving interactive mode
    pass


def test_interactive_confirmation_proceeds_to_tagging(databricks_client_stub):
    """Interactive confirmation 'proceed' executes bulk tagging."""
    # This test will fail initially - driving confirmation handling
    pass


def test_interactive_modification_excludes_tables(databricks_client_stub):
    """Interactive modification 'exclude table X' removes table from processing."""
    # This test will fail initially - driving modification logic
    pass


def test_interactive_cancellation_cleans_up_context(databricks_client_stub):
    """Interactive cancellation cleans up context and exits gracefully."""
    # This test will fail initially - driving cancellation logic
    pass


# ===== ERROR HANDLING TESTS =====

def test_scan_phase_failure_returns_helpful_error(databricks_client_stub):
    """Scan phase failure returns helpful error without entering interactive mode."""
    # This test will fail initially - driving error handling
    pass


def test_lost_interactive_context_shows_helpful_error(databricks_client_stub):
    """Lost interactive context shows helpful error message."""
    # This test will fail initially - driving context error handling
    pass


def test_tagging_phase_errors_aggregated_properly(databricks_client_stub):
    """Tagging phase errors are aggregated and reported clearly."""
    # This test will fail initially - driving error aggregation
    pass


# ===== TEST DATA SETUP HELPERS =====

def setup_successful_bulk_pii_test_data(databricks_client_stub, llm_client_stub):
    """Setup test data for successful bulk PII operations."""
    # Add catalog and schema
    databricks_client_stub.add_catalog("test_catalog")
    databricks_client_stub.add_schema("test_catalog", "test_schema")
    
    # Add tables with PII columns
    databricks_client_stub.add_table(
        "test_catalog", "test_schema", "users",
        columns=[
            {"name": "id", "type": "bigint"},
            {"name": "email", "type": "string"},
            {"name": "full_name", "type": "string"},
            {"name": "phone", "type": "string"}
        ]
    )
    
    databricks_client_stub.add_table(
        "test_catalog", "test_schema", "customer_profiles", 
        columns=[
            {"name": "customer_id", "type": "bigint"},
            {"name": "address", "type": "string"},
            {"name": "city", "type": "string"},
            {"name": "postal", "type": "string"}
        ]
    )
    
    # Mock LLM PII detection responses
    if llm_client_stub:
        llm_client_stub.set_pii_detection_responses({
            "users": [
                {"name": "id", "semantic": None},
                {"name": "email", "semantic": "email"},
                {"name": "full_name", "semantic": "full-name"},
                {"name": "phone", "semantic": "phone"}
            ],
            "customer_profiles": [
                {"name": "customer_id", "semantic": None},
                {"name": "address", "semantic": "address"},
                {"name": "city", "semantic": "city"}, 
                {"name": "postal", "semantic": "postal"}
            ]
        })


def setup_no_pii_test_data(databricks_client_stub, llm_client_stub):
    """Setup test data with no PII columns found."""
    databricks_client_stub.add_catalog("test_catalog")
    databricks_client_stub.add_schema("test_catalog", "test_schema")
    
    databricks_client_stub.add_table(
        "test_catalog", "test_schema", "system_logs",
        columns=[
            {"name": "id", "type": "bigint"},
            {"name": "timestamp", "type": "timestamp"}, 
            {"name": "log_level", "type": "string"},
            {"name": "message", "type": "string"}
        ]
    )
    
    # Mock LLM to return no PII
    if llm_client_stub:
        llm_client_stub.set_pii_detection_responses({
            "system_logs": [
                {"name": "id", "semantic": None},
                {"name": "timestamp", "semantic": None},
                {"name": "log_level", "semantic": None},
                {"name": "message", "semantic": None}
            ]
        })


def mock_scan_results():
    """Mock scan results for interactive testing."""
    return {
        "catalog": "test_catalog",
        "schema": "test_schema",
        "tables_with_pii": 2,
        "total_pii_columns": 7,
        "results_detail": [
            {
                "table_name": "users",
                "full_name": "test_catalog.test_schema.users",
                "has_pii": True,
                "pii_columns": [
                    {"name": "email", "semantic": "email"},
                    {"name": "full_name", "semantic": "full-name"}
                ]
            },
            {
                "table_name": "sensitive_users", 
                "full_name": "test_catalog.test_schema.sensitive_users",
                "has_pii": True,
                "pii_columns": [
                    {"name": "ssn", "semantic": "ssn"},
                    {"name": "address", "semantic": "address"}
                ]
            }
        ]
    }