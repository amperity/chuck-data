# TDD Plan: bulk-tag-pii Interactive Command

## Overview

This document outlines a comprehensive Test-Driven Development plan for implementing the `bulk-tag-pii` command, which combines scan-pii and tag-pii functionality into a 3-phase interactive workflow:

1. **Phase 1 (Scan)**: Scan schema for PII columns
2. **Phase 2 (Review)**: Display results, allow modifications, get confirmation  
3. **Phase 3 (Tag)**: Execute bulk tagging operations

## Architecture Analysis

### Integration with Existing Commands

```
Workflow Integration:
┌─────────────────────────────────────┐
│ bulk-tag-pii Command Handler        │
├─────────────────────────────────────┤
│ Phase 1: scan-pii integration       │
│ ├─ _helper_scan_schema_for_pii      │
│ └─ LLMClient for PII detection      │
├─────────────────────────────────────┤
│ Phase 2: Interactive review         │
│ ├─ InteractiveContext state mgmt    │
│ ├─ User confirmation patterns       │
│ └─ LLM-powered modifications        │
├─────────────────────────────────────┤
│ Phase 3: tag-pii bulk execution     │
│ ├─ Batch SQL ALTER TABLE commands  │
│ ├─ Progress reporting per table     │
│ └─ Error aggregation & recovery     │
└─────────────────────────────────────┘
```

### Data Flow Architecture

```python
# Phase 1: Scan Results
scan_results = {
    "catalog": str,
    "schema": str,
    "tables_with_pii": int,
    "total_pii_columns": int,
    "results_detail": [
        {
            "table_name": str,
            "full_name": str,
            "pii_columns": [
                {"name": str, "type": str, "semantic": str}
            ]
        }
    ]
}

# Phase 2: User Review State
review_state = {
    "phase": "review",
    "scan_results": scan_results,
    "user_modifications": [],
    "excluded_tables": [],
    "modified_columns": {}
}

# Phase 3: Tagging Operations
tagging_plan = {
    "tables_to_process": [
        {
            "table_name": str,
            "pii_columns": [{"name": str, "semantic": str}]
        }
    ],
    "total_operations": int
}
```

## TDD Implementation Plan

### Phase 1: Red - Write Failing Tests

#### 1.1 Parameter Validation Tests

```python
# tests/unit/commands/test_bulk_tag_pii.py

def test_missing_catalog_uses_active_config(databricks_client_stub, temp_config):
    """Missing catalog parameter uses active catalog from config."""
    with patch("bulk_tag_pii.config._config_manager", temp_config):
        temp_config.update(active_catalog="active_catalog", active_schema="active_schema")
        
        result = handle_bulk_tag_pii(databricks_client_stub, auto_confirm=True)
        
        assert result.success
        assert "active_catalog.active_schema" in result.message

def test_missing_warehouse_returns_error(databricks_client_stub, temp_config):
    """Missing warehouse configuration returns helpful error."""
    with patch("bulk_tag_pii.config._config_manager", temp_config):
        # Don't set active_warehouse_id
        
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
    databricks_client_stub.add_catalog("test_catalog")
    databricks_client_stub.add_schema("test_catalog", "existing_schema")
    
    result = handle_bulk_tag_pii(
        databricks_client_stub,
        catalog_name="test_catalog",
        schema_name="nonexistent_schema", 
        auto_confirm=True
    )
    
    assert not result.success
    assert "schema 'nonexistent_schema' not found" in result.message
    assert "Available schemas: existing_schema" in result.message
```

#### 1.2 Direct Command Success Tests

```python
def test_direct_command_successful_bulk_tagging(databricks_client_stub, temp_config, llm_client_stub):
    """Direct command with auto_confirm successfully scans and tags PII."""
    with patch("bulk_tag_pii.config._config_manager", temp_config), \
         patch("bulk_tag_pii.LLMClient", return_value=llm_client_stub):
        
        setup_successful_bulk_pii_test_data(databricks_client_stub, llm_client_stub)
        temp_config.update(active_warehouse_id="warehouse123")
        
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
        assert result.data["tables_processed"] > 0

def test_direct_command_no_pii_found_returns_informative_message(databricks_client_stub, llm_client_stub):
    """Direct command with no PII found returns informative message."""
    with patch("bulk_tag_pii.LLMClient", return_value=llm_client_stub):
        setup_no_pii_test_data(databricks_client_stub, llm_client_stub)
        
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

def test_direct_command_partial_failures_handled_gracefully(databricks_client_stub, llm_client_stub):
    """Direct command handles partial tagging failures gracefully."""
    with patch("bulk_tag_pii.LLMClient", return_value=llm_client_stub):
        setup_partial_failure_test_data(databricks_client_stub, llm_client_stub)
        
        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog", 
            schema_name="test_schema",
            auto_confirm=True
        )
        
        assert result.success  # Overall success despite partial failures
        assert "with some errors" in result.message
        assert result.data["tables_with_errors"] > 0
        assert result.data["successful_operations"] > 0
```

#### 1.3 Agent Integration Tests

```python
def test_agent_shows_progress_during_bulk_operations(databricks_client_stub, llm_client_stub):
    """Agent execution shows detailed progress during bulk tagging."""
    with patch("bulk_tag_pii.LLMClient", return_value=llm_client_stub):
        setup_successful_bulk_pii_test_data(databricks_client_stub, llm_client_stub)
        
        progress_steps = []
        def capture_progress(tool_name, data):
            if "step" in data:
                progress_steps.append(f"→ Bulk Tag PII: ({data['step']})")
        
        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
            auto_confirm=True,
            tool_output_callback=capture_progress
        )
        
        assert result.success
        assert len(progress_steps) >= 4
        assert any("scanning schema for pii" in step.lower() for step in progress_steps)
        assert any("found" in step.lower() and "pii columns" in step.lower() for step in progress_steps)
        assert any("tagging table" in step.lower() for step in progress_steps)
        assert any("bulk tagging completed" in step.lower() for step in progress_steps)

def test_agent_tool_executor_integration(databricks_client_stub, llm_client_stub):
    """Agent tool_executor integration works end-to-end."""
    from chuck_data.agent.tool_executor import execute_tool
    
    with patch("bulk_tag_pii.LLMClient", return_value=llm_client_stub):
        setup_successful_bulk_pii_test_data(databricks_client_stub, llm_client_stub)
        
        result = execute_tool(
            api_client=databricks_client_stub,
            tool_name="bulk-tag-pii",
            tool_args={
                "catalog_name": "test_catalog", 
                "schema_name": "test_schema",
                "auto_confirm": True
            }
        )
        
        assert "tables_processed" in result
        assert "columns_tagged" in result
        assert result["catalog_name"] == "test_catalog"
        assert result["schema_name"] == "test_schema"
```

#### 1.4 Interactive Workflow Tests

```python
def test_interactive_mode_phase_1_scanning(databricks_client_stub, llm_client_stub):
    """Interactive mode Phase 1 scans schema and shows PII preview."""
    with patch("bulk_tag_pii.LLMClient", return_value=llm_client_stub):
        setup_successful_bulk_pii_test_data(databricks_client_stub, llm_client_stub)
        
        result = handle_bulk_tag_pii(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema"
            # No auto_confirm - enters interactive mode
        )
        
        assert result.success
        assert result.message == ""  # Console handles display
        
        # Verify context state was set up
        context = InteractiveContext()
        context_data = context.get_context_data("bulk-tag-pii")
        assert context_data is not None
        assert context_data.get("phase") == "review"
        assert "scan_results" in context_data
        assert context_data["scan_results"]["tables_with_pii"] > 0

def test_interactive_confirmation_proceeds_to_tagging():
    """Interactive confirmation 'proceed' executes bulk tagging."""
    context = InteractiveContext()
    context.set_active_context("bulk-tag-pii")
    context.store_context_data("bulk-tag-pii", "phase", "review") 
    context.store_context_data("bulk-tag-pii", "scan_results", mock_scan_results())
    
    result = handle_bulk_tag_pii(
        databricks_client_stub,
        interactive_input="proceed"
    )
    
    assert result.success
    assert "Bulk PII tagging completed" in result.message
    
    # Verify context was cleaned up
    assert not context.is_in_interactive_mode()

def test_interactive_modification_excludes_tables():
    """Interactive modification 'exclude table X' removes table from processing.""" 
    context = InteractiveContext()
    context.set_active_context("bulk-tag-pii")
    context.store_context_data("bulk-tag-pii", "phase", "review")
    context.store_context_data("bulk-tag-pii", "scan_results", mock_scan_results())
    
    result = handle_bulk_tag_pii(
        databricks_client_stub,
        interactive_input="exclude table sensitive_users"
    )
    
    # Should stay in interactive mode
    assert result.success
    assert result.message == ""
    assert context.is_in_interactive_mode()
    
    # Verify modification was recorded
    context_data = context.get_context_data("bulk-tag-pii")
    assert "excluded_tables" in context_data
    assert "sensitive_users" in context_data["excluded_tables"]

def test_interactive_cancellation_cleans_up_context():
    """Interactive cancellation cleans up context and exits gracefully."""
    context = InteractiveContext()
    context.set_active_context("bulk-tag-pii")
    context.store_context_data("bulk-tag-pii", "phase", "review")
    
    result = handle_bulk_tag_pii(
        databricks_client_stub,
        interactive_input="cancel"
    )
    
    assert not result.success
    assert "cancelled" in result.message.lower()
    assert not context.is_in_interactive_mode()
```

#### 1.5 Error Handling Tests

```python
def test_scan_phase_failure_returns_helpful_error():
    """Scan phase failure returns helpful error without entering interactive mode."""
    databricks_client_stub.set_list_tables_error("Permission denied")
    
    result = handle_bulk_tag_pii(
        databricks_client_stub,
        catalog_name="test_catalog",
        schema_name="restricted_schema"
    )
    
    assert not result.success
    assert "Unable to scan schema" in result.message
    assert "Permission denied" in result.message

def test_lost_interactive_context_shows_helpful_error():
    """Lost interactive context shows helpful error message."""
    # Simulate lost context
    context = InteractiveContext()
    context.clear_active_context("bulk-tag-pii")
    
    result = handle_bulk_tag_pii(
        databricks_client_stub,
        interactive_input="proceed"
    )
    
    assert not result.success
    assert "session expired" in result.message.lower() or "lost context" in result.message.lower()

def test_tagging_phase_errors_aggregated_properly():
    """Tagging phase errors are aggregated and reported clearly."""
    # This will be implemented when we get to the implementation phase
    pass
```

### Phase 2: Green - Minimal Implementation

#### 2.1 Command Handler Structure

```python
# chuck_data/commands/bulk_tag_pii.py

from chuck_data.interactive_context import InteractiveContext
from chuck_data.commands.base import CommandResult
from chuck_data.commands.pii_tools import _helper_scan_schema_for_pii_logic
from chuck_data.llm.client import LLMClient

def handle_bulk_tag_pii(client, **kwargs):
    """
    Handle bulk PII tagging with interactive confirmation.
    
    3-Phase Workflow:
    1. Scan: Use scan-pii logic to find PII columns
    2. Review: Show results, handle modifications/confirmations  
    3. Tag: Execute bulk tag-pii operations
    """
    try:
        # Parameter validation
        validation_result = _validate_parameters(client, **kwargs)
        if not validation_result.success:
            return validation_result
        
        # Route to appropriate handler
        interactive_input = kwargs.get("interactive_input")
        auto_confirm = kwargs.get("auto_confirm", False)
        
        if interactive_input:
            return _handle_interactive_input(client, interactive_input, **kwargs)
        elif auto_confirm:
            return _execute_directly(client, **kwargs)
        else:
            return _start_interactive_mode(client, **kwargs)
            
    except Exception as e:
        # Cleanup context on any error
        context = InteractiveContext()
        context.clear_active_context("bulk-tag-pii")
        return CommandResult(False, error=e, message=f"Error: {str(e)}")

def _validate_parameters(client, **kwargs):
    """Validate parameters and configuration."""
    # Implementation to pass parameter validation tests
    pass

def _execute_directly(client, **kwargs):
    """Execute full workflow without interaction."""
    # Implementation to pass direct command tests
    pass

def _start_interactive_mode(client, **kwargs):
    """Start interactive workflow - Phase 1: Scan."""
    # Implementation to pass interactive mode tests
    pass

def _handle_interactive_input(client, user_input, **kwargs):
    """Handle user input during interactive mode."""
    # Implementation to pass interactive input tests
    pass
```

#### 2.2 Command Registry Integration

```python
# chuck_data/command_registry.py

COMMANDS = {
    # ... existing commands ...
    "/bulk-tag-pii": CommandDefinition(
        handler=bulk_tag_pii.handle_bulk_tag_pii,
        parameters=[
            CommandParameter("catalog_name", str, "Catalog name (uses active if not provided)", required=False),
            CommandParameter("schema_name", str, "Schema name (uses active if not provided)", required=False),
            CommandParameter("auto_confirm", bool, "Skip interactive confirmation", required=False),
        ],
        supports_interactive_input=True,
        visible_to_user=True,
        visible_to_agent=True,
        description="Scan schema for PII and bulk tag columns with confirmation"
    )
}
```

### Phase 3: Refactor - Complete Implementation

#### 3.1 Enhanced Implementation

```python
def _execute_directly(client, **kwargs):
    """Execute complete scan → tag workflow directly."""
    tool_output_callback = kwargs.get("tool_output_callback")
    
    # Phase 1: Scan for PII
    _report_progress("Scanning schema for PII columns", tool_output_callback)
    scan_result = _execute_scan_phase(client, **kwargs)
    if not scan_result.success:
        return scan_result
    
    scan_data = scan_result.data
    if scan_data["tables_with_pii"] == 0:
        return CommandResult(
            True, 
            message="No PII columns found in schema - nothing to tag",
            data={"tables_processed": 0, "columns_tagged": 0}
        )
    
    _report_progress(
        f"Found {scan_data['total_pii_columns']} PII columns in {scan_data['tables_with_pii']} tables",
        tool_output_callback
    )
    
    # Phase 3: Execute bulk tagging
    _report_progress("Beginning bulk PII tagging operations", tool_output_callback)
    tag_result = _execute_tagging_phase(client, scan_data, tool_output_callback, **kwargs)
    
    _report_progress("Bulk tagging completed", tool_output_callback)
    return tag_result

def _execute_scan_phase(client, **kwargs):
    """Execute PII scanning phase."""
    catalog_name = kwargs.get("catalog_name") or _get_active_catalog()
    schema_name = kwargs.get("schema_name") or _get_active_schema()
    
    llm_client = LLMClient()
    
    try:
        scan_results = _helper_scan_schema_for_pii_logic(
            client, catalog_name, schema_name, llm_client
        )
        return CommandResult(True, data=scan_results)
    except Exception as e:
        return CommandResult(False, message=f"Unable to scan schema: {str(e)}")

def _execute_tagging_phase(client, scan_data, tool_output_callback, **kwargs):
    """Execute bulk tagging operations."""
    results = {
        "tables_processed": 0,
        "columns_tagged": 0, 
        "tables_with_errors": 0,
        "successful_operations": 0,
        "tagging_details": []
    }
    
    for table_data in scan_data["results_detail"]:
        if not table_data.get("has_pii"):
            continue
            
        _report_progress(f"Tagging table {table_data['table_name']}", tool_output_callback)
        
        tag_result = _tag_single_table(client, table_data)
        results["tagging_details"].append(tag_result)
        results["tables_processed"] += 1
        
        if tag_result["success"]:
            results["columns_tagged"] += len(tag_result["pii_columns"])
            results["successful_operations"] += 1
        else:
            results["tables_with_errors"] += 1
    
    # Generate summary message
    if results["tables_with_errors"] == 0:
        message = f"Bulk PII tagging completed successfully. Tagged {results['columns_tagged']} columns in {results['tables_processed']} tables."
    else:
        message = f"Bulk PII tagging completed with some errors. Successfully tagged {results['columns_tagged']} columns in {results['successful_operations']} tables, {results['tables_with_errors']} tables had errors."
    
    return CommandResult(True, message=message, data=results)

def _tag_single_table(client, table_data):
    """Tag PII columns in a single table."""
    from chuck_data.commands.tag_pii import handle_tag_pii_columns
    
    try:
        result = handle_tag_pii_columns(
            client,
            table_name=table_data["full_name"],
            pii_columns=table_data["pii_columns"]
        )
        return {
            "table_name": table_data["table_name"], 
            "success": result.success,
            "pii_columns": table_data["pii_columns"],
            "error": None if result.success else result.message
        }
    except Exception as e:
        return {
            "table_name": table_data["table_name"],
            "success": False, 
            "pii_columns": table_data["pii_columns"],
            "error": str(e)
        }
```

## Test Data Setup Utilities

```python
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
```

## Implementation Timeline

### Week 1: Foundation
- [ ] Write parameter validation tests
- [ ] Write direct command success tests
- [ ] Create minimal command handler structure
- [ ] Add command to registry
- [ ] Implement basic parameter validation

### Week 2: Core Functionality  
- [ ] Write agent integration tests
- [ ] Implement direct execution path
- [ ] Integrate with existing scan-pii logic
- [ ] Implement bulk tag-pii execution
- [ ] Add progress reporting

### Week 3: Interactive Features
- [ ] Write interactive workflow tests
- [ ] Implement interactive mode starter
- [ ] Add confirmation pattern handling
- [ ] Implement modification logic (exclude tables, etc.)
- [ ] Add context state management

### Week 4: Polish & Integration
- [ ] Write comprehensive error handling tests
- [ ] Implement robust error handling and recovery
- [ ] Add end-to-end integration tests
- [ ] Performance testing with large schemas
- [ ] Documentation and help text

## Success Criteria

### Functional Requirements
- [ ] Successfully scans schema for PII columns
- [ ] Displays interactive preview of found PII
- [ ] Accepts user confirmations and modifications
- [ ] Executes bulk tagging operations
- [ ] Handles partial failures gracefully
- [ ] Provides detailed progress reporting

### Non-Functional Requirements  
- [ ] Follows established codebase patterns
- [ ] Comprehensive test coverage (>90%)
- [ ] Proper error handling and recovery
- [ ] Agent compatibility with progress reporting
- [ ] Performance acceptable for large schemas (100+ tables)
- [ ] Security: no sensitive data leakage

### Integration Requirements
- [ ] Works with existing scan-pii and tag-pii commands
- [ ] Integrates with InteractiveContext state management  
- [ ] Compatible with both TUI and agent execution
- [ ] Respects configuration (active catalog/schema/warehouse)
- [ ] Follows command registry patterns

This comprehensive TDD plan provides a structured approach to implementing the bulk-tag-pii command while maintaining consistency with existing patterns and ensuring robust functionality through test-driven development.