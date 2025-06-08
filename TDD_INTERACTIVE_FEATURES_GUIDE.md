# TDD Instruction Set: Implementing Interactive Confirmation Features

## Overview

This comprehensive guide provides a Test-Driven Development (TDD) approach for implementing interactive command features similar to `setup_stitch`. The instruction set follows the established patterns in the chuck-data codebase and emphasizes behavioral testing over implementation details.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Pre-Development Analysis](#pre-development-analysis)
3. [TDD Development Phases](#tdd-development-phases)
4. [Test Categories and Patterns](#test-categories-and-patterns)
5. [State Management Testing](#state-management-testing)
6. [Interactive Flow Testing](#interactive-flow-testing)
7. [Agent Integration Testing](#agent-integration-testing)
8. [Implementation Guidelines](#implementation-guidelines)
9. [Common Patterns and Utilities](#common-patterns-and-utilities)
10. [Quality Assurance Checklist](#quality-assurance-checklist)

---

## Architecture Overview

### Core Components for Interactive Features

```
Interactive Command Architecture:
┌─────────────────────────────────────┐
│ TUI (user interface)                │
├─────────────────────────────────────┤
│ Service Layer (command routing)     │
├─────────────────────────────────────┤
│ Command Handler (main logic)        │ ← Your implementation here
│ ├─ Phase 1: Preparation             │
│ ├─ Phase 2: Review/Modification     │
│ └─ Phase 3: Execution               │
├─────────────────────────────────────┤
│ InteractiveContext (state mgmt)     │
├─────────────────────────────────────┤
│ External APIs (Databricks, LLM)     │
└─────────────────────────────────────┘
```

### Key Architectural Principles

1. **Phase-Based State Machine**: Clear separation of workflow phases
2. **Context Persistence**: State survives across user interactions
3. **Flexible Confirmation Patterns**: Multiple natural language acceptance
4. **Progressive Disclosure**: Information revealed step-by-step
5. **Error Recovery**: Comprehensive error handling with context cleanup
6. **Agent Compatibility**: Works with both direct and agent execution

---

## Pre-Development Analysis

### Step 1: Define Your Interactive Feature

Before writing any code, clearly define:

#### A. Feature Specification
```yaml
Feature: Interactive Database Migration
Description: Multi-step workflow for migrating data between schemas
User Story: "As a data engineer, I want to review and confirm migration 
           plans before executing them"

Phases:
  1. Preparation: Analyze source/target, generate migration plan
  2. Review: Display plan, allow modifications, get confirmation  
  3. Execution: Run migration, provide progress updates
```

#### B. User Interaction Flows
```yaml
Direct Command Flow:
  - User: "/migrate-data source_schema target_schema --auto-confirm"
  - System: Executes automatically without interaction
  - Result: Migration completed or error

Interactive Flow:
  - User: "/migrate-data source_schema target_schema"
  - System: Shows migration plan, waits for input
  - User: "looks good, proceed" or "exclude table X"
  - System: Updates plan or proceeds to execution
  - User: "confirm" 
  - System: Executes migration

Agent Flow:
  - User: "migrate my data from staging to prod"
  - Agent: Interprets request, shows progress
  - System: Uses same interactive logic with progress callbacks
```

#### C. State Requirements Analysis
```yaml
Context Data Needed:
  - migration_plan: Dict with source/target mapping
  - phase: str (prepare/review/ready_to_execute)
  - metadata: Dict with job info, timestamps
  - user_preferences: Dict with user modifications

Validation Requirements:
  - Source schema exists and accessible
  - Target schema writable
  - No conflicting operations
  - User confirmation patterns

Error Scenarios:
  - Lost context (user session expired)
  - External API failures (Databricks down)
  - Invalid user input
  - Partial execution failures
```

---

## TDD Development Phases

### Phase 1: Red - Write Failing Tests First

#### Step 1.1: Create Test File Structure

```python
# tests/unit/commands/test_your_feature.py
"""
Tests for [your_feature] handler.

Behavioral tests focused on command execution patterns rather than implementation details.
"""

import pytest
from unittest.mock import patch, MagicMock
from chuck_data.commands.your_feature import handle_your_feature
from chuck_data.interactive_context import InteractiveContext

# Test organization sections:
# 1. Parameter validation tests (universal)
# 2. Direct command tests (no tool_output_callback)  
# 3. Agent-specific behavioral tests
# 4. Interactive workflow tests
# 5. Error handling and recovery tests
```

#### Step 1.2: Write Parameter Validation Tests (Start Here!)

```python
# ===== PARAMETER VALIDATION TESTS =====
def test_missing_required_parameter_returns_error():
    """Missing required parameter returns helpful error."""
    # This will fail initially - good! Write it first.
    result = handle_your_feature(databricks_client_stub)
    
    assert not result.success
    assert "required parameter" in result.message.lower()
    # Test drives the need for parameter validation

def test_invalid_parameter_values_return_errors():
    """Invalid parameter values return specific error messages."""
    result = handle_your_feature(
        databricks_client_stub,
        source_schema="nonexistent"
    )
    
    assert not result.success
    assert "schema 'nonexistent' not found" in result.message
    # Test drives the need for parameter validation logic
```

#### Step 1.3: Write Direct Command Success Tests

```python
# ===== DIRECT COMMAND TESTS =====
def test_direct_command_successful_execution(databricks_client_stub, temp_config):
    """Direct command with auto_confirm successfully executes workflow."""
    with patch("your_module.config._config_manager", temp_config):
        # Setup test data
        setup_successful_test_data(databricks_client_stub)
        
        result = handle_your_feature(
            databricks_client_stub,
            source_schema="source",
            target_schema="target", 
            auto_confirm=True  # Skip interactive flow
        )
        
        # These assertions will fail initially - that's the point!
        assert result.success
        assert "workflow completed" in result.message
        assert "execution_id" in result.data
        # Test drives implementation requirements

def test_direct_command_shows_helpful_error_on_failure(databricks_client_stub):
    """Direct command failure shows error with available options."""
    # Setup failure conditions
    databricks_client_stub.add_schema("existing_schema")
    # Don't add target schema - this should cause failure
    
    result = handle_your_feature(
        databricks_client_stub,
        source_schema="existing_schema",
        target_schema="missing_schema",
        auto_confirm=True
    )
    
    assert not result.success
    assert "schema 'missing_schema' not found" in result.message
    assert "Available schemas:" in result.message
    # Test drives error message requirements
```

#### Step 1.4: Write Agent Integration Tests

```python
# ===== AGENT BEHAVIORAL TESTS =====
def test_agent_execution_shows_progress_steps(databricks_client_stub, temp_config):
    """Agent execution shows progress during workflow execution."""
    with patch("your_module.config._config_manager", temp_config):
        setup_successful_test_data(databricks_client_stub)
        
        progress_steps = []
        def capture_progress(tool_name, data):
            if "step" in data:
                progress_steps.append(f"→ Your Feature: ({data['step']})")
        
        result = handle_your_feature(
            databricks_client_stub,
            source_schema="source",
            target_schema="target",
            auto_confirm=True,
            tool_output_callback=capture_progress
        )
        
        assert result.success
        # Test drives progress reporting requirements
        assert len(progress_steps) >= 2
        assert any("analyzing schemas" in step.lower() for step in progress_steps)
        assert any("executing workflow" in step.lower() for step in progress_steps)

def test_agent_tool_executor_end_to_end_integration(databricks_client_stub):
    """Agent tool_executor integration works end-to-end."""
    from chuck_data.agent.tool_executor import execute_tool
    
    setup_successful_test_data(databricks_client_stub)
    
    result = execute_tool(
        api_client=databricks_client_stub,
        tool_name="your-feature",
        tool_args={
            "source_schema": "source",
            "target_schema": "target",
            "auto_confirm": True
        }
    )
    
    # Test drives agent result format requirements
    assert "execution_id" in result
    assert "source_schema" in result
    assert result["source_schema"] == "source"
```

#### Step 1.5: Write Interactive Workflow Tests

```python
# ===== INTERACTIVE WORKFLOW TESTS =====
def test_interactive_mode_phase_1_preparation(databricks_client_stub, temp_config):
    """Interactive mode Phase 1 prepares workflow and shows preview."""
    with patch("your_module.config._config_manager", temp_config):
        setup_successful_test_data(databricks_client_stub)
        
        # Call without auto_confirm to enter interactive mode
        result = handle_your_feature(
            databricks_client_stub,
            source_schema="source",
            target_schema="target"
            # No auto_confirm = enters interactive mode
        )
        
        # Phase 1 should prepare and wait for user input
        assert result.success
        assert result.message == ""  # Console handles display
        
        # Verify context state was set up
        context = InteractiveContext()
        context_data = context.get_context_data("your-feature")
        assert context_data is not None
        assert context_data.get("phase") == "review"
        # Test drives interactive context requirements

def test_interactive_confirmation_patterns():
    """Test different user confirmation patterns."""
    context = InteractiveContext()
    context.set_active_context("your-feature")
    
    # Setup some test context data
    context.store_context_data("your-feature", "phase", "review")
    context.store_context_data("your-feature", "workflow_plan", {"test": "data"})
    
    # Test various confirmation inputs
    confirmation_inputs = [
        ("proceed", True),
        ("yes", True), 
        ("launch", True),
        ("go", True),
        ("cancel", False),
        ("abort", False),
        ("modify table X", "modification")  # Should trigger modification flow
    ]
    
    for user_input, expected_result in confirmation_inputs:
        result = handle_your_feature(
            databricks_client_stub,
            interactive_input=user_input
        )
        
        # Test drives confirmation parsing requirements
        if expected_result is True:
            assert result.success
        elif expected_result is False:
            assert "cancelled" in result.message.lower()
        elif expected_result == "modification":
            # Test drives modification flow requirements
            assert result.success
            assert result.message == ""  # Still in interactive mode
```

### Phase 2: Green - Make Tests Pass

#### Step 2.1: Create Minimal Implementation

```python
# chuck_data/commands/your_feature.py
from chuck_data.interactive_context import InteractiveContext
from chuck_data.commands.base import CommandResult

def handle_your_feature(client, **kwargs):
    """Handle your interactive feature - minimal implementation to pass tests."""
    
    # Parameter validation (driven by failing tests)
    source_schema = kwargs.get("source_schema")
    if not source_schema:
        return CommandResult(False, message="Error: source_schema is required")
    
    # Check if schema exists (driven by failing tests)
    if not client.schema_exists(source_schema):
        available = ", ".join(client.list_schemas())
        return CommandResult(
            False, 
            message=f"Schema '{source_schema}' not found. Available schemas: {available}"
        )
    
    # Handle interactive vs direct execution (driven by failing tests)
    auto_confirm = kwargs.get("auto_confirm", False)
    interactive_input = kwargs.get("interactive_input")
    
    if interactive_input:
        return _handle_interactive_input(client, interactive_input, **kwargs)
    elif auto_confirm:
        return _execute_directly(client, **kwargs)
    else:
        return _start_interactive_mode(client, **kwargs)

def _execute_directly(client, **kwargs):
    """Direct execution path (driven by failing tests)."""
    # Minimal implementation to pass tests
    tool_output_callback = kwargs.get("tool_output_callback")
    
    if tool_output_callback:
        tool_output_callback("your-feature", {"step": "analyzing schemas"})
        tool_output_callback("your-feature", {"step": "executing workflow"})
    
    return CommandResult(
        True, 
        message="Workflow completed successfully", 
        data={"execution_id": "test_execution_123"}
    )

def _start_interactive_mode(client, **kwargs):
    """Start interactive workflow (driven by failing tests)."""
    context = InteractiveContext()
    context.set_active_context("your-feature")
    context.store_context_data("your-feature", "phase", "review")
    context.store_context_data("your-feature", "workflow_plan", {"test": "data"})
    
    return CommandResult(True, message="")  # Console displays preview

def _handle_interactive_input(client, user_input, **kwargs):
    """Handle user input during interactive mode (driven by failing tests)."""
    user_input_lower = user_input.lower().strip()
    
    if user_input_lower in ["proceed", "yes", "launch", "go"]:
        return _execute_directly(client, **kwargs)
    elif user_input_lower in ["cancel", "abort"]:
        context = InteractiveContext()
        context.clear_active_context("your-feature")
        return CommandResult(False, message="Workflow cancelled by user")
    else:
        # Modification request - stay in interactive mode
        return CommandResult(True, message="")
```

#### Step 2.2: Add to Command Registry

```python
# chuck_data/command_registry.py
# Add your command to the registry (driven by failing tests)

COMMANDS = {
    # ... existing commands ...
    "/your-feature": CommandDefinition(
        handler=your_feature.handle_your_feature,
        parameters=[
            CommandParameter("source_schema", str, "Source schema name", required=True),
            CommandParameter("target_schema", str, "Target schema name", required=True),
            CommandParameter("auto_confirm", bool, "Skip interactive confirmation", required=False),
        ],
        supports_interactive_input=True,  # Key for interactive features!
        visible_to_user=True,
        visible_to_agent=True,
        description="Interactive workflow for your feature"
    )
}
```

### Phase 3: Refactor - Improve Implementation

#### Step 3.1: Add More Comprehensive Tests

```python
# Add edge cases and error conditions
def test_lost_context_shows_helpful_error():
    """Lost interactive context shows helpful error message."""
    # Simulate lost context
    context = InteractiveContext() 
    context.clear_active_context("your-feature")  # Clear any existing context
    
    result = handle_your_feature(
        databricks_client_stub,
        interactive_input="proceed"  # Try to proceed without context
    )
    
    assert not result.success
    assert "session expired" in result.message.lower() or "lost context" in result.message.lower()

def test_external_api_errors_handled_gracefully():
    """External API errors are handled gracefully with helpful messages."""
    # Simulate Databricks API failure
    databricks_client_stub.should_fail_next_call = True
    
    result = handle_your_feature(
        databricks_client_stub,
        source_schema="source",
        target_schema="target",
        auto_confirm=True
    )
    
    assert not result.success
    assert "unable to connect" in result.message.lower() or "api error" in result.message.lower()

def test_concurrent_interactive_sessions():
    """Multiple interactive sessions can run independently."""
    context = InteractiveContext()
    
    # Start two different interactive workflows  
    context.set_active_context("your-feature")
    context.store_context_data("your-feature", "user_id", "user1")
    
    context.set_active_context("other-feature") 
    context.store_context_data("other-feature", "user_id", "user2")
    
    # Verify isolation
    your_data = context.get_context_data("your-feature")
    other_data = context.get_context_data("other-feature")
    
    assert your_data["user_id"] == "user1"
    assert other_data["user_id"] == "user2"
```

#### Step 3.2: Refactor Implementation

```python
# Improve implementation with better error handling, logging, etc.
def handle_your_feature(client, **kwargs):
    """Improved implementation with proper error handling."""
    try:
        # Parameter validation with better errors
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
        context.clear_active_context("your-feature")
        return CommandResult(False, error=e, message=f"Error: {str(e)}")

def _validate_parameters(client, **kwargs):
    """Comprehensive parameter validation."""
    errors = []
    
    source_schema = kwargs.get("source_schema")
    if not source_schema:
        errors.append("source_schema is required")
        
    target_schema = kwargs.get("target_schema")
    if not target_schema:
        errors.append("target_schema is required")
        
    if errors:
        return CommandResult(False, message=f"Validation errors: {'; '.join(errors)}")
    
    # Check schema existence
    try:
        if not client.schema_exists(source_schema):
            available = ", ".join(client.list_schemas())
            return CommandResult(
                False,
                message=f"Schema '{source_schema}' not found. Available schemas: {available}"
            )
    except Exception as e:
        return CommandResult(False, message=f"Unable to validate schema: {str(e)}")
    
    return CommandResult(True, message="Parameters valid")
```

---

## Test Categories and Patterns

### Category 1: Parameter Validation Tests

```python
# Always test these scenarios first - they're universal
def test_missing_required_parameters():
    """Missing required parameters return specific error messages."""
    
def test_invalid_parameter_types():
    """Invalid parameter types are rejected with helpful errors."""
    
def test_parameter_constraints():
    """Parameter constraints (length, format, etc.) are enforced."""
    
def test_parameter_combinations():
    """Invalid parameter combinations are detected."""
```

### Category 2: Direct Command Tests (No Interaction)

```python
# Test the auto_confirm=True path
def test_direct_command_success_cases():
    """Direct command execution succeeds with expected outputs."""
    
def test_direct_command_failure_cases():
    """Direct command failures show helpful error messages."""
    
def test_direct_command_edge_cases():
    """Direct command handles edge cases gracefully."""
```

### Category 3: Agent Integration Tests

```python
# Test tool_output_callback integration
def test_agent_progress_reporting():
    """Agent receives progress updates during execution."""
    
def test_agent_error_handling():
    """Agent receives proper error information."""
    
def test_agent_tool_executor_integration():
    """Agent tool executor integration works end-to-end."""
```

### Category 4: Interactive Workflow Tests

```python
# Test the interactive confirmation flows
def test_interactive_phase_transitions():
    """Interactive mode transitions between phases correctly."""
    
def test_confirmation_pattern_recognition():
    """Different confirmation patterns are recognized correctly."""
    
def test_modification_flows():
    """User modifications are processed correctly."""
    
def test_cancellation_flows():
    """User cancellations are handled at all phases."""
```

### Category 5: Error Handling and Recovery Tests

```python
# Test error scenarios and recovery
def test_context_loss_recovery():
    """Lost context scenarios are handled gracefully."""
    
def test_external_api_failure_handling():
    """External API failures don't crash the system."""
    
def test_partial_execution_recovery():
    """Partially completed workflows can be recovered or cleaned up."""
```

---

## State Management Testing

### InteractiveContext Testing Patterns

```python
def test_context_lifecycle_management():
    """Context is properly created, used, and cleaned up."""
    context = InteractiveContext()
    
    # Test creation
    context.set_active_context("test-feature")
    assert context.is_in_interactive_mode()
    assert context.current_command == "test-feature"
    
    # Test data storage/retrieval
    test_data = {"phase": "review", "config": {"key": "value"}}
    context.store_context_data("test-feature", "phase", test_data["phase"])
    context.store_context_data("test-feature", "config", test_data["config"])
    
    retrieved_data = context.get_context_data("test-feature")
    assert retrieved_data["phase"] == "review"
    assert retrieved_data["config"]["key"] == "value"
    
    # Test cleanup
    context.clear_active_context("test-feature")
    assert not context.is_in_interactive_mode()
    assert context.current_command is None

def test_context_isolation_between_commands():
    """Different commands maintain separate context data."""
    context = InteractiveContext()
    
    # Store data for two different commands
    context.store_context_data("feature-a", "data", "value-a")
    context.store_context_data("feature-b", "data", "value-b")
    
    # Verify isolation
    data_a = context.get_context_data("feature-a")
    data_b = context.get_context_data("feature-b")
    
    assert data_a["data"] == "value-a"
    assert data_b["data"] == "value-b"

def test_context_persistence_across_phases():
    """Context data persists across multiple phase transitions."""
    context = InteractiveContext()
    context.set_active_context("test-feature")
    
    # Phase 1: Store initial data
    context.store_context_data("test-feature", "phase", "preparation")
    context.store_context_data("test-feature", "initial_config", {"setup": True})
    
    # Phase 2: Add more data
    context.store_context_data("test-feature", "phase", "review")
    context.store_context_data("test-feature", "user_modifications", ["mod1", "mod2"])
    
    # Phase 3: Verify all data still available
    final_data = context.get_context_data("test-feature")
    assert final_data["phase"] == "review"
    assert final_data["initial_config"]["setup"] is True
    assert "mod1" in final_data["user_modifications"]
    assert len(final_data["user_modifications"]) == 2
```

### Configuration State Testing

```python
def test_configuration_state_interaction():
    """Interactive commands properly use configuration state."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        config_manager.update(
            active_catalog="test_catalog",
            active_schema="test_schema"
        )
        
        with patch("your_module.config._config_manager", config_manager):
            result = handle_your_feature(
                databricks_client_stub,
                auto_confirm=True  # Use config defaults
            )
            
            # Should use active catalog/schema from config
            assert result.success
            assert "test_catalog.test_schema" in result.message

def test_configuration_updates_during_workflow():
    """Workflow can update configuration state."""
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        
        with patch("your_module.config._config_manager", config_manager):
            result = handle_your_feature(
                databricks_client_stub,
                source_schema="new_schema",
                target_schema="target_schema",
                auto_confirm=True,
                update_active_schema=True  # Should update config
            )
            
            assert result.success
            
            # Verify config was updated
            updated_config = config_manager.get_config()
            assert updated_config.active_schema == "target_schema"
```

---

## Interactive Flow Testing

### Confirmation Pattern Testing

```python
def test_confirmation_input_classification():
    """Test how different user inputs are classified."""
    
    # Test data: (input, expected_classification)
    test_cases = [
        # Positive confirmations
        ("yes", "confirm"),
        ("y", "confirm"), 
        ("YES", "confirm"),
        ("  yes  ", "confirm"),  # whitespace handling
        ("proceed", "confirm"),
        ("launch", "confirm"),
        ("go", "confirm"),
        ("make it so", "confirm"),
        
        # Negative confirmations  
        ("no", "cancel"),
        ("n", "cancel"),
        ("cancel", "cancel"),
        ("abort", "cancel"),
        ("stop", "cancel"),
        ("quit", "cancel"),
        
        # Modification requests
        ("remove table X", "modification"),
        ("change the timeout to 30 minutes", "modification"),
        ("exclude sensitive data", "modification"),
        ("add column Y to the migration", "modification"),
        
        # Ambiguous cases
        ("", "unknown"),
        ("maybe", "unknown"),
        ("what does this do?", "unknown")
    ]
    
    for user_input, expected_type in test_cases:
        classification = _classify_user_input(user_input)
        assert classification == expected_type, \
            f"Input '{user_input}' classified as '{classification}', expected '{expected_type}'"

def test_confirmation_security():
    """User input is properly sanitized and doesn't leak sensitive data."""
    context = InteractiveContext()
    context.set_active_context("test-feature")
    
    # Test with potentially sensitive input
    sensitive_input = "yes and my password is secret123"
    
    result = handle_your_feature(
        databricks_client_stub,
        interactive_input=sensitive_input
    )
    
    # Verify sensitive data doesn't leak into logs or context
    context_data = context.get_context_data("test-feature")
    for key, value in context_data.items():
        if isinstance(value, str):
            assert "secret123" not in value, \
                f"Sensitive data found in context key '{key}': {value}"
    
    # Verify response doesn't echo sensitive data
    assert "secret123" not in result.message
```

### Multi-Phase Workflow Testing

```python
def test_complete_interactive_workflow():
    """Test a complete interactive workflow from start to finish."""
    
    # Phase 1: Start interactive mode
    result_1 = handle_your_feature(
        databricks_client_stub,
        source_schema="source",
        target_schema="target"
        # No auto_confirm - starts interactive mode
    )
    
    assert result_1.success
    assert result_1.message == ""  # Console handles display
    
    # Verify we're in interactive mode
    context = InteractiveContext()
    assert context.is_in_interactive_mode()
    assert context.current_command == "your-feature"
    
    # Phase 2: User provides confirmation
    result_2 = handle_your_feature(
        databricks_client_stub,
        interactive_input="proceed"
    )
    
    assert result_2.success
    assert "completed successfully" in result_2.message
    
    # Verify context was cleaned up
    assert not context.is_in_interactive_mode()
    assert context.current_command is None

def test_interactive_workflow_with_modifications():
    """Test interactive workflow with user modifications."""
    
    # Start interactive mode
    handle_your_feature(
        databricks_client_stub,
        source_schema="source", 
        target_schema="target"
    )
    
    # User requests modification
    result_mod = handle_your_feature(
        databricks_client_stub,
        interactive_input="exclude table sensitive_data"
    )
    
    # Should stay in interactive mode
    assert result_mod.success
    assert result_mod.message == ""
    
    context = InteractiveContext()
    assert context.is_in_interactive_mode()
    
    # Verify modification was recorded
    context_data = context.get_context_data("your-feature")
    assert "modifications" in context_data or "excluded_tables" in context_data
    
    # Final confirmation
    result_final = handle_your_feature(
        databricks_client_stub,
        interactive_input="launch"
    )
    
    assert result_final.success
    assert "completed successfully" in result_final.message

def test_interactive_workflow_cancellation():
    """Test user can cancel at any phase."""
    
    # Start interactive mode
    handle_your_feature(
        databricks_client_stub,
        source_schema="source",
        target_schema="target" 
    )
    
    # User cancels
    result_cancel = handle_your_feature(
        databricks_client_stub,
        interactive_input="cancel"
    )
    
    assert not result_cancel.success
    assert "cancelled" in result_cancel.message.lower()
    
    # Verify context was cleaned up
    context = InteractiveContext()
    assert not context.is_in_interactive_mode()
```

---

## Agent Integration Testing

### Progress Reporting Testing

```python
def test_agent_receives_structured_progress():
    """Agent receives structured progress data during execution."""
    
    progress_data = []
    def capture_progress(tool_name, data):
        progress_data.append({
            "tool": tool_name,
            "timestamp": data.get("timestamp"),
            "step": data.get("step"),
            "details": data.get("details", {})
        })
    
    result = handle_your_feature(
        databricks_client_stub,
        source_schema="source",
        target_schema="target", 
        auto_confirm=True,
        tool_output_callback=capture_progress
    )
    
    assert result.success
    assert len(progress_data) >= 2
    
    # Verify progress structure
    for progress in progress_data:
        assert progress["tool"] == "your-feature"
        assert "step" in progress
        assert isinstance(progress["step"], str)
        assert len(progress["step"]) > 0

def test_agent_error_reporting():
    """Agent receives proper error information when workflow fails."""
    
    # Setup failure condition
    databricks_client_stub.should_fail_schema_check = True
    
    error_data = []
    def capture_errors(tool_name, data):
        if "error" in data:
            error_data.append(data)
    
    result = handle_your_feature(
        databricks_client_stub,
        source_schema="source",
        target_schema="nonexistent", 
        auto_confirm=True,
        tool_output_callback=capture_errors
    )
    
    assert not result.success
    
    # Verify error was reported to agent
    assert len(error_data) > 0
    assert "error" in error_data[0]
    assert isinstance(error_data[0]["error"], str)

def test_agent_callback_error_handling():
    """Callback errors don't crash the command but are properly reported."""
    
    def failing_callback(tool_name, data):
        raise Exception("Agent display system crashed")
    
    result = handle_your_feature(
        databricks_client_stub,
        source_schema="source",
        target_schema="target",
        auto_confirm=True, 
        tool_output_callback=failing_callback
    )
    
    # Current behavior: callback errors bubble up as command errors
    # Document this behavior even if it seems suboptimal
    assert not result.success
    assert "Agent display system crashed" in result.message
```

### Tool Executor Integration Testing

```python
def test_tool_executor_parameter_mapping():
    """Tool executor properly maps parameters for the command."""
    from chuck_data.agent.tool_executor import execute_tool
    
    result = execute_tool(
        api_client=databricks_client_stub,
        tool_name="your-feature",
        tool_args={
            "source_schema": "mapped_source",
            "target_schema": "mapped_target", 
            "auto_confirm": True
        }
    )
    
    # Verify agent gets proper result format
    assert isinstance(result, dict)
    assert "source_schema" in result
    assert "target_schema" in result
    assert result["source_schema"] == "mapped_source"
    assert result["target_schema"] == "mapped_target"

def test_tool_executor_error_format():
    """Tool executor returns properly formatted errors."""
    from chuck_data.agent.tool_executor import execute_tool
    
    result = execute_tool(
        api_client=databricks_client_stub,
        tool_name="your-feature", 
        tool_args={
            "source_schema": "nonexistent"
            # Missing required target_schema
        }
    )
    
    # Verify error format for agent consumption
    assert isinstance(result, dict)
    assert "error" in result
    assert isinstance(result["error"], str)
    assert "target_schema is required" in result["error"]
```

---

## Implementation Guidelines

### Command Handler Structure

```python
def handle_your_feature(client, **kwargs):
    """
    Main command handler following established patterns.
    
    Args:
        client: Databricks client instance
        **kwargs: Command parameters including:
            - source_schema: str (required)
            - target_schema: str (required) 
            - auto_confirm: bool (optional, default False)
            - interactive_input: str (provided during interactive mode)
            - tool_output_callback: callable (for agent progress reporting)
    
    Returns:
        CommandResult: Success/failure with appropriate data
    """
    
    try:
        # 1. Parameter validation (always first)
        validation_result = _validate_parameters(client, **kwargs)
        if not validation_result.success:
            return validation_result
        
        # 2. Route to appropriate handler based on execution mode
        interactive_input = kwargs.get("interactive_input")
        auto_confirm = kwargs.get("auto_confirm", False)
        
        if interactive_input:
            # Handle user input during interactive session
            return _handle_interactive_input(client, interactive_input, **kwargs)
        elif auto_confirm:
            # Direct execution without interaction
            return _execute_directly(client, **kwargs)
        else:
            # Start interactive workflow
            return _start_interactive_mode(client, **kwargs)
            
    except Exception as e:
        # Always cleanup context on any error
        context = InteractiveContext()
        context.clear_active_context("your-feature")
        return CommandResult(False, error=e, message=f"Error: {str(e)}")

def _validate_parameters(client, **kwargs):
    """Comprehensive parameter validation."""
    # Implementation details...
    
def _execute_directly(client, **kwargs):
    """Execute workflow directly without interaction."""
    # Implementation details...
    
def _start_interactive_mode(client, **kwargs):
    """Start interactive workflow."""
    # Implementation details...
    
def _handle_interactive_input(client, user_input, **kwargs):
    """Handle user input during interactive mode."""
    # Implementation details...
```

### Progress Reporting Pattern

```python
def _report_progress(step_message, details=None, tool_output_callback=None):
    """
    Standard progress reporting for agent integration.
    
    Args:
        step_message: str - Human readable progress message
        details: dict - Additional structured data
        tool_output_callback: callable - Agent callback function
    """
    if tool_output_callback:
        progress_data = {
            "step": step_message,
            "timestamp": datetime.now().isoformat()
        }
        if details:
            progress_data["details"] = details
            
        tool_output_callback("your-feature", progress_data)

# Usage in workflow
def _execute_workflow_phase(client, config, **kwargs):
    tool_output_callback = kwargs.get("tool_output_callback")
    
    _report_progress("Starting workflow analysis", 
                    {"phase": "analysis"}, tool_output_callback)
    
    # Do work...
    analysis_result = client.analyze_schemas(config)
    
    _report_progress("Analysis complete, beginning execution",
                    {"tables_found": len(analysis_result.tables)}, 
                    tool_output_callback)
    
    # More work...
```

### Context Management Pattern

```python
def _manage_interactive_context(command_name, phase_data=None, cleanup_on_error=True):
    """
    Context manager for interactive workflows.
    
    Usage:
        with _manage_interactive_context("your-feature", {"phase": "review"}):
            # Do interactive work
            # Context automatically cleaned up on error
    """
    context = InteractiveContext()
    try:
        context.set_active_context(command_name)
        if phase_data:
            for key, value in phase_data.items():
                context.store_context_data(command_name, key, value)
        yield context
    except Exception as e:
        if cleanup_on_error:
            context.clear_active_context(command_name)
        raise

# Usage
def _start_interactive_mode(client, **kwargs):
    initial_data = {
        "phase": "review",
        "source_schema": kwargs["source_schema"],
        "target_schema": kwargs["target_schema"]
    }
    
    with _manage_interactive_context("your-feature", initial_data) as context:
        # Prepare workflow
        workflow_plan = _prepare_workflow(client, **kwargs)
        context.store_context_data("your-feature", "workflow_plan", workflow_plan)
        
        # Display would happen in console/TUI
        return CommandResult(True, message="")
```

---

## Common Patterns and Utilities

### Test Data Setup Helpers

```python
def setup_successful_test_data(databricks_client_stub, llm_client_stub=None):
    """
    Standard test data setup for successful workflow execution.
    
    This pattern should be consistent across all interactive feature tests.
    """
    # Add basic Databricks resources
    databricks_client_stub.add_catalog("test_catalog")
    databricks_client_stub.add_schema("test_catalog", "source_schema")
    databricks_client_stub.add_schema("test_catalog", "target_schema")
    
    # Add sample tables with realistic column structures
    databricks_client_stub.add_table(
        "test_catalog", "source_schema", "users",
        columns=[
            {"name": "id", "type": "bigint"},
            {"name": "email", "type": "string"},
            {"name": "created_at", "type": "timestamp"}
        ]
    )
    
    databricks_client_stub.add_table(
        "test_catalog", "source_schema", "orders", 
        columns=[
            {"name": "order_id", "type": "bigint"},
            {"name": "user_id", "type": "bigint"}, 
            {"name": "amount", "type": "decimal(10,2)"}
        ]
    )
    
    # Setup LLM responses if needed
    if llm_client_stub:
        llm_client_stub.set_default_response({
            "analysis": "Workflow looks good",
            "recommendations": ["Proceed with migration"]
        })

def setup_failure_conditions(databricks_client_stub, failure_type="missing_schema"):
    """
    Setup various failure conditions for testing error handling.
    """
    if failure_type == "missing_schema":
        databricks_client_stub.add_catalog("test_catalog")
        # Don't add the required schema - will cause failure
        
    elif failure_type == "permission_denied":
        databricks_client_stub.add_catalog("test_catalog")
        databricks_client_stub.add_schema("test_catalog", "restricted_schema")
        databricks_client_stub.set_permission_error("restricted_schema")
        
    elif failure_type == "api_timeout":
        databricks_client_stub.set_timeout_error(True)
```

### Input Classification Utilities

```python
def _classify_user_input(user_input):
    """
    Classify user input for interactive workflows.
    
    Returns: str - One of: 'confirm', 'cancel', 'modification', 'unknown'
    """
    if not user_input or not isinstance(user_input, str):
        return 'unknown'
    
    cleaned_input = user_input.lower().strip()
    
    # Confirmation patterns
    confirm_patterns = [
        'yes', 'y', 'proceed', 'launch', 'go', 'confirm', 
        'make it so', 'execute', 'run', 'continue'
    ]
    
    # Cancellation patterns
    cancel_patterns = [
        'no', 'n', 'cancel', 'abort', 'stop', 'quit', 
        'exit', 'back', 'nevermind'
    ]
    
    if cleaned_input in confirm_patterns:
        return 'confirm'
    elif cleaned_input in cancel_patterns:
        return 'cancel'
    elif len(cleaned_input) > 10:  # Likely a modification request
        return 'modification'
    else:
        return 'unknown'

def _extract_modification_intent(user_input):
    """
    Extract structured modification intent from user input.
    
    Returns: dict with modification details
    """
    # This would typically use LLM or NLP to parse intent
    # For now, simple keyword matching
    
    intent = {
        'action': 'unknown',
        'target': None,
        'details': user_input
    }
    
    lower_input = user_input.lower()
    
    if 'exclude' in lower_input or 'remove' in lower_input:
        intent['action'] = 'exclude'
    elif 'include' in lower_input or 'add' in lower_input:
        intent['action'] = 'include'
    elif 'change' in lower_input or 'modify' in lower_input:
        intent['action'] = 'modify'
        
    # Extract table names, column names, etc.
    # This would be more sophisticated in a real implementation
    
    return intent
```

### Error Recovery Utilities

```python
def _handle_context_loss():
    """Handle lost interactive context gracefully."""
    return CommandResult(
        False,
        message="Interactive session expired. Please restart the command to begin a new workflow."
    )

def _handle_external_api_error(error, operation_name):
    """Standardized external API error handling."""
    error_message = f"Unable to {operation_name}"
    
    if "timeout" in str(error).lower():
        error_message += " due to network timeout. Please try again."
    elif "permission" in str(error).lower():
        error_message += " due to insufficient permissions. Check your access rights."
    elif "not found" in str(error).lower():
        error_message += " because the resource was not found. Verify your parameters."
    else:
        error_message += f" due to an unexpected error: {str(error)}"
    
    return CommandResult(False, message=error_message)

def _cleanup_partial_execution(context_data):
    """Clean up any partial execution state."""
    # This would clean up any created resources, temporary files, etc.
    # Implementation depends on what your workflow creates
    pass
```

---

## Quality Assurance Checklist

### Pre-Implementation Checklist

- [ ] Feature specification clearly defined with user stories
- [ ] User interaction flows documented with examples
- [ ] State requirements analyzed (what needs to persist)
- [ ] Error scenarios identified and planned
- [ ] Integration points with existing systems mapped

### Test Coverage Checklist

#### Parameter Validation Tests
- [ ] Missing required parameters tested
- [ ] Invalid parameter types tested  
- [ ] Parameter constraint validation tested
- [ ] Invalid parameter combinations tested

#### Direct Command Tests  
- [ ] Successful execution with auto_confirm tested
- [ ] Error cases with helpful messages tested
- [ ] Edge cases and boundary conditions tested
- [ ] Configuration state interaction tested

#### Agent Integration Tests
- [ ] Progress reporting during execution tested
- [ ] Error reporting to agent tested
- [ ] Tool executor integration tested end-to-end
- [ ] Agent callback error handling tested

#### Interactive Workflow Tests
- [ ] Phase transitions tested
- [ ] Confirmation pattern recognition tested
- [ ] Modification flow handling tested
- [ ] Cancellation at all phases tested
- [ ] Context persistence across phases tested

#### Error Handling Tests
- [ ] Context loss recovery tested
- [ ] External API failure handling tested
- [ ] Partial execution cleanup tested
- [ ] Concurrent session isolation tested

### Implementation Quality Checklist

#### Code Structure
- [ ] Command handler follows established pattern
- [ ] Parameter validation comprehensive and clear
- [ ] Error handling includes context cleanup
- [ ] Progress reporting implemented for agent compatibility
- [ ] Command registered with proper metadata

#### State Management
- [ ] InteractiveContext used correctly
- [ ] Context cleanup on all error paths
- [ ] Session isolation maintained
- [ ] Configuration state properly integrated

#### Security Considerations
- [ ] User input properly sanitized
- [ ] Sensitive data not logged or stored in context
- [ ] Error messages don't leak sensitive information
- [ ] Authentication/authorization respected

#### User Experience
- [ ] Error messages helpful and actionable
- [ ] Progress reporting informative
- [ ] Confirmation patterns intuitive
- [ ] Cancellation available at all phases

### Integration Testing Checklist

- [ ] Command registry integration tested
- [ ] TUI interactive mode integration tested
- [ ] Service layer routing tested
- [ ] Agent tool executor integration tested
- [ ] Configuration system integration tested

### Performance and Reliability Checklist

- [ ] External API timeouts handled gracefully
- [ ] Large data sets don't cause memory issues
- [ ] Context data size reasonable (not unbounded)
- [ ] Cleanup prevents resource leaks
- [ ] Concurrent usage doesn't cause conflicts

### Documentation Checklist

- [ ] Command help text clear and complete
- [ ] Interactive workflow documented for users
- [ ] Error scenarios documented
- [ ] Agent integration capabilities documented
- [ ] Configuration requirements documented

---

## Final TDD Workflow Summary

### Phase 1: Red (Write Failing Tests)
1. **Start with parameter validation tests** - these are universal and drive basic structure
2. **Add direct command tests** - test the auto_confirm=True path
3. **Add agent integration tests** - test tool_output_callback integration  
4. **Add interactive workflow tests** - test the multi-phase confirmation flows
5. **Add error handling tests** - test recovery and cleanup scenarios

### Phase 2: Green (Make Tests Pass)
1. **Create minimal command handler** - just enough to pass parameter validation
2. **Add direct execution path** - implement auto_confirm=True logic
3. **Add interactive mode starter** - implement context setup
4. **Add confirmation handling** - implement user input classification
5. **Add error handling** - implement cleanup and recovery

### Phase 3: Refactor (Improve Implementation)
1. **Add comprehensive error handling** - improve edge case handling
2. **Enhance progress reporting** - add detailed agent integration
3. **Improve user experience** - better error messages, more intuitive flows
4. **Add performance optimizations** - handle large data sets, optimize API calls
5. **Add advanced features** - LLM integration, complex modification flows

### Continuous Improvement
1. **Run tests frequently** - catch regressions early
2. **Add integration tests** - test real system interactions
3. **Monitor user feedback** - improve based on actual usage
4. **Update documentation** - keep help text and examples current
5. **Refactor as needed** - maintain code quality as features evolve

This TDD approach ensures that your interactive features are robust, well-tested, and consistent with the existing chuck-data patterns while being resilient to future changes.