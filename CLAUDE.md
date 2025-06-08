# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Essential Commands
```bash
# Install with development dependencies
uv pip install -e .[dev]

# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/unit/core/test_config.py

# Run single test
uv run pytest tests/unit/core/test_config.py::TestPydanticConfig::test_config_update

# Linting and formatting
uv run ruff check           # Lint check
uv run ruff check --fix     # Auto-fix linting issues
uv run black chuck_data tests  # Format code
uv run pyright             # Type checking

# Run application locally
python -m chuck_data        # Or: uv run python -m chuck_data
chuck-data --no-color      # Disable colors for testing
```

### Test Categories
Tests are organized with pytest markers:
- Default: Unit tests only (fast)
- `pytest -m integration`: Integration tests (requires Databricks access)
- `pytest -m data_test`: Tests that create Databricks resources
- `pytest -m e2e`: End-to-end tests (slow, comprehensive)

### Test Structure (Recently Reorganized)
```
tests/
├── unit/
│   ├── commands/     # Command handler tests
│   ├── clients/      # API client tests  
│   ├── ui/          # TUI/display tests
│   └── core/        # Core functionality tests
├── integration/     # Integration tests
└── fixtures/        # Test stubs and fixtures
```

## Architecture Overview

### Command Processing Flow
1. **TUI** (`ui/tui.py`) receives user input
2. **Command Registry** (`command_registry.py`) maps commands to handlers
3. **Service Layer** (`service.py`) orchestrates business logic
4. **Command Handlers** (`commands/`) execute specific operations
5. **API Clients** (`clients/`) interact with external services

### Key Components

**ChuckService** - Main service facade that:
- Initializes Databricks API client from config
- Routes commands through the command registry
- Handles error reporting and metrics collection
- Acts as bridge between TUI and business logic

**Command Registry** - Unified registry where each command is defined with:
- Handler function, parameters, and validation rules
- Visibility flags (user vs agent accessible)
- Display preferences (condensed vs full output)
- Interactive input support flags

**Configuration System** - Pydantic-based config that:
- Supports both file storage (~/.chuck_config.json) and environment variables
- Environment variables use CHUCK_ prefix (e.g., CHUCK_WORKSPACE_URL)
- Handles workspace URLs, tokens, active catalog/schema/model settings
- Includes usage tracking consent management

**Agent System** - AI-powered assistant that:
- Uses LLM clients (OpenAI-compatible) with configurable models
- Has specialized modes: general queries, PII detection, bulk PII scanning, Stitch setup
- Executes commands through the same registry as TUI
- Maintains conversation history and context

**Interactive Context** - Session state management for:
- Multi-step command workflows (like setup wizards)
- Command-specific context data
- Cross-command state sharing

### External Integrations

**Databricks Integration** - Primary platform integration:
- Unity Catalog operations (catalogs, schemas, tables, volumes)
- SQL Warehouse management and query execution
- Model serving endpoints for LLM access
- Job management and cluster operations
- Authentication via personal access tokens

**Amperity Integration** - Data platform operations:
- Authentication flow with browser-based OAuth
- Bug reporting and metrics submission
- Stitch integration for data pipeline setup

### Test Mocking Guidelines
Core Principle

Mock external boundaries only. Use real objects for all internal business logic to catch integration bugs.

✅ ALWAYS Mock These (External Boundaries)

HTTP/Network Calls

# Databricks SDK and API calls
@patch('databricks.sdk.WorkspaceClient')
@patch('requests.get')
@patch('requests.post')

# OpenAI/LLM API calls
@patch('openai.OpenAI')
# OR use LLMClientStub fixture

File System Operations

# Only when testing file I/O behavior
@patch('builtins.open')
@patch('os.path.exists')
@patch('os.makedirs')
@patch('tempfile.TemporaryDirectory')

# Log file operations
@patch('chuck_data.logger.setup_file_logging')

System/Environment

# Environment variables (when testing env behavior)
@patch.dict('os.environ', {'CHUCK_TOKEN': 'test'})

# System calls
@patch('subprocess.run')
@patch('datetime.datetime.now')  # for deterministic timestamps

User Input/Terminal

# Interactive prompts
@patch('prompt_toolkit.prompt')
@patch('readchar.readkey')
@patch('sys.stdout.write')  # when testing specific output

❌ NEVER Mock These (Internal Logic)

Configuration Objects

# ❌ DON'T DO THIS:
@patch('chuck_data.config.ConfigManager')

# ✅ DO THIS:
config_manager = ConfigManager('/tmp/test_config.json')

Business Logic Classes

# ❌ DON'T DO THIS:
@patch('chuck_data.service.ChuckService')

# ✅ DO THIS:
service = ChuckService(client=mocked_databricks_client)

Data Objects

# ❌ DON'T DO THIS:
@patch('chuck_data.commands.base.CommandResult')

# ✅ DO THIS:
result = CommandResult(success=True, data="test")

Utility Functions

# ❌ DON'T DO THIS:
@patch('chuck_data.utils.normalize_workspace_url')

# ✅ DO THIS:
from chuck_data.utils import normalize_workspace_url
normalized = normalize_workspace_url("https://test.databricks.com")

Command Registry/Routing

# ❌ DON'T DO THIS:
@patch('chuck_data.command_registry.get_command')

# ✅ DO THIS:
from chuck_data.command_registry import get_command
command_def = get_command('/status')  # Test real routing

Amperity Client

# ❌ DON'T DO THIS:
@patch('chuck_data.clients.amperity.AmperityClient')

# ✅ DO THIS:
Use the fixture `AmperityClientStub` to stub only the external API calls, while using the real command logic.

Databricks Client

# ❌ DON'T DO THIS:
@patch('chuck_data.clients.databricks.DatabricksClient')

# ✅ DO THIS:
Use the fixture `DatabricksClientStub` to stub only the external API calls, while using the real command logic. Fixtures for stubbing external API clients follow the naming convention `<ClientName>Stub`.

LLM Client

# ❌ DON'T DO THIS:
@patch('chuck_data.clients.llm.LLMClient')

# ✅ DO THIS:
Use the fixture `LLMClientStub` to stub only the external API calls, while using the real command logic.


🎯 Approved Test Patterns

Pattern 1: External Client + Real Internal Logic

def test_list_catalogs_command():
  # Mock external boundary
  mock_client = DatabricksClientStub()
  mock_client.add_catalog("test_catalog")

  # Use real service
  service = ChuckService(client=mock_client)

  # Test real command execution
  result = service.execute_command("/list_catalogs")

  assert result.success
  assert "test_catalog" in result.data

Pattern 2: Real Config with Temporary Files

def test_config_update():
  with tempfile.NamedTemporaryFile() as tmp:
      # Use real config manager
      config_manager = ConfigManager(tmp.name)

      # Test real config logic
      config_manager.update(workspace_url="https://test.databricks.com")

      # Verify real file operations
      reloaded = ConfigManager(tmp.name)
      assert reloaded.get_config().workspace_url == "https://test.databricks.com"

Pattern 3: Stub Only External APIs

def test_auth_flow():
  # Stub external API
  amperity_stub = AmperityClientStub()
  amperity_stub.set_auth_completion_failure(True)

  # Use real command logic
  result = handle_amperity_login(amperity_stub)

  # Test real error handling
  assert not result.success
  assert "Authentication failed" in result.message

🚫 Red Flags (Stop and Reconsider)

- @patch('chuck_data.config.*')
- @patch('chuck_data.commands.*.handle_*')
- @patch('chuck_data.service.*')
- @patch('chuck_data.utils.*')
- @patch('chuck_data.models.*')
- Any patch of internal business logic functions

✅ Quick Decision Tree

Before mocking anything, ask:

1. Does this cross a process boundary? (network, file, subprocess) → Mock it
2. Is this user input or system interaction? → Mock it
3. Is this internal business logic? → Use real object
4. Is this a data transformation? → Use real function
5. When in doubt → Use real object

Exception: Only mock internal logic when testing error conditions that are impossible to trigger naturally.

Instruction Set: Writing Behavioral Tests with Agent Coverage

  Overview

  This guide explains how to write comprehensive behavioral tests for command handlers that support both direct execution and agent interaction via tool_output_callback. The goal is to test user-visible behavior rather than implementation
  details.

  1. Analyze the Command Handler First

  Step 1.1: Identify Key Behavioral Patterns

  Look for these patterns in the command handler:

  # Pattern 1: tool_output_callback parameter
  def handle_command(client, **kwargs):
      tool_output_callback = kwargs.get("tool_output_callback")

  # Pattern 2: Progress reporting function
  def _report_step(message: str, tool_output_callback=None):
      if tool_output_callback:
          tool_output_callback("command-name", {"step": message})

  # Pattern 3: Different execution paths
  try:
      # Direct path - no callback
      direct_result = some_direct_lookup()
      if direct_result:
          return success_without_callback()
  except:
      # Search path - with callback
      _report_step("Looking for...", tool_output_callback)
      search_result = search_function()
      _report_step("Selecting...", tool_output_callback)

  Step 1.2: Map User-Visible Behaviors

  Document what users actually see:

  Direct Command (no progress):
  chuck > /select-catalog production
  Success: Active catalog is now set to 'production'

  Agent Exact Match (no progress):
  chuck > select production catalog
  → Setting catalog: (Catalog set - Name: production)

  Agent Fuzzy Match (with progress):
  chuck > select prod catalog
  → Setting catalog: (Looking for catalog matching 'prod')
  → Setting catalog: (Selecting 'production_data')
  → Setting catalog: (Catalog set - Name: production_data)

  Agent Failure (with progress):
  chuck > select nonexistent catalog
  → Setting catalog: (Looking for catalog matching 'nonexistent')
  Error: No catalog found matching 'nonexistent'. Available catalogs: production, dev

  2. Test Structure and Naming

  Step 2.1: Test Organization

  Organize tests into clear sections:

  # Parameter validation tests (universal)
  def test_missing_parameter_returns_error():
  def test_invalid_parameter_returns_error():

  # Direct command tests (no tool_output_callback)
  def test_direct_command_success_case():
  def test_direct_command_failure_case():
  def test_direct_command_edge_case():

  # Agent-specific behavioral tests
  def test_agent_exact_match_behavior():
  def test_agent_search_behavior():
  def test_agent_failure_behavior():
  def test_agent_error_handling():
  def test_agent_tool_executor_integration():

  Step 2.2: Test Naming Convention

  DO:
  - test_direct_command_selects_existing_catalog - Clear execution path
  - test_agent_fuzzy_match_shows_multiple_progress_steps - Behavioral outcome
  - test_missing_catalog_parameter_returns_error - Expected behavior

  DON'T:
  - test_user_gets_success_message - Avoid "user" concept
  - test_catalog_selection_works - Too vague
  - test_handle_command_with_callback - Implementation focused

  3. Direct Command Tests

  Step 3.1: Basic Test Pattern

  def test_direct_command_success_case(client_stub, temp_config):
      """Direct command description of expected behavior."""
      with patch("module.config._config_manager", temp_config):
          # Setup test data
          client_stub.add_resource("test_resource")

          # Execute command (no tool_output_callback)
          result = handle_command(client_stub, parameter="test_resource")

          # Verify behavioral outcome
          assert result.success
          assert "expected success message" in result.message
          assert get_active_resource() == "test_resource"  # State change

  Step 3.2: Failure Cases

  def test_direct_command_failure_shows_helpful_error(client_stub, temp_config):
      """Direct command failure shows error with available options."""
      with patch("module.config._config_manager", temp_config):
          client_stub.add_resource("available_resource")

          result = handle_command(client_stub, parameter="missing_resource")

          # Verify helpful error behavior
          assert not result.success
          assert "No resource found matching 'missing_resource'" in result.message
          assert "Available resources: available_resource" in result.message

  4. Agent Tests with tool_output_callback

  Step 4.1: Progress Capture Pattern

  Always use this exact pattern for capturing agent progress:

  def test_agent_behavior_description(client_stub, temp_config):
      """Agent execution shows expected progress behavior."""
      with patch("module.config._config_manager", temp_config):
          # Setup test data
          client_stub.add_resource("target_resource")

          # Capture progress during agent execution
          progress_steps = []
          def capture_progress(tool_name, data):
              progress_steps.append(f"→ Setting resource: ({data['step']})")

          # Execute with tool_output_callback
          result = handle_command(
              client_stub,
              parameter="target_resource",
              tool_output_callback=capture_progress
          )

          # Verify command success
          assert result.success
          assert get_active_resource() == "target_resource"

          # Verify progress behavior
          assert len(progress_steps) == expected_count
          assert "expected progress message" in progress_steps[0]

  Step 4.2: Different Agent Scenarios

  Exact Match (No Search):
  def test_agent_exact_match_shows_no_progress_steps():
      # Should have 0 progress steps (direct lookup succeeds)
      assert len(progress_steps) == 0

  Search Required:
  def test_agent_search_shows_multiple_progress_steps():
      # Force search path
      client_stub.get_resource = lambda name: None

      # Should have 2+ progress steps
      assert len(progress_steps) >= 2
      assert any("Looking for resource matching" in step for step in progress_steps)
      assert any("Selecting 'found_resource'" in step for step in progress_steps)

  Search Failure:
  def test_agent_shows_progress_before_failure():
      # Should show search attempt before failure
      assert not result.success
      assert len(progress_steps) == 1
      assert "Looking for resource matching 'nonexistent'" in progress_steps[0]

  Step 4.3: Agent Error Handling

  def test_agent_callback_errors_bubble_up_as_command_errors():
      """Agent callback failures bubble up as command errors (current behavior)."""
      def failing_callback(tool_name, data):
          raise Exception("Display system crashed")

      result = handle_command(
          client_stub,
          parameter="trigger_search",  # Force callback usage
          tool_output_callback=failing_callback
      )

      # Document current behavior
      assert not result.success
      assert "Display system crashed" in result.message

  Step 4.4: End-to-End Integration

  def test_agent_tool_executor_end_to_end_integration():
      """Agent tool_executor integration works end-to-end."""
      from chuck_data.agent.tool_executor import execute_tool

      result = execute_tool(
          api_client=client_stub,
          tool_name="command-name",
          tool_args={"parameter": "test_value"}
      )

      # Verify agent gets proper result format
      assert "expected_field" in result
      assert result["expected_field"] == "test_value"

      # Verify state actually changed
      assert get_active_resource() == "test_value"

  5. Test Data Setup Patterns

  Step 5.1: Client Stub Setup

  # Add test data to stub
  client_stub.add_catalog("production", catalog_type="MANAGED")
  client_stub.add_schema("test_schema", catalog="production")
  client_stub.add_table("test_table", schema="test_schema")

  # Force specific code paths
  client_stub.get_catalog = lambda name: None  # Force search path
  original_method = client_stub.get_catalog    # Save for restoration

  Step 5.2: Config Management

  Always use this pattern for config tests:

  def test_function(client_stub, temp_config):
      with patch("module.config._config_manager", temp_config):
          # Test code here
          pass

  6. Assertion Patterns

  Step 6.1: What TO Test (Behavioral)

  # Command outcome
  assert result.success
  assert not result.success

  # User-visible messages
  assert "Active catalog is now set to 'production'" in result.message
  assert "No catalog found matching 'missing'" in result.message

  # State changes
  assert get_active_catalog() == "production"
  assert get_active_schema() == "test_schema"

  # Progress behavior
  assert len(progress_steps) == 2
  assert "Looking for catalog matching 'prod'" in progress_steps[0]
  assert "Selecting 'production'" in progress_steps[1]

  # Agent result format
  assert "catalog_name" in result
  assert result["catalog_name"] == "production"

  Step 6.2: What NOT to Test (Implementation)

  # DON'T test internal data structures
  assert result.data["internal_field"] == "value"  # ❌

  # DON'T test method calls
  mock_method.assert_called_with("arg")  # ❌

  # DON'T test implementation details
  assert len(result.data.keys()) == 5  # ❌

  # DON'T over-assert on message format
  assert result.message == "Exact message text"  # ❌ (too brittle)

  7. Checklist for Complete Coverage

  Step 7.1: Core Scenarios

  - Missing/invalid parameters
  - Successful execution (direct command)
  - Failure with helpful error (direct command)
  - Fuzzy matching (if applicable)
  - API error handling

  Step 7.2: Agent Scenarios

  - Agent exact match (no progress steps)
  - Agent search required (multiple progress steps)
  - Agent failure (progress before error)
  - Agent callback error handling
  - Agent tool_executor integration

  Step 7.3: Test Quality

  - Test names describe behavior, not implementation
  - Clear delineation between direct_command_* and agent_* tests
  - Comments explain expected behavior, not code mechanics
  - No "user" language in test names or descriptions
  - Tests focus on observable outcomes

  8. Example Complete Test File Structure

  """
  Tests for [command] handler.

  Behavioral tests focused on command execution patterns rather than implementation details.
  """

  from unittest.mock import patch
  from command_module import handle_command
  from config_module import get_active_resource

  # Parameter validation
  def test_missing_parameter_returns_error():
  def test_invalid_parameter_returns_error():

  # Direct command execution
  def test_direct_command_success_case():
  def test_direct_command_failure_case():
  def test_direct_command_fuzzy_matching():
  def test_databricks_api_errors_handled_gracefully():

  # Agent-specific behavioral tests
  def test_agent_exact_match_shows_no_progress_steps():
  def test_agent_fuzzy_match_shows_multiple_progress_steps():
  def test_agent_shows_progress_before_failure():
  def test_agent_callback_errors_bubble_up_as_command_errors():
  def test_agent_tool_executor_end_to_end_integration():

  9. Final Tips

  10. Run tests frequently during development to catch behavioral regressions
  11. Use descriptive test data like "production_catalog" instead of "test1"
  12. Test both success and failure paths for every major code branch
  13. Document current behavior even if it seems suboptimal (helps with future changes)
  14. Focus on what users see rather than how the code works internally

  This approach ensures comprehensive, maintainable tests that catch behavioral regressions while being resilient to internal implementation changes.
