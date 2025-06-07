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
Use the fixture `Da:tabricksClientStub` to stub only the external API calls, while using the real command logic.

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
