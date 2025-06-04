# Chuck Data

[![Python Tests and Linting](https://github.com/amperity/clamp-internal/actions/workflows/python-tests.yml/badge.svg)](https://github.com/amperity/clamp-internal/actions/workflows/python-tests.yml)

A command-line and text-based user interface for managing Databricks resources including Unity Catalog, SQL warehouses, models, and volumes.

## Features

- Interactive CLI for managing Databricks resources
- Authentication with Databricks using personal access tokens
- List and select available LLM models from Databricks Model Serving
- Browse Unity Catalog resources (catalogs, schemas, tables)
- Manage SQL warehouses
- Profile database tables with automated PII detection (via LLMs)
- Support for Amperity operations

## Installation

`pip install chuck-data`

## Usage


`chuck-data` can be run from the command line or as a Python module.

## Available Commands

Chuck Data supports a command-based interface with both modern commands and slash commands. Commands can be used in either the modern CLI or legacy TUI interface.

### Authentication & Workspace
- `/login`, `/amperity-login` - Log in to Amperity
- `/databricks-login`, `/set-token` - Set Databricks API token
- `/logout` - Log out from Amperity or other authentication services
- `/workspace` - Select a workspace configuration

### Catalog & Schema Management
- `/catalogs` - List catalogs in Unity Catalog
- `/schemas` - List available schemas in Unity Catalog
- `/tables` - List available tables in Unity Catalog
- `/catalog` - Get information about a specific catalog
- `/schema` - Get information about a specific schema
- `/table` - Get specific table from Unity Catalog
- `/select-catalog` - Select a catalog for future operations
- `/select-schema` - Select a schema for future operations

### Model & Endpoint Management
- `/models` - List available models from Databricks API
- `/list-models` - List available models with filtering and detailed information
- `/model` - Set the active model

### SQL Warehouse Management
- `/warehouses` - List available SQL warehouses
- `/warehouse` - Set SQL warehouse ID
- `/select-warehouse` - Set the active SQL warehouse for database operations
- `/create-warehouse` - Create a new SQL warehouse in the Databricks workspace
- `/run-sql` - Execute SQL query against the active warehouse

### Volume Management
- `/list-volumes`, `/volumes` - List volumes in a Unity Catalog schema
- `/create-volume` - Create a new volume in Unity Catalog
- `/upload-file` - Upload a file to a Unity Catalog volume

### PII & Data Management
- `/scan-pii` - Scan a table for PII data using active model
- `/tag-pii` - Tag detected PII columns in a table
- `/setup-stitch` - Configure Stitch integration
- `/add-stitch-report` - Add a Stitch report to a Unity Catalog table

### Job Management
- `/jobs` - List available jobs in Databricks workspace
- `/job-status` - Check status of a specific job

### Utilities
- `/help` - Display help information about available commands
- `/status` - Show current connection status
- `/agent` - Interact with the AI agent
- `/bug` - Report a bug
- `/exit` - Exit the application

## Development

### Requirements

- Python 3.10 or higher
- [uv](https://github.com/astral-sh/uv) - Python package installer and resolver

Install the project with development dependencies:

```bash
uv pip install -e .[dev]
```

### Testing

Run the test suite:

```bash
uv run -m pytest
```

Run linters and static analysis:

```bash
uv run ruff .
uv run black --check --diff src tests
uv run pyright
```

For test coverage:

```bash
uv run -m pytest --cov=src
```

### CI/CD

This project uses GitHub Actions for continuous integration:

- Automated testing on Python 3.10
- Code linting with flake8
- Format checking with Black

The CI workflow runs on every push to `main` and on pull requests. You can also trigger it manually from the Actions tab in GitHub.
