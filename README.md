[![chuck-banner](https://github.com/user-attachments/assets/abcd9545-e0aa-47a9-bf7f-041fe0c0bc0e)](https://chuckdata.ai)

# Chuck Data

Chuck is a text-based user interface (TUI) for managing Databricks resources including Unity Catalog, SQL warehouses, models, and volumes. Chuck Data provides an interactive shell environment for customer data engineering tasks with AI-powered assistance.

Check us out at [chuckdata.ai](https://chuckdata.ai).

Join our community on [Discord](https://discord.gg/f3UZwyuQqe).

## Features

- Interactive TUI for managing Databricks resources
- AI-powered "agentic" data engineering assistant
- Identity resolution powered by [Amperity's Stitch](https://docs.amperity.com/reference/stitch.html)
- Use LLMs from your Databricks account via Databricks Model Serving
- Browse Unity Catalog resources (catalogs, schemas, tables)
- Profile database tables with automated PII detection (via LLMs)
- Tag tables in Unity Catalog with semantic tags for PII to power compliance and data governance use cases
- Command-based interface with both natural language commands and slash commands

## Authentication
- Authenticates with Databricks using personal access tokens
- Authenticates with Amperity using API keys (/login and /logout commands)

## Installation

```bash
pip install chuck-data
```

## Usage

Chuck Data provides an interactive text-based user interface. Run the application using:

```bash
chuck 
```

Or run directly with Python:

```bash
python -m chuck_data 
```

## Available Commands

Chuck Data supports a command-based interface with slash commands that can be used within the interactive TUI. Type `/help` within the application to see all available commands.

### Some general commands to be aware of are:
- `/status` - Show current connection status and application context
- `/login`, `/logout` - Log in/out of Amperity, this is how Chuck interacts with Amperity to run Stitch
- `/list-models`, `/select-model <model_name>`  - Configure which LLM Chuck should use (Pick one designed for tools, we recommend databricks-claude-3-7-sonnet)
- `/list-warehouses`, `/select-warehouse <warehouse_name>` - Many Chuck tools run SQL so make sure to select a warehouse

Many of Chuck's tools will use your selected Catalog and Schema so that you don't have to constantly specify them. Use these commands to manage your application context.

### Catalog & Schema Management
- `/catalogs`, `/select-catalog <catalog_name>` - Manage Catalog context
- `/schemas`, `/select-schema <schema_name>` - Manage Schema context

## Known Limitations & Best Practices

### Known Limitations
- Unstructured data - Stitch will ignore fields in formats that are not supported
- GCP Support - Currently only AWS and Azure are formally supported, GCP will be added very soon
- Stitching across Catalogs - Technically if you manually create Stitch manifests it can work but Chuck doesn't automatically handle this well

### Best Practices
- Use models designed for tools, we recommend databricks-claude-3-7-sonnet but have also tested extensively with databricks-llama-3.2-7b-instruct
- Denormalized data models will work best with Stitch
- Sample data to try out Stitch is [available on the Databricks marketplace](https://marketplace.databricks.com/details/6bc4843f-3809-4995-8461-9756f6164ddf/Amperity_Amperitys-Identity-Resolution-Agent-30-Day-Trial). (Use the bronze schema PII datasets)

## Amperity Stitch

A key tool Chuck can use is Amperity's Stitch algorithm. This is a ML based identity resolution algorithm that has been refined with the world's biggest companies over the last decade.
- Stitch outputs two tables in a schema called `stitch_outputs`. `unified_coalesced` is a table of standardized PII with Amperity IDs. `unified_scores` are the "edges" of the graph that have links and confidence scores for each match.
- Stitch will create a new notebook in your workspace each time it runs that you can use to understand the results, be sure to check it out!
- For a detailed breakdown of how Stitch works, [see this great article breaking it down step by step](https://docs.amperity.com/reference/stitch.html)

## Support

Chuck is a research preview application that is actively being improved based on your usage and feedback. Always be sure to update to the latest version of Chuck to get the best experience!

### Support Options

1. **GitHub Issues**  
   Report bugs or request features on our GitHub repository:  
   https://github.com/amperity/chuck-data/issues

2. **Discord Community**  
   Join our community to chat with other users and developers:  
   https://discord.gg/f3UZwyuQqe  
   Or run `/discord` in the application

3. **Email Support**  
   Contact our dedicated support team:  
   chuck-support@amperity.com

4. **In-app Bug Reports**  
   Let Chuck submit a bug report automatically with the `/bug` command

## Development

### Requirements

- Python 3.10 or higher
- [uv](https://github.com/astral-sh/uv) - Python package installer and resolver (technically this is not required but it sure makes life easier)

### Project Structure

```
chuck_data/             # Main package
├── __init__.py
├── __main__.py         # CLI entry point
├── commands/           # Command implementations
├── ui/                 # User interface components
├── agent/              # AI agent functionality
├── clients/            # External service clients
├── databricks/         # Databricks utilities
└── ...                 # Other modules
```

### Installation

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
uv run black --check --diff chuck_data tests
uv run ruff check
uv run pyright
```

For test coverage:

```bash
uv run -m pytest --cov=chuck_data
```

### CI/CD

This project uses GitHub Actions for continuous integration:

- Automated testing on Python 3.10
- Code linting with flake8
- Format checking with Black

The CI workflow runs on every push to `main` and on pull requests. You can also trigger it manually from the Actions tab in GitHub.
