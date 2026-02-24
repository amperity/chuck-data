[![chuck-banner](https://github.com/user-attachments/assets/abcd9545-e0aa-47a9-bf7f-041fe0c0bc0e)](https://chuckdata.ai)

# Chuck Data

Chuck is a text-based user interface (TUI) for data engineering across cloud platforms. It provides an interactive shell environment with AI-powered assistance for managing data infrastructure on **Databricks** and **AWS** (Redshift + EMR), with identity resolution powered by Amperity Stitch.

Check us out at [chuckdata.ai](https://chuckdata.ai).

Join our community on [Discord](https://discord.gg/f3UZwyuQqe).

## Features

- Interactive TUI for data engineering — type `/commands` or ask in plain English
- AI-powered "agentic" assistant — ask questions or issue instructions in plain English
- Identity resolution powered by [Amperity's Stitch](https://docs.amperity.com/reference/stitch.html)
- Profile tables with automated PII detection and semantic tagging (via LLMs)
- Multiple LLM providers — Databricks Model Serving, AWS Bedrock, and more

### Databricks

- Browse and manage Unity Catalog (catalogs, schemas, tables, volumes)
- SQL warehouse management and query execution
- Tag Unity Catalog columns with PII semantic tags for compliance and data governance
- Databricks job monitoring

### AWS

- AWS Redshift as a data source — browse schemas and run Stitch on Redshift data
- AWS EMR as a compute platform for running Stitch jobs
- AWS Bedrock for LLM inference (Claude, Nova, Llama)

## Authentication

- **Databricks** — personal access token (`/set-token`)
- **AWS** — standard AWS credential chain (SSO, environment variables, `~/.aws/credentials`, or IAM role)
- **Amperity** — browser-based OAuth (`/login` and `/logout`)

## LLM Provider Support

Chuck supports multiple LLM providers, allowing you to choose the best option for your use case:

### Supported Providers

- **Databricks** (default) — Use LLMs from your Databricks account via Model Serving
- **AWS Bedrock** — Use AWS Bedrock foundation models (Claude, Llama, Nova, and more)
- **OpenAI** — Direct OpenAI API integration (coming soon)
- **Anthropic** — Direct Anthropic API integration (coming soon)

### AWS Bedrock Setup

To use AWS Bedrock as your LLM provider:

1. **Install AWS dependencies:**
   ```bash
   pip install chuck-data[aws]
   ```

2. **Configure AWS credentials:**

   **Option 1: AWS SSO (Recommended for enterprise)**
   ```bash
   # Login via SSO
   aws sso login --profile your-profile

   # Set profile for session
   export AWS_PROFILE=your-profile
   export AWS_REGION=us-east-1
   ```

   **Option 2: Environment variables**
   ```bash
   export AWS_REGION=us-east-1
   export AWS_ACCESS_KEY_ID=your-access-key
   export AWS_SECRET_ACCESS_KEY=your-secret-key
   ```

   **Option 3: AWS CLI configuration** (`~/.aws/credentials`)
   ```ini
   [default]
   aws_access_key_id = your-access-key
   aws_secret_access_key = your-secret-key
   region = us-east-1
   ```

   **Option 4: IAM role** (for EC2/ECS/Lambda deployments)

3. **Set LLM provider:**

   Via environment variable:
   ```bash
   export CHUCK_LLM_PROVIDER=aws_bedrock
   chuck
   ```

   Or via config file (`~/.chuck_config.json`):
   ```json
   {
     "llm_provider": "aws_bedrock",
     "active_model": "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
     "llm_provider_config": {
       "aws_bedrock": {
         "region": "us-east-1"
       }
     }
   }
   ```

4. **Request model access in AWS Bedrock console:**

   Some models require explicit approval before use. Visit the [AWS Bedrock console](https://console.aws.amazon.com/bedrock/) and request access to your desired models.

Use `/list-models` within Chuck to see all available models in your AWS account.

#### Supported AWS Bedrock Models

**Anthropic Claude** (recommended for tool calling):
- `us.anthropic.claude-sonnet-4-5-20250929-v1:0` — Claude Sonnet 4.5 (recommended)
- `us.anthropic.claude-sonnet-4-20250514-v1:0` — Claude Sonnet 4
- `anthropic.claude-3-5-sonnet-20240620-v1:0` — Claude 3.5 Sonnet
- `anthropic.claude-3-haiku-20240307-v1:0` — Claude 3 Haiku

**Amazon Nova**:
- `amazon.nova-pro-v1:0` — Nova Pro (default, most capable)
- `amazon.nova-lite-v1:0` — Nova Lite
- `amazon.nova-micro-v1:0` — Nova Micro

**Meta Llama**:
- `us.meta.llama4-scout-17b-instruct-v1:0` — Llama 4 Scout (cross-region)
- `us.meta.llama4-maverick-17b-instruct-v1:0` — Llama 4 Maverick (cross-region)
- `meta.llama3-1-70b-instruct-v1:0` — Llama 3.1 70B

Cross-region inference profiles (prefixed with `us.`, `eu.`, or `global.`) automatically route requests across multiple AWS regions for better throughput and resilience. Newer models like Claude 4 and 4.5 require inference profiles.

### Provider Selection Priority

Chuck resolves the LLM provider in this order:
1. `CHUCK_LLM_PROVIDER` environment variable (highest priority)
2. `llm_provider` in config file
3. Default: `databricks`

## Installation

### Homebrew (Recommended)

```bash
brew tap amperity/chuck-data
brew install chuck-data
```

### pip

```bash
pip install chuck-data
```

### AWS support (Redshift, EMR, Bedrock)

```bash
pip install chuck-data[aws]
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

### CLI Options

```
chuck [--version] [--no-color]

Options:
  --version    Show program version and exit
  --no-color   Disable color output (also respects NO_COLOR env var)
```

## Available Commands

Chuck Data supports a command-based interface with slash commands that can be used within the interactive TUI. Type `/help` within the application to see all available commands. You can also ask questions or issue instructions in natural language and the AI agent will execute the appropriate commands for you.

### Getting Started

- `/help` — Show all available commands
- `/getting-started` — Display the getting-started guide
- `/status` — Show current connection status and application context
- `/setup` — Run the interactive initial setup wizard

### Authentication

- `/login` — Log in to Amperity (browser-based OAuth)
- `/logout` — Log out of Amperity, Databricks, or all services
- `/set-token <token>` — Set or update your Databricks personal access token

### Models

- `/list-models` (alias: `/models`) — List available LLM models
- `/select-model <model_name>` — Set the active LLM model

### Warehouses (Databricks)

- `/list-warehouses` (alias: `/warehouses`) — List SQL warehouses
- `/select-warehouse <warehouse_name>` — Set the active warehouse for query execution
- `/create-warehouse <name>` — Create a new SQL warehouse

### Catalog & Schema Management (Databricks)

- `/catalogs` — List Unity Catalog catalogs
- `/select-catalog <catalog_name>` — Set the active catalog context
- `/schemas` — List schemas in the active catalog
- `/select-schema <schema_name>` — Set the active schema context
- `/list-tables` — List tables in the active catalog/schema
- `/table <table_name>` — Show detailed schema and metadata for a table

### Databases & Schemas (AWS Redshift)

- `/list-databases` — List databases in Redshift
- `/select-database <database_name>` — Set the active Redshift database
- `/list-redshift-schemas` — List schemas in the active Redshift database
- `/select-redshift-schema <schema_name>` — Set the active Redshift schema
- `/redshift-status` — Show Redshift connection status and configuration

### Data Operations

- `/run-sql <query>` — Execute a SQL query on the active warehouse or Redshift cluster
- `/volumes` — List volumes in the active catalog
- `/create-volume <name>` — Create a new volume
- `/upload-file` — Upload a file to a volume

### PII Detection & Tagging

- `/scan-pii [table_name]` — Scan a table for PII columns using an LLM
- `/tag-pii [columns]` — Tag specific columns with PII semantic tags in Unity Catalog
- `/bulk-tag-pii` — Scan all tables in the active schema for PII

### Stitch Integration (Identity Resolution)

- `/setup-stitch` — Run the interactive setup wizard for Amperity Stitch
- `/stitch-status` — Check the status of a running Stitch job
- `/add-stitch-report` — Add an analysis report to Stitch output

### Jobs & Monitoring

- `/jobs` — List jobs
- `/job-status <job_id>` — Get the status of a specific job
- `/list-jobs` — List recent job runs
- `/monitor-job <job_id>` — Monitor a job in real time

### Support & Community

- `/bug` — Submit a bug report automatically with context
- `/discord` — Open the Discord community link
- `/support` — Display support options

## AWS Redshift Support

Chuck supports AWS Redshift as a data source, enabling you to run Stitch identity resolution on data stored in Redshift.

### Setup

1. **Install AWS dependencies:**
   ```bash
   pip install chuck-data[aws]
   ```

2. **Configure your Redshift connection** in `~/.chuck_config.json` or via environment variables:
   ```json
   {
     "data_provider": "aws_redshift",
     "aws_region": "us-east-1",
     "aws_profile": "your-profile",
     "aws_account_id": "123456789012",
     "redshift_workgroup_name": "my-workgroup",
     "s3_bucket": "my-stitch-artifacts-bucket"
   }
   ```
   For provisioned clusters, use `redshift_cluster_identifier` instead of `redshift_workgroup_name`.

3. **Check your connection:**
   ```
   chuck > /redshift-status
   ```

### Compute Options

When using Redshift as a data source, you can choose your Stitch compute platform:

- **`databricks`** (default) — Run Stitch jobs on your Databricks cluster
- **`emr`** — Run Stitch jobs on AWS EMR

Set the compute provider in the config:
```json
{
  "compute_provider": "emr"
}
```

## Known Limitations & Best Practices

### Known Limitations
- Unstructured data — Stitch will ignore fields in formats that are not supported
- GCP Support — Currently only AWS and Azure are formally supported; GCP will be added very soon
- Stitching across Catalogs — Technically supported if you manually create Stitch manifests, but Chuck doesn't automatically handle this well

### Best Practices
- Use models designed for tool calling. For Databricks, `databricks-claude-sonnet-4-5` is recommended. For AWS Bedrock, `us.anthropic.claude-sonnet-4-5-20250929-v1:0` is recommended
- Denormalized data models work best with Stitch
- Sample data to try out Stitch is [available on the Databricks Marketplace](https://marketplace.databricks.com/details/6bc4843f-3809-4995-8461-9756f6164ddf/Amperity_Amperitys-Identity-Resolution-Agent-30-Day-Trial) (use the bronze schema PII datasets)

## Amperity Stitch

A key tool Chuck can use is Amperity's Stitch algorithm. This is a ML-based identity resolution algorithm that has been refined with the world's biggest companies over the last decade.
- Stitch outputs two tables in a schema called `stitch_outputs`. `unified_coalesced` is a table of standardized PII with Amperity IDs. `unified_scores` are the "edges" of the graph that have links and confidence scores for each match.
- Stitch will create a new notebook in your workspace each time it runs that you can use to understand the results — be sure to check it out!
- For a detailed breakdown of how Stitch works, [see this great article breaking it down step by step](https://docs.amperity.com/reference/stitch.html)
- Stitch can now run on data from both Databricks Unity Catalog and AWS Redshift, with compute on either Databricks or EMR.

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
   chuck-team@amperity.com

4. **In-app Bug Reports**
   Let Chuck submit a bug report automatically with the `/bug` command

## Development

### Requirements

- Python 3.10 or higher
- [uv](https://github.com/astral-sh/uv) — Python package installer and resolver (recommended but not required)

### Project Structure

```
chuck_data/                   # Main package
├── __main__.py               # CLI entry point
├── version.py                # Version tracking
├── config.py                 # Configuration management (Pydantic)
├── service.py                # Service facade
├── command_registry.py       # Unified command registry
├── constants.py              # Application constants
│
├── commands/                 # 50+ command handlers
│   ├── auth.py               # Authentication (Amperity, Databricks)
│   ├── list_catalogs.py      # Catalog/schema/table commands
│   ├── list_databases.py     # Redshift database commands
│   ├── scan_pii.py           # PII detection
│   ├── tag_pii.py            # PII tagging
│   ├── bulk_tag_pii.py       # Bulk PII scanning
│   ├── setup_stitch.py       # Stitch setup wizard
│   ├── run_sql.py            # SQL execution
│   ├── job_status.py         # Job monitoring
│   └── ...                   # Other commands
│
├── agent/                    # AI agent system
│   ├── manager.py            # Agent orchestrator (multi-turn conversations)
│   ├── tool_executor.py      # LLM tool call execution engine
│   └── prompts/              # System prompts for different agent modes
│
├── llm/                      # LLM provider abstraction layer
│   ├── provider.py           # Provider protocol definition
│   ├── factory.py            # Provider factory (selects provider at runtime)
│   └── providers/
│       ├── databricks.py     # Databricks Model Serving provider
│       └── aws_bedrock.py    # AWS Bedrock provider (Converse API)
│
├── clients/                  # External API clients
│   ├── databricks.py         # Databricks SDK client
│   ├── amperity.py           # Amperity OAuth client
│   ├── redshift.py           # AWS Redshift client
│   └── emr.py                # AWS EMR client
│
├── data_providers/           # Data source abstraction
│   ├── provider.py           # Provider protocol
│   └── factory.py            # Factory (selects databricks or aws_redshift)
│
├── compute_providers/        # Compute platform abstraction
│   ├── provider.py           # Provider protocol
│   ├── databricks.py         # Databricks compute
│   ├── emr.py                # EMR compute
│   └── factory.py            # Factory (selects databricks or emr)
│
├── storage/                  # Stitch artifact storage
│   └── manifest.py           # Manifest generation and S3/volume upload
│
├── ui/                       # User interface
│   ├── tui.py                # Main TUI loop
│   ├── theme.py              # Color/styling
│   ├── table_formatter.py    # Result tables
│   └── help_formatter.py     # Help text generation
│
└── databricks/               # Databricks utilities
    ├── url_utils.py          # URL normalization
    └── permission_validator.py  # Permission checks
```

### Installation for Development

Install the project with development dependencies:

```bash
uv pip install -e .[dev]
```

### Testing

Run all unit tests:

```bash
uv run pytest
```

Run specific test categories:

```bash
uv run pytest tests/unit/                  # Unit tests only
uv run pytest -m integration               # Integration tests (requires live cloud access)
uv run pytest -m data_test                 # Tests that create cloud resources
uv run pytest -m e2e                       # End-to-end tests (slow, comprehensive)
```

Run a single test:

```bash
uv run pytest tests/unit/core/test_config.py::TestPydanticConfig::test_config_update
```

For test coverage:

```bash
uv run pytest --cov=chuck_data
```

### Linting and Formatting

```bash
uv run ruff check           # Lint check
uv run ruff check --fix     # Auto-fix linting issues
uv run black chuck_data tests  # Format code
uv run pyright              # Type checking
```

### CI/CD

This project uses GitHub Actions for continuous integration:

- Automated testing on Python 3.10
- Code linting with Ruff
- Format checking with Black
- Type checking with Pyright

The CI workflow runs on every push to `main` and on pull requests. You can also trigger it manually from the Actions tab in GitHub.
