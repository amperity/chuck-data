"""Application-wide constants."""

# Default LLM models for setup wizard and UI
# These models are shown first and marked as "(default)" in the UI
DEFAULT_MODELS = [
    # Databricks default
    "databricks-claude-sonnet-4-5",
    # AWS Bedrock default (Amazon Nova Pro - AWS partner recommendation)
    "amazon.nova-pro-v1:0",
]
