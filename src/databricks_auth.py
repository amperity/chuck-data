"""Helper functions for Databricks authentication."""

import logging
import os

from src.clients.databricks import DatabricksAPIClient
from src.config import get_workspace_url, get_databricks_token as get_token_from_config


def get_databricks_token() -> str:
    """Return the Databricks token from config or environment."""
    token = get_token_from_config()
    if not token:
        token = os.getenv("DATABRICKS_TOKEN")
        if token:
            logging.info(
                "Using Databricks token from environment variable. Consider using /set-token to save it to config."
            )
    if not token:
        raise EnvironmentError("Databricks token not found in config or environment!")
    return token


def validate_databricks_token(token: str) -> bool:
    """Validate the provided Databricks token."""
    try:
        workspace_url = get_workspace_url()
        client = DatabricksAPIClient(workspace_url, token)
        return client.validate_token()
    except ValueError as e:
        logging.error("Token validation failed: %s", e)
        return False
    except ConnectionError as e:
        logging.error("Connection error during token validation: %s", e)
        raise ConnectionError(f"Connection Error: {e}")
