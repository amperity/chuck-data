"""
Module for interacting with Databricks model serving endpoints.
"""

import logging


def list_models(client):
    """
    Fetch a list of models from the Databricks Serving API.

    Args:
        client: DatabricksAPIClient instance

    Returns:
        List of available model endpoints
    """
    try:
        return client.list_models()
    except ValueError as e:
        logging.error(f"Failed to list models: {e}")
        raise ValueError(f"Model serving API error: {e}")
    except ConnectionError as e:
        logging.error(f"Connection error when listing models: {e}")
        raise ConnectionError(f"Failed to connect to serving endpoint: {e}")


def get_model(client, model_name):
    """
    Get details of a specific model from Databricks Serving API.

    Args:
        client: DatabricksAPIClient instance
        model_name: Name of the model to retrieve

    Returns:
        Model details if found, None otherwise
    """
    try:
        return client.get_model(model_name)
    except ValueError as e:
        logging.error(f"Failed to get model: {e}")
        raise ValueError(f"Model serving API error: {e}")
    except ConnectionError as e:
        logging.error(f"Connection error when getting model: {e}")
        raise ConnectionError(f"Failed to connect to serving endpoint: {e}")
