"""Utility functions for working with data providers."""

from typing import Union, Optional
from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.clients.redshift import RedshiftAPIClient
from chuck_data.data_providers.adapters import (
    DatabricksProviderAdapter,
    RedshiftProviderAdapter,
)


def get_provider_name_from_client(
    client: Union[
        DatabricksAPIClient,
        RedshiftAPIClient,
        DatabricksProviderAdapter,
        RedshiftProviderAdapter,
        None,
    ],
) -> Optional[str]:
    """
    Get the provider name string from a client instance.

    Args:
        client: Either DatabricksAPIClient, RedshiftAPIClient, or their adapters/stubs/mocks

    Returns:
        Provider name string ("databricks" or "aws_redshift"), or None if client is None

    Examples:
        >>> client = DatabricksAPIClient(...)
        >>> get_provider_name_from_client(client)
        'databricks'

        >>> client = RedshiftAPIClient(...)
        >>> get_provider_name_from_client(client)
        'aws_redshift'
    """
    if client is None:
        return None

    # Check for adapters first
    if isinstance(client, DatabricksProviderAdapter):
        return "databricks"
    elif isinstance(client, RedshiftProviderAdapter):
        return "aws_redshift"

    # Check by isinstance for real clients
    if isinstance(client, DatabricksAPIClient):
        return "databricks"
    elif isinstance(client, RedshiftAPIClient):
        return "aws_redshift"

    # Check by class name (for stubs/mocks in tests)
    client_class_name = client.__class__.__name__
    if "Databricks" in client_class_name or "databricks" in client_class_name.lower():
        return "databricks"
    elif "Redshift" in client_class_name or "redshift" in client_class_name.lower():
        return "aws_redshift"

    return None


def is_redshift_client(
    client: Union[
        DatabricksAPIClient,
        RedshiftAPIClient,
        DatabricksProviderAdapter,
        RedshiftProviderAdapter,
        None,
    ],
) -> bool:
    """
    Check if the client is a Redshift client.

    Args:
        client: Client instance to check

    Returns:
        True if the client is a Redshift client, False otherwise

    Examples:
        >>> client = RedshiftAPIClient(...)
        >>> is_redshift_client(client)
        True

        >>> client = DatabricksAPIClient(...)
        >>> is_redshift_client(client)
        False
    """
    return get_provider_name_from_client(client) == "aws_redshift"


def is_databricks_client(
    client: Union[
        DatabricksAPIClient,
        RedshiftAPIClient,
        DatabricksProviderAdapter,
        RedshiftProviderAdapter,
        None,
    ],
) -> bool:
    """
    Check if the client is a Databricks client.

    Args:
        client: Client instance to check

    Returns:
        True if the client is a Databricks client, False otherwise

    Examples:
        >>> client = DatabricksAPIClient(...)
        >>> is_databricks_client(client)
        True

        >>> client = RedshiftAPIClient(...)
        >>> is_databricks_client(client)
        False
    """
    return get_provider_name_from_client(client) == "databricks"


def get_provider_adapter(
    client: Union[DatabricksAPIClient, RedshiftAPIClient, None],
) -> Union[DatabricksProviderAdapter, RedshiftProviderAdapter, None]:
    """
    Create a data provider adapter from a client instance.

    This is useful for commands that need to use the provider abstraction layer
    for operations that differ between providers.

    Args:
        client: Either DatabricksAPIClient, RedshiftAPIClient, or their stubs/mocks

    Returns:
        DataProvider adapter (DatabricksProviderAdapter or RedshiftProviderAdapter), or None if client is None

    Raises:
        ValueError: If client type is not supported

    Examples:
        >>> client = DatabricksAPIClient(...)
        >>> adapter = get_provider_adapter(client)
        >>> isinstance(adapter, DatabricksProviderAdapter)
        True
    """
    if client is None:
        return None

    # Check by isinstance first (for real clients)
    if isinstance(client, DatabricksAPIClient):
        adapter = DatabricksProviderAdapter.__new__(DatabricksProviderAdapter)
        adapter.client = client
        return adapter
    elif isinstance(client, RedshiftAPIClient):
        adapter = RedshiftProviderAdapter.__new__(RedshiftProviderAdapter)
        adapter.client = client
        return adapter

    # Check by class name (for stubs/mocks in tests)
    client_class_name = client.__class__.__name__
    if "Databricks" in client_class_name or "databricks" in client_class_name.lower():
        adapter = DatabricksProviderAdapter.__new__(DatabricksProviderAdapter)
        adapter.client = client
        return adapter
    elif "Redshift" in client_class_name or "redshift" in client_class_name.lower():
        adapter = RedshiftProviderAdapter.__new__(RedshiftProviderAdapter)
        adapter.client = client
        return adapter

    raise ValueError(f"Unsupported client type: {type(client)}")
