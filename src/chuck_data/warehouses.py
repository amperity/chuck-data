"""
Module for interacting with Databricks SQL warehouses.
"""


def list_warehouses(client):
    """
    Lists all SQL warehouses in the Databricks workspace.

    Args:
        client: DatabricksAPIClient instance

    Returns:
        List of warehouses
    """
    return client.list_warehouses()


def get_warehouse(client, warehouse_id):
    """
    Gets information about a specific SQL warehouse.

    Args:
        client: DatabricksAPIClient instance
        warehouse_id: ID of the SQL warehouse

    Returns:
        Warehouse information
    """
    return client.get_warehouse(warehouse_id)


def create_warehouse(client, opts):
    """
    Creates a new SQL warehouse.

    Args:
        client: DatabricksAPIClient instance
        opts: Dictionary containing warehouse configuration options

    Returns:
        Created warehouse information
    """
    return client.create_warehouse(opts)
