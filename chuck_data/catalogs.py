"""
Module for interacting with Databricks Unity Catalog catalogs, schemas, and tables.
"""


def list_catalogs(client, include_browse=False, max_results=None, page_token=None):
    """
    Gets an array of catalogs in the metastore.

    Args:
        client: DatabricksAPIClient instance
        include_browse: Whether to include catalogs for which the principal can only access selective metadata
        max_results: Maximum number of catalogs to return (optional)
        page_token: Opaque pagination token to go to next page (optional)

    Returns:
        Dictionary containing:
        - catalogs: List of catalogs
        - next_page_token: Token for retrieving the next page (if available)
    """
    return client.list_catalogs(include_browse, max_results, page_token)


def get_catalog(client, catalog_name):
    """
    Gets a catalog from Unity Catalog.

    Args:
        client: DatabricksAPIClient instance
        catalog_name: Name of the catalog

    Returns:
        Catalog information
    """
    return client.get_catalog(catalog_name)


def list_schemas(
    client, catalog_name, include_browse=False, max_results=None, page_token=None
):
    """
    Gets an array of schemas for a catalog in the metastore.

    Args:
        client: DatabricksAPIClient instance
        catalog_name: Parent catalog for schemas of interest (required)
        include_browse: Whether to include schemas for which the principal can only access selective metadata
        max_results: Maximum number of schemas to return (optional)
        page_token: Opaque pagination token to go to next page (optional)

    Returns:
        Dictionary containing:
        - schemas: List of schemas
        - next_page_token: Token for retrieving the next page (if available)
    """
    return client.list_schemas(catalog_name, include_browse, max_results, page_token)


def get_schema(client, full_name):
    """
    Gets a schema from Unity Catalog.

    Args:
        client: DatabricksAPIClient instance
        full_name: Full name of the schema in the format 'catalog_name.schema_name'

    Returns:
        Schema information
    """
    return client.get_schema(full_name)


def list_volumes(
    client,
    catalog_name,
    schema_name,
    max_results=None,
    page_token=None,
    include_browse=False,
):
    """
    Gets an array of volumes for the current metastore under the parent catalog and schema.
    Args:
        client: DatabricksAPIClient instance
        catalog_name: Name of parent catalog (required)
        schema_name: Name of parent schema (required)
        max_results: Maximum number of volumes to return (optional)
        page_token: Opaque token for pagination (optional)
        include_browse: Whether to include volumes with selective metadata access (optional)
    Returns:
        Dictionary containing:
        - volumes: List of volumes
        - next_page_token: Token for retrieving the next page (if available)
    """
    return client.list_volumes(
        catalog_name, schema_name, max_results, page_token, include_browse
    )


def list_tables(
    client,
    catalog_name,
    schema_name,
    max_results=None,
    page_token=None,
    include_delta_metadata=False,
    omit_columns=False,
    omit_properties=False,
    omit_username=False,
    include_browse=False,
    include_manifest_capabilities=False,
):
    """
    Gets an array of all tables for the current metastore under the parent catalog and schema.

    Args:
        client: DatabricksAPIClient instance
        catalog_name: Name of parent catalog for tables of interest (required)
        schema_name: Parent schema of tables (required)
        max_results: Maximum number of tables to return (optional)
        page_token: Opaque token to send for the next page of results (optional)
        include_delta_metadata: Whether delta metadata should be included (optional)
        omit_columns: Whether to omit columns from the response (optional)
        omit_properties: Whether to omit properties from the response (optional)
        omit_username: Whether to omit username from the response (optional)
        include_browse: Whether to include tables with selective metadata access (optional)
        include_manifest_capabilities: Whether to include table capabilities (optional)

    Returns:
        Dictionary containing:
        - tables: List of tables
        - next_page_token: Token for retrieving the next page (if available)
    """
    return client.list_tables(
        catalog_name,
        schema_name,
        max_results,
        page_token,
        include_delta_metadata,
        omit_columns,
        omit_properties,
        omit_username,
        include_browse,
        include_manifest_capabilities,
    )


def get_table(
    client,
    full_name,
    include_delta_metadata=False,
    include_browse=False,
    include_manifest_capabilities=False,
):
    """
    Gets a table from the metastore for a specific catalog and schema.

    Args:
        client: DatabricksAPIClient instance
        full_name: Full name of the table in format 'catalog_name.schema_name.table_name'
        include_delta_metadata: Whether delta metadata should be included (optional)
        include_browse: Whether to include tables with selective metadata access (optional)
        include_manifest_capabilities: Whether to include table capabilities (optional)

    Returns:
        Table information
    """
    return client.get_table(
        full_name, include_delta_metadata, include_browse, include_manifest_capabilities
    )
