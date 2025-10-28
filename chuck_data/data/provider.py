"""Data Provider Protocol."""

from typing import Protocol, Optional, List, Dict, Any, runtime_checkable


@runtime_checkable
class DataProvider(Protocol):
    """Protocol that all data providers must implement.

    This protocol defines the standard interface for data source providers
    like Databricks, Snowflake, Redshift, Shopify, etc.
    """

    def validate_connection(self) -> bool:
        """Validate connection to the data provider.

        Returns:
            True if connection is valid, False otherwise
        """
        ...

    def list_catalogs(self, **kwargs) -> List[Dict[str, Any]]:
        """List all available catalogs.

        For providers without a catalog concept, this can return a
        single default catalog representing the data source.

        Returns:
            List of catalog dicts with at minimum a 'name' field
        """
        ...

    def get_catalog(self, name: str) -> Optional[Dict[str, Any]]:
        """Get catalog details by name.

        Args:
            name: Catalog name

        Returns:
            Catalog details dict or None if not found
        """
        ...

    def list_schemas(self, catalog: str, **kwargs) -> List[Dict[str, Any]]:
        """List schemas in a catalog.

        Args:
            catalog: Catalog name
            **kwargs: Provider-specific options

        Returns:
            List of schema dicts with at minimum 'name' and 'catalog_name' fields
        """
        ...

    def get_schema(self, catalog: str, schema: str) -> Optional[Dict[str, Any]]:
        """Get schema details.

        Args:
            catalog: Catalog name
            schema: Schema name

        Returns:
            Schema details dict or None if not found
        """
        ...

    def list_tables(self, catalog: str, schema: str, **kwargs) -> List[Dict[str, Any]]:
        """List tables in a schema.

        Args:
            catalog: Catalog name
            schema: Schema name
            **kwargs: Provider-specific options

        Returns:
            List of table dicts with at minimum 'name', 'schema_name',
            and 'catalog_name' fields
        """
        ...

    def get_table(
        self, catalog: str, schema: str, table: str
    ) -> Optional[Dict[str, Any]]:
        """Get table details.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name

        Returns:
            Table details dict or None if not found
        """
        ...

    def execute_query(self, query: str, **kwargs) -> Dict[str, Any]:
        """Execute a query and return results.

        Args:
            query: Query string (typically SQL)
            **kwargs: Provider-specific execution options

        Returns:
            Query results dict with provider-specific format

        Raises:
            NotImplementedError: If provider doesn't support query execution
        """
        ...

    def get_provider_name(self) -> str:
        """Return the provider name.

        Returns:
            Provider name (e.g., 'databricks', 'snowflake', 'shopify')
        """
        ...

    def get_capabilities(self) -> Dict[str, bool]:
        """Return provider capabilities.

        Common capabilities:
            - supports_sql: Can execute SQL queries
            - supports_catalogs: Has catalog concept
            - supports_schemas: Has schema concept
            - supports_tables: Has table concept
            - supports_volumes: Supports volumes/file storage
            - supports_models: Supports model serving
            - supports_jobs: Supports job submission
            - supports_warehouses: Has compute warehouses

        Returns:
            Dict mapping capability names to boolean values
        """
        ...
