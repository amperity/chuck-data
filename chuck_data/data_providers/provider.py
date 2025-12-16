"""Data Provider Protocol."""

from typing import Protocol, List, Dict, Optional, Any


class DataProvider(Protocol):
    """Protocol that all data providers must implement.

    Data providers enable browsing and querying data tables
    from different platforms (Databricks, AWS Redshift, etc.)
    """

    def validate_connection(self) -> bool:
        """Validate the connection to the data provider.

        Returns:
            True if connection is valid, False otherwise
        """
        ...

    def list_databases(self) -> List[str]:
        """List available catalogs/databases.

        For Databricks: Unity Catalog catalogs
        For Redshift: Databases

        Returns:
            List of catalog/database names
        """
        ...

    def list_schemas(self, catalog: Optional[str] = None) -> List[str]:
        """List schemas in a catalog.

        Args:
            catalog: Catalog/database name (uses default if not specified)

        Returns:
            List of schema names
        """
        ...

    def list_tables(
        self, catalog: Optional[str] = None, schema: Optional[str] = None, **kwargs: Any
    ) -> List[Dict]:
        """List tables in a schema.

        Args:
            catalog: Catalog/database name (uses default if not specified)
            schema: Schema name (optional, may filter by pattern)
            **kwargs: Provider-specific filters (e.g., table_pattern)

        Returns:
            List of dictionaries containing table metadata
        """
        ...

    def get_table(
        self,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> Dict:
        """Get table schema/metadata.

        Args:
            catalog: Catalog/database name (uses default if not specified)
            schema: Schema name
            table: Table name

        Returns:
            Dictionary containing table metadata and column information
        """
        ...

    def execute_query(
        self, query: str, catalog: Optional[str] = None, **kwargs: Any
    ) -> Dict:
        """Execute SQL query.

        Args:
            query: SQL query to execute
            catalog: Catalog/database name (uses default if not specified)
            **kwargs: Provider-specific options (e.g., wait=True)

        Returns:
            Dictionary containing query results
        """
        ...

    def tag_columns(
        self,
        tags: List[Dict[str, str]],
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict:
        """Apply semantic tags to columns.

        For Databricks: Uses ALTER TABLE ... SET TAGS SQL statements
        For Redshift: Stores tags in chuck_metadata.semantic_tags table
        (Redshift doesn't support native column tags)

        Args:
            tags: List of tag dictionaries with keys:
                - table: Table name
                - column: Column name
                - semantic_type: Semantic type (e.g., 'pii/email', 'pii/phone')
            catalog: Catalog/database name (uses default if not specified)
            schema: Schema name (optional for Databricks, required for Redshift)
            **kwargs: Provider-specific options (e.g., warehouse_id for Databricks)

        Returns:
            Dictionary containing results:
                - success: bool
                - tags_applied: int (number of tags successfully applied)
                - errors: List[Dict] (any errors that occurred)
        """
        ...
