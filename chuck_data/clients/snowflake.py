"""
Reusable Snowflake API client for authentication and metadata operations.

Uses the Snowflake Python connector (snowflake-connector-python) to provide
interactive browsing capabilities parallel to DatabricksAPIClient and
RedshiftAPIClient.

Supports both password and key-pair (RSA) authentication.
"""

import logging
import time
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class SnowflakeAPIClient:
    """Reusable Snowflake API client for authentication and metadata operations."""

    def __init__(
        self,
        account: str,
        user: str,
        database: str = "SNOWFLAKE",
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        password: Optional[str] = None,
        private_key_path: Optional[str] = None,
    ):
        """
        Initialize the Snowflake API client.

        Args:
            account: Snowflake account identifier (e.g. 'myorg-myaccount')
            user: Snowflake user login name
            database: Default database name
            schema: Default schema name (optional)
            warehouse: Default virtual warehouse (optional but strongly recommended)
            role: Snowflake role to use (optional, uses user's default role)
            password: Plain-text password (mutually exclusive with private_key_path)
            private_key_path: Path to an unencrypted RSA private key PEM file
                              (mutually exclusive with password)

        Note:
            Either password or private_key_path must be provided.
        """
        if not password and not private_key_path:
            raise ValueError(
                "Either password or private_key_path must be provided for Snowflake authentication"
            )

        self.account = account
        self.user = user
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.role = role
        self._password = password
        self._private_key_path = private_key_path

        # Lazy connection — created on first use
        self._conn = None

    def close(self):
        """Explicitly close the Snowflake connection.

        Call this before process exit so the Snowflake connector's atexit
        handler sees an already-closed connection and skips the network
        teardown call — preventing a noisy KeyboardInterrupt traceback
        on Ctrl+C.
        """
        if self._conn is not None:
            try:
                if not self._conn.is_closed():
                    self._conn.close()
            except Exception:
                pass
            finally:
                self._conn = None

    def __del__(self):
        self.close()

    def _get_private_key_bytes(self):
        """Read and return the RSA private key bytes from the PEM file."""
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.backends import default_backend

        with open(self._private_key_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend(),
            )
        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    def _connect(self):
        """Create a new Snowflake connector connection."""
        import atexit
        import snowflake.connector

        connect_params: Dict[str, Any] = {
            "account": self.account,
            "user": self.user,
            "database": self.database,
        }
        if self.schema:
            connect_params["schema"] = self.schema
        if self.warehouse:
            connect_params["warehouse"] = self.warehouse
        if self.role:
            connect_params["role"] = self.role

        if self._password:
            connect_params["password"] = self._password
        else:
            connect_params["private_key"] = self._get_private_key_bytes()

        conn = snowflake.connector.connect(**connect_params)

        # The Snowflake connector registers a _close_at_exit atexit handler that
        # sends a DELETE request to terminate the session. When Ctrl+C is pressed,
        # a KeyboardInterrupt fires inside that network call and produces a noisy
        # traceback. Replace it with a silent handler that swallows all exceptions.
        try:
            atexit.unregister(conn._close_at_exit)

            def _silent_close(c=conn):
                try:
                    if not c.is_closed():
                        c.close()
                except BaseException:
                    pass

            atexit.register(_silent_close)
        except Exception:
            pass

        return conn

    def _get_connection(self):
        """Return the current connection, creating one if it doesn't exist or is closed."""
        if self._conn is None or self._conn.is_closed():
            self._conn = self._connect()
        return self._conn

    def _execute(self, sql: str, database: Optional[str] = None) -> List[Dict]:
        """Execute SQL and return results as a list of dicts."""
        conn = self._get_connection()
        effective_sql = sql
        if database:
            effective_sql = f"USE DATABASE {database}; {sql}"
        try:
            cursor = conn.cursor(snowflake.connector.DictCursor)
            cursor.execute(sql)
            return cursor.fetchall()
        except Exception as e:
            logger.debug(f"Snowflake SQL error: {e}")
            raise

    #
    # Connection validation
    #

    def validate_connection(self) -> bool:
        """Validate connection by attempting to list databases.

        Returns:
            True if connection is valid, False otherwise
        """
        try:
            self._get_connection()
            return True
        except Exception as e:
            logger.debug(f"Snowflake connection validation failed: {e}")
            return False

    #
    # SQL execution
    #

    def execute_sql(
        self, sql: str, database: Optional[str] = None, wait: bool = True
    ) -> Dict:
        """
        Execute SQL using the Snowflake connector.

        Args:
            sql: SQL statement to execute
            database: Database context to USE before executing (optional)
            wait: Accepted for API parity with RedshiftAPIClient; Snowflake connector
                  is synchronous so this parameter has no effect.

        Returns:
            Dictionary with 'statement_id', 'status', and 'result' keys.
            'result' contains {'Records': [...rows as dicts...]} or None for DDL.

        Raises:
            ValueError: If execution fails
        """
        import snowflake.connector

        conn = self._get_connection()
        try:
            with conn.cursor(snowflake.connector.DictCursor) as cursor:
                if database:
                    cursor.execute(f"USE DATABASE {database}")
                cursor.execute(sql)
                rows = cursor.fetchall()
                return {
                    "statement_id": str(cursor.sfqid),
                    "status": "FINISHED",
                    "result": {"Records": rows} if rows else None,
                }
        except Exception as e:
            logger.debug(f"Snowflake SQL execution error: {e}")
            raise ValueError(f"Snowflake SQL execution failed: {e}")

    #
    # Database / Schema / Table metadata
    #

    def list_databases(self, database: Optional[str] = None) -> Dict:
        """
        List Snowflake databases visible to the current user.

        Returns:
            Dictionary in format: {"databases": [{"name": "db1"}, ...]}
        """
        try:
            import snowflake.connector

            conn = self._get_connection()
            with conn.cursor(snowflake.connector.DictCursor) as cursor:
                cursor.execute("SHOW DATABASES")
                rows = cursor.fetchall()
            return {"databases": [{"name": row["name"]} for row in rows]}
        except Exception as e:
            logger.debug(f"Error listing Snowflake databases: {e}")
            raise ValueError(f"Error listing databases: {e}")

    def list_schemas(self, database: Optional[str] = None) -> Dict:
        """
        List schemas in a database.

        Args:
            database: Database name (uses default if not specified)

        Returns:
            Dictionary in format: {"schemas": [{"name": "schema1"}, ...]}
        """
        import snowflake.connector

        db = database or self.database
        try:
            conn = self._get_connection()
            with conn.cursor(snowflake.connector.DictCursor) as cursor:
                cursor.execute(f"SHOW SCHEMAS IN DATABASE {db}")
                rows = cursor.fetchall()
            return {"schemas": [{"name": row["name"]} for row in rows]}
        except Exception as e:
            logger.debug(f"Error listing Snowflake schemas: {e}")
            raise ValueError(f"Error listing schemas in {db}: {e}")

    def list_tables(
        self,
        database: Optional[str] = None,
        schema_pattern: Optional[str] = None,
        table_pattern: Optional[str] = None,
        omit_columns: bool = False,
    ) -> Dict[str, List[Dict]]:
        """
        List tables in a schema (or all schemas in a database).

        Args:
            database: Database name (uses default if not specified)
            schema_pattern: Schema name to list tables for (exact match)
            table_pattern: Accepted for API parity; not used by Snowflake SHOW TABLES
            omit_columns: Accepted for API parity; Snowflake SHOW TABLES doesn't include columns

        Returns:
            Dictionary with "tables" key containing list of table metadata dicts
        """
        import snowflake.connector

        db = database or self.database
        try:
            conn = self._get_connection()
            with conn.cursor(snowflake.connector.DictCursor) as cursor:
                if schema_pattern:
                    cursor.execute(f"SHOW TABLES IN SCHEMA {db}.{schema_pattern}")
                else:
                    cursor.execute(f"SHOW TABLES IN DATABASE {db}")
                rows = cursor.fetchall()
            return {"tables": rows}
        except Exception as e:
            logger.debug(f"Error listing Snowflake tables: {e}")
            raise ValueError(f"Error listing tables in {db}: {e}")

    def describe_table(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> Dict:
        """
        Get table column metadata.

        Args:
            database: Database name (uses default if not specified)
            schema: Schema name (required)
            table: Table name (required)

        Returns:
            Dictionary with column information
        """
        if not schema or not table:
            raise ValueError("Both schema and table must be specified")

        import snowflake.connector

        db = database or self.database
        try:
            conn = self._get_connection()
            with conn.cursor(snowflake.connector.DictCursor) as cursor:
                cursor.execute(f"DESCRIBE TABLE {db}.{schema}.{table}")
                columns = cursor.fetchall()
            return {
                "database": db,
                "schema": schema,
                "table": table,
                "columns": columns,
            }
        except Exception as e:
            logger.debug(f"Error describing Snowflake table: {e}")
            raise ValueError(f"Error describing {db}.{schema}.{table}: {e}")

    #
    # Semantic tag metadata (metadata table workaround — Snowflake lacks Spark-visible column tags)
    #

    def read_semantic_tags(self, database: str, schema_name: str) -> Dict[str, Any]:
        """Read semantic tags from chuck_metadata.semantic_tags table.

        Args:
            database: Database name
            schema_name: Schema name

        Returns:
            Dict with 'success': bool, 'tags': list of dicts or 'error': str
        """
        try:
            sql = f"""
            SELECT table_name, column_name, semantic_type
            FROM chuck_metadata.semantic_tags
            WHERE database_name = '{database}'
            AND schema_name = '{schema_name}'
            ORDER BY table_name, column_name
            """
            result = self.execute_sql(sql, database=database)
            rows = (
                result.get("result", {}).get("Records", [])
                if result.get("result")
                else []
            )
            tags = [
                {
                    "table": row["table_name"],
                    "column": row["column_name"],
                    "semantic": row["semantic_type"],
                }
                for row in rows
            ]
            return {"success": True, "tags": tags}
        except Exception as e:
            logger.error(f"Error reading Snowflake semantic tags: {e}", exc_info=True)
            return {
                "success": False,
                "error": f"Failed to read semantic tags: {str(e)}",
            }

    def read_table_schemas(
        self, database: str, schema_name: str, semantic_tags: list
    ) -> Dict[str, Any]:
        """Read table schemas from Snowflake for tables that have semantic tags.

        Args:
            database: Database name
            schema_name: Schema name
            semantic_tags: List of tag dicts with 'table', 'column', 'semantic' keys

        Returns:
            Dict with 'success': bool, 'tables': list of table dicts or 'error': str
        """
        try:
            table_names = list({tag["table"] for tag in semantic_tags})
            tables = []
            for table_name in table_names:
                result = self.describe_table(
                    database=database, schema=schema_name, table=table_name
                )
                col_semantics = {
                    tag["column"]: tag["semantic"]
                    for tag in semantic_tags
                    if tag["table"] == table_name
                }
                columns = [
                    {
                        "name": col["name"],
                        "type": col.get("type", col.get("data_type", "")),
                        "semantic": col_semantics.get(col["name"]),
                    }
                    for col in result.get("columns", [])
                ]
                tables.append({"table_name": table_name, "columns": columns})
            return {"success": True, "tables": tables}
        except Exception as e:
            logger.error(f"Error reading Snowflake table schemas: {e}", exc_info=True)
            return {
                "success": False,
                "error": f"Failed to read table schemas: {str(e)}",
            }

    def read_snowflake_semantic_tags(
        self, database: str, schema: str
    ) -> Dict[str, Any]:
        """Read semantic tags from native Snowflake column tags for Stitch manifest generation.

        Queries INFORMATION_SCHEMA.TAG_REFERENCES to find all columns in the schema
        that have the `semantic_type` tag applied (via chuck's bulk tagging).

        Args:
            database: Snowflake database name
            schema: Snowflake schema name

        Returns:
            Dict with 'success': bool, 'tags': list of
            {'table': str, 'column': str, 'semantic': str} or 'error': str
        """
        # Use TAG_REFERENCES_ALL_COLUMNS per table — works across all Snowflake editions
        # and privilege levels. We first list tables in the schema, then for each table
        # query which columns have the SEMANTIC_TYPE tag applied.
        import snowflake.connector

        tags: list = []
        try:
            conn = self._get_connection()

            # Step 1: list tables in the schema
            with conn.cursor(snowflake.connector.DictCursor) as cur:
                cur.execute(f"SHOW TABLES IN SCHEMA {database}.{schema}")
                tables = [
                    (row.get("name") or row.get("NAME") or "")
                    for row in cur.fetchall()
                    if row.get("name") or row.get("NAME")
                ]

            # Step 2: for each table query TAG_REFERENCES_ALL_COLUMNS
            for tbl in tables:
                try:
                    with conn.cursor(snowflake.connector.DictCursor) as cur:
                        cur.execute(
                            f"SELECT OBJECT_NAME, COLUMN_NAME, TAG_VALUE "
                            f"FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS("
                            f"'{database}.{schema}.{tbl}', 'TABLE')) "
                            f"WHERE TAG_NAME = 'SEMANTIC_TYPE'"
                        )
                        for row in cur.fetchall():
                            col = row.get("COLUMN_NAME") or row.get("column_name") or ""
                            sem = row.get("TAG_VALUE") or row.get("tag_value") or ""
                            if col:
                                tags.append(
                                    {"table": tbl, "column": col, "semantic": sem}
                                )
                except Exception as tbl_err:
                    logger.debug(f"No tags on {tbl} (or inaccessible): {tbl_err}")

            return {"success": True, "tags": tags}
        except Exception as e:
            logger.error(f"Error reading Snowflake semantic tags: {e}", exc_info=True)
            return {
                "success": False,
                "error": f"Failed to read semantic tags: {str(e)}",
            }

    def read_table_schemas_for_stitch(
        self, database: str, schema: str, tagged_tables: list
    ) -> Dict[str, Any]:
        """Read column definitions for tables that have semantic tags, for manifest generation.

        Uses INFORMATION_SCHEMA.COLUMNS for an efficient bulk query across all tables
        rather than calling DESCRIBE TABLE per table.

        Args:
            database: Snowflake database name
            schema: Snowflake schema name
            tagged_tables: List of table names to fetch schemas for

        Returns:
            Dict with 'success': bool, 'tables': list of
            {'table_name': str, 'columns': list of {'name', 'type'}} or 'error': str
        """
        if not tagged_tables:
            return {"success": True, "tables": []}

        # Build the IN list for the SQL query
        table_list = ", ".join(f"'{t.upper()}'" for t in tagged_tables)
        sql = f"""
        SELECT
            table_name,
            column_name AS name,
            data_type   AS type,
            ordinal_position
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = '{schema.upper()}'
          AND table_name IN ({table_list})
        ORDER BY table_name, ordinal_position
        """
        try:
            result = self.execute_sql(sql, database=database)
            rows = (
                result.get("result", {}).get("Records", [])
                if result.get("result")
                else []
            )

            # Group columns by table.
            # Snowflake DictCursor returns unquoted aliases in UPPERCASE.
            tables_map: Dict[str, list] = {}
            for row in rows:
                tbl = row.get("TABLE_NAME") or row.get("table_name", "")
                col_name = row.get("NAME") or row.get("name", "")
                col_type = row.get("TYPE") or row.get("type", "string")
                if tbl not in tables_map:
                    tables_map[tbl] = []
                tables_map[tbl].append({"name": col_name, "type": col_type})

            tables = [
                {"table_name": tbl, "columns": cols} for tbl, cols in tables_map.items()
            ]
            return {"success": True, "tables": tables}
        except Exception as e:
            logger.error(f"Error reading table schemas for stitch: {e}", exc_info=True)
            return {
                "success": False,
                "error": f"Failed to read table schemas: {str(e)}",
            }
