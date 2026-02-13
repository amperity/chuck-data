"""
Command for listing tables in a schema (works with both Databricks and Redshift).
"""

from typing import Optional, Any, Union
from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.clients.redshift import RedshiftAPIClient
from chuck_data.command_registry import CommandDefinition
from chuck_data.commands.base import CommandResult
from chuck_data.config import get_active_catalog, get_active_schema, get_active_database
from chuck_data.data_providers import is_redshift_client
import logging


def handle_command(
    client: Optional[Union[DatabricksAPIClient, RedshiftAPIClient]], **kwargs: Any
) -> CommandResult:
    """
    List tables in a schema (provider-aware).

    Args:
        client: DatabricksAPIClient or RedshiftAPIClient instance for API calls
        **kwargs: Command parameters
            For Databricks:
                - catalog_name: Name of the catalog containing the schema
                - schema_name: Name of the schema to list tables from
                - include_delta_metadata: Whether delta metadata should be included (optional)
                - omit_columns: Whether to omit columns from the response (optional)
                - include_browse: Whether to include tables with selective metadata access (optional)
            For Redshift:
                - database: Database name
                - schema_name: Schema name to list tables from
                - Column counts are automatically fetched via describe_table API
            Common:
                - display: bool, whether to display the table (default: False)

    Returns:
        CommandResult with list of tables if successful
    """
    if not client:
        return CommandResult(
            False,
            message="No API client available. Please set up your connection first.",
        )

    # Check if display should be shown (default to False for agent calls)
    display = kwargs.get("display", False)

    # Determine which provider we're using
    is_redshift = is_redshift_client(client)

    # Extract common parameters
    schema_name = kwargs.get("schema_name")
    omit_columns = kwargs.get("omit_columns", False)

    try:
        if is_redshift:
            # Redshift path: use database and schema
            database = kwargs.get("database")

            # If database not provided, try to use active database
            if not database:
                database = get_active_database()
                if not database:
                    return CommandResult(
                        False,
                        message="No database specified and no active database selected. Please provide a database or select a database first using /select-database.",
                    )

            # If schema_name not provided, try to use active schema
            if not schema_name:
                schema_name = get_active_schema()
                if not schema_name:
                    return CommandResult(
                        False,
                        message="No schema specified and no active schema selected. Please provide a schema_name or select a schema first using /select-schema.",
                    )

            # List tables in Redshift
            tables_response = client.list_tables(
                database=database, schema_pattern=schema_name
            )

            # Extract tables from response (now returns {"tables": [...]})
            result_tables = tables_response.get("tables", [])

            # Fetch table metadata using SQL query to system tables
            # This gives us column counts, row counts, and timestamps in a single query
            table_metadata = {}
            if result_tables:
                logging.info(
                    f"Fetching metadata for {len(result_tables)} tables in {database}.{schema_name}"
                )

                # Build a SQL query to get metadata for all tables at once
                table_names = [t.get("name") for t in result_tables]
                table_names_str = ", ".join([f"'{name}'" for name in table_names])

                # Query system tables for metadata
                # Note: Redshift uses 'column' not 'columnname' in pg_table_def
                # We get column counts from pg_table_def
                # For row counts, we'll fetch them separately using COUNT queries for accuracy
                metadata_sql = f"""
                SELECT
                    tablename as table_name,
                    COUNT(DISTINCT "column") as column_count
                FROM pg_table_def
                WHERE schemaname = '{schema_name}'
                    AND tablename IN ({table_names_str})
                GROUP BY tablename
                """

                try:
                    result = client.execute_sql(metadata_sql, database=database)

                    # AWS Redshift Data API returns results in result["result"]["Records"]
                    # Each record is a list of dicts with keys like "stringValue", "longValue", etc.
                    if result and "result" in result and "Records" in result["result"]:
                        for record in result["result"]["Records"]:
                            # Extract table_name (first column - stringValue)
                            table_name = record[0].get("stringValue", "")
                            # Extract column_count (second column - longValue)
                            column_count = int(record[1].get("longValue", 0))

                            table_metadata[table_name] = {
                                "column_count": column_count,
                                "row_count": "-",  # Will be fetched separately
                            }
                            logging.info(f"Table {table_name}: {column_count} columns")

                    # If the SQL query succeeded but returned no rows, fall back to describe_table
                    # This can happen if pg_table_def is not accessible or empty
                    if not table_metadata:
                        logging.warning(
                            f"SQL query to pg_table_def returned 0 rows, falling back to describe_table for each table"
                        )
                        for table in result_tables:
                            table_name = table.get("name")
                            try:
                                table_details = client.describe_table(
                                    database=database,
                                    schema=schema_name,
                                    table=table_name,
                                )
                                columns = table_details.get("ColumnList", [])
                                table_metadata[table_name] = {
                                    "column_count": len(columns),
                                    "row_count": "-",
                                }
                                logging.info(
                                    f"Table {table_name}: {len(columns)} columns (from describe_table)"
                                )
                            except Exception as e2:
                                logging.error(
                                    f"Failed to fetch columns for table {table_name}: {str(e2)}"
                                )
                                table_metadata[table_name] = {
                                    "column_count": 0,
                                    "row_count": "-",
                                }
                    else:
                        # Note: Redshift doesn't provide table creation/modification timestamps
                        # in accessible system tables without special permissions (STL_DDLTEXT requires elevated access)

                        # Now fetch row counts for each table using actual COUNT queries
                        # This is more accurate but slower than system table statistics
                        logging.info(
                            f"Fetching row counts for {len(table_metadata)} tables..."
                        )
                        for table_name in table_metadata.keys():
                            try:
                                count_sql = f'SELECT COUNT(*) as row_count FROM "{schema_name}"."{table_name}"'
                                count_result = client.execute_sql(
                                    count_sql, database=database
                                )

                                if (
                                    count_result
                                    and "result" in count_result
                                    and "Records" in count_result["result"]
                                    and len(count_result["result"]["Records"]) > 0
                                ):
                                    row_count = int(
                                        count_result["result"]["Records"][0][0].get(
                                            "longValue", 0
                                        )
                                    )
                                    table_metadata[table_name]["row_count"] = row_count
                                    logging.info(
                                        f"Table {table_name}: {row_count} rows"
                                    )
                            except Exception as e_count:
                                logging.warning(
                                    f"Failed to get row count for {table_name}: {str(e_count)}"
                                )
                                # Keep row_count as "-" if count fails
                except Exception as e:
                    logging.error(
                        f"Failed to fetch table metadata via SQL: {str(e)}",
                        exc_info=True,
                    )
                    # Fallback: fetch column counts using describe_table
                    for table in result_tables:
                        table_name = table.get("name")
                        try:
                            table_details = client.describe_table(
                                database=database, schema=schema_name, table=table_name
                            )
                            columns = table_details.get("ColumnList", [])
                            table_metadata[table_name] = {
                                "column_count": len(columns),
                                "row_count": "-",
                            }
                        except Exception as e2:
                            logging.error(
                                f"Failed to fetch columns for table {table_name}: {str(e2)}"
                            )
                            table_metadata[table_name] = {
                                "column_count": 0,
                                "row_count": "-",
                            }

                logging.info(f"Table metadata collected: {table_metadata}")

        else:
            # Databricks path: use catalog and schema
            catalog_name = kwargs.get("catalog_name")
            include_delta_metadata = kwargs.get("include_delta_metadata", False)
            include_browse = kwargs.get("include_browse", False)

            # If catalog_name not provided, try to use active catalog
            if not catalog_name:
                catalog_name = get_active_catalog()
                if not catalog_name:
                    return CommandResult(
                        False,
                        message="No catalog specified and no active catalog selected. Please provide a catalog_name or select a catalog first using /select-catalog.",
                    )

            # If schema_name not provided, try to use active schema
            if not schema_name:
                schema_name = get_active_schema()
                if not schema_name:
                    return CommandResult(
                        False,
                        message="No schema specified and no active schema selected. Please provide a schema_name or select a schema first using /select-schema.",
                    )

            # List tables in Databricks
            result = client.list_tables(
                catalog_name=catalog_name,
                schema_name=schema_name,
                include_delta_metadata=include_delta_metadata,
                omit_columns=omit_columns,
                include_browse=include_browse,
            )

            result_tables = result.get("tables", [])

        if not result_tables:
            # Build appropriate message based on provider
            if is_redshift:
                message = f"No tables found in schema '{database}.{schema_name}'."
                location_data = {
                    "database": database,
                    "schema_name": schema_name,
                }
            else:
                message = f"No tables found in schema '{catalog_name}.{schema_name}'."
                location_data = {
                    "catalog_name": catalog_name,
                    "schema_name": schema_name,
                }

            return CommandResult(
                True,
                message=message,
                data={
                    "tables": [],
                    "total_count": 0,
                    "display": display,
                    **location_data,
                },
            )

        # Format table information for display
        formatted_tables = []
        for table in result_tables:
            if is_redshift:
                # Redshift table format
                table_name = table.get("name")
                table_info = {
                    "name": table_name,
                    "full_name": f"{database}.{schema_name}.{table_name}",
                    "table_type": table.get("type", ""),  # TABLE, VIEW, etc.
                    "data_source_format": "",  # Not available in Redshift
                    "comment": "",  # Not available from list_tables
                    "created_at": None,  # Not available from list_tables
                    "updated_at": None,  # Not available from list_tables
                    "created_by": "",  # Not available
                    "owner": "",  # Not available from list_tables
                    "row_count": "-",  # Will be set from table_metadata below
                    "size_bytes": "Unknown",  # Not available from list_tables
                }

                # Add column count and row count from the fetched metadata
                if table_name in table_metadata:
                    metadata = table_metadata[table_name]
                    if not omit_columns:
                        table_info["column_count"] = metadata.get("column_count", 0)
                        logging.info(
                            f"Set column_count={metadata.get('column_count', 0)} for table {table_name}"
                        )
                    table_info["row_count"] = metadata.get("row_count", 0)
                    logging.info(
                        f"Set row_count={metadata.get('row_count', 0)} for table {table_name}"
                    )
                elif not omit_columns:
                    table_info["column_count"] = 0
                    logging.warning(
                        f"Table {table_name} not found in table_metadata, setting counts to 0"
                    )

            else:
                # Databricks table format
                table_info = {
                    "name": table.get("name"),
                    "full_name": table.get("full_name"),
                    "table_type": table.get("table_type", ""),
                    "data_source_format": table.get("data_source_format", ""),
                    "comment": table.get("comment", ""),
                    "created_at": table.get("created_at"),
                    "updated_at": table.get("updated_at"),
                    "created_by": table.get("created_by", ""),
                    "owner": table.get("owner", ""),
                    "row_count": table.get("properties", {}).get(
                        "spark.sql.statistics.numRows", "-"
                    ),
                    "size_bytes": table.get("properties", {}).get(
                        "size_bytes", "Unknown"
                    ),
                }

            # Include columns if available and not omitted (Databricks only)
            if not omit_columns and not is_redshift:
                columns = table.get("columns", [])
                table_info["column_count"] = len(columns)
                if columns:
                    column_list = []
                    for col in columns:
                        column_list.append(
                            {
                                "name": col.get("name"),
                                "type": col.get(
                                    "type_text", col.get("type", {}).get("name", "")
                                ),
                                "nullable": col.get("nullable", True),
                            }
                        )
                    table_info["columns"] = column_list

            formatted_tables.append(table_info)

        # Build result data based on provider
        if is_redshift:
            # Log the formatted tables for debugging
            for t in formatted_tables:
                logging.info(
                    f"Formatted table: {t.get('name')} - column_count: {t.get('column_count', 'NOT SET')}"
                )

            result_data = {
                "tables": formatted_tables,
                "total_count": len(formatted_tables),
                "database": database,
                "schema_name": schema_name,
                "display": display,
            }
            message = (
                f"Found {len(formatted_tables)} table(s) in '{database}.{schema_name}'."
            )
        else:
            result_data = {
                "tables": formatted_tables,
                "total_count": len(formatted_tables),
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "display": display,
            }
            message = f"Found {len(formatted_tables)} table(s) in '{catalog_name}.{schema_name}'."

        return CommandResult(
            True,
            data=result_data,
            message=message,
        )
    except Exception as e:
        logging.error(f"Error listing tables: {str(e)}")
        return CommandResult(False, message=f"Failed to list tables: {str(e)}", error=e)


DEFINITION = CommandDefinition(
    name="list_tables",
    description="List tables in a schema (works with both Databricks and Redshift). By default returns data without showing table. Use display=true when user asks to see tables. For Databricks, use catalog_name and schema_name. For Redshift, use database and schema_name. Redshift automatically fetches column counts.",
    handler=handle_command,
    parameters={
        "catalog_name": {
            "type": "string",
            "description": "Name of the catalog containing the schema (Databricks only).",
        },
        "database": {
            "type": "string",
            "description": "Name of the database (Redshift only).",
        },
        "schema_name": {
            "type": "string",
            "description": "Name of the schema to list tables from.",
        },
        "include_delta_metadata": {
            "type": "boolean",
            "description": "Whether delta metadata should be included.",
            "default": False,
        },
        "omit_columns": {
            "type": "boolean",
            "description": "Whether to omit columns from the response.",
            "default": False,
        },
        "include_browse": {
            "type": "boolean",
            "description": "Whether to include tables with selective metadata access.",
            "default": False,
        },
        "display": {
            "type": "boolean",
            "description": "Whether to display the table list to the user (default: false). Set to true when user asks to see tables.",
        },
    },
    required_params=[],  # Not required anymore as we'll try to get them from active config
    tui_aliases=["/list-tables", "/tables"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    agent_display="conditional",  # Use conditional display based on display parameter
    display_condition=lambda result: result.get(
        "display", False
    ),  # Show full table only when display=True
    condensed_action="Listing tables",  # Friendly name for condensed display
    usage_hint="Usage: /list-tables [--catalog_name <catalog>] [--database <database>] [--schema_name <schema>] [--display true|false]\n(Uses active catalog/database/schema if not specified. Redshift automatically fetches column counts)",
)
