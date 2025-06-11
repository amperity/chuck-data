"""
Tests for run_sql command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
These tests verify what users see when they interact with the run-sql command,
including SQL execution results, error handling, and result formatting.
"""

import pytest
import tempfile
from unittest.mock import patch

from chuck_data.commands.run_sql import handle_command, format_sql_results_for_agent
from chuck_data.config import ConfigManager


class TestRunSQLParameterValidation:
    """Test parameter validation for run_sql command."""

    def test_missing_query_parameter_returns_error(self, databricks_client_stub):
        """Missing query parameter returns helpful error."""
        result = handle_command(databricks_client_stub, warehouse_id="test_warehouse")

        assert not result.success
        assert "query" in result.message.lower()
        assert "specify" in result.message.lower()

    def test_none_client_returns_setup_error(self):
        """None client returns workspace setup error."""
        result = handle_command(None, query="SELECT 1")

        assert not result.success
        assert "workspace" in result.message.lower()
        assert "set up" in result.message.lower()

    def test_missing_warehouse_without_active_warehouse_returns_error(
        self, databricks_client_stub
    ):
        """Missing warehouse_id without active warehouse returns helpful error."""
        with patch("chuck_data.commands.run_sql.get_warehouse_id", return_value=None):
            result = handle_command(databricks_client_stub, query="SELECT 1")

            assert not result.success
            assert "warehouse" in result.message.lower()
            assert "select-warehouse" in result.message


class TestDirectRunSQLCommand:
    """Test direct run_sql command execution."""

    def test_direct_command_executes_simple_query_successfully(
        self, databricks_client_stub
    ):
        """Direct run_sql command executes simple query and returns formatted results."""
        # Setup successful SQL execution result
        sql_result = {
            "status": {"state": "SUCCEEDED"},
            "result": {
                "data_array": [["John", 30], ["Jane", 25], ["Bob", 35]],
                "schema": {
                    "columns": [
                        {"name": "name", "type": "string"},
                        {"name": "age", "type": "int"},
                    ]
                },
            },
            "manifest": {
                "schema": {
                    "columns": [
                        {"name": "name", "type": "string"},
                        {"name": "age", "type": "int"},
                    ]
                }
            },
            "execution_time_ms": 1500,
        }

        databricks_client_stub.submit_sql_statement = lambda **kwargs: sql_result

        result = handle_command(
            databricks_client_stub,
            query="SELECT name, age FROM users",
            warehouse_id="test_warehouse",
        )

        # Verify successful execution
        assert result.success
        assert "Query executed successfully" in result.message
        assert "3 result(s)" in result.message

        # Verify result data structure
        assert result.data is not None
        assert result.data["columns"] == ["name", "age"]
        assert result.data["rows"] == [["John", 30], ["Jane", 25], ["Bob", 35]]
        assert result.data["row_count"] == 3
        assert result.data["execution_time_ms"] == 1500
        assert result.data["is_paginated"] is False

    def test_direct_command_uses_active_warehouse_when_not_specified(
        self, databricks_client_stub
    ):
        """Direct run_sql command uses active warehouse when warehouse_id not provided."""
        # Mock active warehouse
        with patch(
            "chuck_data.commands.run_sql.get_warehouse_id",
            return_value="active_warehouse_123",
        ):
            # Setup successful execution
            executed_calls = []

            def capture_sql_call(**kwargs):
                executed_calls.append(kwargs)
                return {
                    "status": {"state": "SUCCEEDED"},
                    "result": {"data_array": [["result"]]},
                    "manifest": {"schema": {"columns": [{"name": "output"}]}},
                }

            databricks_client_stub.submit_sql_statement = capture_sql_call

            result = handle_command(databricks_client_stub, query="SELECT 1 as output")

            # Verify successful execution using active warehouse
            assert result.success
            assert len(executed_calls) == 1
            assert executed_calls[0]["warehouse_id"] == "active_warehouse_123"
            assert executed_calls[0]["sql_text"] == "SELECT 1 as output"

    def test_direct_command_uses_active_catalog_when_not_specified(
        self, databricks_client_stub
    ):
        """Direct run_sql command uses active catalog when catalog not provided."""
        with patch(
            "chuck_data.commands.run_sql.get_warehouse_id",
            return_value="test_warehouse",
        ):
            with patch(
                "chuck_data.commands.run_sql.get_active_catalog",
                return_value="default_catalog",
            ):
                # Setup successful execution
                executed_calls = []

                def capture_sql_call(**kwargs):
                    executed_calls.append(kwargs)
                    return {
                        "status": {"state": "SUCCEEDED"},
                        "result": {"data_array": [["result"]]},
                        "manifest": {"schema": {"columns": [{"name": "output"}]}},
                    }

                databricks_client_stub.submit_sql_statement = capture_sql_call

                result = handle_command(
                    databricks_client_stub, query="SELECT * FROM table1"
                )

                # Verify successful execution using active catalog
                assert result.success
                assert len(executed_calls) == 1
                assert executed_calls[0]["catalog"] == "default_catalog"

    def test_direct_command_handles_failed_query_gracefully(
        self, databricks_client_stub
    ):
        """Direct run_sql command handles failed queries with clear error messages."""
        # Setup failed SQL execution
        failed_result = {
            "status": {
                "state": "FAILED",
                "error": {"message": "Table 'nonexistent_table' not found"},
            }
        }

        databricks_client_stub.submit_sql_statement = lambda **kwargs: failed_result

        result = handle_command(
            databricks_client_stub,
            query="SELECT * FROM nonexistent_table",
            warehouse_id="test_warehouse",
        )

        assert not result.success
        assert "Query execution failed" in result.message
        assert "Table 'nonexistent_table' not found" in result.message

    def test_direct_command_handles_canceled_query(self, databricks_client_stub):
        """Direct run_sql command handles canceled queries appropriately."""
        # Setup canceled SQL execution
        canceled_result = {"status": {"state": "CANCELED"}}

        databricks_client_stub.submit_sql_statement = lambda **kwargs: canceled_result

        result = handle_command(
            databricks_client_stub,
            query="SELECT COUNT(*) FROM large_table",
            warehouse_id="test_warehouse",
        )

        assert not result.success
        assert "Query execution was canceled" in result.message

    def test_direct_command_handles_api_errors_gracefully(self, databricks_client_stub):
        """Direct run_sql command handles API errors with user-friendly messages."""

        # Make submit_sql_statement raise an exception
        def failing_submit_sql(**kwargs):
            raise Exception("Warehouse is not running")

        databricks_client_stub.submit_sql_statement = failing_submit_sql

        result = handle_command(
            databricks_client_stub,
            query="SELECT 1",
            warehouse_id="offline_warehouse",
        )

        assert not result.success
        assert "Failed to execute SQL query" in result.message
        assert "Warehouse is not running" in result.message
        assert result.error is not None

    def test_direct_command_handles_large_paginated_results(
        self, databricks_client_stub
    ):
        """Direct run_sql command handles large result sets with pagination."""
        # Setup paginated SQL execution result
        paginated_result = {
            "status": {"state": "SUCCEEDED"},
            "result": {
                "data_array": [],  # Empty for large results
                "external_links": [
                    {"url": "https://databricks.com/results/chunk1.csv"},
                    {"url": "https://databricks.com/results/chunk2.csv"},
                ],
            },
            "manifest": {
                "total_row_count": 50000,
                "chunks": [
                    {"row_offset": 0, "row_count": 25000},
                    {"row_offset": 25000, "row_count": 25000},
                ],
                "schema": {
                    "columns": [
                        {"name": "id", "type": "bigint"},
                        {"name": "value", "type": "string"},
                    ]
                },
            },
            "execution_time_ms": 5000,
        }

        databricks_client_stub.submit_sql_statement = lambda **kwargs: paginated_result

        result = handle_command(
            databricks_client_stub,
            query="SELECT id, value FROM huge_table",
            warehouse_id="test_warehouse",
        )

        # Verify successful execution with pagination
        assert result.success
        assert "Query executed successfully" in result.message

        # Verify paginated result structure
        assert result.data["is_paginated"] is True
        assert result.data["total_row_count"] == 50000
        assert result.data["columns"] == ["id", "value"]
        assert len(result.data["external_links"]) == 2
        assert len(result.data["chunks"]) == 2
        assert result.data["execution_time_ms"] == 5000

    def test_direct_command_generates_column_names_when_missing(
        self, databricks_client_stub
    ):
        """Direct run_sql command generates column names when schema is missing."""
        # Setup result without column schema
        result_without_schema = {
            "status": {"state": "SUCCEEDED"},
            "result": {
                "data_array": [
                    ["value1", "value2", "value3"],
                    ["value4", "value5", "value6"],
                ]
            },
            "execution_time_ms": 1000,
        }

        databricks_client_stub.submit_sql_statement = (
            lambda **kwargs: result_without_schema
        )

        result = handle_command(
            databricks_client_stub,
            query="SELECT col1, col2, col3 FROM table1",
            warehouse_id="test_warehouse",
        )

        # Verify successful execution with generated column names
        assert result.success
        assert result.data["columns"] == ["column_1", "column_2", "column_3"]
        assert result.data["rows"] == [
            ["value1", "value2", "value3"],
            ["value4", "value5", "value6"],
        ]

    def test_direct_command_handles_empty_result_set(self, databricks_client_stub):
        """Direct run_sql command handles queries with no results."""
        # Setup empty result
        empty_result = {
            "status": {"state": "SUCCEEDED"},
            "result": {"data_array": []},
            "manifest": {
                "schema": {
                    "columns": [
                        {"name": "name", "type": "string"},
                        {"name": "age", "type": "int"},
                    ]
                }
            },
            "execution_time_ms": 500,
        }

        databricks_client_stub.submit_sql_statement = lambda **kwargs: empty_result

        result = handle_command(
            databricks_client_stub,
            query="SELECT name, age FROM users WHERE age > 100",
            warehouse_id="test_warehouse",
        )

        # Verify successful execution with empty results
        assert result.success
        assert "0 result(s)" in result.message
        assert result.data["columns"] == ["name", "age"]
        assert result.data["rows"] == []
        assert result.data["row_count"] == 0

    def test_direct_command_with_custom_timeout(self, databricks_client_stub):
        """Direct run_sql command respects custom wait_timeout parameter."""
        executed_calls = []

        def capture_sql_call(**kwargs):
            executed_calls.append(kwargs)
            return {
                "status": {"state": "SUCCEEDED"},
                "result": {"data_array": [["result"]]},
                "manifest": {"schema": {"columns": [{"name": "output"}]}},
            }

        databricks_client_stub.submit_sql_statement = capture_sql_call

        result = handle_command(
            databricks_client_stub,
            query="SELECT COUNT(*) as output FROM large_table",
            warehouse_id="test_warehouse",
            wait_timeout="5m",
        )

        assert result.success
        assert len(executed_calls) == 1
        assert executed_calls[0]["wait_timeout"] == "5m"


class TestRunSQLCommandConfiguration:
    """Test run_sql command configuration and registry integration."""

    def test_run_sql_command_definition_properties(self):
        """Run_sql command definition has correct configuration."""
        from chuck_data.commands.run_sql import DEFINITION

        assert DEFINITION.name == "run-sql"
        assert DEFINITION.handler == handle_command
        assert DEFINITION.needs_api_client is True
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True
        assert DEFINITION.agent_display == "full"
        assert "query" in DEFINITION.required_params
        assert "query" in DEFINITION.parameters
        assert "warehouse_id" in DEFINITION.parameters
        assert "catalog" in DEFINITION.parameters
        assert "wait_timeout" in DEFINITION.parameters

    def test_run_sql_command_parameter_requirements(self):
        """Run_sql command has properly configured parameter requirements."""
        from chuck_data.commands.run_sql import DEFINITION

        # Verify required parameter
        assert "query" in DEFINITION.required_params
        assert (
            "warehouse_id" not in DEFINITION.required_params
        )  # Optional (uses config)
        assert "catalog" not in DEFINITION.required_params  # Optional (uses config)

        # Verify parameter definitions
        query_param = DEFINITION.parameters["query"]
        assert query_param["type"] == "string"
        assert "sql" in query_param["description"].lower()

        timeout_param = DEFINITION.parameters["wait_timeout"]
        assert timeout_param["type"] == "string"
        assert timeout_param["default"] == "30s"

    def test_run_sql_command_has_custom_formatter(self):
        """Run_sql command has custom output formatter configured."""
        from chuck_data.commands.run_sql import DEFINITION

        assert DEFINITION.output_formatter == format_sql_results_for_agent
        assert DEFINITION.condensed_action == "Running sql"


class TestRunSQLDisplayIntegration:
    """Test run_sql command integration with display system."""

    def test_sql_result_contains_display_ready_data(self, databricks_client_stub):
        """Run_sql command result contains data ready for display formatting."""
        sql_result = {
            "status": {"state": "SUCCEEDED"},
            "result": {
                "data_array": [
                    ["Alice", 28, "Engineer"],
                    ["Bob", 32, "Manager"],
                    ["Carol", 26, "Designer"],
                ],
                "schema": {
                    "columns": [
                        {"name": "name", "type": "string"},
                        {"name": "age", "type": "int"},
                        {"name": "role", "type": "string"},
                    ]
                },
            },
            "manifest": {
                "schema": {
                    "columns": [
                        {"name": "name", "type": "string"},
                        {"name": "age", "type": "int"},
                        {"name": "role", "type": "string"},
                    ]
                }
            },
            "execution_time_ms": 2500,
        }

        databricks_client_stub.submit_sql_statement = lambda **kwargs: sql_result

        result = handle_command(
            databricks_client_stub,
            query="SELECT name, age, role FROM employees",
            warehouse_id="test_warehouse",
        )

        assert result.success

        # Verify data structure matches what display layer expects
        assert "columns" in result.data
        assert "rows" in result.data
        assert "row_count" in result.data
        assert "execution_time_ms" in result.data
        assert "is_paginated" in result.data

        # Verify data content
        assert result.data["columns"] == ["name", "age", "role"]
        assert len(result.data["rows"]) == 3
        assert result.data["row_count"] == 3

    def test_sql_complex_schema_locations_handled(self, databricks_client_stub):
        """Run_sql command finds column schema in various API response locations."""
        # Test schema in result_data.schema.columns location
        schema_in_result_data = {
            "status": {"state": "SUCCEEDED"},
            "result": {
                "data_array": [["test_value"]],
                "schema": {"columns": [{"name": "test_column", "type": "string"}]},
            },
            "execution_time_ms": 1000,
        }

        databricks_client_stub.submit_sql_statement = (
            lambda **kwargs: schema_in_result_data
        )

        result = handle_command(
            databricks_client_stub,
            query="SELECT 'test' as test_column",
            warehouse_id="test_warehouse",
        )

        assert result.success
        assert result.data["columns"] == ["test_column"]

    def test_sql_paginated_result_structure(self, databricks_client_stub):
        """Run_sql command formats paginated results for display correctly."""
        paginated_result = {
            "status": {"state": "SUCCEEDED"},
            "result": {
                "data_array": [],
                "external_links": [{"url": "https://databricks.com/chunk1.csv"}],
            },
            "manifest": {
                "total_row_count": 10000,
                "chunks": [{"row_offset": 0, "row_count": 10000}],
                "schema": {
                    "columns": [
                        {"name": "id", "type": "bigint"},
                        {"name": "data", "type": "string"},
                    ]
                },
            },
        }

        databricks_client_stub.submit_sql_statement = lambda **kwargs: paginated_result

        result = handle_command(
            databricks_client_stub,
            query="SELECT id, data FROM large_dataset",
            warehouse_id="test_warehouse",
        )

        assert result.success
        assert result.data["is_paginated"] is True
        assert result.data["total_row_count"] == 10000
        assert "external_links" in result.data
        assert "chunks" in result.data

    def test_sql_result_formatting_for_agent(self, databricks_client_stub):
        """Run_sql command provides proper formatting for agent consumption."""
        sql_result = {
            "status": {"state": "SUCCEEDED"},
            "result": {
                "data_array": [["John", 25], ["Jane", 30]],
                "schema": {
                    "columns": [
                        {"name": "name", "type": "string"},
                        {"name": "age", "type": "int"},
                    ]
                },
            },
            "manifest": {
                "schema": {
                    "columns": [
                        {"name": "name", "type": "string"},
                        {"name": "age", "type": "int"},
                    ]
                }
            },
            "execution_time_ms": 1200,
        }

        databricks_client_stub.submit_sql_statement = lambda **kwargs: sql_result

        result = handle_command(
            databricks_client_stub,
            query="SELECT name, age FROM users",
            warehouse_id="test_warehouse",
        )

        # Test the custom formatter
        formatted = format_sql_results_for_agent(result)

        assert formatted["success"] is True
        assert "results_table" in formatted
        assert "summary" in formatted

        # Verify summary contains expected information
        summary = formatted["summary"]
        assert summary["total_rows"] == 2
        assert summary["columns"] == ["name", "age"]
        assert summary["execution_time_ms"] == 1200

        # Verify raw data is included for small result sets
        assert "raw_data" in formatted
        assert formatted["raw_data"]["columns"] == ["name", "age"]
        assert formatted["raw_data"]["rows"] == [["John", 25], ["Jane", 30]]


class TestRunSQLErrorScenarios:
    """Test various error scenarios for run_sql command."""

    def test_sql_syntax_error_handling(self, databricks_client_stub):
        """Run_sql command handles SQL syntax errors clearly."""
        syntax_error_result = {
            "status": {
                "state": "FAILED",
                "error": {
                    "message": "Syntax error at line 1 position 15: unexpected token 'FORM'"
                },
            }
        }

        databricks_client_stub.submit_sql_statement = (
            lambda **kwargs: syntax_error_result
        )

        result = handle_command(
            databricks_client_stub,
            query="SELECT * FORM users",  # Typo: FORM instead of FROM
            warehouse_id="test_warehouse",
        )

        assert not result.success
        assert "Query execution failed" in result.message
        assert "Syntax error" in result.message
        assert "unexpected token 'FORM'" in result.message

    def test_permission_denied_error_handling(self, databricks_client_stub):
        """Run_sql command handles permission errors clearly."""
        permission_error_result = {
            "status": {
                "state": "FAILED",
                "error": {
                    "message": "Access denied to table 'sensitive_data.customers'"
                },
            }
        }

        databricks_client_stub.submit_sql_statement = (
            lambda **kwargs: permission_error_result
        )

        result = handle_command(
            databricks_client_stub,
            query="SELECT * FROM sensitive_data.customers",
            warehouse_id="test_warehouse",
        )

        assert not result.success
        assert "Query execution failed" in result.message
        assert "Access denied" in result.message

    def test_unknown_query_state_handling(self, databricks_client_stub):
        """Run_sql command handles unknown query states appropriately."""
        unknown_state_result = {"status": {"state": "UNKNOWN_STATE"}}

        databricks_client_stub.submit_sql_statement = (
            lambda **kwargs: unknown_state_result
        )

        result = handle_command(
            databricks_client_stub,
            query="SELECT 1",
            warehouse_id="test_warehouse",
        )

        assert not result.success
        assert "Query did not complete successfully" in result.message
        assert "UNKNOWN_STATE" in result.message

    def test_formatter_handles_failed_queries(self):
        """SQL formatter handles failed query results appropriately."""
        from chuck_data.commands.base import CommandResult

        failed_result = CommandResult(
            success=False, message="Table not found: nonexistent_table"
        )

        formatted = format_sql_results_for_agent(failed_result)

        assert "error" in formatted
        assert formatted["error"] == "Table not found: nonexistent_table"

    def test_formatter_handles_empty_results(self):
        """SQL formatter handles queries with no data appropriately."""
        from chuck_data.commands.base import CommandResult

        empty_result = CommandResult(
            success=True, message="Query completed successfully", data=None
        )

        formatted = format_sql_results_for_agent(empty_result)

        assert formatted["success"] is True
        assert formatted["results"] == "No data returned"
