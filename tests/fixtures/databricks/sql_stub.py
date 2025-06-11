"""SQL operations mixin for DatabricksClientStub."""


class SQLStubMixin:
    """Mixin providing SQL operations for DatabricksClientStub."""

    def __init__(self):
        self.sql_results = {}  # sql_text -> results mapping
        self.sql_calls = []  # Track submit_sql_statement calls
        self.sql_error = None  # Exception to raise on submit_sql_statement

    def execute_sql(self, sql, **kwargs):
        """Execute SQL and return results."""
        # Return pre-configured results or default
        if sql in self.sql_results:
            return self.sql_results[sql]

        # Default response
        return {
            "result": {
                "data_array": [["row1_col1", "row1_col2"], ["row2_col1", "row2_col2"]],
                "column_names": ["col1", "col2"],
            },
            "next_page_token": kwargs.get("return_next_page") and "next_token" or None,
        }

    def submit_sql_statement(self, sql_text=None, sql=None, **kwargs):
        """Submit SQL statement for execution."""
        # Track the call for verification
        call_info = {
            "sql_text": sql_text or sql,
            "warehouse_id": kwargs.get("warehouse_id"),
            "catalog": kwargs.get("catalog"),
            "wait_timeout": kwargs.get("wait_timeout", "30s"),
            **kwargs,
        }
        self.sql_calls.append(call_info)

        # Raise error if configured
        if self.sql_error:
            raise self.sql_error

        # Use sql_text for lookup (primary parameter)
        query = sql_text or sql

        # Return pre-configured results if available
        if query in self.sql_results:
            return self.sql_results[query]

        # Default successful response with basic result structure
        return {
            "status": {"state": "SUCCEEDED"},
            "result": {
                "data_array": [["default_value"]],
                "schema": {"columns": [{"name": "default_column", "type": "string"}]},
            },
            "manifest": {
                "schema": {"columns": [{"name": "default_column", "type": "string"}]}
            },
            "execution_time_ms": 1000,
        }

    def set_sql_result(self, sql_text, result):
        """Set a specific result for a SQL query."""
        self.sql_results[sql_text] = result

    def set_sql_error(self, error):
        """Configure submit_sql_statement to raise an error."""
        self.sql_error = error

    def clear_sql_error(self):
        """Clear any configured error."""
        self.sql_error = None
