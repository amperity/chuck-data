"""SQL operations mixin for DatabricksClientStub."""


class SQLStubMixin:
    """Mixin providing SQL operations for DatabricksClientStub."""

    def __init__(self):
        self.sql_results = {}  # sql -> results mapping

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
        # Support both parameter names for compatibility
        # Return successful SQL submission by default
        return {"status": {"state": "SUCCEEDED"}}

    def set_sql_result(self, sql, result):
        """Set a specific result for a SQL query."""
        self.sql_results[sql] = result
