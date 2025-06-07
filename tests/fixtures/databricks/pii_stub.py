"""PII operations mixin for DatabricksClientStub."""


class PIIStubMixin:
    """Mixin providing PII operations for DatabricksClientStub."""

    def __init__(self):
        self.pii_scan_results = {}  # table_name -> pii results

    def scan_table_pii(self, table_name):
        """Scan table for PII data."""
        if table_name in self.pii_scan_results:
            return self.pii_scan_results[table_name]

        return {
            "table_name": table_name,
            "pii_columns": ["email", "phone"],
            "scan_timestamp": "2023-01-01T00:00:00Z",
        }

    def tag_columns_pii(self, table_name, columns, pii_type):
        """Tag columns as PII."""
        return {
            "table_name": table_name,
            "tagged_columns": columns,
            "pii_type": pii_type,
            "status": "success",
        }

    def set_pii_scan_result(self, table_name, result):
        """Set a specific PII scan result for a table."""
        self.pii_scan_results[table_name] = result
