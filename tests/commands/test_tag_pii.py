"""Unit tests for tag_pii command."""

import os
import tempfile
from unittest.mock import MagicMock, patch

from chuck_data.commands.tag_pii import handle_command, apply_semantic_tags
from chuck_data.commands.base import CommandResult
from chuck_data.config import (
    ConfigManager,
    set_warehouse_id,
    set_active_catalog,
    set_active_schema,
)
from tests.fixtures import DatabricksClientStub


class TestTagPiiCommand:
    """Test cases for the tag_pii command handler."""

    def setup_method(self):
        """Set up test fixtures."""
        self.client_stub = DatabricksClientStub()

        # Set up config management
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.temp_dir.name, "test_config.json")
        self.config_manager = ConfigManager(self.config_path)
        self.patcher = patch("chuck_data.config._config_manager", self.config_manager)
        self.patcher.start()

    def teardown_method(self):
        self.patcher.stop()
        self.temp_dir.cleanup()

    def test_missing_table_name(self):
        """Test that missing table_name parameter is handled correctly."""
        result = handle_command(
            None, pii_columns=[{"name": "test", "semantic": "email"}]
        )

        assert isinstance(result, CommandResult)
        assert not result.success
        assert "table_name parameter is required" in result.message

    def test_missing_pii_columns(self):
        """Test that missing pii_columns parameter is handled correctly."""
        result = handle_command(None, table_name="test_table")

        assert isinstance(result, CommandResult)
        assert not result.success
        assert "pii_columns parameter is required" in result.message

    def test_empty_pii_columns(self):
        """Test that empty pii_columns list is handled correctly."""
        result = handle_command(None, table_name="test_table", pii_columns=[])

        assert isinstance(result, CommandResult)
        assert not result.success
        assert "pii_columns parameter is required" in result.message

    def test_missing_client(self):
        """Test that missing client is handled correctly."""
        result = handle_command(
            None,
            table_name="test_table",
            pii_columns=[{"name": "test", "semantic": "email"}],
        )

        assert isinstance(result, CommandResult)
        assert not result.success
        assert "Client is required for PII tagging" in result.message

    def test_missing_warehouse_id(self):
        """Test that missing warehouse ID is handled correctly."""
        # Don't set warehouse ID in config

        result = handle_command(
            self.client_stub,
            table_name="test_table",
            pii_columns=[{"name": "test", "semantic": "email"}],
        )

        assert isinstance(result, CommandResult)
        assert not result.success
        assert "No warehouse ID configured" in result.message

    def test_missing_catalog_schema_for_simple_table_name(self):
        """Test that missing catalog/schema for simple table name is handled."""
        set_warehouse_id("warehouse123")
        # Don't set active catalog/schema

        result = handle_command(
            self.client_stub,
            table_name="simple_table",  # No dots, so needs catalog/schema
            pii_columns=[{"name": "test", "semantic": "email"}],
        )

        assert isinstance(result, CommandResult)
        assert not result.success
        assert "No active catalog and schema selected" in result.message

    def test_table_not_found(self):
        """Test that table not found is handled correctly."""
        set_warehouse_id("warehouse123")
        set_active_catalog("test_catalog")
        set_active_schema("test_schema")

        # Don't add the table to stub - will cause table not found

        result = handle_command(
            self.client_stub,
            table_name="nonexistent_table",
            pii_columns=[{"name": "test", "semantic": "email"}],
        )

        assert isinstance(result, CommandResult)
        assert not result.success
        assert (
            "Table test_catalog.test_schema.nonexistent_table not found"
            in result.message
        )

    def test_apply_semantic_tags_success(self):
        """Test successful application of semantic tags."""
        pii_columns = [
            {"name": "email_col", "semantic": "email"},
            {"name": "name_col", "semantic": "given-name"},
        ]

        results = apply_semantic_tags(
            self.client_stub, "catalog.schema.table", pii_columns, "warehouse123"
        )

        assert len(results) == 2
        assert all(r["success"] for r in results)
        assert results[0]["column"] == "email_col"
        assert results[0]["semantic_type"] == "email"
        assert results[1]["column"] == "name_col"
        assert results[1]["semantic_type"] == "given-name"

    def test_apply_semantic_tags_missing_data(self):
        """Test handling of missing column data in apply_semantic_tags."""
        pii_columns = [
            {"name": "email_col"},  # Missing semantic type
            {"semantic": "email"},  # Missing column name
            {"name": "good_col", "semantic": "phone"},  # Good data
        ]

        results = apply_semantic_tags(
            self.client_stub, "catalog.schema.table", pii_columns, "warehouse123"
        )

        assert len(results) == 3
        assert not results[0]["success"]  # Missing semantic type
        assert not results[1]["success"]  # Missing column name
        assert results[2]["success"]  # Good data

        assert "Missing column name or semantic type" in results[0]["error"]
        assert "Missing column name or semantic type" in results[1]["error"]

    def test_apply_semantic_tags_sql_failure(self):
        """Test handling of SQL execution failures."""

        # Create a stub that returns SQL failure
        class FailingSQLStub(DatabricksClientStub):
            def submit_sql_statement(self, sql_text=None, sql=None, **kwargs):
                return {
                    "status": {
                        "state": "FAILED",
                        "error": {"message": "SQL execution failed"},
                    }
                }

        failing_client = FailingSQLStub()
        pii_columns = [{"name": "email_col", "semantic": "email"}]

        results = apply_semantic_tags(
            failing_client, "catalog.schema.table", pii_columns, "warehouse123"
        )

        assert len(results) == 1
        assert not results[0]["success"]
        assert "SQL execution failed" in results[0]["error"]

    def test_apply_semantic_tags_exception(self):
        """Test handling of exceptions during SQL execution."""
        mock_client = MagicMock()
        mock_client.submit_sql_statement.side_effect = Exception("Connection error")

        pii_columns = [{"name": "email_col", "semantic": "email"}]

        results = apply_semantic_tags(
            mock_client, "catalog.schema.table", pii_columns, "warehouse123"
        )

        assert len(results) == 1
        assert not results[0]["success"]
        assert "Connection error" in results[0]["error"]
