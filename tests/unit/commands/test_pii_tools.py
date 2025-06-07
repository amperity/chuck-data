"""
Tests for the PII tools helper module.
"""

from unittest.mock import patch, MagicMock
import pytest

from chuck_data.commands.pii_tools import (
    _helper_tag_pii_columns_logic,
    _helper_scan_schema_for_pii_logic,
)


@pytest.fixture
def mock_columns():
    """Mock columns from database."""
    return [
        {"name": "first_name", "type_name": "string"},
        {"name": "email", "type_name": "string"},
        {"name": "signup_date", "type_name": "date"},
    ]


@pytest.fixture
def configured_llm_client(llm_client_stub):
    """LLM client configured for PII detection response."""
    pii_response_content = '[{"name":"first_name","semantic":"given-name"},{"name":"email","semantic":"email"},{"name":"signup_date","semantic":null}]'
    llm_client_stub.set_response_content(pii_response_content)
    return llm_client_stub


@patch("chuck_data.commands.pii_tools.json.loads")
def test_tag_pii_columns_logic_success(
    mock_json_loads,
    databricks_client_stub,
    configured_llm_client,
    mock_columns,
    temp_config,
):
    """Test successful tagging of PII columns."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Set up test data using stub
        databricks_client_stub.add_catalog("mycat")
        databricks_client_stub.add_schema("mycat", "myschema")
        databricks_client_stub.add_table(
            "mycat", "myschema", "users", columns=mock_columns
        )

        # Mock the JSON parsing instead of relying on actual JSON parsing
        mock_json_loads.return_value = [
            {"name": "first_name", "semantic": "given-name"},
            {"name": "email", "semantic": "email"},
            {"name": "signup_date", "semantic": None},
        ]

        # Call the function
        result = _helper_tag_pii_columns_logic(
            databricks_client_stub,
            configured_llm_client,
            "users",
            catalog_name_context="mycat",
            schema_name_context="myschema",
        )

        # Verify the result
        assert result["full_name"] == "mycat.myschema.users"
        assert result["table_name"] == "users"
        assert result["column_count"] == 3
        assert result["pii_column_count"] == 2
        assert result["has_pii"]
        assert not result["skipped"]
        assert result["columns"][0]["semantic"] == "given-name"
        assert result["columns"][1]["semantic"] == "email"
        assert result["columns"][2]["semantic"] is None


@patch("concurrent.futures.ThreadPoolExecutor")
def test_scan_schema_for_pii_logic(
    mock_executor, databricks_client_stub, configured_llm_client, temp_config
):
    """Test scanning a schema for PII."""
    with patch("chuck_data.config._config_manager", temp_config):
        # Set up test data using stub
        databricks_client_stub.add_catalog("test_cat")
        databricks_client_stub.add_schema("test_cat", "test_schema")
        databricks_client_stub.add_table("test_cat", "test_schema", "users")
        databricks_client_stub.add_table("test_cat", "test_schema", "orders")
        databricks_client_stub.add_table("test_cat", "test_schema", "_stitch_temp")

        # Mock the ThreadPoolExecutor
        mock_future = MagicMock()
        mock_future.result.return_value = {
            "table_name": "users",
            "full_name": "test_cat.test_schema.users",
            "pii_column_count": 2,
            "has_pii": True,
            "skipped": False,
        }

        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_context
        mock_context.submit.return_value = mock_future
        mock_executor.return_value = mock_context

        # Mock concurrent.futures.as_completed to return mock_future
        with patch("concurrent.futures.as_completed", return_value=[mock_future]):
            # Call the function
            result = _helper_scan_schema_for_pii_logic(
                databricks_client_stub, configured_llm_client, "test_cat", "test_schema"
            )

            # Verify the result
            assert result["catalog"] == "test_cat"
            assert result["schema"] == "test_schema"
            assert result["tables_scanned_attempted"] == 2  # Excluding _stitch_temp
            assert result["tables_successfully_processed"] == 1
            assert result["tables_with_pii"] == 1
            assert result["total_pii_columns"] == 2
