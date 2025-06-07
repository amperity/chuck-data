"""
Tests for the catalogs module.
"""

from chuck_data.catalogs import (
    list_catalogs,
    get_catalog,
    list_schemas,
    get_schema,
    list_tables,
    get_table,
)


def test_list_catalogs_no_params(databricks_client_stub):
    """Test listing catalogs with no parameters."""
    # Set up stub data
    databricks_client_stub.add_catalog("catalog1", type="MANAGED")
    databricks_client_stub.add_catalog("catalog2", type="EXTERNAL")
    expected_response = {
        "catalogs": [
            {"name": "catalog1", "type": "MANAGED"},
            {"name": "catalog2", "type": "EXTERNAL"},
        ]
    }

    # Call the function
    result = list_catalogs(databricks_client_stub)

    # Verify the result
    assert result == expected_response


def test_list_catalogs_with_params(databricks_client_stub):
    """Test listing catalogs with parameters."""
    # Set up stub data
    databricks_client_stub.add_catalog("catalog1", type="MANAGED")
    databricks_client_stub.add_catalog("catalog2", type="EXTERNAL")

    # Call the function with parameters
    result = list_catalogs(databricks_client_stub, include_browse=True, max_results=10)

    # Verify the call was made with parameters
    assert len(databricks_client_stub.list_catalogs_calls) == 1
    call_args = databricks_client_stub.list_catalogs_calls[0]
    assert call_args == (True, 10, None)

    # Verify the result structure
    assert "catalogs" in result
    assert len(result["catalogs"]) == 2


def test_get_catalog(databricks_client_stub):
    """Test getting a specific catalog."""
    # Set up stub data
    databricks_client_stub.add_catalog(
        "test_catalog", type="MANAGED", comment="Test catalog"
    )

    # Call the function
    result = get_catalog(databricks_client_stub, "test_catalog")

    # Verify the result
    assert result["name"] == "test_catalog"
    assert result["type"] == "MANAGED"
    assert result["comment"] == "Test catalog"


def test_list_schemas_basic(databricks_client_stub):
    """Test listing schemas with basic parameters."""
    # Set up stub data
    databricks_client_stub.add_catalog("test_catalog")
    databricks_client_stub.add_schema("test_catalog", "schema1")
    databricks_client_stub.add_schema("test_catalog", "schema2")

    # Call the function
    result = list_schemas(databricks_client_stub, "test_catalog")

    # Verify the result
    assert "schemas" in result
    assert len(result["schemas"]) == 2
    schema_names = [s["name"] for s in result["schemas"]]
    assert "schema1" in schema_names
    assert "schema2" in schema_names


def test_list_schemas_all_params(databricks_client_stub):
    """Test listing schemas with all parameters."""
    # Set up stub data
    databricks_client_stub.add_catalog("test_catalog")
    databricks_client_stub.add_schema("test_catalog", "schema1")

    # Call the function with all parameters
    list_schemas(
        databricks_client_stub,
        "test_catalog",
        include_browse=True,
        max_results=5,
        page_token="token123",
    )

    # Verify the call was made with parameters
    assert len(databricks_client_stub.list_schemas_calls) == 1
    call_args = databricks_client_stub.list_schemas_calls[0]
    assert call_args == ("test_catalog", True, 5, "token123")


def test_get_schema(databricks_client_stub):
    """Test getting a specific schema."""
    # Set up stub data
    databricks_client_stub.add_catalog("test_catalog")
    databricks_client_stub.add_schema(
        "test_catalog", "test_schema", comment="Test schema"
    )

    # Call the function
    result = get_schema(databricks_client_stub, "test_catalog.test_schema")

    # Verify the result
    assert result["name"] == "test_schema"
    assert result["catalog_name"] == "test_catalog"
    assert result["comment"] == "Test schema"


def test_list_tables_basic(databricks_client_stub):
    """Test listing tables with basic parameters."""
    # Set up stub data
    databricks_client_stub.add_catalog("test_catalog")
    databricks_client_stub.add_schema("test_catalog", "test_schema")
    databricks_client_stub.add_table("test_catalog", "test_schema", "table1")
    databricks_client_stub.add_table("test_catalog", "test_schema", "table2")

    # Call the function
    result = list_tables(databricks_client_stub, "test_catalog", "test_schema")

    # Verify the result
    assert "tables" in result
    assert len(result["tables"]) == 2
    table_names = [t["name"] for t in result["tables"]]
    assert "table1" in table_names
    assert "table2" in table_names


def test_list_tables_all_params(databricks_client_stub):
    """Test listing tables with all parameters."""
    # Set up stub data
    databricks_client_stub.add_catalog("test_catalog")
    databricks_client_stub.add_schema("test_catalog", "test_schema")
    databricks_client_stub.add_table("test_catalog", "test_schema", "table1")

    # Call the function with all parameters
    list_tables(
        databricks_client_stub,
        "test_catalog",
        "test_schema",
        max_results=10,
        page_token="token123",
        include_delta_metadata=True,
        omit_columns=True,
        omit_properties=True,
        omit_username=True,
        include_browse=True,
        include_manifest_capabilities=True,
    )

    # Verify the call was made with parameters
    assert len(databricks_client_stub.list_tables_calls) == 1
    call_args = databricks_client_stub.list_tables_calls[0]
    expected_args = (
        "test_catalog",
        "test_schema",
        10,
        "token123",
        True,
        True,
        True,
        True,
        True,
        True,
    )
    assert call_args == expected_args


def test_get_table_basic(databricks_client_stub):
    """Test getting a specific table with basic parameters."""
    # Set up stub data
    databricks_client_stub.add_catalog("test_catalog")
    databricks_client_stub.add_schema("test_catalog", "test_schema")
    databricks_client_stub.add_table(
        "test_catalog", "test_schema", "test_table", comment="Test table"
    )

    # Call the function
    result = get_table(databricks_client_stub, "test_catalog.test_schema.test_table")

    # Verify the result
    assert result["name"] == "test_table"
    assert result["catalog_name"] == "test_catalog"
    assert result["schema_name"] == "test_schema"
    assert result["comment"] == "Test table"


def test_get_table_all_params(databricks_client_stub):
    """Test getting a specific table with all parameters."""
    # Set up stub data
    databricks_client_stub.add_catalog("test_catalog")
    databricks_client_stub.add_schema("test_catalog", "test_schema")
    databricks_client_stub.add_table("test_catalog", "test_schema", "test_table")

    # Call the function with all parameters
    get_table(
        databricks_client_stub,
        "test_catalog.test_schema.test_table",
        include_delta_metadata=True,
        include_browse=True,
        include_manifest_capabilities=True,
    )

    # Verify the call was made with parameters
    assert len(databricks_client_stub.get_table_calls) == 1
    call_args = databricks_client_stub.get_table_calls[0]
    assert call_args == ("test_catalog.test_schema.test_table", True, True, True)
