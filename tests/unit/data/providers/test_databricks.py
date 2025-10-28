"""Tests for Databricks Data Provider."""

import pytest
import tempfile
from unittest.mock import patch
from chuck_data.data.providers.databricks import DatabricksDataProvider
from chuck_data.data.provider import DataProvider
from chuck_data.config import ConfigManager
from tests.fixtures.databricks import DatabricksDataProviderStub


class TestDatabricksDataProvider:
    """Tests for DatabricksDataProvider."""

    def test_databricks_provider_implements_protocol(self):
        """DatabricksDataProvider implements DataProvider protocol."""
        stub = DatabricksDataProviderStub()
        assert isinstance(stub, DataProvider)

    def test_databricks_provider_validates_connection(self):
        """Provider validates Databricks connection."""
        stub = DatabricksDataProviderStub()
        stub.set_connection_valid(True)

        assert stub.validate_connection() is True

        stub.set_connection_valid(False)
        assert stub.validate_connection() is False

    def test_databricks_provider_lists_catalogs(self):
        """Provider lists Unity Catalog catalogs."""
        stub = DatabricksDataProviderStub()
        stub.add_catalog("test_catalog", catalog_type="MANAGED")
        stub.add_catalog("prod_catalog", catalog_type="MANAGED")

        catalogs = stub.list_catalogs()
        assert len(catalogs) == 2
        catalog_names = [c["name"] for c in catalogs]
        assert "test_catalog" in catalog_names
        assert "prod_catalog" in catalog_names

    def test_databricks_provider_gets_catalog_by_name(self):
        """Provider gets catalog by name."""
        stub = DatabricksDataProviderStub()
        stub.add_catalog("test_catalog", catalog_type="MANAGED")

        catalog = stub.get_catalog("test_catalog")
        assert catalog is not None
        assert catalog["name"] == "test_catalog"
        assert catalog["catalog_type"] == "MANAGED"

        # Non-existent catalog returns None
        assert stub.get_catalog("nonexistent") is None

    def test_databricks_provider_lists_schemas(self):
        """Provider lists schemas in a catalog."""
        stub = DatabricksDataProviderStub()
        stub.add_catalog("test_catalog")
        stub.add_schema("schema1", "test_catalog")
        stub.add_schema("schema2", "test_catalog")

        schemas = stub.list_schemas("test_catalog")
        assert len(schemas) == 2
        schema_names = [s["name"] for s in schemas]
        assert "schema1" in schema_names
        assert "schema2" in schema_names

    def test_databricks_provider_gets_schema_by_name(self):
        """Provider gets schema by name."""
        stub = DatabricksDataProviderStub()
        stub.add_catalog("test_catalog")
        stub.add_schema("test_schema", "test_catalog")

        schema = stub.get_schema("test_catalog", "test_schema")
        assert schema is not None
        assert schema["name"] == "test_schema"
        assert schema["catalog_name"] == "test_catalog"

        # Non-existent schema returns None
        assert stub.get_schema("test_catalog", "nonexistent") is None

    def test_databricks_provider_lists_tables(self):
        """Provider lists tables in a schema."""
        stub = DatabricksDataProviderStub()
        stub.add_catalog("test_catalog")
        stub.add_schema("test_schema", "test_catalog")
        stub.add_table("table1", "test_schema", "test_catalog")
        stub.add_table("table2", "test_schema", "test_catalog")

        tables = stub.list_tables("test_catalog", "test_schema")
        assert len(tables) == 2
        table_names = [t["name"] for t in tables]
        assert "table1" in table_names
        assert "table2" in table_names

    def test_databricks_provider_gets_table_by_name(self):
        """Provider gets table by name."""
        stub = DatabricksDataProviderStub()
        stub.add_catalog("test_catalog")
        stub.add_schema("test_schema", "test_catalog")
        stub.add_table("test_table", "test_schema", "test_catalog", table_type="MANAGED")

        table = stub.get_table("test_catalog", "test_schema", "test_table")
        assert table is not None
        assert table["name"] == "test_table"
        assert table["schema_name"] == "test_schema"
        assert table["catalog_name"] == "test_catalog"

        # Non-existent table returns None
        assert stub.get_table("test_catalog", "test_schema", "nonexistent") is None

    def test_databricks_provider_executes_query(self):
        """Provider executes SQL queries."""
        stub = DatabricksDataProviderStub()

        result = stub.execute_query("SELECT * FROM table", warehouse_id="test-warehouse")
        assert result is not None
        assert "status" in result or "state" in result

    def test_databricks_provider_returns_provider_name(self):
        """Provider returns 'databricks' as name."""
        stub = DatabricksDataProviderStub()
        assert stub.get_provider_name() == "databricks"

    def test_databricks_provider_returns_capabilities(self):
        """Provider returns Databricks capabilities."""
        stub = DatabricksDataProviderStub()
        capabilities = stub.get_capabilities()

        assert capabilities["supports_sql"] is True
        assert capabilities["supports_catalogs"] is True
        assert capabilities["supports_schemas"] is True
        assert capabilities["supports_tables"] is True
        assert capabilities["supports_volumes"] is True
        assert capabilities["supports_models"] is True
        assert capabilities["supports_jobs"] is True
        assert capabilities["supports_warehouses"] is True

    def test_databricks_provider_raises_error_without_credentials(self):
        """Provider raises ValueError when credentials are not available."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            # Empty config - no credentials
            f.write('{}')
            temp_path = f.name

        with patch("chuck_data.data.providers.databricks.get_workspace_url", return_value=None):
            with patch("chuck_data.data.providers.databricks.get_databricks_token", return_value=None):
                with pytest.raises(ValueError, match="Databricks workspace URL and token required"):
                    DatabricksDataProvider()

    def test_databricks_provider_uses_config_credentials(self):
        """Provider uses credentials from config when not provided."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('''{
                "workspace_url": "https://test.databricks.com",
                "databricks_token": "test-token"
            }''')
            temp_path = f.name

        with patch("chuck_data.data.providers.databricks.get_workspace_url", return_value="https://test.databricks.com"):
            with patch("chuck_data.data.providers.databricks.get_databricks_token", return_value="test-token"):
                with patch("chuck_data.data.providers.databricks.DatabricksAPIClient") as mock_client:
                    provider = DatabricksDataProvider()
                    # Should have created client with config credentials
                    mock_client.assert_called_once_with("https://test.databricks.com", "test-token")

    def test_databricks_provider_accepts_explicit_credentials(self):
        """Provider accepts explicit credentials over config."""
        with patch("chuck_data.data.providers.databricks.DatabricksAPIClient") as mock_client:
            provider = DatabricksDataProvider(
                workspace_url="https://explicit.databricks.com",
                token="explicit-token"
            )
            # Should have created client with explicit credentials
            mock_client.assert_called_once_with("https://explicit.databricks.com", "explicit-token")

    def test_databricks_provider_accepts_injected_client(self):
        """Provider accepts injected client for testing."""
        stub = DatabricksDataProviderStub()
        # Should not raise error and should work
        assert stub.get_provider_name() == "databricks"

    def test_databricks_provider_reset_clears_state(self):
        """Provider stub reset clears all test data."""
        stub = DatabricksDataProviderStub()
        stub.add_catalog("test_catalog")
        stub.add_schema("test_schema", "test_catalog")

        assert len(stub.list_catalogs()) == 1
        assert len(stub.list_schemas("test_catalog")) == 1

        stub.reset()

        assert len(stub.list_catalogs()) == 0
        assert len(stub.list_schemas("test_catalog")) == 0

    def test_databricks_provider_supports_volumes(self):
        """Provider supports Databricks-specific volume operations."""
        stub = DatabricksDataProviderStub()
        stub.add_catalog("test_catalog")
        stub.add_schema("test_schema", "test_catalog")
        stub.add_volume("test_volume", "test_catalog", "test_schema")

        volumes = stub.list_volumes("test_catalog", "test_schema")
        assert len(volumes) == 1
        assert volumes[0]["name"] == "test_volume"

    def test_databricks_provider_supports_models(self):
        """Provider supports Databricks-specific model operations."""
        stub = DatabricksDataProviderStub()
        stub.add_model("test-model", state="READY")

        models = stub.list_models()
        assert len(models) == 1
        assert models[0]["name"] == "test-model"

    def test_databricks_provider_supports_warehouses(self):
        """Provider supports Databricks-specific warehouse operations."""
        stub = DatabricksDataProviderStub()
        stub.add_warehouse("warehouse-123", "Test Warehouse")

        warehouses = stub.list_warehouses()
        assert len(warehouses) == 1
        assert warehouses[0]["name"] == "Test Warehouse"
