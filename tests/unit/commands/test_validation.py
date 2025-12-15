"""Unit tests for commands/validation.py"""

import pytest
from chuck_data.commands.validation import (
    validate_single_target_params,
    validate_multi_target_params,
    validate_stitch_config_structure,
    validate_provider_required,
    validate_amperity_token,
)


class TestValidateSingleTargetParams:
    """Tests for validate_single_target_params function."""

    def test_valid_catalog_and_schema(self):
        """Test validation with valid catalog and schema."""
        result = validate_single_target_params("my_catalog", "my_schema")
        assert result["valid"] is True
        assert "error" not in result

    def test_missing_catalog(self):
        """Test validation with missing catalog."""
        result = validate_single_target_params(None, "my_schema")
        assert result["valid"] is False
        assert "catalog and schema must be specified" in result["error"]

    def test_missing_schema(self):
        """Test validation with missing schema."""
        result = validate_single_target_params("my_catalog", None)
        assert result["valid"] is False
        assert "catalog and schema must be specified" in result["error"]

    def test_empty_catalog(self):
        """Test validation with empty catalog string."""
        result = validate_single_target_params("", "my_schema")
        assert result["valid"] is False

    def test_empty_schema(self):
        """Test validation with empty schema string."""
        result = validate_single_target_params("my_catalog", "")
        assert result["valid"] is False


class TestValidateMultiTargetParams:
    """Tests for validate_multi_target_params function."""

    def test_valid_single_target(self):
        """Test validation with single valid target."""
        result = validate_multi_target_params(["catalog1.schema1"])
        assert result["valid"] is True
        assert result["target_locations"] == [
            {"catalog": "catalog1", "schema": "schema1"}
        ]
        assert result["output_catalog"] == "catalog1"

    def test_valid_multiple_targets(self):
        """Test validation with multiple valid targets."""
        result = validate_multi_target_params(
            ["catalog1.schema1", "catalog2.schema2", "catalog3.schema3"]
        )
        assert result["valid"] is True
        assert len(result["target_locations"]) == 3
        assert result["target_locations"][0] == {
            "catalog": "catalog1",
            "schema": "schema1",
        }
        assert result["target_locations"][1] == {
            "catalog": "catalog2",
            "schema": "schema2",
        }
        assert result["output_catalog"] == "catalog1"  # First catalog by default

    def test_valid_with_explicit_output_catalog(self):
        """Test validation with explicit output catalog."""
        result = validate_multi_target_params(
            ["catalog1.schema1", "catalog2.schema2"], output_catalog="output_catalog"
        )
        assert result["valid"] is True
        assert result["output_catalog"] == "output_catalog"

    def test_invalid_empty_list(self):
        """Test validation with empty targets list."""
        result = validate_multi_target_params([])
        assert result["valid"] is False
        assert "non-empty list" in result["error"]

    def test_invalid_none_targets(self):
        """Test validation with None targets."""
        result = validate_multi_target_params(None)
        assert result["valid"] is False
        assert "non-empty list" in result["error"]

    def test_invalid_format_missing_dot(self):
        """Test validation with target missing dot separator."""
        result = validate_multi_target_params(["catalog1schema1"])
        assert result["valid"] is False
        assert "Invalid target format" in result["error"]
        assert "catalog1schema1" in result["error"]

    def test_invalid_format_too_many_dots(self):
        """Test validation with target having too many dots."""
        result = validate_multi_target_params(["catalog1.schema1.extra"])
        assert result["valid"] is False
        assert "Invalid target format" in result["error"]

    def test_invalid_format_only_dot(self):
        """Test validation with target being only a dot."""
        result = validate_multi_target_params(["."])
        assert result["valid"] is False
        assert "Invalid target format" in result["error"]

    def test_invalid_non_string_target(self):
        """Test validation with non-string target."""
        result = validate_multi_target_params([123])
        assert result["valid"] is False
        assert "Invalid target type" in result["error"]


class TestValidateStitchConfigStructure:
    """Tests for validate_stitch_config_structure function."""

    def test_valid_minimal_config(self):
        """Test validation with valid minimal configuration."""
        config = {
            "name": "test-stitch",
            "tables": [
                {
                    "path": "catalog.schema.table",
                    "fields": [
                        {
                            "field-name": "email",
                            "type": "STRING",
                            "semantics": ["email"],
                        }
                    ],
                }
            ],
            "settings": {
                "output_catalog_name": "output_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }
        result = validate_stitch_config_structure(config)
        assert result["valid"] is True
        assert "error" not in result

    def test_valid_config_with_empty_semantics(self):
        """Test validation with valid config but empty semantics."""
        config = {
            "name": "test-stitch",
            "tables": [
                {
                    "path": "catalog.schema.table",
                    "fields": [{"field-name": "id", "type": "LONG", "semantics": []}],
                }
            ],
            "settings": {
                "output_catalog_name": "output_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }
        result = validate_stitch_config_structure(config)
        assert result["valid"] is True

    def test_valid_config_with_multiple_tables(self):
        """Test validation with multiple tables."""
        config = {
            "name": "test-stitch",
            "tables": [
                {
                    "path": "catalog.schema.table1",
                    "fields": [
                        {
                            "field-name": "email",
                            "type": "STRING",
                            "semantics": ["email"],
                        }
                    ],
                },
                {
                    "path": "catalog.schema.table2",
                    "fields": [
                        {
                            "field-name": "phone",
                            "type": "STRING",
                            "semantics": ["phone"],
                        }
                    ],
                },
            ],
            "settings": {
                "output_catalog_name": "output_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }
        result = validate_stitch_config_structure(config)
        assert result["valid"] is True

    def test_invalid_not_dict(self):
        """Test validation with non-dict config."""
        result = validate_stitch_config_structure("not a dict")
        assert result["valid"] is False
        assert "must be a dictionary" in result["error"]

    def test_invalid_missing_name(self):
        """Test validation with missing 'name' key."""
        config = {
            "tables": [],
            "settings": {
                "output_catalog_name": "output_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }
        result = validate_stitch_config_structure(config)
        assert result["valid"] is False
        assert "missing required key: name" in result["error"]

    def test_invalid_missing_tables(self):
        """Test validation with missing 'tables' key."""
        config = {
            "name": "test-stitch",
            "settings": {
                "output_catalog_name": "output_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }
        result = validate_stitch_config_structure(config)
        assert result["valid"] is False
        assert "missing required key: tables" in result["error"]

    def test_invalid_missing_settings(self):
        """Test validation with missing 'settings' key."""
        config = {"name": "test-stitch", "tables": []}
        result = validate_stitch_config_structure(config)
        assert result["valid"] is False
        assert "missing required key: settings" in result["error"]

    def test_invalid_tables_not_list(self):
        """Test validation with 'tables' not being a list."""
        config = {
            "name": "test-stitch",
            "tables": "not a list",
            "settings": {
                "output_catalog_name": "output_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }
        result = validate_stitch_config_structure(config)
        assert result["valid"] is False
        assert "'tables' must be a list" in result["error"]

    def test_invalid_table_not_dict(self):
        """Test validation with table not being a dict."""
        config = {
            "name": "test-stitch",
            "tables": ["not a dict"],
            "settings": {
                "output_catalog_name": "output_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }
        result = validate_stitch_config_structure(config)
        assert result["valid"] is False
        assert "must be a dictionary" in result["error"]

    def test_invalid_table_missing_path(self):
        """Test validation with table missing 'path'."""
        config = {
            "name": "test-stitch",
            "tables": [{"fields": []}],
            "settings": {
                "output_catalog_name": "output_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }
        result = validate_stitch_config_structure(config)
        assert result["valid"] is False
        assert "missing 'path'" in result["error"]

    def test_invalid_table_missing_fields(self):
        """Test validation with table missing 'fields'."""
        config = {
            "name": "test-stitch",
            "tables": [{"path": "catalog.schema.table"}],
            "settings": {
                "output_catalog_name": "output_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }
        result = validate_stitch_config_structure(config)
        assert result["valid"] is False
        assert "missing 'fields'" in result["error"]

    def test_invalid_fields_not_list(self):
        """Test validation with 'fields' not being a list."""
        config = {
            "name": "test-stitch",
            "tables": [{"path": "catalog.schema.table", "fields": "not a list"}],
            "settings": {
                "output_catalog_name": "output_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }
        result = validate_stitch_config_structure(config)
        assert result["valid"] is False
        assert "'fields' must be a list" in result["error"]

    def test_invalid_field_not_dict(self):
        """Test validation with field not being a dict."""
        config = {
            "name": "test-stitch",
            "tables": [{"path": "catalog.schema.table", "fields": ["not a dict"]}],
            "settings": {
                "output_catalog_name": "output_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }
        result = validate_stitch_config_structure(config)
        assert result["valid"] is False
        assert "must be a dictionary" in result["error"]

    def test_invalid_field_missing_field_name(self):
        """Test validation with field missing 'field-name'."""
        config = {
            "name": "test-stitch",
            "tables": [
                {
                    "path": "catalog.schema.table",
                    "fields": [{"type": "STRING", "semantics": []}],
                }
            ],
            "settings": {
                "output_catalog_name": "output_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }
        result = validate_stitch_config_structure(config)
        assert result["valid"] is False
        assert "missing required key: field-name" in result["error"]

    def test_invalid_field_missing_type(self):
        """Test validation with field missing 'type'."""
        config = {
            "name": "test-stitch",
            "tables": [
                {
                    "path": "catalog.schema.table",
                    "fields": [{"field-name": "email", "semantics": []}],
                }
            ],
            "settings": {
                "output_catalog_name": "output_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }
        result = validate_stitch_config_structure(config)
        assert result["valid"] is False
        assert "missing required key: type" in result["error"]

    def test_invalid_field_missing_semantics(self):
        """Test validation with field missing 'semantics'."""
        config = {
            "name": "test-stitch",
            "tables": [
                {
                    "path": "catalog.schema.table",
                    "fields": [{"field-name": "email", "type": "STRING"}],
                }
            ],
            "settings": {
                "output_catalog_name": "output_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }
        result = validate_stitch_config_structure(config)
        assert result["valid"] is False
        assert "missing required key: semantics" in result["error"]

    def test_invalid_semantics_not_list(self):
        """Test validation with 'semantics' not being a list."""
        config = {
            "name": "test-stitch",
            "tables": [
                {
                    "path": "catalog.schema.table",
                    "fields": [
                        {"field-name": "email", "type": "STRING", "semantics": "email"}
                    ],
                }
            ],
            "settings": {
                "output_catalog_name": "output_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }
        result = validate_stitch_config_structure(config)
        assert result["valid"] is False
        assert "'semantics' must be a list" in result["error"]

    def test_invalid_settings_not_dict(self):
        """Test validation with 'settings' not being a dict."""
        config = {
            "name": "test-stitch",
            "tables": [],
            "settings": "not a dict",
        }
        result = validate_stitch_config_structure(config)
        assert result["valid"] is False
        assert "'settings' must be a dictionary" in result["error"]

    def test_invalid_settings_missing_output_catalog(self):
        """Test validation with settings missing 'output_catalog_name'."""
        config = {
            "name": "test-stitch",
            "tables": [],
            "settings": {"output_schema_name": "stitch_outputs"},
        }
        result = validate_stitch_config_structure(config)
        assert result["valid"] is False
        assert "missing required key: output_catalog_name" in result["error"]

    def test_invalid_settings_missing_output_schema(self):
        """Test validation with settings missing 'output_schema_name'."""
        config = {
            "name": "test-stitch",
            "tables": [],
            "settings": {"output_catalog_name": "output_catalog"},
        }
        result = validate_stitch_config_structure(config)
        assert result["valid"] is False
        assert "missing required key: output_schema_name" in result["error"]


class TestValidateProviderRequired:
    """Tests for validate_provider_required function."""

    def test_valid_both_providers_present(self):
        """Test validation with both providers present."""
        mock_data_provider = object()
        mock_compute_provider = object()
        result = validate_provider_required(mock_data_provider, mock_compute_provider)
        assert result["valid"] is True
        assert "error" not in result

    def test_invalid_missing_data_provider(self):
        """Test validation with missing data provider."""
        mock_compute_provider = object()
        result = validate_provider_required(None, mock_compute_provider)
        assert result["valid"] is False
        assert "Data provider is required" in result["error"]

    def test_invalid_missing_compute_provider(self):
        """Test validation with missing compute provider."""
        mock_data_provider = object()
        result = validate_provider_required(mock_data_provider, None)
        assert result["valid"] is False
        assert "Compute provider is required" in result["error"]

    def test_invalid_both_providers_missing(self):
        """Test validation with both providers missing."""
        result = validate_provider_required(None, None)
        assert result["valid"] is False
        assert "Data provider is required" in result["error"]


class TestValidateAmperityToken:
    """Tests for validate_amperity_token function."""

    def test_valid_token(self):
        """Test validation with valid token."""
        result = validate_amperity_token("valid-token-123")
        assert result["valid"] is True
        assert "error" not in result

    def test_invalid_none_token(self):
        """Test validation with None token."""
        result = validate_amperity_token(None)
        assert result["valid"] is False
        assert "Amperity token not found" in result["error"]
        assert "/amp_login" in result["error"]

    def test_invalid_empty_token(self):
        """Test validation with empty token."""
        result = validate_amperity_token("")
        assert result["valid"] is False
        assert "Amperity token not found" in result["error"]

    def test_invalid_whitespace_token(self):
        """Test validation with whitespace-only token."""
        result = validate_amperity_token("   ")
        assert result["valid"] is False
        assert "Amperity token not found" in result["error"]

    def test_invalid_non_string_token(self):
        """Test validation with non-string token."""
        result = validate_amperity_token(12345)
        assert result["valid"] is False
        assert "Amperity token not found" in result["error"]
