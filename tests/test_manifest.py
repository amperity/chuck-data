"""
Tests for manifest generation functionality.
"""

import json
import pytest
from chuck_data.storage.manifest import (
    generate_manifest_from_scan,
    validate_manifest,
    _normalize_type,
)


def test_generate_manifest_basic():
    """Test basic manifest generation from scan results."""
    scan_results = {
        "catalog": "dev",
        "schema": "public",
        "tables_with_pii": 1,
        "total_pii_columns": 3,
        "results_detail": [
            {
                "table_name": "customers",
                "full_name": "dev.public.customers",
                "has_pii": True,
                "pii_columns": [
                    {"name": "email", "type": "varchar", "semantic": "email"},
                    {"name": "phone", "type": "varchar", "semantic": "phone"},
                    {"name": "first_name", "type": "varchar", "semantic": "given-name"},
                ],
                "columns": [
                    {"name": "id", "type": "bigint", "semantic": None},
                    {"name": "email", "type": "varchar", "semantic": "email"},
                    {"name": "phone", "type": "varchar", "semantic": "phone"},
                    {"name": "first_name", "type": "varchar", "semantic": "given-name"},
                ],
            }
        ],
    }

    redshift_config = {
        "host": "test-cluster.us-west-2.redshift.amazonaws.com",
        "port": 5439,
        "database": "dev",
        "schema": "public",
        "user": "testuser",
        "password": "testpass",
    }

    s3_config = {
        "temp_dir": "s3://test-bucket/temp/",
        "iam_role": "arn:aws:iam::123456789:role/TestRole",
    }

    manifest = generate_manifest_from_scan(scan_results, redshift_config, s3_config)

    # Verify structure
    assert "tables" in manifest
    assert "settings" in manifest

    # Verify tables
    assert len(manifest["tables"]) == 1
    table = manifest["tables"][0]
    assert table["path"] == "customers"
    assert "fields" in table

    # Verify all columns are included (not just PII)
    fields = table["fields"]
    assert len(fields) == 4  # id, email, phone, first_name

    # Check field with semantic tag
    email_field = next(f for f in fields if f["field-name"] == "email")
    assert email_field["type"] == "string"
    assert "email" in email_field["semantics"]
    assert "pii" in email_field["semantics"]

    # Check field without semantic tag
    id_field = next(f for f in fields if f["field-name"] == "id")
    assert id_field["type"] == "long"
    assert id_field["semantics"] == []

    # Verify settings
    settings = manifest["settings"]
    assert settings["s3_temp_dir"] == s3_config["temp_dir"]
    assert settings["redshift_iam_role"] == s3_config["iam_role"]
    assert settings["redshift_config"]["host"] == redshift_config["host"]
    assert settings["redshift_config"]["database"] == redshift_config["database"]


def test_generate_manifest_multiple_tables():
    """Test manifest generation with multiple tables."""
    scan_results = {
        "catalog": "dev",
        "schema": "public",
        "tables_with_pii": 2,
        "total_pii_columns": 4,
        "results_detail": [
            {
                "table_name": "customers",
                "full_name": "dev.public.customers",
                "has_pii": True,
                "columns": [
                    {"name": "email", "type": "varchar", "semantic": "email"},
                    {"name": "phone", "type": "varchar", "semantic": "phone"},
                ],
            },
            {
                "table_name": "orders",
                "full_name": "dev.public.orders",
                "has_pii": True,
                "columns": [
                    {"name": "customer_email", "type": "varchar", "semantic": "email"},
                    {
                        "name": "shipping_address",
                        "type": "varchar",
                        "semantic": "address",
                    },
                ],
            },
        ],
    }

    redshift_config = {
        "host": "test.redshift.amazonaws.com",
        "database": "dev",
        "schema": "public",
        "user": "testuser",
    }

    s3_config = {
        "temp_dir": "s3://bucket/temp/",
        "iam_role": "arn:aws:iam::123:role/Role",
    }

    manifest = generate_manifest_from_scan(scan_results, redshift_config, s3_config)

    assert len(manifest["tables"]) == 2
    assert manifest["tables"][0]["path"] == "customers"
    assert manifest["tables"][1]["path"] == "orders"


def test_generate_manifest_skips_non_pii_tables():
    """Test that tables without PII are skipped."""
    scan_results = {
        "catalog": "dev",
        "schema": "public",
        "tables_with_pii": 1,
        "total_pii_columns": 1,
        "results_detail": [
            {
                "table_name": "customers",
                "full_name": "dev.public.customers",
                "has_pii": True,
                "columns": [{"name": "email", "type": "varchar", "semantic": "email"}],
            },
            {
                "table_name": "products",
                "full_name": "dev.public.products",
                "has_pii": False,
                "columns": [
                    {"name": "product_id", "type": "bigint", "semantic": None},
                    {"name": "name", "type": "varchar", "semantic": None},
                ],
            },
        ],
    }

    redshift_config = {
        "host": "test.redshift.amazonaws.com",
        "database": "dev",
        "schema": "public",
        "user": "testuser",
    }

    s3_config = {
        "temp_dir": "s3://bucket/temp/",
        "iam_role": "arn:aws:iam::123:role/Role",
    }

    manifest = generate_manifest_from_scan(scan_results, redshift_config, s3_config)

    # Only the customers table should be in manifest
    assert len(manifest["tables"]) == 1
    assert manifest["tables"][0]["path"] == "customers"


def test_generate_manifest_skips_errors():
    """Test that tables with errors are skipped."""
    scan_results = {
        "catalog": "dev",
        "schema": "public",
        "tables_with_pii": 1,
        "total_pii_columns": 1,
        "results_detail": [
            {
                "table_name": "customers",
                "full_name": "dev.public.customers",
                "has_pii": True,
                "columns": [{"name": "email", "type": "varchar", "semantic": "email"}],
            },
            {
                "table_name": "broken_table",
                "error": "Failed to retrieve table",
                "skipped": True,
            },
        ],
    }

    redshift_config = {
        "host": "test.redshift.amazonaws.com",
        "database": "dev",
        "schema": "public",
        "user": "testuser",
    }

    s3_config = {
        "temp_dir": "s3://bucket/temp/",
        "iam_role": "arn:aws:iam::123:role/Role",
    }

    manifest = generate_manifest_from_scan(scan_results, redshift_config, s3_config)

    assert len(manifest["tables"]) == 1
    assert manifest["tables"][0]["path"] == "customers"


def test_normalize_type():
    """Test type normalization."""
    assert _normalize_type("varchar") == "string"
    assert _normalize_type("VARCHAR(255)") == "string"
    assert _normalize_type("text") == "string"
    assert _normalize_type("bigint") == "long"
    assert _normalize_type("INTEGER") == "long"
    assert _normalize_type("double precision") == "double"
    assert _normalize_type("DECIMAL(10,2)") == "decimal"
    assert _normalize_type("boolean") == "boolean"
    assert _normalize_type("date") == "date"
    assert _normalize_type("timestamp") == "timestamp"
    assert _normalize_type("unknown_type") == "string"  # Default


def test_validate_manifest_valid():
    """Test validation of a valid manifest."""
    manifest = {
        "tables": [
            {
                "path": "customers",
                "fields": [
                    {
                        "field-name": "email",
                        "type": "string",
                        "semantics": ["email", "pii"],
                    }
                ],
            }
        ],
        "settings": {
            "redshift_config": {
                "host": "test.redshift.amazonaws.com",
                "port": 5439,
                "database": "dev",
                "schema": "public",
                "user": "testuser",
            },
            "s3_temp_dir": "s3://bucket/temp/",
            "redshift_iam_role": "arn:aws:iam::123:role/Role",
        },
    }

    is_valid, error = validate_manifest(manifest)
    assert is_valid
    assert error is None


def test_validate_manifest_missing_tables():
    """Test validation fails when tables key is missing."""
    manifest = {
        "settings": {
            "redshift_config": {
                "host": "test.redshift.amazonaws.com",
                "database": "dev",
                "schema": "public",
                "user": "testuser",
            },
            "s3_temp_dir": "s3://bucket/temp/",
            "redshift_iam_role": "arn:aws:iam::123:role/Role",
        }
    }

    is_valid, error = validate_manifest(manifest)
    assert not is_valid
    assert "tables" in error


def test_validate_manifest_missing_settings():
    """Test validation fails when settings key is missing."""
    manifest = {"tables": []}

    is_valid, error = validate_manifest(manifest)
    assert not is_valid
    assert "settings" in error


def test_validate_manifest_invalid_table_structure():
    """Test validation fails with invalid table structure."""
    manifest = {
        "tables": [
            {
                "path": "customers",
                # Missing "fields" key
            }
        ],
        "settings": {
            "redshift_config": {
                "host": "test.redshift.amazonaws.com",
                "database": "dev",
                "schema": "public",
                "user": "testuser",
            },
            "s3_temp_dir": "s3://bucket/temp/",
            "redshift_iam_role": "arn:aws:iam::123:role/Role",
        },
    }

    is_valid, error = validate_manifest(manifest)
    assert not is_valid
    assert "fields" in error


def test_validate_manifest_invalid_field_structure():
    """Test validation fails with invalid field structure."""
    manifest = {
        "tables": [
            {
                "path": "customers",
                "fields": [
                    {
                        "field-name": "email",
                        # Missing "type" and "semantics"
                    }
                ],
            }
        ],
        "settings": {
            "redshift_config": {
                "host": "test.redshift.amazonaws.com",
                "database": "dev",
                "schema": "public",
                "user": "testuser",
            },
            "s3_temp_dir": "s3://bucket/temp/",
            "redshift_iam_role": "arn:aws:iam::123:role/Role",
        },
    }

    is_valid, error = validate_manifest(manifest)
    assert not is_valid
    assert "type" in error or "semantics" in error


def test_validate_manifest_serverless_iam_auth():
    """Test validation succeeds for Serverless with IAM auth (no host/user)."""
    manifest = {
        "tables": [
            {
                "path": "customers",
                "fields": [
                    {
                        "field-name": "email",
                        "type": "string",
                        "semantics": ["email", "pii"],
                    }
                ],
            }
        ],
        "settings": {
            "redshift_config": {
                "database": "dev",
                "schema": "public",
                # No host/port/user for Serverless with IAM auth
            },
            "s3_temp_dir": "s3://bucket/temp/",
            "redshift_iam_role": "arn:aws:iam::123:role/Role",
        },
    }

    is_valid, error = validate_manifest(manifest)
    assert is_valid, f"Validation failed: {error}"
    assert error is None


def test_manifest_json_serializable():
    """Test that generated manifest is JSON serializable."""
    scan_results = {
        "catalog": "dev",
        "schema": "public",
        "tables_with_pii": 1,
        "total_pii_columns": 1,
        "results_detail": [
            {
                "table_name": "customers",
                "full_name": "dev.public.customers",
                "has_pii": True,
                "columns": [{"name": "email", "type": "varchar", "semantic": "email"}],
            }
        ],
    }

    redshift_config = {
        "host": "test.redshift.amazonaws.com",
        "database": "dev",
        "schema": "public",
        "user": "testuser",
    }

    s3_config = {
        "temp_dir": "s3://bucket/temp/",
        "iam_role": "arn:aws:iam::123:role/Role",
    }

    manifest = generate_manifest_from_scan(scan_results, redshift_config, s3_config)

    # Should not raise exception
    json_str = json.dumps(manifest)
    assert json_str is not None

    # Should be able to parse back
    parsed = json.loads(json_str)
    assert parsed == manifest
