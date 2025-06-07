"""
Tests for stitch_tools command handler utilities.

This module contains tests for the Stitch integration utilities.
"""

import pytest
from unittest.mock import patch, MagicMock

from chuck_data.commands.stitch_tools import _helper_setup_stitch_logic
from tests.fixtures.llm import LLMClientStub


@pytest.fixture
def client():
    """Mock client fixture."""
    return MagicMock()


@pytest.fixture
def llm_client():
    """LLM client stub fixture."""
    return LLMClientStub()


@pytest.fixture
def mock_pii_scan_results():
    """Mock successful PII scan result fixture."""
    return {
        "tables_successfully_processed": 5,
        "tables_with_pii": 3,
        "total_pii_columns": 8,
        "results_detail": [
            {
                "full_name": "test_catalog.test_schema.customers",
                "has_pii": True,
                "skipped": False,
                "columns": [
                    {"name": "id", "type": "int", "semantic": None},
                    {"name": "name", "type": "string", "semantic": "full-name"},
                    {"name": "email", "type": "string", "semantic": "email"},
                ],
            },
            {
                "full_name": "test_catalog.test_schema.orders",
                "has_pii": True,
                "skipped": False,
                "columns": [
                    {"name": "id", "type": "int", "semantic": None},
                    {"name": "customer_id", "type": "int", "semantic": None},
                    {
                        "name": "shipping_address",
                        "type": "string",
                        "semantic": "address",
                    },
                ],
            },
            {
                "full_name": "test_catalog.test_schema.metrics",
                "has_pii": False,
                "skipped": False,
                "columns": [
                    {"name": "id", "type": "int", "semantic": None},
                    {"name": "date", "type": "date", "semantic": None},
                ],
            },
        ],
    }


@pytest.fixture
def mock_pii_scan_results_with_unsupported():
    """Mock PII scan results with unsupported types fixture."""
    return {
        "tables_successfully_processed": 2,
        "tables_with_pii": 2,
        "total_pii_columns": 4,
        "results_detail": [
            {
                "full_name": "test_catalog.test_schema.customers",
                "has_pii": True,
                "skipped": False,
                "columns": [
                    {"name": "id", "type": "int", "semantic": None},
                    {"name": "name", "type": "string", "semantic": "full-name"},
                    {
                        "name": "metadata",
                        "type": "STRUCT",
                        "semantic": None,
                    },  # Unsupported
                    {
                        "name": "tags",
                        "type": "ARRAY",
                        "semantic": None,
                    },  # Unsupported
                ],
            },
            {
                "full_name": "test_catalog.test_schema.geo_data",
                "has_pii": True,
                "skipped": False,
                "columns": [
                    {
                        "name": "location",
                        "type": "GEOGRAPHY",
                        "semantic": "address",
                    },  # Unsupported
                    {
                        "name": "geometry",
                        "type": "GEOMETRY",
                        "semantic": None,
                    },  # Unsupported
                    {
                        "name": "properties",
                        "type": "MAP",
                        "semantic": None,
                    },  # Unsupported
                    {
                        "name": "description",
                        "type": "string",
                        "semantic": "full-name",
                    },
                ],
            },
        ],
    }


def test_missing_params(client, llm_client):
    """Test handling when parameters are missing."""
    result = _helper_setup_stitch_logic(client, llm_client, "", "test_schema")
    assert "error" in result
    assert "Target catalog and schema are required" in result["error"]


@patch("chuck_data.commands.stitch_tools._helper_scan_schema_for_pii_logic")
def test_pii_scan_error(mock_scan_pii, client, llm_client):
    """Test handling when PII scan returns an error."""
    # Setup mock
    mock_scan_pii.return_value = {"error": "Failed to access tables"}

    # Call function
    result = _helper_setup_stitch_logic(
        client, llm_client, "test_catalog", "test_schema"
    )

    # Verify results
    assert "error" in result
    assert "PII Scan failed during Stitch setup" in result["error"]


@patch("chuck_data.commands.stitch_tools._helper_scan_schema_for_pii_logic")
def test_volume_list_error(mock_scan_pii, client, llm_client, mock_pii_scan_results):
    """Test handling when listing volumes fails."""
    # Setup mocks
    mock_scan_pii.return_value = mock_pii_scan_results
    client.list_volumes.side_effect = Exception("API Error")

    # Call function
    result = _helper_setup_stitch_logic(
        client, llm_client, "test_catalog", "test_schema"
    )

    # Verify results
    assert "error" in result
    assert "Failed to list volumes" in result["error"]


@patch("chuck_data.commands.stitch_tools._helper_scan_schema_for_pii_logic")
def test_volume_create_error(mock_scan_pii, client, llm_client, mock_pii_scan_results):
    """Test handling when creating volume fails."""
    # Setup mocks
    mock_scan_pii.return_value = mock_pii_scan_results
    client.list_volumes.return_value = {
        "volumes": []
    }  # Empty list, volume doesn't exist
    client.create_volume.return_value = None  # Creation failed

    # Call function
    result = _helper_setup_stitch_logic(
        client, llm_client, "test_catalog", "test_schema"
    )

    # Verify results
    assert "error" in result
    assert "Failed to create volume 'chuck'" in result["error"]


@patch("chuck_data.commands.stitch_tools._helper_scan_schema_for_pii_logic")
def test_no_tables_with_pii(mock_scan_pii, client, llm_client, mock_pii_scan_results):
    """Test handling when no tables with PII are found."""
    # Setup mocks
    no_pii_results = mock_pii_scan_results.copy()
    # Override results_detail with no tables that have PII
    no_pii_results["results_detail"] = [
        {
            "full_name": "test_catalog.test_schema.metrics",
            "has_pii": False,
            "skipped": False,
            "columns": [{"name": "id", "type": "int", "semantic": None}],
        }
    ]
    mock_scan_pii.return_value = no_pii_results
    client.list_volumes.return_value = {"volumes": [{"name": "chuck"}]}  # Volume exists

    # Call function
    result = _helper_setup_stitch_logic(
        client, llm_client, "test_catalog", "test_schema"
    )

    # Verify results
    assert "error" in result
    assert "No tables with PII found" in result["error"]


@patch("chuck_data.commands.stitch_tools._helper_scan_schema_for_pii_logic")
@patch("chuck_data.commands.stitch_tools.get_amperity_token")
def test_missing_amperity_token(
    mock_get_amperity_token, mock_scan_pii, client, llm_client, mock_pii_scan_results
):
    """Test handling when Amperity token is missing."""
    # Setup mocks
    mock_scan_pii.return_value = mock_pii_scan_results
    client.list_volumes.return_value = {"volumes": [{"name": "chuck"}]}  # Volume exists
    client.upload_file.return_value = True  # Config file upload successful
    mock_get_amperity_token.return_value = None  # No token

    # Call function
    result = _helper_setup_stitch_logic(
        client, llm_client, "test_catalog", "test_schema"
    )

    # Verify results
    assert "error" in result
    assert "Amperity token not found" in result["error"]


@patch("chuck_data.commands.stitch_tools._helper_scan_schema_for_pii_logic")
@patch("chuck_data.commands.stitch_tools.get_amperity_token")
def test_amperity_init_script_error(
    mock_get_amperity_token, mock_scan_pii, client, llm_client, mock_pii_scan_results
):
    """Test handling when fetching Amperity init script fails."""
    # Setup mocks
    mock_scan_pii.return_value = mock_pii_scan_results
    client.list_volumes.return_value = {"volumes": [{"name": "chuck"}]}  # Volume exists
    client.upload_file.return_value = True  # Config file upload successful
    mock_get_amperity_token.return_value = "fake_token"
    client.fetch_amperity_job_init.side_effect = Exception("API Error")

    # Call function
    result = _helper_setup_stitch_logic(
        client, llm_client, "test_catalog", "test_schema"
    )

    # Verify results
    assert "error" in result
    assert "Error fetching Amperity init script" in result["error"]


@patch("chuck_data.commands.stitch_tools._helper_scan_schema_for_pii_logic")
@patch("chuck_data.commands.stitch_tools.get_amperity_token")
@patch("chuck_data.commands.stitch_tools._helper_upload_cluster_init_logic")
def test_versioned_init_script_upload_error(
    mock_upload_init,
    mock_get_amperity_token,
    mock_scan_pii,
    client,
    llm_client,
    mock_pii_scan_results,
):
    """Test handling when versioned init script upload fails."""
    # Setup mocks
    mock_scan_pii.return_value = mock_pii_scan_results
    client.list_volumes.return_value = {"volumes": [{"name": "chuck"}]}  # Volume exists
    mock_get_amperity_token.return_value = "fake_token"
    client.fetch_amperity_job_init.return_value = {"cluster-init": "echo 'init script'"}
    # Mock versioned init script upload failure
    mock_upload_init.return_value = {"error": "Failed to upload versioned init script"}

    # Call function
    result = _helper_setup_stitch_logic(
        client, llm_client, "test_catalog", "test_schema"
    )

    # Verify results
    assert "error" in result
    assert result["error"] == "Failed to upload versioned init script"


@patch("chuck_data.commands.stitch_tools._helper_scan_schema_for_pii_logic")
@patch("chuck_data.commands.stitch_tools.get_amperity_token")
@patch("chuck_data.commands.stitch_tools._helper_upload_cluster_init_logic")
def test_successful_setup(
    mock_upload_init,
    mock_get_amperity_token,
    mock_scan_pii,
    client,
    llm_client,
    mock_pii_scan_results,
):
    """Test successful Stitch integration setup with versioned init script."""
    # Setup mocks
    mock_scan_pii.return_value = mock_pii_scan_results
    client.list_volumes.return_value = {"volumes": [{"name": "chuck"}]}  # Volume exists
    client.upload_file.return_value = True  # File uploads successful
    mock_get_amperity_token.return_value = "fake_token"
    client.fetch_amperity_job_init.return_value = {"cluster-init": "echo 'init script'"}
    # Mock versioned init script upload
    mock_upload_init.return_value = {
        "success": True,
        "volume_path": "/Volumes/test_catalog/test_schema/chuck/cluster_init-2025-06-02_14-30.sh",
        "filename": "cluster_init-2025-06-02_14-30.sh",
        "timestamp": "2025-06-02_14-30",
    }
    client.submit_job_run.return_value = {"run_id": "12345"}

    # Call function
    result = _helper_setup_stitch_logic(
        client, llm_client, "test_catalog", "test_schema"
    )

    # Verify results
    assert result.get("success")
    assert "stitch_config" in result
    assert "metadata" in result
    metadata = result["metadata"]
    assert "config_file_path" in metadata
    assert "init_script_path" in metadata
    assert (
        metadata["init_script_path"]
        == "/Volumes/test_catalog/test_schema/chuck/cluster_init-2025-06-02_14-30.sh"
    )

    # Verify versioned init script upload was called
    mock_upload_init.assert_called_once_with(
        client=client,
        target_catalog="test_catalog",
        target_schema="test_schema",
        init_script_content="echo 'init script'",
    )

    # Verify no unsupported columns warning when all columns are supported
    assert "unsupported_columns" in metadata
    assert len(metadata["unsupported_columns"]) == 0
    assert "Note: Some columns were excluded" not in result.get("message", "")


@patch("chuck_data.commands.stitch_tools._helper_scan_schema_for_pii_logic")
@patch("chuck_data.commands.stitch_tools.get_amperity_token")
@patch("chuck_data.commands.stitch_tools._helper_upload_cluster_init_logic")
def test_unsupported_types_filtered(
    mock_upload_init,
    mock_get_amperity_token,
    mock_scan_pii,
    client,
    llm_client,
    mock_pii_scan_results_with_unsupported,
):
    """Test that unsupported column types are filtered out from Stitch config."""
    # Setup mocks
    mock_scan_pii.return_value = mock_pii_scan_results_with_unsupported
    client.list_volumes.return_value = {"volumes": [{"name": "chuck"}]}  # Volume exists
    client.upload_file.return_value = True  # File uploads successful
    mock_get_amperity_token.return_value = "fake_token"
    client.fetch_amperity_job_init.return_value = {"cluster-init": "echo 'init script'"}
    # Mock versioned init script upload
    mock_upload_init.return_value = {
        "success": True,
        "volume_path": "/Volumes/test_catalog/test_schema/chuck/cluster_init-2025-06-02_14-30.sh",
        "filename": "cluster_init-2025-06-02_14-30.sh",
        "timestamp": "2025-06-02_14-30",
    }
    client.submit_job_run.return_value = {"run_id": "12345"}

    # Call function
    result = _helper_setup_stitch_logic(
        client, llm_client, "test_catalog", "test_schema"
    )

    # Verify results
    assert result.get("success")

    # Get the generated config content
    import json

    config_content = json.dumps(result["stitch_config"])

    # Verify unsupported types are not in the config
    unsupported_types = ["STRUCT", "ARRAY", "GEOGRAPHY", "GEOMETRY", "MAP"]
    for unsupported_type in unsupported_types:
        assert (
            unsupported_type not in config_content
        ), f"Config should not contain unsupported type: {unsupported_type}"

    # Verify supported types are still included
    assert "int" in config_content, "Config should contain supported type: int"
    assert "string" in config_content, "Config should contain supported type: string"

    # Verify unsupported columns are reported to user
    assert "metadata" in result
    metadata = result["metadata"]
    assert "unsupported_columns" in metadata
    unsupported_info = metadata["unsupported_columns"]
    assert len(unsupported_info) == 2  # Two tables have unsupported columns

    # Check first table (customers)
    customers_unsupported = next(
        t for t in unsupported_info if "customers" in t["table"]
    )
    assert len(customers_unsupported["columns"]) == 2  # metadata and tags
    column_types = [col["type"] for col in customers_unsupported["columns"]]
    assert "STRUCT" in column_types
    assert "ARRAY" in column_types

    # Check second table (geo_data)
    geo_unsupported = next(t for t in unsupported_info if "geo_data" in t["table"])
    assert len(geo_unsupported["columns"]) == 3  # location, geometry, properties
    geo_column_types = [col["type"] for col in geo_unsupported["columns"]]
    assert "GEOGRAPHY" in geo_column_types
    assert "GEOMETRY" in geo_column_types
    assert "MAP" in geo_column_types

    # Verify warning message includes unsupported columns info in metadata
    assert "unsupported_columns" in metadata


@patch("chuck_data.commands.stitch_tools._helper_scan_schema_for_pii_logic")
@patch("chuck_data.commands.stitch_tools.get_amperity_token")
def test_all_columns_unsupported_types(
    mock_get_amperity_token, mock_scan_pii, client, llm_client
):
    """Test handling when all columns have unsupported types."""
    # Setup mocks with all unsupported types
    all_unsupported_results = {
        "tables_successfully_processed": 1,
        "tables_with_pii": 1,
        "total_pii_columns": 2,
        "results_detail": [
            {
                "full_name": "test_catalog.test_schema.complex_data",
                "has_pii": True,
                "skipped": False,
                "columns": [
                    {"name": "metadata", "type": "STRUCT", "semantic": "full-name"},
                    {"name": "tags", "type": "ARRAY", "semantic": "address"},
                    {"name": "location", "type": "GEOGRAPHY", "semantic": None},
                ],
            },
        ],
    }
    mock_scan_pii.return_value = all_unsupported_results
    client.list_volumes.return_value = {"volumes": [{"name": "chuck"}]}  # Volume exists
    mock_get_amperity_token.return_value = "fake_token"  # Add token mock

    # Call function
    result = _helper_setup_stitch_logic(
        client, llm_client, "test_catalog", "test_schema"
    )

    # Verify results - should fail because no supported columns remain
    assert "error" in result
    assert "No tables with PII found" in result["error"]
