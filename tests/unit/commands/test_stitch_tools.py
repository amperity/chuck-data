"""
Tests for stitch_tools command handler utilities.

This module contains tests for the Stitch integration utilities.
"""

import pytest
from unittest.mock import patch

from chuck_data.commands.stitch_tools import _helper_setup_stitch_logic
from tests.fixtures.llm import LLMClientStub


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


def test_missing_params(databricks_client_stub, llm_client_stub):
    """Test handling when parameters are missing."""
    result = _helper_setup_stitch_logic(
        databricks_client_stub, llm_client_stub, "", "test_schema"
    )
    assert "error" in result
    assert "Target catalog and schema are required" in result["error"]


def test_pii_scan_error(databricks_client_stub, llm_client_stub):
    """Test handling when PII scan returns an error."""
    # Configure databricks_client_stub to fail when listing tables
    databricks_client_stub.set_list_tables_error(Exception("Failed to access tables"))

    # Call function - real PII scan logic will fail and return error
    result = _helper_setup_stitch_logic(
        databricks_client_stub, llm_client_stub, "test_catalog", "test_schema"
    )

    # Verify results
    assert "error" in result
    assert "PII Scan failed during Stitch setup" in result["error"]


def test_volume_list_error(
    databricks_client_stub, llm_client_stub, mock_pii_scan_results
):
    """Test handling when listing volumes fails."""
    # Set up PII scan to succeed by providing tables with PII
    databricks_client_stub.add_table(
        "test_catalog",
        "test_schema",
        "customers",
        columns=[{"name": "email", "type_name": "STRING"}],
    )
    databricks_client_stub.add_table(
        "test_catalog",
        "test_schema",
        "orders",
        columns=[{"name": "shipping_address", "type_name": "STRING"}],
    )

    # Configure LLM to return PII tags
    llm_client_stub.set_pii_detection_result(
        [
            {"column": "email", "semantic": "email"},
            {"column": "shipping_address", "semantic": "address"},
        ]
    )

    # Configure volume listing to fail
    databricks_client_stub.set_list_volumes_error(Exception("API Error"))

    # Call function - real business logic will handle the volume error
    result = _helper_setup_stitch_logic(
        databricks_client_stub, llm_client_stub, "test_catalog", "test_schema"
    )

    # Verify results
    assert "error" in result
    assert "Failed to list volumes" in result["error"]


def test_volume_create_error(
    databricks_client_stub, llm_client_stub, mock_pii_scan_results
):
    """Test handling when creating volume fails."""
    # Set up PII scan to succeed by providing tables with PII
    databricks_client_stub.add_table(
        "test_catalog",
        "test_schema",
        "customers",
        columns=[{"name": "email", "type_name": "STRING"}],
    )

    # Configure LLM to return PII tags
    llm_client_stub.set_pii_detection_result([{"column": "email", "semantic": "email"}])

    # Volume doesn't exist (empty list) and creation will fail
    # databricks_client_stub starts with no volumes by default
    databricks_client_stub.set_create_volume_failure(True)

    # Call function - real business logic will try to create volume and fail
    result = _helper_setup_stitch_logic(
        databricks_client_stub, llm_client_stub, "test_catalog", "test_schema"
    )

    # Verify results
    assert "error" in result
    assert "Failed to create volume 'chuck'" in result["error"]


def test_no_tables_with_pii(
    databricks_client_stub, llm_client_stub, mock_pii_scan_results
):
    """Test handling when no tables with PII are found."""
    # Set up tables with no PII (LLM returns no semantic tags)
    databricks_client_stub.add_table(
        "test_catalog",
        "test_schema",
        "metrics",
        columns=[{"name": "id", "type_name": "INT"}],
    )

    # Configure LLM to return no PII tags
    llm_client_stub.set_pii_detection_result([])

    # Volume exists
    databricks_client_stub.add_volume("test_catalog", "test_schema", "chuck")

    # Call function - real PII scan will find no PII
    result = _helper_setup_stitch_logic(
        databricks_client_stub, llm_client_stub, "test_catalog", "test_schema"
    )

    # Verify results
    assert "error" in result
    assert "No tables with PII found" in result["error"]


def test_missing_amperity_token(
    databricks_client_stub, llm_client_stub, mock_pii_scan_results
):
    """Test handling when Amperity token is missing."""
    import tempfile
    from chuck_data.config import ConfigManager

    # Use real config system with no token set
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            # Set up PII scan to succeed
            databricks_client_stub.add_table(
                "test_catalog",
                "test_schema",
                "customers",
                columns=[{"name": "email", "type_name": "STRING"}],
            )

            # Configure LLM to return PII tags
            llm_client_stub.set_pii_detection_result(
                [{"column": "email", "semantic": "email"}]
            )

            # Volume exists
            databricks_client_stub.add_volume("test_catalog", "test_schema", "chuck")

            # Don't set any amperity token (should be None by default)

            # Call function - real config logic will detect missing token
            result = _helper_setup_stitch_logic(
                databricks_client_stub, llm_client_stub, "test_catalog", "test_schema"
            )

            # Verify results
            assert "error" in result
            assert "Amperity token not found" in result["error"]


def test_amperity_init_script_error(
    databricks_client_stub, llm_client_stub, mock_pii_scan_results
):
    """Test handling when fetching Amperity init script fails."""
    import tempfile
    from chuck_data.config import ConfigManager, set_amperity_token

    # Use real config system with token
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            # Set amperity token using real config
            set_amperity_token("fake_token")

            # Set up PII scan to succeed
            databricks_client_stub.add_table(
                "test_catalog",
                "test_schema",
                "customers",
                columns=[{"name": "email", "type_name": "STRING"}],
            )

            # Configure LLM to return PII tags
            llm_client_stub.set_pii_detection_result(
                [{"column": "email", "semantic": "email"}]
            )

            # Volume exists
            databricks_client_stub.add_volume("test_catalog", "test_schema", "chuck")

            # Configure fetch_amperity_job_init to fail
            databricks_client_stub.set_fetch_amperity_error(Exception("API Error"))

            # Call function - real business logic will handle fetch error
            result = _helper_setup_stitch_logic(
                databricks_client_stub, llm_client_stub, "test_catalog", "test_schema"
            )

            # Verify results
            assert "error" in result
            assert "Error fetching Amperity init script" in result["error"]


def test_versioned_init_script_upload_error(
    databricks_client_stub, llm_client_stub, mock_pii_scan_results
):
    """Test handling when versioned init script upload fails."""
    import tempfile
    from chuck_data.config import ConfigManager, set_amperity_token

    # Use real config system with token
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            # Set amperity token using real config
            set_amperity_token("fake_token")

            # Set up PII scan to succeed
            databricks_client_stub.add_table(
                "test_catalog",
                "test_schema",
                "customers",
                columns=[{"name": "email", "type_name": "STRING"}],
            )

            # Configure LLM to return PII tags
            llm_client_stub.set_pii_detection_result(
                [{"column": "email", "semantic": "email"}]
            )

            # Volume exists
            databricks_client_stub.add_volume("test_catalog", "test_schema", "chuck")

            # For this test, we need to mock the upload cluster init logic to fail
            # since it's complex internal logic, but this represents a compromise
            with patch(
                "chuck_data.commands.stitch_tools._helper_upload_cluster_init_logic"
            ) as mock_upload:
                mock_upload.return_value = {
                    "error": "Failed to upload versioned init script"
                }

                # Call function
                result = _helper_setup_stitch_logic(
                    databricks_client_stub,
                    llm_client_stub,
                    "test_catalog",
                    "test_schema",
                )

                # Verify results
                assert "error" in result
                assert result["error"] == "Failed to upload versioned init script"


def test_successful_setup(
    databricks_client_stub, llm_client_stub, mock_pii_scan_results
):
    """Test successful Stitch integration setup with versioned init script."""
    import tempfile
    from chuck_data.config import ConfigManager, set_amperity_token

    # Use real config system with token
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            # Set amperity token using real config
            set_amperity_token("fake_token")

            # Set up successful PII scan with real tables
            databricks_client_stub.add_table(
                "test_catalog",
                "test_schema",
                "customers",
                columns=[
                    {"name": "id", "type_name": "INT"},
                    {"name": "name", "type_name": "STRING"},
                    {"name": "email", "type_name": "STRING"},
                ],
            )
            databricks_client_stub.add_table(
                "test_catalog",
                "test_schema",
                "orders",
                columns=[
                    {"name": "id", "type_name": "INT"},
                    {"name": "customer_id", "type_name": "INT"},
                    {"name": "shipping_address", "type_name": "STRING"},
                ],
            )
            databricks_client_stub.add_table(
                "test_catalog",
                "test_schema",
                "metrics",
                columns=[
                    {"name": "id", "type_name": "INT"},
                    {"name": "date", "type_name": "DATE"},
                ],
            )

            # Configure LLM to return PII tags matching the mock data
            llm_client_stub.set_pii_detection_result(
                [
                    {"column": "name", "semantic": "full-name"},
                    {"column": "email", "semantic": "email"},
                    {"column": "shipping_address", "semantic": "address"},
                ]
            )

            # Volume exists
            databricks_client_stub.add_volume("test_catalog", "test_schema", "chuck")

            # For the upload logic, we'll mock it since it's complex file handling
            with patch(
                "chuck_data.commands.stitch_tools._helper_upload_cluster_init_logic"
            ) as mock_upload:
                mock_upload.return_value = {
                    "success": True,
                    "volume_path": "/Volumes/test_catalog/test_schema/chuck/cluster_init-2025-06-02_14-30.sh",
                    "filename": "cluster_init-2025-06-02_14-30.sh",
                    "timestamp": "2025-06-02_14-30",
                }

                # Call function - should succeed with real business logic
                result = _helper_setup_stitch_logic(
                    databricks_client_stub,
                    llm_client_stub,
                    "test_catalog",
                    "test_schema",
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

                # Verify versioned init script upload was called with real business logic
                mock_upload.assert_called_once_with(
                    client=databricks_client_stub,
                    target_catalog="test_catalog",
                    target_schema="test_schema",
                    init_script_content="echo 'Amperity init script'",
                )

                # Verify no unsupported columns warning when all columns are supported
                assert "unsupported_columns" in metadata
                assert len(metadata["unsupported_columns"]) == 0


def test_unsupported_types_filtered(
    databricks_client_stub, llm_client_stub, mock_pii_scan_results_with_unsupported
):
    """Test that unsupported column types are filtered out from Stitch config."""
    import tempfile
    from chuck_data.config import ConfigManager, set_amperity_token

    # Use real config system with token
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            # Set amperity token using real config
            set_amperity_token("fake_token")

            # Set up tables with unsupported column types
            databricks_client_stub.add_table(
                "test_catalog",
                "test_schema",
                "customers",
                columns=[
                    {"name": "id", "type_name": "INT"},
                    {"name": "name", "type_name": "STRING"},
                    {"name": "metadata", "type_name": "STRUCT"},
                    {"name": "tags", "type_name": "ARRAY"},
                ],
            )
            databricks_client_stub.add_table(
                "test_catalog",
                "test_schema",
                "geo_data",
                columns=[
                    {"name": "location", "type_name": "GEOGRAPHY"},
                    {"name": "geometry", "type_name": "GEOMETRY"},
                    {"name": "properties", "type_name": "MAP"},
                    {"name": "description", "type_name": "STRING"},
                ],
            )

            # Configure LLM to return PII tags for all columns (including unsupported ones)
            llm_client_stub.set_pii_detection_result(
                [
                    {"column": "name", "semantic": "full-name"},
                    {"column": "metadata", "semantic": "full-name"},  # Will be filtered
                    {"column": "tags", "semantic": "address"},  # Will be filtered
                    {"column": "location", "semantic": "address"},  # Will be filtered
                    {"column": "geometry", "semantic": None},  # Will be filtered
                    {"column": "properties", "semantic": None},  # Will be filtered
                    {"column": "description", "semantic": "full-name"},
                ]
            )

            # Volume exists
            databricks_client_stub.add_volume("test_catalog", "test_schema", "chuck")

            # Mock upload logic
            with patch(
                "chuck_data.commands.stitch_tools._helper_upload_cluster_init_logic"
            ) as mock_upload:
                mock_upload.return_value = {
                    "success": True,
                    "volume_path": "/Volumes/test_catalog/test_schema/chuck/cluster_init-2025-06-02_14-30.sh",
                    "filename": "cluster_init-2025-06-02_14-30.sh",
                    "timestamp": "2025-06-02_14-30",
                }

                # Call function - real business logic should filter unsupported types
                result = _helper_setup_stitch_logic(
                    databricks_client_stub,
                    llm_client_stub,
                    "test_catalog",
                    "test_schema",
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
                assert (
                    "string" in config_content.lower()
                ), "Config should contain supported type: string"

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
                geo_unsupported = next(
                    t for t in unsupported_info if "geo_data" in t["table"]
                )
                assert (
                    len(geo_unsupported["columns"]) == 3
                )  # location, geometry, properties
                geo_column_types = [col["type"] for col in geo_unsupported["columns"]]
                assert "GEOGRAPHY" in geo_column_types
                assert "GEOMETRY" in geo_column_types
                assert "MAP" in geo_column_types


def test_all_columns_unsupported_types(databricks_client_stub, llm_client_stub):
    """Test handling when all columns have unsupported types."""
    import tempfile
    from chuck_data.config import ConfigManager, set_amperity_token

    # Use real config system with token
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            # Set amperity token using real config
            set_amperity_token("fake_token")

            # Set up table with only unsupported column types
            databricks_client_stub.add_table(
                "test_catalog",
                "test_schema",
                "complex_data",
                columns=[
                    {"name": "metadata", "type_name": "STRUCT"},
                    {"name": "tags", "type_name": "ARRAY"},
                    {"name": "location", "type_name": "GEOGRAPHY"},
                ],
            )

            # Configure LLM to return PII tags for all columns (but they're all unsupported)
            llm_client_stub.set_pii_detection_result(
                [
                    {"column": "metadata", "semantic": "full-name"},
                    {"column": "tags", "semantic": "address"},
                    {"column": "location", "semantic": None},
                ]
            )

            # Volume exists
            databricks_client_stub.add_volume("test_catalog", "test_schema", "chuck")

            # Call function - real business logic will filter out all unsupported types
            result = _helper_setup_stitch_logic(
                databricks_client_stub, llm_client_stub, "test_catalog", "test_schema"
            )

            # Verify results - should fail because no supported columns remain after filtering
            assert "error" in result
            assert "No tables with PII found" in result["error"]
