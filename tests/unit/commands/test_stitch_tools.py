"""
Tests for stitch_tools command handler utilities.

This module contains tests for the Stitch integration utilities.
"""

import pytest
from unittest.mock import Mock, patch

from chuck_data.commands.stitch_tools import (
    _helper_setup_stitch_logic,
    _helper_prepare_stitch_config,
    _helper_prepare_multi_location_stitch_config,
    validate_multi_location_access,
)
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
    # Add schema first for validation
    databricks_client_stub.add_schema("test_catalog", "test_schema")

    # Configure databricks_client_stub to fail when listing tables
    databricks_client_stub.set_list_tables_error(Exception("Failed to access tables"))

    # Call function - real PII scan logic will fail and return error
    result = _helper_setup_stitch_logic(
        databricks_client_stub, llm_client_stub, "test_catalog", "test_schema"
    )

    # Verify results
    assert "error" in result
    # Error message changed due to multi-location refactor
    assert "PII" in result["error"] or "No" in result["error"]


def test_volume_list_error(
    databricks_client_stub, llm_client_stub, mock_pii_scan_results
):
    """Test handling when listing volumes fails."""
    # Add schema first for validation
    databricks_client_stub.add_schema("test_catalog", "test_schema")

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
    # Add schema first for validation
    databricks_client_stub.add_schema("test_catalog", "test_schema")
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
    # Add schema first for validation
    databricks_client_stub.add_schema("test_catalog", "test_schema")
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
    # Error message can vary based on implementation details
    assert "No" in result["error"] and "PII" in result["error"]


def test_missing_amperity_token(
    databricks_client_stub, llm_client_stub, mock_pii_scan_results
):
    """Test handling when Amperity token is missing."""
    import tempfile

    # Add schema first for validation
    databricks_client_stub.add_schema("test_catalog", "test_schema")
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

    # Add schema first for validation
    databricks_client_stub.add_schema("test_catalog", "test_schema")
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

    # Add schema first for validation
    databricks_client_stub.add_schema("test_catalog", "test_schema")
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

            # Mock fetch_amperity_job_init to return job-id
            with patch.object(
                databricks_client_stub, "fetch_amperity_job_init"
            ) as mock_fetch:
                mock_fetch.return_value = {
                    "cluster-init": "#!/bin/bash\necho 'init script'",
                    "job-id": "test-job-123",
                }

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

    # Add schema first for validation
    databricks_client_stub.add_schema("test_catalog", "test_schema")
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

            # Mock fetch_amperity_job_init to return job-id
            with patch.object(
                databricks_client_stub, "fetch_amperity_job_init"
            ) as mock_fetch:
                mock_fetch.return_value = {
                    "cluster-init": "#!/bin/bash\necho 'init script'",
                    "job-id": "test-job-456",
                }

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

                    # Verify versioned init script upload was called with content from mocked API
                    mock_upload.assert_called_once_with(
                        client=databricks_client_stub,
                        target_catalog="test_catalog",
                        target_schema="test_schema",
                        init_script_content="#!/bin/bash\necho 'init script'",
                    )

                # Verify no unsupported columns warning when all columns are supported
                assert "unsupported_columns" in metadata
                assert len(metadata["unsupported_columns"]) == 0


def test_unsupported_types_filtered(
    databricks_client_stub, llm_client_stub, mock_pii_scan_results_with_unsupported
):
    """Test that unsupported column types are filtered out from Stitch config."""
    import tempfile

    # Add schema first for validation
    databricks_client_stub.add_schema("test_catalog", "test_schema")
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

            # Mock fetch_amperity_job_init to return job-id
            with patch.object(
                databricks_client_stub, "fetch_amperity_job_init"
            ) as mock_fetch:
                mock_fetch.return_value = {
                    "cluster-init": "#!/bin/bash\necho 'init script'",
                    "job-id": "test-job-789",
                }

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

    # Add schema first for validation
    databricks_client_stub.add_schema("test_catalog", "test_schema")
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
            # Error message varies based on implementation
            assert "No tables" in result["error"] or "No PII" in result["error"]


class TestJobIdTracking:
    """Test job-id tracking from Amperity API in stitch_tools."""

    @pytest.mark.usefixtures("temp_config")
    @patch("chuck_data.commands.stitch_tools.get_amperity_token")
    @patch("chuck_data.commands.stitch_tools._helper_upload_cluster_init_logic")
    @patch("chuck_data.commands.stitch_tools._helper_scan_schema_for_pii_logic")
    def test_prepare_stitch_config_tracks_job_id(
        self,
        mock_pii_scan,
        mock_upload_init,
        mock_get_token,
        databricks_client_stub,
    ):
        """Test that _helper_prepare_stitch_config captures job-id from API."""
        from chuck_data.commands.stitch_tools import _helper_prepare_stitch_config

        # Add schema first for validation
        databricks_client_stub.add_schema("catalog", "schema")

        # Mock token
        mock_get_token.return_value = "test-token"

        # Mock PII scan
        mock_pii_scan.return_value = {
            "tables_successfully_processed": 1,
            "results_detail": [
                {
                    "full_name": "catalog.schema.table",
                    "has_pii": True,
                    "skipped": False,
                    "columns": [
                        {"name": "email", "type": "string", "semantic": "email"}
                    ],
                }
            ],
        }

        # Mock upload init script
        mock_upload_init.return_value = {
            "volume_path": "/Volumes/catalog/schema/chuck/init.sh"
        }

        # Mock fetch_amperity_job_init to return job-id
        def mock_fetch_init(token):
            return {
                "cluster-init": "#!/bin/bash\necho 'init'",
                "job-id": "chk-test-123",
            }

        databricks_client_stub.fetch_amperity_job_init = mock_fetch_init
        databricks_client_stub.add_volume("catalog", "schema", "chuck")

        result = _helper_prepare_stitch_config(
            databricks_client_stub, None, "catalog", "schema"
        )

        assert result.get("success") is True
        assert "metadata" in result
        assert result["metadata"]["job_id"] == "chk-test-123"

    @pytest.mark.usefixtures("temp_config")
    @patch("chuck_data.commands.stitch_tools.get_amperity_token")
    @patch("chuck_data.commands.stitch_tools._helper_upload_cluster_init_logic")
    @patch("chuck_data.commands.stitch_tools._helper_scan_schema_for_pii_logic")
    def test_prepare_stitch_config_fails_without_job_id(
        self,
        mock_pii_scan,
        mock_upload_init,
        mock_get_token,
        databricks_client_stub,
    ):
        """Test that _helper_prepare_stitch_config fails if job-id is missing."""
        from chuck_data.commands.stitch_tools import _helper_prepare_stitch_config

        # Add schema first for validation
        databricks_client_stub.add_schema("catalog", "schema")

        # Mock token
        mock_get_token.return_value = "test-token"

        # Mock PII scan
        mock_pii_scan.return_value = {
            "tables_successfully_processed": 1,
            "results_detail": [
                {
                    "full_name": "catalog.schema.table",
                    "has_pii": True,
                    "skipped": False,
                    "columns": [
                        {"name": "email", "type": "string", "semantic": "email"}
                    ],
                }
            ],
        }

        # Mock fetch_amperity_job_init WITHOUT job-id
        def mock_fetch_init(token):
            return {
                "cluster-init": "#!/bin/bash\necho 'init'",
                # job-id is missing
            }

        databricks_client_stub.fetch_amperity_job_init = mock_fetch_init
        databricks_client_stub.add_volume("catalog", "schema", "chuck")

        result = _helper_prepare_stitch_config(
            databricks_client_stub, None, "catalog", "schema"
        )

        assert "error" in result
        assert "job-id" in result["error"]

    @pytest.mark.usefixtures("temp_config")
    @patch("chuck_data.commands.stitch_tools.get_amperity_token")
    @patch("chuck_data.commands.stitch_tools._helper_upload_cluster_init_logic")
    @patch("chuck_data.commands.stitch_tools._helper_scan_schema_for_pii_logic")
    def test_launch_stitch_job_returns_job_id(
        self,
        mock_pii_scan,
        mock_upload_init,
        mock_get_token,
        databricks_client_stub,
    ):
        """Test that _helper_launch_stitch_job returns job-id from metadata."""
        from chuck_data.commands.stitch_tools import _helper_launch_stitch_job

        # Mock token
        mock_get_token.return_value = "test-token"

        stitch_config = {
            "name": "test-job",
            "tables": [
                {
                    "path": "catalog.schema.table",
                    "fields": [
                        {
                            "field-name": "email",
                            "type": "string",
                            "semantics": ["email"],
                        }
                    ],
                }
            ],
            "settings": {
                "output_catalog_name": "catalog",
                "output_schema_name": "stitch_outputs",
            },
        }

        metadata = {
            "target_catalog": "catalog",
            "target_schema": "schema",
            "volume_name": "chuck",
            "stitch_job_name": "test-job",
            "config_file_path": "/Volumes/catalog/schema/chuck/test-job.json",
            "init_script_path": "/Volumes/catalog/schema/chuck/init.sh",
            "init_script_content": "#!/bin/bash\necho 'init'",
            "amperity_token": "test-token",
            "job_id": "chk-test-456",  # Job ID from prepare phase
            "pii_scan_output": {"message": "PII scan complete"},
            "unsupported_columns": [],
        }

        # Mock upload and submit
        databricks_client_stub.upload_file = lambda path, content, overwrite: True

        def mock_submit(config_path, init_script_path, run_name, policy_id=None):
            return {"run_id": "run-789"}

        databricks_client_stub.submit_job_run = mock_submit

        # Mock notebook creation
        databricks_client_stub.create_stitch_notebook = lambda **kwargs: {
            "notebook_path": "/Workspace/Users/test/notebook"
        }

        result = _helper_launch_stitch_job(
            databricks_client_stub, stitch_config, metadata
        )

        assert result["success"] is True
        assert result["job_id"] == "chk-test-456"
        assert result["run_id"] == "run-789"

    @pytest.mark.usefixtures("temp_config")
    @patch("chuck_data.clients.amperity.AmperityAPIClient")
    def test_launch_stitch_job_records_submission(
        self,
        mock_amperity_client_class,
        databricks_client_stub,
    ):
        """Test that _helper_launch_stitch_job calls record_job_submission."""
        from chuck_data.commands.stitch_tools import _helper_launch_stitch_job

        # Mock Amperity client
        mock_client = Mock()
        mock_client.record_job_submission.return_value = True
        mock_amperity_client_class.return_value = mock_client

        stitch_config = {
            "name": "test-job",
            "tables": [
                {
                    "path": "catalog.schema.table",
                    "fields": [
                        {
                            "field-name": "email",
                            "type": "string",
                            "semantics": ["email"],
                        }
                    ],
                }
            ],
            "settings": {
                "output_catalog_name": "catalog",
                "output_schema_name": "stitch_outputs",
            },
        }

        metadata = {
            "target_catalog": "catalog",
            "target_schema": "schema",
            "volume_name": "chuck",
            "stitch_job_name": "test-job",
            "config_file_path": "/Volumes/catalog/schema/chuck/test-job.json",
            "init_script_path": "/Volumes/catalog/schema/chuck/init.sh",
            "init_script_content": "#!/bin/bash\necho 'init'",
            "amperity_token": "test-token",
            "job_id": "chk-test-789",
            "pii_scan_output": {"message": "PII scan complete"},
            "unsupported_columns": [],
        }

        # Mock upload and submit
        databricks_client_stub.upload_file = lambda path, content, overwrite: True

        def mock_submit(config_path, init_script_path, run_name, policy_id=None):
            return {"run_id": "run-456"}

        databricks_client_stub.submit_job_run = mock_submit

        # Mock notebook creation
        databricks_client_stub.create_stitch_notebook = lambda **kwargs: {
            "notebook_path": "/Workspace/Users/test/notebook"
        }

        result = _helper_launch_stitch_job(
            databricks_client_stub, stitch_config, metadata
        )

        assert result["success"] is True
        assert result["job_id"] == "chk-test-789"
        assert result["run_id"] == "run-456"

        # Verify AmperityAPIClient was instantiated and record_job_submission was called
        # with job_id from metadata
        mock_amperity_client_class.assert_called_once()
        mock_client.record_job_submission.assert_called_once_with(
            databricks_run_id="run-456", token="test-token", job_id="chk-test-789"
        )

    @pytest.mark.usefixtures("temp_config")
    @patch("chuck_data.clients.amperity.AmperityAPIClient")
    def test_launch_stitch_job_records_submission_no_job_id(
        self,
        mock_amperity_client_class,
        databricks_client_stub,
    ):
        """Test that record_job_submission is not called when job_id is missing."""
        from chuck_data.commands.stitch_tools import _helper_launch_stitch_job

        # Mock Amperity client
        mock_client = Mock()
        mock_client.record_job_submission.return_value = True
        mock_amperity_client_class.return_value = mock_client

        stitch_config = {
            "name": "test-job",
            "tables": [
                {
                    "path": "catalog.schema.table",
                    "fields": [
                        {
                            "field-name": "email",
                            "type": "string",
                            "semantics": ["email"],
                        }
                    ],
                }
            ],
            "settings": {
                "output_catalog_name": "catalog",
                "output_schema_name": "stitch_outputs",
            },
        }

        # Metadata without job_id
        metadata = {
            "target_catalog": "catalog",
            "target_schema": "schema",
            "volume_name": "chuck",
            "stitch_job_name": "test-job",
            "config_file_path": "/Volumes/catalog/schema/chuck/test-job.json",
            "init_script_path": "/Volumes/catalog/schema/chuck/init.sh",
            "init_script_content": "#!/bin/bash\necho 'init'",
            "amperity_token": "test-token",
            # No job_id here
            "pii_scan_output": {"message": "PII scan complete"},
            "unsupported_columns": [],
        }

        # Mock upload and submit
        databricks_client_stub.upload_file = lambda path, content, overwrite: True

        def mock_submit(config_path, init_script_path, run_name, policy_id=None):
            return {"run_id": "run-999"}

        databricks_client_stub.submit_job_run = mock_submit

        # Mock notebook creation
        databricks_client_stub.create_stitch_notebook = lambda **kwargs: {
            "notebook_path": "/Workspace/Users/test/notebook"
        }

        result = _helper_launch_stitch_job(
            databricks_client_stub, stitch_config, metadata
        )

        assert result["success"] is True
        assert result["run_id"] == "run-999"

        # Verify record_job_submission was NOT called (missing job_id)
        mock_client.record_job_submission.assert_not_called()

    @pytest.mark.usefixtures("temp_config")
    @patch("chuck_data.commands.stitch_tools.get_amperity_token")
    @patch("chuck_data.clients.amperity.AmperityAPIClient")
    def test_launch_stitch_job_records_submission_no_token(
        self,
        mock_amperity_client_class,
        mock_get_token,
        databricks_client_stub,
    ):
        """Test that record_job_submission is not called when token is not available."""
        from chuck_data.commands.stitch_tools import _helper_launch_stitch_job

        # Mock get_amperity_token to return None (no token configured)
        mock_get_token.return_value = None

        # Mock Amperity client
        mock_client = Mock()
        mock_client.record_job_submission.return_value = True
        mock_amperity_client_class.return_value = mock_client

        stitch_config = {
            "name": "test-job",
            "tables": [
                {
                    "path": "catalog.schema.table",
                    "fields": [
                        {
                            "field-name": "email",
                            "type": "string",
                            "semantics": ["email"],
                        }
                    ],
                }
            ],
            "settings": {
                "output_catalog_name": "catalog",
                "output_schema_name": "stitch_outputs",
            },
        }

        # Metadata without token
        metadata = {
            "target_catalog": "catalog",
            "target_schema": "schema",
            "volume_name": "chuck",
            "stitch_job_name": "test-job",
            "config_file_path": "/Volumes/catalog/schema/chuck/test-job.json",
            "init_script_path": "/Volumes/catalog/schema/chuck/init.sh",
            "init_script_content": "#!/bin/bash\necho 'init'",
            # No amperity_token here
            "job_id": "chk-test-999",
            "pii_scan_output": {"message": "PII scan complete"},
            "unsupported_columns": [],
        }

        # Mock upload and submit
        databricks_client_stub.upload_file = lambda path, content, overwrite: True

        def mock_submit(config_path, init_script_path, run_name, policy_id=None):
            return {"run_id": "run-888"}

        databricks_client_stub.submit_job_run = mock_submit

        # Mock notebook creation
        databricks_client_stub.create_stitch_notebook = lambda **kwargs: {
            "notebook_path": "/Workspace/Users/test/notebook"
        }

        result = _helper_launch_stitch_job(
            databricks_client_stub, stitch_config, metadata
        )

        assert result["success"] is True
        assert result["run_id"] == "run-888"

        # Verify get_amperity_token was called as fallback
        mock_get_token.assert_called_once()

        # Verify record_job_submission was NOT called (no token available)
        mock_client.record_job_submission.assert_not_called()


class TestMultiCatalogSupport:
    """Test multi-catalog and multi-schema support."""

    def test_validate_multi_location_access_all_accessible(
        self, databricks_client_stub
    ):
        """Test validation when all locations are accessible."""
        # Add schemas
        databricks_client_stub.add_schema("catalog1", "schema1")
        databricks_client_stub.add_schema("catalog1", "schema2")
        databricks_client_stub.add_schema("catalog2", "schema1")

        locations = [
            {"catalog": "catalog1", "schema": "schema1"},
            {"catalog": "catalog1", "schema": "schema2"},
            {"catalog": "catalog2", "schema": "schema1"},
        ]

        results = validate_multi_location_access(databricks_client_stub, locations)

        assert len(results) == 3
        assert all(r["accessible"] for r in results)

    def test_validate_multi_location_access_partial(self, databricks_client_stub):
        """Test validation when some locations are inaccessible."""
        # Only add first schema
        databricks_client_stub.add_schema("catalog1", "schema1")

        locations = [
            {"catalog": "catalog1", "schema": "schema1"},  # Accessible
            {"catalog": "catalog1", "schema": "schema2"},  # Not accessible
            {"catalog": "catalog2", "schema": "schema1"},  # Not accessible
        ]

        results = validate_multi_location_access(databricks_client_stub, locations)

        assert len(results) == 3
        assert results[0]["accessible"] is True
        assert results[1]["accessible"] is False
        assert results[2]["accessible"] is False
        assert "error" in results[1]
        assert "error" in results[2]

    def test_validate_multi_location_access_missing_params(
        self, databricks_client_stub
    ):
        """Test validation with missing catalog or schema."""
        locations = [
            {"catalog": "catalog1"},  # Missing schema
            {"schema": "schema1"},  # Missing catalog
            {"catalog": "", "schema": "schema1"},  # Empty catalog
        ]

        results = validate_multi_location_access(databricks_client_stub, locations)

        assert len(results) == 3
        assert all(not r["accessible"] for r in results)
        assert all("error" in r for r in results)

    @pytest.mark.usefixtures("temp_config")
    @patch("chuck_data.commands.stitch_tools.get_amperity_token")
    @patch("chuck_data.commands.stitch_tools._helper_upload_cluster_init_logic")
    @patch("chuck_data.commands.stitch_tools._helper_scan_schema_for_pii_logic")
    def test_prepare_multi_location_stitch_config_success(
        self,
        mock_pii_scan,
        mock_upload_init,
        mock_get_token,
        databricks_client_stub,
        llm_client_stub,
    ):
        """Test successful multi-location stitch configuration."""
        # Mock token
        mock_get_token.return_value = "test-token"

        # Add schemas
        databricks_client_stub.add_schema("catalog1", "schema1")
        databricks_client_stub.add_schema("catalog1", "schema2")

        # Mock PII scan for first location
        def pii_scan_side_effect(client, llm, catalog, schema):
            if catalog == "catalog1" and schema == "schema1":
                return {
                    "results_detail": [
                        {
                            "full_name": "catalog1.schema1.customers",
                            "has_pii": True,
                            "columns": [
                                {"name": "email", "type": "string", "semantic": "email"}
                            ],
                        }
                    ]
                }
            elif catalog == "catalog1" and schema == "schema2":
                return {
                    "results_detail": [
                        {
                            "full_name": "catalog1.schema2.orders",
                            "has_pii": True,
                            "columns": [
                                {
                                    "name": "address",
                                    "type": "string",
                                    "semantic": "address",
                                }
                            ],
                        }
                    ]
                }
            return {"results_detail": []}

        mock_pii_scan.side_effect = pii_scan_side_effect

        # Mock upload
        mock_upload_init.return_value = {
            "volume_path": "/Volumes/catalog1/schema1/chuck/init.sh"
        }

        # Mock fetch_amperity_job_init
        def mock_fetch_init(_token):
            return {
                "cluster-init": "#!/bin/bash\necho 'init'",
                "job-id": "chk-multi-123",
            }

        databricks_client_stub.fetch_amperity_job_init = mock_fetch_init
        databricks_client_stub.add_volume("catalog1", "schema1", "chuck")

        target_locations = [
            {"catalog": "catalog1", "schema": "schema1"},
            {"catalog": "catalog1", "schema": "schema2"},
        ]

        result = _helper_prepare_multi_location_stitch_config(
            databricks_client_stub, llm_client_stub, target_locations, "catalog1"
        )

        assert result["success"] is True
        assert "stitch_config" in result
        assert "metadata" in result

        # Verify config has tables from both locations
        config = result["stitch_config"]
        assert len(config["tables"]) == 2
        assert any("catalog1.schema1.customers" in t["path"] for t in config["tables"])
        assert any("catalog1.schema2.orders" in t["path"] for t in config["tables"])

        # Verify metadata
        metadata = result["metadata"]
        assert metadata["target_locations"] == target_locations
        assert metadata["output_catalog"] == "catalog1"
        assert "scan_summary" in metadata
        assert len(metadata["scan_summary"]) == 2

    @pytest.mark.usefixtures("temp_config")
    @patch("chuck_data.commands.stitch_tools.get_amperity_token")
    @patch("chuck_data.commands.stitch_tools._helper_upload_cluster_init_logic")
    @patch("chuck_data.commands.stitch_tools._helper_scan_schema_for_pii_logic")
    def test_prepare_multi_location_partial_failure(
        self,
        mock_pii_scan,
        mock_upload_init,
        mock_get_token,
        databricks_client_stub,
        llm_client_stub,
    ):
        """Test multi-location when one location fails but others succeed."""
        # Mock token
        mock_get_token.return_value = "test-token"

        # Add only first schema (second will fail validation)
        databricks_client_stub.add_schema("catalog1", "schema1")

        # Mock PII scan for successful location
        def pii_scan_side_effect(client, llm, catalog, schema):
            if catalog == "catalog1" and schema == "schema1":
                return {
                    "results_detail": [
                        {
                            "full_name": "catalog1.schema1.customers",
                            "has_pii": True,
                            "columns": [
                                {"name": "email", "type": "string", "semantic": "email"}
                            ],
                        }
                    ]
                }
            return {"error": "Schema not found"}

        mock_pii_scan.side_effect = pii_scan_side_effect

        # Mock upload
        mock_upload_init.return_value = {
            "volume_path": "/Volumes/catalog1/schema1/chuck/init.sh"
        }

        # Mock fetch_amperity_job_init
        def mock_fetch_init(_token):
            return {
                "cluster-init": "#!/bin/bash\necho 'init'",
                "job-id": "chk-multi-456",
            }

        databricks_client_stub.fetch_amperity_job_init = mock_fetch_init
        databricks_client_stub.add_volume("catalog1", "schema1", "chuck")

        target_locations = [
            {"catalog": "catalog1", "schema": "schema1"},  # Will succeed
            {"catalog": "catalog1", "schema": "schema2"},  # Will fail
        ]

        result = _helper_prepare_multi_location_stitch_config(
            databricks_client_stub, llm_client_stub, target_locations, "catalog1"
        )

        # Should succeed with partial results
        assert result["success"] is True
        assert len(result["stitch_config"]["tables"]) == 1
        assert "scan_summary" in result["metadata"]

    @pytest.mark.usefixtures("temp_config")
    def test_prepare_multi_location_all_inaccessible(
        self, databricks_client_stub, llm_client_stub
    ):
        """Test multi-location when all locations are inaccessible."""
        # Don't add any schemas
        target_locations = [
            {"catalog": "catalog1", "schema": "schema1"},
            {"catalog": "catalog2", "schema": "schema2"},
        ]

        result = _helper_prepare_multi_location_stitch_config(
            databricks_client_stub, llm_client_stub, target_locations, "catalog1"
        )

        assert "error" in result
        assert "No accessible locations" in result["error"]

    @pytest.mark.usefixtures("temp_config")
    @patch("chuck_data.commands.stitch_tools.get_amperity_token")
    @patch("chuck_data.commands.stitch_tools._helper_upload_cluster_init_logic")
    @patch("chuck_data.commands.stitch_tools._helper_scan_schema_for_pii_logic")
    def test_prepare_multi_location_no_pii_found(
        self,
        mock_pii_scan,
        mock_upload_init,
        mock_get_token,
        databricks_client_stub,
        llm_client_stub,
    ):
        """Test multi-location when no PII is found in any location."""
        # Mock token
        mock_get_token.return_value = "test-token"

        # Add schemas
        databricks_client_stub.add_schema("catalog1", "schema1")
        databricks_client_stub.add_schema("catalog1", "schema2")

        # Mock PII scan - no PII found
        mock_pii_scan.return_value = {
            "results_detail": [
                {
                    "full_name": "catalog1.schema1.metrics",
                    "has_pii": False,
                    "columns": [],
                }
            ]
        }

        target_locations = [
            {"catalog": "catalog1", "schema": "schema1"},
            {"catalog": "catalog1", "schema": "schema2"},
        ]

        result = _helper_prepare_multi_location_stitch_config(
            databricks_client_stub, llm_client_stub, target_locations, "catalog1"
        )

        assert "error" in result
        assert "No PII found" in result["error"]
        assert "scan_summary" in result

    @pytest.mark.usefixtures("temp_config")
    @patch("chuck_data.commands.stitch_tools.get_amperity_token")
    @patch("chuck_data.commands.stitch_tools._helper_upload_cluster_init_logic")
    @patch("chuck_data.commands.stitch_tools._helper_scan_schema_for_pii_logic")
    def test_prepare_stitch_config_backward_compatibility(
        self,
        mock_pii_scan,
        mock_upload_init,
        mock_get_token,
        databricks_client_stub,
        llm_client_stub,
    ):
        """Test that single-location mode still works (backward compatibility)."""
        # Mock token
        mock_get_token.return_value = "test-token"

        # Add schema
        databricks_client_stub.add_schema("catalog1", "schema1")

        # Mock PII scan
        mock_pii_scan.return_value = {
            "results_detail": [
                {
                    "full_name": "catalog1.schema1.customers",
                    "has_pii": True,
                    "columns": [
                        {"name": "email", "type": "string", "semantic": "email"}
                    ],
                }
            ]
        }

        # Mock upload
        mock_upload_init.return_value = {
            "volume_path": "/Volumes/catalog1/schema1/chuck/init.sh"
        }

        # Mock fetch_amperity_job_init
        def mock_fetch_init(_token):
            return {
                "cluster-init": "#!/bin/bash\necho 'init'",
                "job-id": "chk-single-789",
            }

        databricks_client_stub.fetch_amperity_job_init = mock_fetch_init
        databricks_client_stub.add_volume("catalog1", "schema1", "chuck")

        # Call with single catalog/schema (old way)
        result = _helper_prepare_stitch_config(
            databricks_client_stub, llm_client_stub, "catalog1", "schema1"
        )

        assert result["success"] is True
        assert "stitch_config" in result
        assert "metadata" in result

        # Should use multi-location path internally but behave the same
        metadata = result["metadata"]
        assert metadata["target_locations"] == [
            {"catalog": "catalog1", "schema": "schema1"}
        ]

    @pytest.mark.usefixtures("temp_config")
    @patch("chuck_data.commands.stitch_tools.get_amperity_token")
    @patch("chuck_data.commands.stitch_tools._helper_upload_cluster_init_logic")
    @patch("chuck_data.commands.stitch_tools._helper_scan_schema_for_pii_logic")
    def test_prepare_stitch_config_multi_location_mode(
        self,
        mock_pii_scan,
        mock_upload_init,
        mock_get_token,
        databricks_client_stub,
        llm_client_stub,
    ):
        """Test using new multi-location parameters."""
        # Mock token
        mock_get_token.return_value = "test-token"

        # Add schemas
        databricks_client_stub.add_schema("catalog1", "schema1")
        databricks_client_stub.add_schema("catalog2", "schema1")

        # Mock PII scan
        def pii_scan_side_effect(client, llm, catalog, schema):
            return {
                "results_detail": [
                    {
                        "full_name": f"{catalog}.{schema}.table",
                        "has_pii": True,
                        "columns": [
                            {"name": "email", "type": "string", "semantic": "email"}
                        ],
                    }
                ]
            }

        mock_pii_scan.side_effect = pii_scan_side_effect

        # Mock upload
        mock_upload_init.return_value = {
            "volume_path": "/Volumes/catalog1/schema1/chuck/init.sh"
        }

        # Mock fetch_amperity_job_init
        def mock_fetch_init(_token):
            return {
                "cluster-init": "#!/bin/bash\necho 'init'",
                "job-id": "chk-multi-999",
            }

        databricks_client_stub.fetch_amperity_job_init = mock_fetch_init
        databricks_client_stub.add_volume("catalog1", "schema1", "chuck")

        target_locations = [
            {"catalog": "catalog1", "schema": "schema1"},
            {"catalog": "catalog2", "schema": "schema1"},
        ]

        # Call with new multi-location parameters
        result = _helper_prepare_stitch_config(
            databricks_client_stub,
            llm_client_stub,
            target_locations=target_locations,
            output_catalog="catalog1",
        )

        assert result["success"] is True
        assert len(result["stitch_config"]["tables"]) == 2
        assert result["metadata"]["output_catalog"] == "catalog1"
        assert result["metadata"]["target_locations"] == target_locations

    def test_prepare_stitch_config_missing_params(
        self, databricks_client_stub, llm_client_stub
    ):
        """Test error when neither single nor multi-location params provided."""
        result = _helper_prepare_stitch_config(databricks_client_stub, llm_client_stub)

        assert "error" in result
        assert "Target catalog and schema are required" in result["error"]
