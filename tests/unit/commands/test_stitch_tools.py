"""
Tests for stitch_tools utility functions.

Behavioral tests focused on utility function behavior rather than implementation details.
Tests cover all helper functions: preparation, modification, and launch phases.
"""

import tempfile
from unittest.mock import patch

from chuck_data.commands.stitch_tools import (
    _helper_setup_stitch_logic,
    _helper_prepare_stitch_config,
    _helper_modify_stitch_config,
    _helper_launch_stitch_job,
    _create_stitch_report_notebook,
)
from chuck_data.config import ConfigManager, set_amperity_token


class TestHelperSetupStitchLogicParameterValidation:
    """Test parameter validation for _helper_setup_stitch_logic function."""

    def test_missing_catalog_parameter_returns_error(
        self, databricks_client_stub, llm_client_stub
    ):
        """Missing catalog parameter returns clear error message."""
        result = _helper_setup_stitch_logic(
            databricks_client_stub, llm_client_stub, "", "test_schema"
        )

        assert "error" in result
        assert "Target catalog and schema are required" in result["error"]

    def test_missing_schema_parameter_returns_error(
        self, databricks_client_stub, llm_client_stub
    ):
        """Missing schema parameter returns clear error message."""
        result = _helper_setup_stitch_logic(
            databricks_client_stub, llm_client_stub, "test_catalog", ""
        )

        assert "error" in result
        assert "Target catalog and schema are required" in result["error"]

    def test_none_catalog_parameter_returns_error(
        self, databricks_client_stub, llm_client_stub
    ):
        """None catalog parameter returns clear error message."""
        result = _helper_setup_stitch_logic(
            databricks_client_stub, llm_client_stub, None, "test_schema"
        )

        assert "error" in result
        assert "Target catalog and schema are required" in result["error"]


class TestHelperSetupStitchLogicDirectExecution:
    """Test direct execution of _helper_setup_stitch_logic function."""

    def test_pii_scan_failure_returns_error(
        self, databricks_client_stub, llm_client_stub
    ):
        """PII scan failures are handled gracefully with helpful error messages."""
        # Configure databricks_client_stub to fail when listing tables
        databricks_client_stub.set_list_tables_error(
            Exception("Failed to access tables")
        )

        # Call function - real PII scan logic will fail and return error
        result = _helper_setup_stitch_logic(
            databricks_client_stub, llm_client_stub, "test_catalog", "test_schema"
        )

        # Verify results
        assert "error" in result
        assert "PII Scan failed during Stitch setup" in result["error"]

    def test_volume_list_error_returns_error(
        self, databricks_client_stub, llm_client_stub
    ):
        """Volume list failures are handled gracefully."""
        # Set up PII scan to succeed by providing tables with PII
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

        # Configure volume listing to fail
        databricks_client_stub.set_list_volumes_error(Exception("API Error"))

        # Call function - real business logic will handle the volume error
        result = _helper_setup_stitch_logic(
            databricks_client_stub, llm_client_stub, "test_catalog", "test_schema"
        )

        # Verify results
        assert "error" in result
        assert "Failed to list volumes" in result["error"]

    def test_volume_creation_failure_returns_error(
        self, databricks_client_stub, llm_client_stub
    ):
        """Volume creation failures are handled gracefully."""
        # Set up PII scan to succeed by providing tables with PII
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

        # Volume doesn't exist (empty list) and creation will fail
        databricks_client_stub.set_create_volume_failure(True)

        # Call function - real business logic will try to create volume and fail
        result = _helper_setup_stitch_logic(
            databricks_client_stub, llm_client_stub, "test_catalog", "test_schema"
        )

        # Verify results
        assert "error" in result
        assert "Failed to create volume 'chuck'" in result["error"]

    def test_no_tables_with_pii_returns_error(
        self, databricks_client_stub, llm_client_stub
    ):
        """No tables with PII returns helpful error message."""
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

    def test_missing_amperity_token_returns_error(
        self, databricks_client_stub, llm_client_stub
    ):
        """Missing Amperity token returns helpful error message."""
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
                databricks_client_stub.add_volume(
                    "test_catalog", "test_schema", "chuck"
                )

                # Don't set any amperity token (should be None by default)

                # Call function - real config logic will detect missing token
                result = _helper_setup_stitch_logic(
                    databricks_client_stub,
                    llm_client_stub,
                    "test_catalog",
                    "test_schema",
                )

                # Verify results
                assert "error" in result
                assert "Amperity token not found" in result["error"]

    def test_amperity_init_script_fetch_error_returns_error(
        self, databricks_client_stub, llm_client_stub
    ):
        """Amperity init script fetch failures are handled gracefully."""
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
                databricks_client_stub.add_volume(
                    "test_catalog", "test_schema", "chuck"
                )

                # Configure fetch_amperity_job_init to fail
                databricks_client_stub.set_fetch_amperity_error(Exception("API Error"))

                # Call function - real business logic will handle fetch error
                result = _helper_setup_stitch_logic(
                    databricks_client_stub,
                    llm_client_stub,
                    "test_catalog",
                    "test_schema",
                )

                # Verify results
                assert "error" in result
                assert "Error fetching Amperity init script" in result["error"]

    def test_successful_stitch_setup_returns_configuration(
        self, databricks_client_stub, llm_client_stub
    ):
        """Successful Stitch setup returns complete configuration."""
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

                # Configure LLM to return PII tags matching the mock data
                llm_client_stub.set_pii_detection_result(
                    [
                        {"column": "name", "semantic": "given-name"},
                        {"column": "email", "semantic": "email"},
                        {"column": "shipping_address", "semantic": "address"},
                    ]
                )

                # Volume exists
                databricks_client_stub.add_volume(
                    "test_catalog", "test_schema", "chuck"
                )

                # Mock upload logic since it's complex file handling
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

                    # Verify configuration structure
                    stitch_config = result["stitch_config"]
                    assert "name" in stitch_config
                    assert "tables" in stitch_config
                    assert "settings" in stitch_config

                    # Verify metadata structure
                    metadata = result["metadata"]
                    assert "config_file_path" in metadata
                    assert "init_script_path" in metadata
                    assert (
                        metadata["init_script_path"]
                        == "/Volumes/test_catalog/test_schema/chuck/cluster_init-2025-06-02_14-30.sh"
                    )

                    # Verify versioned init script upload was called
                    mock_upload.assert_called_once_with(
                        client=databricks_client_stub,
                        target_catalog="test_catalog",
                        target_schema="test_schema",
                        init_script_content="echo 'Amperity init script'",
                    )

                    # Verify no unsupported columns warning when all columns are supported
                    assert len(metadata["unsupported_columns"]) == 0


class TestHelperSetupStitchLogicEdgeCases:
    """Test edge cases and boundary conditions for _helper_setup_stitch_logic."""

    def test_unsupported_column_types_filtered_out(
        self, databricks_client_stub, llm_client_stub
    ):
        """Unsupported column types are filtered out from Stitch configuration."""
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
                        {"name": "metadata", "type_name": "STRUCT"},  # Unsupported
                        {"name": "tags", "type_name": "ARRAY"},  # Unsupported
                    ],
                )

                # Configure LLM to return PII tags for all columns (including unsupported ones)
                llm_client_stub.set_pii_detection_result(
                    [
                        {"column": "name", "semantic": "given-name"},
                        {
                            "column": "metadata",
                            "semantic": "given-name",
                        },  # Will be filtered
                        {"column": "tags", "semantic": "address"},  # Will be filtered
                    ]
                )

                # Volume exists
                databricks_client_stub.add_volume(
                    "test_catalog", "test_schema", "chuck"
                )

                # Mock upload logic
                with patch(
                    "chuck_data.commands.stitch_tools._helper_upload_cluster_init_logic"
                ) as mock_upload:
                    mock_upload.return_value = {
                        "success": True,
                        "volume_path": "/Volumes/test_catalog/test_schema/chuck/cluster_init-2025-06-02_14-30.sh",
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

                    # Verify unsupported types are not in the config
                    import json

                    config_content = json.dumps(result["stitch_config"])
                    assert "STRUCT" not in config_content
                    assert "ARRAY" not in config_content

                    # Verify supported types are still included
                    assert (
                        "STRING" in config_content or "string" in config_content.lower()
                    )

                    # Verify unsupported columns are reported to user
                    metadata = result["metadata"]
                    unsupported_info = metadata["unsupported_columns"]
                    assert (
                        len(unsupported_info) == 1
                    )  # One table has unsupported columns

                    customers_unsupported = unsupported_info[0]
                    assert "customers" in customers_unsupported["table"]
                    assert (
                        len(customers_unsupported["columns"]) == 2
                    )  # metadata and tags

    def test_all_columns_unsupported_returns_error(
        self, databricks_client_stub, llm_client_stub
    ):
        """All columns with unsupported types returns helpful error."""
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
                        {"column": "metadata", "semantic": "given-name"},
                        {"column": "tags", "semantic": "address"},
                        {"column": "location", "semantic": None},
                    ]
                )

                # Volume exists
                databricks_client_stub.add_volume(
                    "test_catalog", "test_schema", "chuck"
                )

                # Call function - real business logic will filter out all unsupported types
                result = _helper_setup_stitch_logic(
                    databricks_client_stub,
                    llm_client_stub,
                    "test_catalog",
                    "test_schema",
                )

                # Verify results - should fail because no supported columns remain after filtering
                assert "error" in result
                assert "No tables with PII found" in result["error"]

    def test_init_script_upload_failure_returns_error(
        self, databricks_client_stub, llm_client_stub
    ):
        """Init script upload failures are handled gracefully."""
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
                databricks_client_stub.add_volume(
                    "test_catalog", "test_schema", "chuck"
                )

                # Mock upload logic to fail
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


class TestHelperModifyStitchConfigFunction:
    """Test the _helper_modify_stitch_config function behavior."""

    def test_successful_config_modification_returns_updated_config(
        self, llm_client_stub
    ):
        """Successful configuration modification returns updated config."""
        # Setup original config
        original_config = {
            "name": "stitch-test",
            "tables": [
                {
                    "path": "test_catalog.test_schema.users",
                    "fields": [
                        {
                            "field-name": "email",
                            "type": "STRING",
                            "semantics": ["email"],
                        },
                        {
                            "field-name": "name",
                            "type": "STRING",
                            "semantics": ["given-name"],
                        },
                    ],
                }
            ],
            "settings": {
                "output_catalog_name": "test_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }

        # Configure LLM to return modified config (remove email field)
        modified_config = original_config.copy()
        modified_config["tables"][0]["fields"] = [
            {"field-name": "name", "type": "STRING", "semantics": ["given-name"]}
        ]

        import json

        llm_client_stub.set_response_content(json.dumps(modified_config))

        # Execute function
        result = _helper_modify_stitch_config(
            original_config,
            "remove email field from users table",
            llm_client_stub,
            {"test": "metadata"},
        )

        # Verify successful modification
        assert result.get("success")
        assert "stitch_config" in result
        assert "modification_summary" in result

        # Verify config was actually modified
        updated_config = result["stitch_config"]
        assert len(updated_config["tables"][0]["fields"]) == 1
        assert updated_config["tables"][0]["fields"][0]["field-name"] == "name"

    def test_llm_json_code_blocks_handled_correctly(self, llm_client_stub):
        """LLM responses wrapped in JSON code blocks are handled correctly."""
        original_config = {
            "name": "stitch-test",
            "tables": [],
            "settings": {
                "output_catalog_name": "test_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }

        # Configure LLM to return JSON wrapped in code blocks
        import json

        config_json = json.dumps(original_config)
        llm_client_stub.set_response_content(f"```json\n{config_json}\n```")

        # Execute function
        result = _helper_modify_stitch_config(
            original_config, "no changes", llm_client_stub, {"test": "metadata"}
        )

        # Verify code block parsing
        assert result.get("success")
        assert result["stitch_config"] == original_config

    def test_llm_plain_code_blocks_handled_correctly(self, llm_client_stub):
        """LLM responses wrapped in plain code blocks are handled correctly."""
        original_config = {
            "name": "stitch-test",
            "tables": [],
            "settings": {
                "output_catalog_name": "test_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }

        # Configure LLM to return JSON wrapped in plain code blocks
        import json

        config_json = json.dumps(original_config)
        llm_client_stub.set_response_content(f"```\n{config_json}\n```")

        # Execute function
        result = _helper_modify_stitch_config(
            original_config, "no changes", llm_client_stub, {"test": "metadata"}
        )

        # Verify plain code block parsing
        assert result.get("success")
        assert result["stitch_config"] == original_config

    def test_invalid_llm_json_response_returns_error(self, llm_client_stub):
        """Invalid LLM JSON responses are handled gracefully."""
        original_config = {
            "name": "stitch-test",
            "tables": [],
            "settings": {
                "output_catalog_name": "test_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }

        # Configure LLM to return invalid JSON
        llm_client_stub.set_response_content("Invalid JSON response")

        # Execute function
        result = _helper_modify_stitch_config(
            original_config, "modify config", llm_client_stub, {"test": "metadata"}
        )

        # Verify JSON error handling
        assert "error" in result
        assert "LLM returned invalid JSON" in result["error"]

    def test_llm_api_error_handled_gracefully(self, llm_client_stub):
        """LLM API errors are handled gracefully."""
        original_config = {
            "name": "stitch-test",
            "tables": [],
            "settings": {
                "output_catalog_name": "test_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }

        # Configure LLM to throw exception
        llm_client_stub.set_exception(True)

        # Execute function
        result = _helper_modify_stitch_config(
            original_config, "modify config", llm_client_stub, {"test": "metadata"}
        )

        # Verify graceful error handling
        assert "error" in result
        assert "Error modifying configuration" in result["error"]

    def test_config_validation_missing_required_keys_returns_error(
        self, llm_client_stub
    ):
        """Config validation detects missing required keys."""
        original_config = {
            "name": "stitch-test",
            "tables": [],
            "settings": {
                "output_catalog_name": "test_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }

        # Configure LLM to return config missing required keys
        invalid_config = {"name": "test"}  # Missing tables and settings
        import json

        llm_client_stub.set_response_content(json.dumps(invalid_config))

        # Execute function
        result = _helper_modify_stitch_config(
            original_config, "modify config", llm_client_stub, {"test": "metadata"}
        )

        # Verify validation error
        assert "error" in result
        assert "Modified config missing required key" in result["error"]

    def test_config_validation_invalid_table_structure_returns_error(
        self, llm_client_stub
    ):
        """Config validation detects invalid table structure."""
        original_config = {
            "name": "stitch-test",
            "tables": [],
            "settings": {
                "output_catalog_name": "test_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }

        # Configure LLM to return config with invalid table structure
        invalid_config = {
            "name": "test",
            "tables": [{"path": "test.table"}],  # Missing fields array
            "settings": {
                "output_catalog_name": "test_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }
        import json

        llm_client_stub.set_response_content(json.dumps(invalid_config))

        # Execute function
        result = _helper_modify_stitch_config(
            original_config, "modify config", llm_client_stub, {"test": "metadata"}
        )

        # Verify validation error
        assert "error" in result
        assert "Each table must have 'path' and 'fields' properties" in result["error"]


class TestHelperLaunchStitchJobFunction:
    """Test the _helper_launch_stitch_job function behavior."""

    def test_successful_job_launch_returns_run_details(self, databricks_client_stub):
        """Successful job launch returns complete run details."""
        # Setup test configuration and metadata
        stitch_config = {
            "name": "stitch-test",
            "tables": [
                {
                    "path": "test_catalog.test_schema.users",
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
                "output_catalog_name": "test_catalog",
                "output_schema_name": "stitch_outputs",
            },
        }

        metadata = {
            "target_catalog": "test_catalog",
            "target_schema": "test_schema",
            "stitch_job_name": "stitch-test",
            "config_file_path": "/Volumes/test_catalog/test_schema/chuck/stitch-test.json",
            "init_script_path": "/Volumes/test_catalog/test_schema/chuck/cluster_init.sh",
            "init_script_content": "#!/bin/bash\necho init",
            "pii_scan_output": {"message": "PII scan completed"},
            "unsupported_columns": [],
        }

        # Execute function
        result = _helper_launch_stitch_job(
            databricks_client_stub, stitch_config, metadata
        )

        # Verify successful launch
        assert result.get("success")
        assert "run_id" in result
        assert result["run_id"]  # Should have a run_id
        assert "config_path" in result
        assert "init_script_path" in result
        assert "message" in result

    def test_config_file_upload_failure_returns_error(self, databricks_client_stub):
        """Config file upload failures are handled gracefully."""
        stitch_config = {"name": "test", "tables": [], "settings": {}}
        metadata = {
            "target_catalog": "test_catalog",
            "target_schema": "test_schema",
            "stitch_job_name": "stitch-test",
            "config_file_path": "/Volumes/test_catalog/test_schema/chuck/stitch-test.json",
            "init_script_path": "/Volumes/test_catalog/test_schema/chuck/cluster_init.sh",
            "init_script_content": "#!/bin/bash\necho init",
            "pii_scan_output": {},
            "unsupported_columns": [],
        }

        # Configure upload to fail
        def failing_upload_file(path, content=None, overwrite=False, **kwargs):
            if "stitch-test.json" in path:
                return False  # Config file upload fails
            return True

        databricks_client_stub.upload_file = failing_upload_file

        # Execute function
        result = _helper_launch_stitch_job(
            databricks_client_stub, stitch_config, metadata
        )

        # Verify error handling
        assert "error" in result
        assert "Failed to write Stitch config" in result["error"]

    def test_init_script_upload_failure_returns_error(self, databricks_client_stub):
        """Init script upload failures are handled gracefully."""
        stitch_config = {"name": "test", "tables": [], "settings": {}}
        metadata = {
            "target_catalog": "test_catalog",
            "target_schema": "test_schema",
            "stitch_job_name": "stitch-test",
            "config_file_path": "/Volumes/test_catalog/test_schema/chuck/stitch-test.json",
            "init_script_path": "/Volumes/test_catalog/test_schema/chuck/cluster_init.sh",
            "init_script_content": "#!/bin/bash\necho init",
            "pii_scan_output": {},
            "unsupported_columns": [],
        }

        # Configure upload to fail for init script
        def failing_upload_file(path, content=None, overwrite=False, **kwargs):
            if "cluster_init.sh" in path:
                return False  # Init script upload fails
            return True

        databricks_client_stub.upload_file = failing_upload_file

        # Execute function
        result = _helper_launch_stitch_job(
            databricks_client_stub, stitch_config, metadata
        )

        # Verify error handling
        assert "error" in result
        assert "Failed to write init script" in result["error"]

    def test_job_submission_failure_returns_error(self, databricks_client_stub):
        """Job submission failures are handled gracefully."""
        stitch_config = {"name": "test", "tables": [], "settings": {}}
        metadata = {
            "target_catalog": "test_catalog",
            "target_schema": "test_schema",
            "stitch_job_name": "stitch-test",
            "config_file_path": "/Volumes/test_catalog/test_schema/chuck/stitch-test.json",
            "init_script_path": "/Volumes/test_catalog/test_schema/chuck/cluster_init.sh",
            "init_script_content": "#!/bin/bash\necho init",
            "pii_scan_output": {},
            "unsupported_columns": [],
        }

        # Configure job submission to fail
        def failing_submit_job_run(**kwargs):
            raise Exception("Job submission failed")

        databricks_client_stub.submit_job_run = failing_submit_job_run

        # Execute function
        result = _helper_launch_stitch_job(
            databricks_client_stub, stitch_config, metadata
        )

        # Verify error handling
        assert "error" in result
        assert "Failed to launch Stitch job" in result["error"]

    def test_unsupported_columns_included_in_summary(self, databricks_client_stub):
        """Unsupported columns are included in the launch summary."""
        stitch_config = {"name": "test", "tables": [], "settings": {}}
        metadata = {
            "target_catalog": "test_catalog",
            "target_schema": "test_schema",
            "stitch_job_name": "stitch-test",
            "config_file_path": "/Volumes/test_catalog/test_schema/chuck/stitch-test.json",
            "init_script_path": "/Volumes/test_catalog/test_schema/chuck/cluster_init.sh",
            "init_script_content": "#!/bin/bash\necho init",
            "pii_scan_output": {},
            "unsupported_columns": [
                {
                    "table": "test_catalog.test_schema.complex_table",
                    "columns": [
                        {"column": "metadata", "type": "STRUCT", "semantic": "address"},
                        {"column": "tags", "type": "ARRAY", "semantic": None},
                    ],
                }
            ],
        }

        # Execute function
        result = _helper_launch_stitch_job(
            databricks_client_stub, stitch_config, metadata
        )

        # Verify unsupported columns in summary
        assert result.get("success")
        assert "unsupported_columns" in result
        assert len(result["unsupported_columns"]) == 1

        # Verify summary message includes unsupported columns info
        assert "Some columns were excluded" in result["message"]
        assert "complex_table" in result["message"]
        assert "metadata (STRUCT)" in result["message"]


class TestCreateStitchReportNotebookFunction:
    """Test the _create_stitch_report_notebook function behavior."""

    def test_successful_notebook_creation_returns_path(self, databricks_client_stub):
        """Successful notebook creation returns notebook path."""
        stitch_config = {"name": "test", "tables": []}

        # Execute function
        result = _create_stitch_report_notebook(
            databricks_client_stub,
            stitch_config,
            "test_catalog",
            "test_schema",
            "stitch-test",
        )

        # Verify successful creation
        assert result.get("success")
        assert "notebook_path" in result
        assert "/Workspace/test" in result["notebook_path"]  # From stub default
        assert "message" in result

    def test_notebook_creation_api_error_handled_gracefully(
        self, databricks_client_stub
    ):
        """Notebook creation API errors are handled gracefully."""
        stitch_config = {"name": "test", "tables": []}

        # Configure notebook creation to fail
        def failing_create_notebook(**kwargs):
            raise Exception("Notebook creation failed")

        databricks_client_stub.create_stitch_notebook = failing_create_notebook

        # Execute function
        result = _create_stitch_report_notebook(
            databricks_client_stub,
            stitch_config,
            "test_catalog",
            "test_schema",
            "stitch-test",
        )

        # Verify error handling
        assert not result.get("success")
        assert "error" in result
        assert "Notebook creation failed" in result["error"]

    def test_notebook_creation_with_unicode_names(self, databricks_client_stub):
        """Notebook creation with unicode catalog/schema names works correctly."""
        stitch_config = {"name": "test", "tables": []}
        unicode_catalog = "目录_測試"
        unicode_schema = "スキーマ_test"

        # Execute function
        result = _create_stitch_report_notebook(
            databricks_client_stub,
            stitch_config,
            unicode_catalog,
            unicode_schema,
            "stitch-test",
        )

        # Verify unicode handling
        assert result.get("success")
        assert "notebook_path" in result
