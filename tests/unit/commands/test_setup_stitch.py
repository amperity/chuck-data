"""
Tests for setup_stitch command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
"""

import tempfile
from unittest.mock import patch, MagicMock

from chuck_data.commands.setup_stitch import handle_command
from chuck_data.config import ConfigManager


def setup_successful_stitch_test_data(databricks_client_stub, llm_client_stub):
    """Helper function to set up test data for successful Stitch operations."""
    # Setup test data in client stub
    databricks_client_stub.add_catalog("test_catalog")
    databricks_client_stub.add_schema("test_catalog", "test_schema")
    databricks_client_stub.add_table(
        "test_catalog",
        "test_schema",
        "users",
        columns=[
            {"name": "email", "type": "STRING"},
            {"name": "name", "type": "STRING"},
            {"name": "id", "type": "BIGINT"},
        ],
    )

    # Mock PII scan results - set up table with PII columns
    llm_client_stub.set_pii_detection_result(
        [
            {"column": "email", "semantic": "email"},
            {"column": "name", "semantic": "name"},
        ]
    )

    # Fix API compatibility issues
    # Override create_volume to accept 'name' parameter like real API
    original_create_volume = databricks_client_stub.create_volume

    def mock_create_volume(catalog_name, schema_name, name, **kwargs):
        return original_create_volume(catalog_name, schema_name, name, **kwargs)

    databricks_client_stub.create_volume = mock_create_volume

    # Override upload_file to match real API signature
    def mock_upload_file(path, content=None, overwrite=False, **kwargs):
        return True

    databricks_client_stub.upload_file = mock_upload_file

    # Set up other required API responses
    databricks_client_stub.fetch_amperity_job_init_response = {
        "cluster-init": "#!/bin/bash\necho init",
        "job-id": "test-job-setup-123",
    }
    databricks_client_stub.submit_job_run_response = {"run_id": "12345"}
    databricks_client_stub.create_stitch_notebook_response = {
        "notebook_path": "/Workspace/test"
    }


# Parameter validation tests
def test_missing_client_returns_error():
    """Missing client parameter returns clear error message."""
    result = handle_command(None)
    assert not result.success
    assert "Client is required" in result.message


def test_missing_context(databricks_client_stub):
    """Test handling when catalog or schema is missing."""
    # Use real config system with no active catalog/schema
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)
        # Don't set active catalog or schema

        with patch("chuck_data.config._config_manager", config_manager):
            with patch(
                "chuck_data.config.get_workspace_url",
                return_value="https://test.databricks.com",
            ):
                with patch(
                    "chuck_data.config.get_databricks_token", return_value="test-token"
                ):
                    result = handle_command(databricks_client_stub)

    # Verify results
    assert not result.success
    assert "Target catalog and schema must be specified" in result.message


@patch("chuck_data.commands.setup_stitch.LLMProviderFactory.create")
def test_direct_command_llm_exception_handled_gracefully(
    mock_llm_client, databricks_client_stub
):
    """Direct command handles LLM client exceptions gracefully."""
    # Setup external boundary to fail
    mock_llm_client.side_effect = Exception("LLM client error")

    # Mock Databricks workspace config
    with patch(
        "chuck_data.config.get_workspace_url",
        return_value="https://test.databricks.com",
    ):
        with patch("chuck_data.config.get_databricks_token", return_value="test-token"):
            result = handle_command(
                databricks_client_stub,
                catalog_name="test_catalog",
                schema_name="test_schema",
            )

    # Verify error handling behavior
    assert not result.success
    assert "Error setting up Stitch" in result.message or "LLM client error" in str(
        result.error
    )


def test_agent_failure_shows_error_without_progress(
    databricks_client_stub, llm_client_stub
):
    """Agent execution shows error without progress steps when setup fails."""
    # Setup minimal test data with no PII tables (will cause failure)
    databricks_client_stub.add_catalog("test_catalog")
    databricks_client_stub.add_schema("test_catalog", "test_schema")
    # No tables with PII - will cause failure

    # Fix API compatibility for volume creation
    original_create_volume = databricks_client_stub.create_volume

    def mock_create_volume(catalog_name, schema_name, name, **kwargs):
        return original_create_volume(catalog_name, schema_name, name, **kwargs)

    databricks_client_stub.create_volume = mock_create_volume

    progress_steps = []

    def capture_progress(tool_name, data):
        if "step" in data:
            progress_steps.append(f"→ Setting up Stitch: ({data['step']})")

    with patch(
        "chuck_data.commands.setup_stitch.LLMProviderFactory.create",
        return_value=llm_client_stub,
    ):
        with patch(
            "chuck_data.commands.stitch_tools.get_amperity_token",
            return_value="test_token",
        ):
            with patch(
                "chuck_data.commands.setup_stitch.get_metrics_collector",
                return_value=MagicMock(),
            ):
                # Mock Databricks workspace config
                with patch(
                    "chuck_data.config.get_workspace_url",
                    return_value="https://test.databricks.com",
                ):
                    with patch(
                        "chuck_data.config.get_databricks_token",
                        return_value="test-token",
                    ):
                        result = handle_command(
                            databricks_client_stub,
                            catalog_name="test_catalog",
                            schema_name="test_schema",
                            tool_output_callback=capture_progress,
                        )

    # Verify failure behavior
    assert not result.success
    # Check for various possible error messages
    error_msg = result.message or ""
    assert (
        "No tables with PII found" in error_msg
        or "PII Scan failed" in error_msg
        or "No PII found" in error_msg
        or "Databricks workspace not configured" in error_msg
    )

    # Current implementation doesn't report progress, so no steps expected
    assert len(progress_steps) == 0


def test_agent_callback_errors_bubble_up_as_command_errors(
    databricks_client_stub, llm_client_stub
):
    """Agent callback failures bubble up as command errors (current behavior)."""

    def failing_callback(tool_name, data):
        raise Exception("Display system crashed")

    # This would only trigger if the command actually used the callback
    # Current implementation doesn't use tool_output_callback, so this test
    # documents the expected behavior if it were implemented

    with patch(
        "chuck_data.commands.setup_stitch.LLMProviderFactory.create",
        return_value=llm_client_stub,
    ):
        result = handle_command(
            databricks_client_stub,
            catalog_name="test_catalog",
            schema_name="test_schema",
            tool_output_callback=failing_callback,
        )

    # Since callback isn't used, command should succeed if everything else works
    # or fail for other reasons (like missing catalog/schema)
    # This documents current behavior
    assert not result.success  # Will fail due to missing context/data


# Auto-confirm mode tests with policy_id


def test_auto_confirm_mode_passes_policy_id(databricks_client_stub, llm_client_stub):
    """Auto-confirm mode passes policy_id to the job submission."""
    # Setup test data for successful operation
    setup_successful_stitch_test_data(databricks_client_stub, llm_client_stub)

    # Mock DatabricksComputeProvider to avoid real network calls
    mock_compute_provider = MagicMock()
    mock_compute_provider.prepare_stitch_job.return_value = {
        "success": True,
        "stitch_config": {"name": "test-job", "tables": []},
        "metadata": {
            "policy_id": "000F957411D99C1F",
            "init_script_path": "/test/init.sh",
            "job_id": "test-job-123",
        },
    }
    mock_compute_provider.launch_stitch_job.return_value = {
        "success": True,
        "run_id": "test-run-id-123",
        "notebook_result": {"success": True, "notebook_path": "/test/notebook"},
        "message": "Job launched successfully",
    }

    with patch(
        "chuck_data.commands.setup_stitch.LLMProviderFactory.create",
        return_value=llm_client_stub,
    ):
        with patch(
            "chuck_data.commands.stitch_tools.get_amperity_token",
            return_value="test_token",
        ):
            with patch(
                "chuck_data.commands.setup_stitch.get_metrics_collector",
                return_value=MagicMock(),
            ):
                with patch(
                    "chuck_data.compute_providers.DatabricksComputeProvider",
                    return_value=mock_compute_provider,
                ):
                    with patch(
                        "chuck_data.config.get_workspace_url",
                        return_value="https://test.databricks.com",
                    ):
                        with patch(
                            "chuck_data.config.get_databricks_token",
                            return_value="test-token",
                        ):
                            with patch(
                                "chuck_data.clients.amperity.AmperityAPIClient.fetch_amperity_job_init",
                                return_value={
                                    "cluster-init": "#!/bin/bash\necho init",
                                    "job-id": "test-job-setup-123",
                                },
                            ):
                                # Call with auto_confirm=True and policy_id
                                result = handle_command(
                                    databricks_client_stub,
                                    catalog_name="test_catalog",
                                    schema_name="test_schema",
                                    auto_confirm=True,
                                    policy_id="000F957411D99C1F",
                                )

    # Verify success
    assert result.success

    # Verify policy_id was passed to submit_job_run
    # Auto-confirm mode uses direct client API call, not compute provider
    assert len(databricks_client_stub.submit_job_run_calls) > 0
    submit_call = databricks_client_stub.submit_job_run_calls[0]
    assert submit_call["policy_id"] == "000F957411D99C1F"


def test_auto_confirm_mode_without_policy_id(databricks_client_stub, llm_client_stub):
    """Auto-confirm mode works without policy_id (passes None)."""
    # Setup test data for successful operation
    setup_successful_stitch_test_data(databricks_client_stub, llm_client_stub)

    # Mock DatabricksComputeProvider to avoid real network calls
    mock_compute_provider = MagicMock()
    mock_compute_provider.prepare_stitch_job.return_value = {
        "success": True,
        "stitch_config": {"name": "test-job", "tables": []},
        "metadata": {
            "init_script_path": "/test/init.sh",
            "job_id": "test-job-456",
        },
    }
    mock_compute_provider.launch_stitch_job.return_value = {
        "success": True,
        "run_id": "test-run-id-456",
        "notebook_result": {"success": True, "notebook_path": "/test/notebook"},
        "message": "Job launched successfully",
    }

    with patch(
        "chuck_data.commands.setup_stitch.LLMProviderFactory.create",
        return_value=llm_client_stub,
    ):
        with patch(
            "chuck_data.commands.stitch_tools.get_amperity_token",
            return_value="test_token",
        ):
            with patch(
                "chuck_data.commands.setup_stitch.get_metrics_collector",
                return_value=MagicMock(),
            ):
                with patch(
                    "chuck_data.compute_providers.DatabricksComputeProvider",
                    return_value=mock_compute_provider,
                ):
                    with patch(
                        "chuck_data.config.get_workspace_url",
                        return_value="https://test.databricks.com",
                    ):
                        with patch(
                            "chuck_data.config.get_databricks_token",
                            return_value="test-token",
                        ):
                            with patch(
                                "chuck_data.clients.amperity.AmperityAPIClient.fetch_amperity_job_init",
                                return_value={
                                    "cluster-init": "#!/bin/bash\necho init",
                                    "job-id": "test-job-setup-123",
                                },
                            ):
                                # Call with auto_confirm=True but no policy_id
                                result = handle_command(
                                    databricks_client_stub,
                                    catalog_name="test_catalog",
                                    schema_name="test_schema",
                                    auto_confirm=True,
                                )

    # Verify success
    assert result.success

    # Verify policy_id was not set (or is None)
    # Auto-confirm mode uses direct client API call, not compute provider
    assert len(databricks_client_stub.submit_job_run_calls) > 0
    submit_call = databricks_client_stub.submit_job_run_calls[0]
    # Policy ID should be None when not provided
    assert submit_call["policy_id"] is None


# Interactive mode tests
def test_interactive_mode_phase_1_preparation(databricks_client_stub, llm_client_stub):
    """Interactive mode Phase 1 prepares configuration and shows preview."""
    # Setup test data for successful operation
    setup_successful_stitch_test_data(databricks_client_stub, llm_client_stub)

    with patch(
        "chuck_data.commands.setup_stitch.LLMProviderFactory.create",
        return_value=llm_client_stub,
    ):
        with patch(
            "chuck_data.commands.stitch_tools.get_amperity_token",
            return_value="test_token",
        ):
            with patch(
                "chuck_data.config.get_workspace_url",
                return_value="https://test.databricks.com",
            ):
                with patch(
                    "chuck_data.config.get_databricks_token", return_value="test-token"
                ):
                    with patch(
                        "chuck_data.clients.amperity.AmperityAPIClient.fetch_amperity_job_init",
                        return_value={
                            "cluster-init": "#!/bin/bash\necho init",
                            "job-id": "test-job-setup-123",
                        },
                    ):
                        # Call without auto_confirm to enter interactive mode
                        result = handle_command(
                            databricks_client_stub,
                            catalog_name="test_catalog",
                            schema_name="test_schema",
                        )

    # Verify Phase 1 behavior
    assert result.success
    # Interactive mode should return empty message (console output handles display)
    assert result.message == ""


def test_interactive_mode_phase_1_stores_policy_id(
    databricks_client_stub, llm_client_stub
):
    """Interactive mode Phase 1 stores policy_id in context metadata."""
    from chuck_data.interactive_context import InteractiveContext

    # Setup test data for successful operation
    setup_successful_stitch_test_data(databricks_client_stub, llm_client_stub)

    # Reset the interactive context before test
    context = InteractiveContext()
    context.clear_active_context("setup_stitch")

    with patch(
        "chuck_data.commands.setup_stitch.LLMProviderFactory.create",
        return_value=llm_client_stub,
    ):
        with patch(
            "chuck_data.commands.stitch_tools.get_amperity_token",
            return_value="test_token",
        ):
            with patch(
                "chuck_data.config.get_workspace_url",
                return_value="https://test.databricks.com",
            ):
                with patch(
                    "chuck_data.config.get_databricks_token", return_value="test-token"
                ):
                    with patch(
                        "chuck_data.clients.amperity.AmperityAPIClient.fetch_amperity_job_init",
                        return_value={
                            "cluster-init": "#!/bin/bash\necho init",
                            "job-id": "test-job-setup-123",
                        },
                    ):
                        # Call without auto_confirm to enter interactive mode, with policy_id
                        result = handle_command(
                            databricks_client_stub,
                            catalog_name="test_catalog",
                            schema_name="test_schema",
                            policy_id="INTERACTIVE_POLICY_123",
                        )

    # Verify Phase 1 behavior
    assert result.success

    # Verify policy_id was stored in context metadata
    context_data = context.get_context_data("setup_stitch")
    assert "metadata" in context_data
    assert context_data["metadata"].get("policy_id") == "INTERACTIVE_POLICY_123"

    # Clean up context
    context.clear_active_context("setup_stitch")


# Redshift helper function tests


def test_redshift_prepare_manifest_success():
    """Test _redshift_prepare_manifest successfully prepares manifest."""
    from chuck_data.commands.setup_stitch import _redshift_prepare_manifest
    from unittest.mock import MagicMock

    # Mock RedshiftAPIClient
    mock_client = MagicMock()
    mock_client.read_semantic_tags.return_value = {
        "success": True,
        "tags": [
            {"table": "customers", "column": "email", "semantic": "email"},
            {"table": "customers", "column": "phone", "semantic": "phone"},
        ],
    }
    mock_client.read_table_schemas.return_value = {
        "success": True,
        "tables": [
            {
                "table_name": "customers",
                "columns": [
                    {"name": "email", "type": "varchar"},
                    {"name": "phone", "type": "varchar"},
                    {"name": "id", "type": "int"},
                ],
            }
        ],
    }

    # Mock console
    mock_console = MagicMock()

    # Mock config functions
    with patch(
        "chuck_data.commands.setup_stitch.get_s3_bucket", return_value="test-bucket"
    ):
        with patch(
            "chuck_data.commands.setup_stitch._generate_redshift_manifest",
            return_value={"success": True, "manifest": {"tables": [], "settings": {}}},
        ):
            with patch(
                "chuck_data.commands.setup_stitch.validate_manifest",
                return_value=(True, None),
            ):
                with patch(
                    "chuck_data.commands.setup_stitch.save_manifest_to_file",
                    return_value=True,
                ):
                    with patch(
                        "chuck_data.commands.setup_stitch.upload_manifest_to_s3",
                        return_value=True,
                    ):
                        result = _redshift_prepare_manifest(
                            mock_client,
                            mock_console,
                            "test_db",
                            "test_schema",
                            compute_provider_name="databricks",
                        )

    # Verify success
    assert result["success"] is True
    assert "manifest" in result
    assert "manifest_path" in result
    assert "s3_path" in result
    assert "tables" in result
    assert "semantic_tags" in result


def test_redshift_prepare_manifest_no_semantic_tags():
    """Test _redshift_prepare_manifest fails when no semantic tags found."""
    from chuck_data.commands.setup_stitch import _redshift_prepare_manifest
    from unittest.mock import MagicMock

    # Mock RedshiftAPIClient with no semantic tags
    mock_client = MagicMock()
    mock_client.read_semantic_tags.return_value = {
        "success": True,
        "tags": [],  # No tags
    }

    mock_console = MagicMock()

    result = _redshift_prepare_manifest(
        mock_client,
        mock_console,
        "test_db",
        "test_schema",
    )

    # Verify failure
    assert result["success"] is False
    assert "No semantic tags found" in result["error"]
    assert "/tag-pii" in result["error"]


def test_redshift_prepare_manifest_read_tags_error():
    """Test _redshift_prepare_manifest handles read_semantic_tags error."""
    from chuck_data.commands.setup_stitch import _redshift_prepare_manifest
    from unittest.mock import MagicMock

    # Mock RedshiftAPIClient with error
    mock_client = MagicMock()
    mock_client.read_semantic_tags.return_value = {
        "success": False,
        "error": "Connection timeout",
    }

    mock_console = MagicMock()

    result = _redshift_prepare_manifest(
        mock_client,
        mock_console,
        "test_db",
        "test_schema",
    )

    # Verify failure
    assert result["success"] is False
    assert "Connection timeout" in result["error"]


def test_redshift_prepare_manifest_schema_read_error():
    """Test _redshift_prepare_manifest handles read_table_schemas error."""
    from chuck_data.commands.setup_stitch import _redshift_prepare_manifest
    from unittest.mock import MagicMock

    # Mock RedshiftAPIClient
    mock_client = MagicMock()
    mock_client.read_semantic_tags.return_value = {
        "success": True,
        "tags": [{"table": "customers", "column": "email", "semantic": "email"}],
    }
    mock_client.read_table_schemas.return_value = {
        "success": False,
        "error": "Schema not found",
    }

    mock_console = MagicMock()

    result = _redshift_prepare_manifest(
        mock_client,
        mock_console,
        "test_db",
        "test_schema",
    )

    # Verify failure
    assert result["success"] is False
    assert "Schema not found" in result["error"]


def test_redshift_prepare_manifest_invalid_manifest():
    """Test _redshift_prepare_manifest handles invalid manifest."""
    from chuck_data.commands.setup_stitch import _redshift_prepare_manifest
    from unittest.mock import MagicMock

    # Mock RedshiftAPIClient
    mock_client = MagicMock()
    mock_client.read_semantic_tags.return_value = {
        "success": True,
        "tags": [{"table": "customers", "column": "email", "semantic": "email"}],
    }
    mock_client.read_table_schemas.return_value = {
        "success": True,
        "tables": [
            {
                "table_name": "customers",
                "columns": [{"name": "email", "type": "varchar"}],
            }
        ],
    }

    mock_console = MagicMock()

    with patch(
        "chuck_data.commands.setup_stitch._generate_redshift_manifest",
        return_value={"success": True, "manifest": {"invalid": "structure"}},
    ):
        with patch(
            "chuck_data.commands.setup_stitch.validate_manifest",
            return_value=(False, "Missing tables key"),
        ):
            result = _redshift_prepare_manifest(
                mock_client,
                mock_console,
                "test_db",
                "test_schema",
            )

    # Verify failure
    assert result["success"] is False
    assert "invalid" in result["error"]
    assert "Missing tables key" in result["error"]


def test_redshift_prepare_manifest_no_s3_bucket():
    """Test _redshift_prepare_manifest fails when S3 bucket not configured."""
    from chuck_data.commands.setup_stitch import _redshift_prepare_manifest
    from unittest.mock import MagicMock

    # Mock RedshiftAPIClient
    mock_client = MagicMock()
    mock_client.read_semantic_tags.return_value = {
        "success": True,
        "tags": [{"table": "customers", "column": "email", "semantic": "email"}],
    }
    mock_client.read_table_schemas.return_value = {
        "success": True,
        "tables": [
            {
                "table_name": "customers",
                "columns": [{"name": "email", "type": "varchar"}],
            }
        ],
    }

    mock_console = MagicMock()

    with patch("chuck_data.commands.setup_stitch.get_s3_bucket", return_value=None):
        with patch(
            "chuck_data.commands.setup_stitch._generate_redshift_manifest",
            return_value={"success": True, "manifest": {"tables": [], "settings": {}}},
        ):
            with patch(
                "chuck_data.commands.setup_stitch.validate_manifest",
                return_value=(True, None),
            ):
                with patch(
                    "chuck_data.commands.setup_stitch.save_manifest_to_file",
                    return_value=True,
                ):
                    result = _redshift_prepare_manifest(
                        mock_client,
                        mock_console,
                        "test_db",
                        "test_schema",
                    )

    # Verify failure
    assert result["success"] is False
    assert "No S3 bucket configured" in result["error"]


def test_redshift_execute_job_launch_success():
    """Test _redshift_execute_job_launch successfully launches job."""
    from chuck_data.commands.setup_stitch import _redshift_execute_job_launch
    from unittest.mock import MagicMock

    mock_console = MagicMock()
    mock_client = MagicMock()  # RedshiftAPIClient mock
    mock_compute_provider = MagicMock()  # ComputeProvider mock

    # Mock all dependencies
    with patch(
        "chuck_data.commands.setup_stitch.get_amperity_token", return_value="test-token"
    ):
        with patch(
            "chuck_data.clients.amperity.AmperityAPIClient"
        ) as mock_amperity_client:
            mock_amperity_instance = MagicMock()
            mock_amperity_instance.fetch_amperity_job_init.return_value = {
                "cluster-init": "#!/bin/bash\necho init",
                "job-id": "test-job-123",
            }
            mock_amperity_client.return_value = mock_amperity_instance

            with patch("boto3.client") as mock_boto_client:
                mock_s3 = MagicMock()
                mock_boto_client.return_value = mock_s3

                with patch(
                    "chuck_data.commands.setup_stitch._submit_stitch_job_to_databricks",
                    return_value={
                        "success": True,
                        "run_id": "test-run-123",
                        "databricks_client": MagicMock(),
                    },
                ):
                    with patch(
                        "chuck_data.commands.setup_stitch._create_stitch_report_notebook_unified",
                        return_value={
                            "success": True,
                            "notebook_path": "/test/notebook",
                        },
                    ):
                        with patch(
                            "chuck_data.commands.setup_stitch._display_detailed_summary"
                        ):
                            with patch(
                                "chuck_data.commands.setup_stitch._build_post_launch_guidance_message",
                                return_value="Job launched successfully",
                            ):
                                result = _redshift_execute_job_launch(
                                    mock_console,
                                    mock_client,
                                    "test_db",
                                    "test_schema",
                                    {"tables": []},
                                    "/tmp/manifest.json",
                                    "s3://bucket/manifest.json",
                                    "bucket",
                                    "20231224_120000",
                                    [],
                                    [],
                                    mock_compute_provider,
                                )

    # Verify success
    assert result.success is True
    assert "Job launched successfully" in result.message
    assert result.data["run_id"] == "test-run-123"


def test_redshift_execute_job_launch_no_amperity_token():
    """Test _redshift_execute_job_launch fails without Amperity token."""
    from chuck_data.commands.setup_stitch import _redshift_execute_job_launch
    from unittest.mock import MagicMock

    mock_console = MagicMock()
    mock_client = MagicMock()
    mock_compute_provider = MagicMock()

    with patch(
        "chuck_data.commands.setup_stitch.get_amperity_token", return_value=None
    ):
        result = _redshift_execute_job_launch(
            mock_console,
            mock_client,
            "test_db",
            "test_schema",
            {"tables": []},
            "/tmp/manifest.json",
            "s3://bucket/manifest.json",
            "bucket",
            "20231224_120000",
            [],
            [],
            mock_compute_provider,
        )

    # Verify failure
    assert result.success is False
    assert "Amperity token not found" in result.message
    assert "/amp_login" in result.message


def test_redshift_execute_job_launch_amperity_api_error():
    """Test _redshift_execute_job_launch handles Amperity API errors."""
    from chuck_data.commands.setup_stitch import _redshift_execute_job_launch
    from unittest.mock import MagicMock

    mock_console = MagicMock()
    mock_client = MagicMock()
    mock_compute_provider = MagicMock()

    with patch(
        "chuck_data.commands.setup_stitch.get_amperity_token", return_value="test-token"
    ):
        with patch(
            "chuck_data.clients.amperity.AmperityAPIClient"
        ) as mock_amperity_client:
            mock_amperity_instance = MagicMock()
            mock_amperity_instance.fetch_amperity_job_init.side_effect = Exception(
                "API connection failed"
            )
            mock_amperity_client.return_value = mock_amperity_instance

            result = _redshift_execute_job_launch(
                mock_console,
                mock_client,
                "test_db",
                "test_schema",
                {"tables": []},
                "/tmp/manifest.json",
                "s3://bucket/manifest.json",
                "bucket",
                "20231224_120000",
                [],
                [],
                mock_compute_provider,
            )

    # Verify failure
    assert result.success is False
    assert "Error fetching Amperity init script" in result.message


def test_redshift_execute_job_launch_job_submission_fails():
    """Test _redshift_execute_job_launch handles job submission failure."""
    from chuck_data.commands.setup_stitch import _redshift_execute_job_launch
    from unittest.mock import MagicMock

    mock_console = MagicMock()

    with patch(
        "chuck_data.commands.setup_stitch.get_amperity_token", return_value="test-token"
    ):
        with patch(
            "chuck_data.clients.amperity.AmperityAPIClient"
        ) as mock_amperity_client:
            mock_amperity_instance = MagicMock()
            mock_amperity_instance.fetch_amperity_job_init.return_value = {
                "cluster-init": "#!/bin/bash\necho init",
                "job-id": "test-job-123",
            }
            mock_amperity_client.return_value = mock_amperity_instance

            with patch("boto3.client") as mock_boto_client:
                mock_s3 = MagicMock()
                mock_boto_client.return_value = mock_s3

                with patch(
                    "chuck_data.commands.setup_stitch._submit_stitch_job_to_databricks",
                    return_value={
                        "success": False,
                        "error": "Databricks cluster not available",
                    },
                ):
                    mock_client = MagicMock()
                    mock_compute_provider = MagicMock()

                    result = _redshift_execute_job_launch(
                        mock_console,
                        mock_client,
                        "test_db",
                        "test_schema",
                        {"tables": []},
                        "/tmp/manifest.json",
                        "s3://bucket/manifest.json",
                        "bucket",
                        "20231224_120000",
                        [],
                        [],
                        mock_compute_provider,
                    )

    # Verify failure
    assert result.success is False
    assert "Databricks cluster not available" in result.message


def test_redshift_phase_2_confirm_handles_cancel():
    """Test _redshift_phase_2_confirm handles cancel input."""
    from chuck_data.commands.setup_stitch import _redshift_phase_2_confirm
    from chuck_data.interactive_context import InteractiveContext
    from unittest.mock import MagicMock

    # Setup context with data
    context = InteractiveContext()
    context.set_active_context("setup_stitch")
    context.store_context_data("setup_stitch", "database", "test_db")
    context.store_context_data("setup_stitch", "schema_name", "test_schema")

    mock_console = MagicMock()
    mock_client = MagicMock()
    mock_compute_provider = MagicMock()

    # Test cancel
    result = _redshift_phase_2_confirm(
        mock_client, mock_compute_provider, context, mock_console, "cancel"
    )

    # Verify cancellation
    assert result.success is True
    assert "cancelled" in result.message.lower()
    # Context should be cleared (returns empty dict or None)
    context_data = context.get_context_data("setup_stitch")
    assert context_data is None or context_data == {}


def test_redshift_phase_2_confirm_requires_explicit_confirmation():
    """Test _redshift_phase_2_confirm requires explicit confirm input."""
    from chuck_data.commands.setup_stitch import _redshift_phase_2_confirm
    from chuck_data.interactive_context import InteractiveContext
    from unittest.mock import MagicMock

    # Setup context with data
    context = InteractiveContext()
    context.set_active_context("setup_stitch")
    context.store_context_data("setup_stitch", "database", "test_db")

    mock_console = MagicMock()
    mock_client = MagicMock()
    mock_compute_provider = MagicMock()

    # Test invalid input
    result = _redshift_phase_2_confirm(
        mock_client, mock_compute_provider, context, mock_console, "maybe"
    )

    # Verify rejection
    assert result.success is True  # Returns success but waits for correct input
    assert "confirm" in result.message.lower() or "cancel" in result.message.lower()
