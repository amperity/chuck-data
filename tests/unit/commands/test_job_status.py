"""
Tests for job_status command handler.

Behavioral tests focused on command execution patterns rather than implementation details.
These tests verify what users see when they interact with the job-status command,
including job run status display, error handling, and various job run states.
"""

import pytest
from unittest.mock import patch

from chuck_data.commands.job_status import handle_command
from chuck_data.config import ConfigManager


class TestJobStatusParameterValidation:
    """Test parameter validation for job_status command."""

    def test_missing_run_id_parameter_returns_error(self, databricks_client_stub):
        """Missing run_id parameter returns helpful error."""
        result = handle_command(databricks_client_stub)

        # Current implementation passes None run_id to API which returns None response
        assert not result.success
        assert "No job run found with ID: None" in result.message

    def test_none_client_returns_setup_error(self):
        """None client returns workspace setup error."""
        result = handle_command(None, run_id="12345")

        assert not result.success
        assert "workspace" in result.message.lower()
        assert "set up" in result.message.lower()


class TestDirectJobStatusCommand:
    """Test direct job_status command execution."""

    def test_direct_command_shows_running_job_status(self, databricks_client_stub):
        """Direct job_status command shows running job status details."""
        # Setup running job
        databricks_client_stub.add_simple_job_run(
            run_id="12345",
            job_id=67890,
            run_name="ETL Processing Job",
            life_cycle_state="RUNNING",
            creator_user_name="data.engineer@company.com",
            start_time=1640995200000,
            setup_duration=30000,
            execution_duration=150000,
        )

        result = handle_command(databricks_client_stub, run_id="12345")

        # Verify successful execution
        assert result.success
        assert "Job run 12345 is RUNNING" in result.message

        # Verify complete job run data is returned
        assert result.data is not None
        assert result.data["run_id"] == 12345
        assert result.data["job_id"] == 67890
        assert result.data["run_name"] == "ETL Processing Job"
        assert result.data["state"] == "RUNNING"
        assert result.data["creator_user_name"] == "data.engineer@company.com"
        assert result.data["start_time"] == 1640995200000
        assert result.data["setup_duration"] == 30000
        assert result.data["execution_duration"] == 150000

    def test_direct_command_shows_completed_job_with_result_state(
        self, databricks_client_stub
    ):
        """Direct job_status command shows completed job with result state."""
        databricks_client_stub.add_simple_job_run(
            run_id="54321",
            job_id=11111,
            run_name="Data Validation Job",
            life_cycle_state="TERMINATED",
            result_state="SUCCESS",
            setup_duration=15000,
            execution_duration=45000,
            cleanup_duration=5000,
        )

        result = handle_command(databricks_client_stub, run_id="54321")

        assert result.success
        assert "Job run 54321 is TERMINATED (SUCCESS)" in result.message

        # Verify result includes both states
        assert result.data["state"] == "TERMINATED"
        assert result.data["result_state"] == "SUCCESS"
        assert result.data["setup_duration"] == 15000
        assert result.data["execution_duration"] == 45000
        assert result.data["cleanup_duration"] == 5000

    def test_direct_command_shows_failed_job_status(self, databricks_client_stub):
        """Direct job_status command shows failed job status clearly."""
        databricks_client_stub.add_simple_job_run(
            run_id="99999",
            run_name="Failed Analysis Job",
            life_cycle_state="TERMINATED",
            result_state="FAILED",
            execution_duration=5000,
        )

        result = handle_command(databricks_client_stub, run_id="99999")

        assert result.success
        assert "Job run 99999 is TERMINATED (FAILED)" in result.message
        assert result.data["state"] == "TERMINATED"
        assert result.data["result_state"] == "FAILED"

    def test_direct_command_shows_job_with_tasks(self, databricks_client_stub):
        """Direct job_status command shows job with multiple tasks."""
        # Setup job with multiple tasks
        tasks = [
            {
                "task_key": "extract_data",
                "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
                "start_time": 1640995200000,
                "execution_duration": 30000,
            },
            {
                "task_key": "transform_data",
                "state": {"life_cycle_state": "RUNNING"},
                "start_time": 1640995230000,
                "setup_duration": 5000,
            },
            {
                "task_key": "load_data",
                "state": {"life_cycle_state": "PENDING"},
            },
        ]

        databricks_client_stub.add_simple_job_run(
            run_id="77777",
            run_name="Multi-Task ETL Job",
            life_cycle_state="RUNNING",
            tasks=tasks,
        )

        result = handle_command(databricks_client_stub, run_id="77777")

        assert result.success
        assert result.data["tasks"] is not None
        assert len(result.data["tasks"]) == 3

        # Verify task information is correctly extracted
        task_data = result.data["tasks"]
        assert task_data[0]["task_key"] == "extract_data"
        assert task_data[0]["state"] == "TERMINATED"
        assert task_data[0]["result_state"] == "SUCCESS"
        assert task_data[0]["execution_duration"] == 30000

        assert task_data[1]["task_key"] == "transform_data"
        assert task_data[1]["state"] == "RUNNING"
        assert task_data[1]["setup_duration"] == 5000

        assert task_data[2]["task_key"] == "load_data"
        assert task_data[2]["state"] == "PENDING"

    def test_direct_command_shows_various_job_states(self, databricks_client_stub):
        """Direct job_status command shows jobs in various lifecycle states."""
        test_states = [
            ("PENDING", None, "pending_run", "Job run pending_run is PENDING"),
            ("RUNNING", None, "running_run", "Job run running_run is RUNNING"),
            (
                "TERMINATING",
                None,
                "terminating_run",
                "Job run terminating_run is TERMINATING",
            ),
            (
                "TERMINATED",
                "SUCCESS",
                "success_run",
                "Job run success_run is TERMINATED (SUCCESS)",
            ),
            (
                "TERMINATED",
                "FAILED",
                "failed_run",
                "Job run failed_run is TERMINATED (FAILED)",
            ),
            (
                "TERMINATED",
                "CANCELED",
                "canceled_run",
                "Job run canceled_run is TERMINATED (CANCELED)",
            ),
            ("SKIPPED", None, "skipped_run", "Job run skipped_run is SKIPPED"),
        ]

        for life_cycle_state, result_state, run_id, expected_message in test_states:
            # Clear previous runs
            databricks_client_stub.job_runs.clear()

            databricks_client_stub.add_simple_job_run(
                run_id=run_id,
                run_name=f"Test {life_cycle_state} Job",
                life_cycle_state=life_cycle_state,
                result_state=result_state,
            )

            result = handle_command(databricks_client_stub, run_id=run_id)

            assert result.success
            assert expected_message in result.message
            assert result.data["state"] == life_cycle_state
            if result_state:
                assert result.data["result_state"] == result_state

    def test_direct_command_handles_nonexistent_job_run(self, databricks_client_stub):
        """Direct job_status command handles non-existent job run gracefully."""
        # Don't add any job runs to the stub
        result = handle_command(databricks_client_stub, run_id="nonexistent_run")

        assert not result.success
        assert "No job run found with ID: nonexistent_run" in result.message

    def test_direct_command_handles_null_run_id(self, databricks_client_stub):
        """Direct job_status command handles null run_id parameter."""
        result = handle_command(databricks_client_stub, run_id=None)

        # Current implementation behavior with None run_id
        assert not result.success
        assert "No job run found with ID: None" in result.message

    def test_direct_command_handles_api_errors_gracefully(self, databricks_client_stub):
        """Direct job_status command handles API errors gracefully."""
        # Configure stub to raise exception
        databricks_client_stub.set_get_job_run_status_error(
            Exception("API service unavailable")
        )

        result = handle_command(databricks_client_stub, run_id="12345")

        # Reset error state
        databricks_client_stub.clear_get_job_run_status_error()

        assert not result.success
        assert "Failed to get job run status" in result.message
        assert "API service unavailable" in result.message
        assert result.error is not None

    def test_direct_command_with_job_run_missing_optional_fields(
        self, databricks_client_stub
    ):
        """Direct job_status command handles job runs with missing optional fields."""
        # Add minimal job run data
        minimal_job_run = {
            "job_id": 12345,
            "run_id": 99999,
            "state": {"life_cycle_state": "RUNNING"},
            # Missing optional fields like run_name, creator_user_name, etc.
        }
        databricks_client_stub.add_job_run("99999", minimal_job_run)

        result = handle_command(databricks_client_stub, run_id="99999")

        assert result.success
        assert "Job run 99999 is RUNNING" in result.message
        assert result.data["run_id"] == 99999
        assert result.data["job_id"] == 12345
        assert result.data["state"] == "RUNNING"
        # Optional fields should be None
        assert result.data["run_name"] is None
        assert result.data["creator_user_name"] is None

    def test_direct_command_with_comprehensive_job_run_data(
        self, databricks_client_stub
    ):
        """Direct job_status command handles comprehensive job run data."""
        comprehensive_job_run = {
            "job_id": 55555,
            "run_id": 77777,
            "run_name": "Comprehensive Data Pipeline",
            "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
            "creator_user_name": "pipeline.admin@company.com",
            "start_time": 1640995200000,
            "setup_duration": 45000,
            "execution_duration": 300000,
            "cleanup_duration": 15000,
            "tasks": [
                {
                    "task_key": "validate_inputs",
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "SUCCESS",
                    },
                    "start_time": 1640995200000,
                    "setup_duration": 5000,
                    "execution_duration": 10000,
                    "cleanup_duration": 2000,
                },
                {
                    "task_key": "process_data",
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "SUCCESS",
                    },
                    "start_time": 1640995220000,
                    "setup_duration": 8000,
                    "execution_duration": 280000,
                    "cleanup_duration": 5000,
                },
            ],
        }
        databricks_client_stub.add_job_run("77777", comprehensive_job_run)

        result = handle_command(databricks_client_stub, run_id="77777")

        assert result.success
        assert "Job run 77777 is TERMINATED (SUCCESS)" in result.message

        # Verify all fields are captured
        data = result.data
        assert data["job_id"] == 55555
        assert data["run_id"] == 77777
        assert data["run_name"] == "Comprehensive Data Pipeline"
        assert data["state"] == "TERMINATED"
        assert data["result_state"] == "SUCCESS"
        assert data["creator_user_name"] == "pipeline.admin@company.com"
        assert data["start_time"] == 1640995200000
        assert data["setup_duration"] == 45000
        assert data["execution_duration"] == 300000
        assert data["cleanup_duration"] == 15000

        # Verify task data
        assert len(data["tasks"]) == 2
        assert data["tasks"][0]["task_key"] == "validate_inputs"
        assert data["tasks"][1]["task_key"] == "process_data"


class TestJobStatusCommandConfiguration:
    """Test job_status command configuration and registry integration."""

    def test_job_status_command_definition_properties(self):
        """Job_status command definition has correct configuration."""
        from chuck_data.commands.job_status import DEFINITION

        assert DEFINITION.name == "job-status"
        assert DEFINITION.handler == handle_command
        assert DEFINITION.needs_api_client is True
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is True
        assert "run_id" in DEFINITION.required_params
        assert "run_id" in DEFINITION.parameters

    def test_job_status_command_parameter_requirements(self):
        """Job_status command has properly configured parameter requirements."""
        from chuck_data.commands.job_status import DEFINITION

        # Verify required parameter
        assert "run_id" in DEFINITION.required_params

        # Verify parameter definition
        run_id_param = DEFINITION.parameters["run_id"]
        assert run_id_param["type"] == "string"
        assert "run id" in run_id_param["description"].lower()

    def test_job_status_command_has_aliases(self):
        """Job_status command has proper TUI aliases configured."""
        from chuck_data.commands.job_status import DEFINITION

        assert "/job-status" in DEFINITION.tui_aliases
        assert "/job" in DEFINITION.tui_aliases

    def test_job_status_command_has_condensed_action(self):
        """Job_status command has condensed action for progress display."""
        from chuck_data.commands.job_status import DEFINITION

        assert DEFINITION.condensed_action == "Checking job status"

    def test_job_status_command_usage_hint(self):
        """Job_status command has helpful usage hint."""
        from chuck_data.commands.job_status import DEFINITION

        assert DEFINITION.usage_hint is not None
        assert "run_id" in DEFINITION.usage_hint
        assert "/job-status" in DEFINITION.usage_hint


class TestJobStatusDisplayIntegration:
    """Test job_status command integration with display system."""

    def test_job_status_result_contains_display_ready_data(
        self, databricks_client_stub
    ):
        """Job_status command result contains data ready for display formatting."""
        databricks_client_stub.add_simple_job_run(
            run_id="display_test",
            job_id=12345,
            run_name="Display Test Job",
            life_cycle_state="RUNNING",
            creator_user_name="test.user@example.com",
            start_time=1640995200000,
            execution_duration=60000,
        )

        result = handle_command(databricks_client_stub, run_id="display_test")

        assert result.success

        # Verify data structure is suitable for display
        data = result.data
        assert isinstance(data, dict)
        assert "job_id" in data
        assert "run_id" in data
        assert "run_name" in data
        assert "state" in data
        assert "creator_user_name" in data

        # Verify message is user-friendly
        message = result.message
        assert "Job run display_test is RUNNING" in message

    def test_job_status_message_formats_correctly_for_different_states(
        self, databricks_client_stub
    ):
        """Job_status command message formats correctly for different states."""
        # Test lifecycle state only
        databricks_client_stub.add_simple_job_run(
            run_id="lifecycle_only", life_cycle_state="RUNNING"
        )

        result1 = handle_command(databricks_client_stub, run_id="lifecycle_only")
        assert result1.success
        assert "Job run lifecycle_only is RUNNING" in result1.message

        # Test lifecycle + result state
        databricks_client_stub.add_simple_job_run(
            run_id="with_result",
            life_cycle_state="TERMINATED",
            result_state="SUCCESS",
        )

        result2 = handle_command(databricks_client_stub, run_id="with_result")
        assert result2.success
        assert "Job run with_result is TERMINATED (SUCCESS)" in result2.message

    def test_job_status_handles_jobs_with_long_names(self, databricks_client_stub):
        """Job_status command handles jobs with very long names."""
        long_name = "Very Long Job Name That Exceeds Normal Length Expectations And Contains Multiple Words And Descriptive Information About The Job Purpose"

        databricks_client_stub.add_simple_job_run(
            run_id="long_name_job", run_name=long_name
        )

        result = handle_command(databricks_client_stub, run_id="long_name_job")

        assert result.success
        assert result.data["run_name"] == long_name

    def test_job_status_handles_unicode_in_job_names(self, databricks_client_stub):
        """Job_status command handles Unicode characters in job names."""
        unicode_name = "Análisis de Datos 数据分析 🚀 Çomplex-Jøb"

        databricks_client_stub.add_simple_job_run(
            run_id="unicode_job", run_name=unicode_name
        )

        result = handle_command(databricks_client_stub, run_id="unicode_job")

        assert result.success
        assert result.data["run_name"] == unicode_name


class TestJobStatusEdgeCases:
    """Test edge cases and boundary conditions for job_status command."""

    def test_job_status_with_zero_durations(self, databricks_client_stub):
        """Job_status command handles zero duration values."""
        databricks_client_stub.add_simple_job_run(
            run_id="zero_durations",
            setup_duration=0,
            execution_duration=0,
            cleanup_duration=0,
        )

        result = handle_command(databricks_client_stub, run_id="zero_durations")

        assert result.success
        assert result.data["setup_duration"] == 0
        assert result.data["execution_duration"] == 0
        assert result.data["cleanup_duration"] == 0

    def test_job_status_with_very_large_run_id(self, databricks_client_stub):
        """Job_status command handles very large run ID values."""
        large_run_id = "999999999999999999"

        databricks_client_stub.add_simple_job_run(run_id=large_run_id)

        result = handle_command(databricks_client_stub, run_id=large_run_id)

        assert result.success
        assert result.data["run_id"] == int(large_run_id)

    def test_job_status_with_empty_task_list(self, databricks_client_stub):
        """Job_status command handles jobs with empty task list."""
        databricks_client_stub.add_simple_job_run(run_id="empty_tasks", tasks=[])

        result = handle_command(databricks_client_stub, run_id="empty_tasks")

        assert result.success
        # Current implementation only includes tasks key when tasks exist
        # Empty task list results in no tasks key being added
        assert "tasks" not in result.data or result.data.get("tasks") == []

    def test_job_status_with_missing_task_states(self, databricks_client_stub):
        """Job_status command handles tasks with missing state information."""
        tasks_with_missing_states = [
            {"task_key": "task_1"},  # Missing state
            {"task_key": "task_2", "state": {}},  # Empty state
            {
                "task_key": "task_3",
                "state": {"life_cycle_state": "RUNNING"},
            },  # Normal state
        ]

        databricks_client_stub.add_simple_job_run(
            run_id="missing_task_states", tasks=tasks_with_missing_states
        )

        result = handle_command(databricks_client_stub, run_id="missing_task_states")

        assert result.success
        assert len(result.data["tasks"]) == 3

        # Verify task data extraction handles missing states
        task_data = result.data["tasks"]
        assert task_data[0]["task_key"] == "task_1"
        assert task_data[0]["state"] is None  # Missing state
        assert task_data[1]["task_key"] == "task_2"
        assert task_data[1]["state"] is None  # Empty state
        assert task_data[2]["task_key"] == "task_3"
        assert task_data[2]["state"] == "RUNNING"  # Normal state

    def test_job_status_api_returns_none_response(self, databricks_client_stub):
        """Job_status command handles API returning None response."""
        # Mock get_job_run_status to return None
        original_method = databricks_client_stub.get_job_run_status

        def return_none(run_id):
            return None

        databricks_client_stub.get_job_run_status = return_none

        result = handle_command(databricks_client_stub, run_id="12345")

        # Restore original method
        databricks_client_stub.get_job_run_status = original_method

        assert not result.success
        assert "No job run found with ID: 12345" in result.message

    def test_job_status_with_malformed_state_structure(self, databricks_client_stub):
        """Job_status command handles malformed state structure in API response."""
        malformed_job_run = {
            "job_id": 12345,
            "run_id": 67890,
            "run_name": "Malformed State Job",
            "state": "RUNNING",  # Should be dict, not string
            "creator_user_name": "test@example.com",
        }

        databricks_client_stub.add_job_run("67890", malformed_job_run)

        result = handle_command(databricks_client_stub, run_id="67890")

        # Current implementation fails when state is not a dict (calls .get() on string)
        assert not result.success
        assert "Failed to get job run status" in result.message
        assert "'str' object has no attribute 'get'" in result.message
