"""
Behavioral tests for the bug command handler.

These tests focus on user-visible behaviors and outcomes rather than
implementation details, following the behavioral testing patterns in CLAUDE.md.
"""

import json
import os
import tempfile
from unittest import mock

from chuck_data.commands.bug import handle_command
from chuck_data.config import ConfigManager, set_amperity_token
from tests.fixtures.amperity import AmperityClientStub


class TestBugCommandBehavior:
    """Behavioral tests for the bug command focusing on user-visible outcomes."""

    def test_user_sees_clear_error_when_no_description_provided(self):
        """User gets helpful error message when submitting bug report without description."""
        result = handle_command(None, tool_output_callback=None)

        assert not result.success
        assert "Bug description is required" in result.message
        assert "Usage: /bug Your bug description here" in result.message

    def test_user_sees_clear_error_when_description_is_empty_whitespace(self):
        """User gets error when providing only whitespace as bug description."""
        result = handle_command(None, tool_output_callback=None, description="   ")

        assert not result.success
        assert "Bug description is required" in result.message

    def test_user_can_provide_description_through_multiple_input_methods(self):
        """User can provide bug description via description, rest, or raw_args parameters."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with mock.patch("chuck_data.config._config_manager", config_manager):
                # Test 'rest' parameter
                result = handle_command(
                    None, tool_output_callback=None, rest="Bug found in UI"
                )
                assert not result.success  # No token set, but description was accepted
                assert "Amperity authentication required" in result.message

                # Test 'raw_args' as list
                result = handle_command(
                    None, tool_output_callback=None, raw_args=["Bug", "in", "API"]
                )
                assert not result.success  # No token set, but description was accepted
                assert "Amperity authentication required" in result.message

                # Test 'raw_args' as string
                result = handle_command(
                    None, tool_output_callback=None, raw_args="Bug in database"
                )
                assert not result.success  # No token set, but description was accepted
                assert "Amperity authentication required" in result.message

    def test_user_sees_auth_error_when_not_authenticated_with_amperity(self):
        """User gets clear message to authenticate when Amperity token is missing."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with mock.patch("chuck_data.config._config_manager", config_manager):
                result = handle_command(
                    None, tool_output_callback=None, description="Test bug report"
                )

                assert not result.success
                assert "Amperity authentication required" in result.message
                assert "Please run /auth to authenticate first" in result.message

    def test_user_receives_success_confirmation_when_bug_report_submitted(
        self, amperity_client_stub
    ):
        """User gets positive confirmation when bug report is successfully submitted."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with mock.patch("chuck_data.config._config_manager", config_manager):
                set_amperity_token("valid-token")

                with mock.patch(
                    "chuck_data.commands.bug.AmperityAPIClient"
                ) as mock_client_class:
                    mock_client_class.return_value = amperity_client_stub

                    result = handle_command(
                        None,
                        tool_output_callback=None,
                        description="Critical UI bug affecting all users",
                    )

                    assert result.success
                    assert "Bug report submitted successfully" in result.message
                    assert "Thank you for your feedback!" in result.message

    def test_user_sees_helpful_error_when_bug_submission_fails(
        self, amperity_client_stub
    ):
        """User gets clear error message when bug report submission fails due to API issues."""
        amperity_client_stub.set_bug_report_failure(True)

        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with mock.patch("chuck_data.config._config_manager", config_manager):
                set_amperity_token("valid-token")

                with mock.patch(
                    "chuck_data.commands.bug.AmperityAPIClient"
                ) as mock_client_class:
                    mock_client_class.return_value = amperity_client_stub

                    result = handle_command(
                        None,
                        tool_output_callback=None,
                        description="Bug that fails to submit",
                    )

                    assert not result.success
                    assert "Failed to submit bug report: 500" in result.message

    def test_user_sees_network_error_when_connection_fails(self):
        """User gets clear error message when network connectivity issues prevent submission."""

        class NetworkFailureStub(AmperityClientStub):
            def submit_bug_report(self, payload: dict, token: str) -> tuple[bool, str]:
                raise ConnectionError("Network unreachable")

        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with mock.patch("chuck_data.config._config_manager", config_manager):
                set_amperity_token("valid-token")

                with mock.patch(
                    "chuck_data.commands.bug.AmperityAPIClient"
                ) as mock_client_class:
                    mock_client_class.return_value = NetworkFailureStub()

                    result = handle_command(
                        None,
                        tool_output_callback=None,
                        description="Bug report with network issues",
                    )

                    assert not result.success
                    assert "Error submitting bug report" in result.message
                    assert "Network unreachable" in result.message

    def test_bug_report_includes_user_configuration_without_sensitive_data(
        self, amperity_client_stub
    ):
        """Bug report includes user's configuration context but excludes sensitive tokens."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            config_data = {
                "workspace_url": "https://company.databricks.com",
                "active_catalog": "production",
                "active_schema": "analytics",
                "active_model": "llama-model",
                "warehouse_id": "warehouse-123",
                "usage_tracking_consent": True,
                "amperity_token": "SECRET-TOKEN",
                "databricks_token": "ANOTHER-SECRET",
            }
            json.dump(config_data, f)
            temp_config_path = f.name

        try:
            config_manager = ConfigManager(temp_config_path)

            with mock.patch("chuck_data.config._config_manager", config_manager):
                set_amperity_token("valid-token")

                captured_payload = None
                original_submit = amperity_client_stub.submit_bug_report

                def capture_payload(payload, token):
                    nonlocal captured_payload
                    captured_payload = payload
                    return original_submit(payload, token)

                amperity_client_stub.submit_bug_report = capture_payload

                with mock.patch(
                    "chuck_data.commands.bug.AmperityAPIClient"
                ) as mock_client_class:
                    mock_client_class.return_value = amperity_client_stub

                    result = handle_command(
                        None,
                        tool_output_callback=None,
                        description="Test configuration inclusion",
                    )

                    assert result.success
                    assert captured_payload is not None

                    # Verify configuration is included
                    config_in_payload = captured_payload["config"]
                    assert (
                        config_in_payload["workspace_url"]
                        == "https://company.databricks.com"
                    )
                    assert config_in_payload["active_catalog"] == "production"
                    assert config_in_payload["active_schema"] == "analytics"

                    # Verify sensitive data is excluded
                    assert "amperity_token" not in captured_payload
                    assert "databricks_token" not in captured_payload
                    assert "SECRET" not in str(captured_payload)
        finally:
            os.unlink(temp_config_path)

    def test_bug_report_includes_session_context_and_system_information(
        self, amperity_client_stub
    ):
        """Bug report includes relevant session logs and system info to help with debugging."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as log_file:
            log_file.write("2024-01-01 10:00:00 - User executed /list_catalogs\n")
            log_file.write("2024-01-01 10:00:01 - Error: Connection timeout\n")
            log_file.write("2024-01-01 10:00:02 - User retried command\n")
            temp_log_path = log_file.name

        with tempfile.NamedTemporaryFile() as config_file:
            config_manager = ConfigManager(config_file.name)

            try:
                with mock.patch("chuck_data.config._config_manager", config_manager):
                    set_amperity_token("valid-token")

                    captured_payload = None
                    original_submit = amperity_client_stub.submit_bug_report

                    def capture_payload(payload, token):
                        nonlocal captured_payload
                        captured_payload = payload
                        return original_submit(payload, token)

                    amperity_client_stub.submit_bug_report = capture_payload

                    with mock.patch(
                        "chuck_data.commands.bug.get_current_log_file",
                        return_value=temp_log_path,
                    ):
                        with mock.patch(
                            "chuck_data.commands.bug.AmperityAPIClient"
                        ) as mock_client_class:
                            mock_client_class.return_value = amperity_client_stub

                            result = handle_command(
                                None,
                                tool_output_callback=None,
                                description="Connection timeouts happening frequently",
                            )

                            assert result.success
                            assert captured_payload is not None

                            # Verify session logs are included
                            assert "session_log" in captured_payload
                            assert (
                                "Connection timeout" in captured_payload["session_log"]
                            )
                            assert (
                                "User executed /list_catalogs"
                                in captured_payload["session_log"]
                            )

                            # Verify system information is included
                            assert "system_info" in captured_payload
                            system_info = captured_payload["system_info"]
                            assert "platform" in system_info
                            assert "python_version" in system_info
                            assert "system" in system_info
                            assert "machine" in system_info

                            # Verify bug report metadata
                            assert captured_payload["type"] == "bug_report"
                            assert "timestamp" in captured_payload
                            assert (
                                captured_payload["description"]
                                == "Connection timeouts happening frequently"
                            )
            finally:
                os.unlink(temp_log_path)

    def test_agent_sees_intermediate_status_updates_during_bug_submission(
        self, amperity_client_stub
    ):
        """Agent receives step-by-step status updates during bug report submission."""
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)

            with mock.patch("chuck_data.config._config_manager", config_manager):
                set_amperity_token("valid-token")

                # Capture status updates
                status_updates = []

                def capture_status(tool_name, tool_result):
                    if "step" in tool_result:
                        status_updates.append(tool_result["step"])

                with mock.patch(
                    "chuck_data.commands.bug.AmperityAPIClient"
                ) as mock_client_class:
                    mock_client_class.return_value = amperity_client_stub

                    result = handle_command(
                        None,
                        tool_output_callback=capture_status,
                        description="Agent-submitted bug report",
                    )

                    assert result.success

                    # Verify we got the expected status updates
                    expected_steps = [
                        "Gathering bug report details...",
                        "Checking authentication...",
                        "Preparing bug report with system information...",
                        "Submitting bug report...",
                    ]

                    assert len(status_updates) == len(expected_steps)
                    for expected, actual in zip(expected_steps, status_updates):
                        assert expected == actual
