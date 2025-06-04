"""
Tests for the bug command handler.
"""

import json
import os
import tempfile
from unittest import mock

from src.commands.bug import (
    handle_command,
    _get_sanitized_config,
    _prepare_bug_report,
    _get_session_log,
)
from src.config import ConfigManager
from tests.fixtures import AmperityClientStub


class TestBugCommand:
    """Test cases for the bug command."""

    def test_handle_command_no_description(self):
        """Test bug command without description."""
        result = handle_command(None)
        assert not result.success
        assert "Bug description is required" in result.message

    def test_handle_command_with_rest_parameter(self):
        """Test bug command with rest parameter (free-form text)."""
        with mock.patch("src.commands.bug.get_amperity_token") as mock_token:
            mock_token.return_value = None  # No token, so it should fail at auth

            result = handle_command(None, rest="Hi caleb!")
            assert not result.success
            assert "Amperity authentication required" in result.message

    def test_handle_command_with_raw_args_list(self):
        """Test bug command with raw_args as list."""
        with mock.patch("src.commands.bug.get_amperity_token") as mock_token:
            mock_token.return_value = None  # No token, so it should fail at auth

            result = handle_command(None, raw_args=["Hi", "caleb!"])
            assert not result.success
            assert "Amperity authentication required" in result.message

    def test_handle_command_with_raw_args_string(self):
        """Test bug command with raw_args as string."""
        with mock.patch("src.commands.bug.get_amperity_token") as mock_token:
            mock_token.return_value = None  # No token, so it should fail at auth

            result = handle_command(None, raw_args="Hi caleb!")
            assert not result.success
            assert "Amperity authentication required" in result.message

    def test_handle_command_empty_description(self):
        """Test bug command with empty description."""
        result = handle_command(None, description="   ")
        assert not result.success
        assert "Bug description is required" in result.message

    @mock.patch("src.commands.bug.get_amperity_token")
    def test_handle_command_no_token(self, mock_get_token):
        """Test bug command without Amperity token."""
        mock_get_token.return_value = None

        result = handle_command(None, description="Test bug")
        assert not result.success
        assert "Amperity authentication required" in result.message

    @mock.patch("src.commands.bug.get_amperity_token")
    @mock.patch("src.commands.bug.AmperityAPIClient")
    @mock.patch("src.commands.bug._prepare_bug_report")
    def test_handle_command_success(
        self, mock_prepare, mock_client_class, mock_get_token
    ):
        """Test successful bug report submission."""
        mock_get_token.return_value = "test-token"
        mock_prepare.return_value = {"test": "payload"}

        # Use AmperityClientStub instead of MagicMock
        client_stub = AmperityClientStub()
        mock_client_class.return_value = client_stub

        result = handle_command(None, description="Test bug description")

        assert result.success
        assert "Bug report submitted successfully" in result.message

    @mock.patch("src.commands.bug.get_amperity_token")
    @mock.patch("src.commands.bug.AmperityAPIClient")
    def test_handle_command_api_failure(self, mock_client_class, mock_get_token):
        """Test bug report submission with API failure."""
        mock_get_token.return_value = "test-token"

        # Use AmperityClientStub configured to fail
        client_stub = AmperityClientStub()
        client_stub.set_bug_report_failure(True)
        mock_client_class.return_value = client_stub

        result = handle_command(None, description="Test bug")

        assert not result.success
        assert "Failed to submit bug report: 500" in result.message

    @mock.patch("src.commands.bug.get_amperity_token")
    @mock.patch("src.commands.bug.AmperityAPIClient")
    def test_handle_command_network_error(self, mock_client_class, mock_get_token):
        """Test bug report submission with network error."""
        mock_get_token.return_value = "test-token"

        # Create a stub that raises an exception
        class FailingAmperityStub(AmperityClientStub):
            def submit_bug_report(self, payload: dict, token: str) -> tuple[bool, str]:
                raise Exception("Network error")

        failing_client = FailingAmperityStub()
        mock_client_class.return_value = failing_client

        result = handle_command(None, description="Test bug")

        assert not result.success
        assert "Error submitting bug report" in result.message

    def test_get_sanitized_config(self):
        """Test config sanitization removes tokens."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            config_data = {
                "workspace_url": "https://test.databricks.com",
                "active_model": "test-model",
                "warehouse_id": "test-warehouse",
                "active_catalog": "test-catalog",
                "active_schema": "test-schema",
                "amperity_token": "SECRET-TOKEN",
                "databricks_token": "ANOTHER-SECRET",
                "usage_tracking_consent": True,
            }
            json.dump(config_data, f)
            temp_path = f.name

        try:
            # Create a config manager with the temp file
            config_manager = ConfigManager(temp_path)

            with mock.patch(
                "src.commands.bug.get_config_manager", return_value=config_manager
            ):
                sanitized = _get_sanitized_config()

                # Check that tokens are NOT included
                assert "amperity_token" not in sanitized
                assert "databricks_token" not in sanitized

                # Check that other fields are included
                assert sanitized["workspace_url"] == "https://test.databricks.com"
                assert sanitized["active_model"] == "test-model"
                assert sanitized["warehouse_id"] == "test-warehouse"
                assert sanitized["active_catalog"] == "test-catalog"
                assert sanitized["active_schema"] == "test-schema"
                assert sanitized["usage_tracking_consent"] is True
        finally:
            os.unlink(temp_path)

    def test_get_sanitized_config_with_none_values(self):
        """Test config sanitization removes None values."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            config_data = {
                "workspace_url": "https://test.databricks.com",
                "active_model": None,
                "warehouse_id": None,
            }
            json.dump(config_data, f)
            temp_path = f.name

        try:
            config_manager = ConfigManager(temp_path)

            with mock.patch(
                "src.commands.bug.get_config_manager", return_value=config_manager
            ):
                sanitized = _get_sanitized_config()

                # Check that None values are not included
                assert "active_model" not in sanitized
                assert "warehouse_id" not in sanitized
                assert sanitized["workspace_url"] == "https://test.databricks.com"
        finally:
            os.unlink(temp_path)

    @mock.patch("src.commands.bug._get_session_log")
    @mock.patch("src.commands.bug._get_sanitized_config")
    def test_prepare_bug_report(self, mock_config, mock_log):
        """Test bug report payload preparation."""
        mock_config.return_value = {"workspace_url": "test"}
        mock_log.return_value = "test log content"

        payload = _prepare_bug_report("Test bug description")

        assert payload["type"] == "bug_report"
        assert payload["description"] == "Test bug description"
        assert payload["config"] == {"workspace_url": "test"}
        assert payload["session_log"] == "test log content"
        assert "timestamp" in payload
        assert "system_info" in payload
        assert "platform" in payload["system_info"]
        assert "python_version" in payload["system_info"]

    @mock.patch("src.commands.bug.get_current_log_file")
    def test_get_session_log_no_file(self, mock_get_log_file):
        """Test session log retrieval when no log file exists."""
        mock_get_log_file.return_value = None

        log_content = _get_session_log()
        assert log_content == "Session log not available"

    @mock.patch("src.commands.bug.get_current_log_file")
    def test_get_session_log_file_not_found(self, mock_get_log_file):
        """Test session log retrieval when log file doesn't exist."""
        mock_get_log_file.return_value = "/nonexistent/file.log"

        log_content = _get_session_log()
        assert log_content == "Session log not available"

    @mock.patch("src.commands.bug.get_current_log_file")
    def test_get_session_log_success(self, mock_get_log_file):
        """Test successful session log retrieval."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("Test log line 1\n")
            f.write("Test log line 2\n")
            f.write("Test log line 3\n")
            temp_path = f.name

        try:
            mock_get_log_file.return_value = temp_path

            log_content = _get_session_log()
            assert "Test log line 1" in log_content
            assert "Test log line 2" in log_content
            assert "Test log line 3" in log_content
        finally:
            os.unlink(temp_path)

    @mock.patch("src.commands.bug.get_current_log_file")
    def test_get_session_log_large_file(self, mock_get_log_file):
        """Test session log retrieval for large files (should read last 10KB)."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            # Write more than 10KB of data
            for i in range(2000):
                f.write(f"Line {i}: " + "X" * 50 + "\n")
            temp_path = f.name

        try:
            mock_get_log_file.return_value = temp_path

            log_content = _get_session_log()
            # Should be around 10KB
            assert len(log_content) <= 10240
            assert len(log_content) > 9000  # Should be close to 10KB
            # Should contain later lines, not earlier ones
            assert "Line 1999" in log_content
            assert "Line 0" not in log_content
        finally:
            os.unlink(temp_path)

    @mock.patch("src.commands.bug.get_amperity_token")
    @mock.patch("src.commands.bug.AmperityAPIClient")
    @mock.patch("src.commands.bug._prepare_bug_report")
    def test_handle_command_with_rest_success(
        self, mock_prepare, mock_client_class, mock_get_token
    ):
        """Test successful bug report submission using rest parameter."""
        mock_get_token.return_value = "test-token"
        mock_prepare.return_value = {"test": "payload"}

        # Mock the AmperityAPIClient instance and its submit_bug_report method
        mock_client = mock.Mock()
        mock_client.submit_bug_report.return_value = (
            True,
            "Bug report submitted successfully",
        )
        mock_client_class.return_value = mock_client

        result = handle_command(None, rest="Hi caleb!")

        assert result.success
        assert "Bug report submitted successfully" in result.message

        # Verify the bug report was prepared with the correct description
        mock_prepare.assert_called_once_with("Hi caleb!")
        # Verify the client method was called
        mock_client.submit_bug_report.assert_called_once()
