"""
Tests for the bug command handler.
"""

import json
import os
import tempfile
from unittest import mock

from chuck_data.commands.bug import (
    handle_command,
    _get_sanitized_config,
    _prepare_bug_report,
    _get_session_log,
)
from chuck_data.config import ConfigManager
from tests.fixtures.amperity import AmperityClientStub


class TestBugCommand:
    """Test cases for the bug command."""

    def test_handle_command_no_description(self):
        """Test bug command without description."""
        result = handle_command(None)
        assert not result.success
        assert "Bug description is required" in result.message

    def test_handle_command_with_rest_parameter(self):
        """Test bug command with rest parameter (free-form text)."""
        import tempfile
        from chuck_data.config import ConfigManager
        
        # Use real config system with no token set
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            
            with mock.patch("chuck_data.config._config_manager", config_manager):
                result = handle_command(None, rest="Hi caleb!")
                assert not result.success
                assert "Amperity authentication required" in result.message

    def test_handle_command_with_raw_args_list(self):
        """Test bug command with raw_args as list."""
        import tempfile
        from chuck_data.config import ConfigManager
        
        # Use real config system with no token set
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            
            with mock.patch("chuck_data.config._config_manager", config_manager):
                result = handle_command(None, raw_args=["Hi", "caleb!"])
                assert not result.success
                assert "Amperity authentication required" in result.message

    def test_handle_command_with_raw_args_string(self):
        """Test bug command with raw_args as string."""
        import tempfile
        from chuck_data.config import ConfigManager
        
        # Use real config system with no token set
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            
            with mock.patch("chuck_data.config._config_manager", config_manager):
                result = handle_command(None, raw_args="Hi caleb!")
                assert not result.success
                assert "Amperity authentication required" in result.message

    def test_handle_command_empty_description(self):
        """Test bug command with empty description."""
        result = handle_command(None, description="   ")
        assert not result.success
        assert "Bug description is required" in result.message

    def test_handle_command_no_token(self):
        """Test bug command without Amperity token."""
        import tempfile
        from chuck_data.config import ConfigManager
        
        # Use real config system with no token set
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            
            with mock.patch("chuck_data.config._config_manager", config_manager):
                result = handle_command(None, description="Test bug")
                assert not result.success
                assert "Amperity authentication required" in result.message

    def test_handle_command_success(self, amperity_client_stub):
        """Test successful bug report submission."""
        import tempfile
        from chuck_data.config import ConfigManager, set_amperity_token
        
        # Use real config system with token set
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            
            with mock.patch("chuck_data.config._config_manager", config_manager):
                # Set amperity token using real config
                set_amperity_token("test-token")
                
                # Mock only the external client instantiation, not internal logic
                with mock.patch("chuck_data.commands.bug.AmperityAPIClient") as mock_client_class:
                    mock_client_class.return_value = amperity_client_stub
                    
                    result = handle_command(None, description="Test bug description")

                    assert result.success
                    assert "Bug report submitted successfully" in result.message

    def test_handle_command_api_failure(self, amperity_client_stub):
        """Test bug report submission with API failure."""
        import tempfile
        from chuck_data.config import ConfigManager, set_amperity_token
        
        # Configure client stub to fail
        amperity_client_stub.set_bug_report_failure(True)
        
        # Use real config system with token set
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            
            with mock.patch("chuck_data.config._config_manager", config_manager):
                # Set amperity token using real config
                set_amperity_token("test-token")
                
                # Mock only the external client instantiation
                with mock.patch("chuck_data.commands.bug.AmperityAPIClient") as mock_client_class:
                    mock_client_class.return_value = amperity_client_stub
                    
                    result = handle_command(None, description="Test bug")

                    assert not result.success
                    assert "Failed to submit bug report: 500" in result.message

    def test_handle_command_network_error(self):
        """Test bug report submission with network error."""
        import tempfile
        from chuck_data.config import ConfigManager, set_amperity_token
        
        # Create a stub that raises an exception
        class FailingAmperityStub(AmperityClientStub):
            def submit_bug_report(self, payload: dict, token: str) -> tuple[bool, str]:
                raise Exception("Network error")

        failing_client = FailingAmperityStub()
        
        # Use real config system with token set
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            
            with mock.patch("chuck_data.config._config_manager", config_manager):
                # Set amperity token using real config
                set_amperity_token("test-token")
                
                # Mock only the external client instantiation
                with mock.patch("chuck_data.commands.bug.AmperityAPIClient") as mock_client_class:
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

            with mock.patch("chuck_data.config._config_manager", config_manager):
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

            with mock.patch("chuck_data.config._config_manager", config_manager):
                sanitized = _get_sanitized_config()

                # Check that None values are not included
                assert "active_model" not in sanitized
                assert "warehouse_id" not in sanitized
                assert sanitized["workspace_url"] == "https://test.databricks.com"
        finally:
            os.unlink(temp_path)

    def test_prepare_bug_report(self):
        """Test bug report payload preparation with real business logic."""
        import tempfile
        from chuck_data.config import ConfigManager
        
        # Create a temporary config file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            config_data = {
                "workspace_url": "https://test.databricks.com",
                "active_model": "test-model",
                "warehouse_id": "test-warehouse",
            }
            json.dump(config_data, f)
            temp_config_path = f.name

        # Create a temporary log file
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("Test log line 1\n")
            f.write("Test log line 2\n")
            temp_log_path = f.name

        try:
            config_manager = ConfigManager(temp_config_path)
            
            with mock.patch("chuck_data.config._config_manager", config_manager):
                # Mock only the external system calls (file access)
                with mock.patch("chuck_data.commands.bug.get_current_log_file", return_value=temp_log_path):
                    # Call the real function with real business logic
                    payload = _prepare_bug_report("Test bug description")

                    # Verify the payload structure
                    assert payload["type"] == "bug_report"
                    assert payload["description"] == "Test bug description"
                    assert isinstance(payload["config"], dict)
                    assert payload["config"]["workspace_url"] == "https://test.databricks.com"
                    assert payload["config"]["active_model"] == "test-model"
                    assert isinstance(payload["session_log"], str)
                    assert "Test log line 1" in payload["session_log"]
                    assert "Test log line 2" in payload["session_log"]
                    assert "timestamp" in payload
                    assert "system_info" in payload
                    assert "platform" in payload["system_info"]
                    assert "python_version" in payload["system_info"]
        finally:
            os.unlink(temp_config_path)
            os.unlink(temp_log_path)

    @mock.patch("chuck_data.commands.bug.get_current_log_file")
    def test_get_session_log_no_file(self, mock_get_log_file):
        """Test session log retrieval when no log file exists."""
        mock_get_log_file.return_value = None

        log_content = _get_session_log()
        assert log_content == "Session log not available"

    @mock.patch("chuck_data.commands.bug.get_current_log_file")
    def test_get_session_log_file_not_found(self, mock_get_log_file):
        """Test session log retrieval when log file doesn't exist."""
        mock_get_log_file.return_value = "/nonexistent/file.log"

        log_content = _get_session_log()
        assert log_content == "Session log not available"

    @mock.patch("chuck_data.commands.bug.get_current_log_file")
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

    @mock.patch("chuck_data.commands.bug.get_current_log_file")
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

    def test_handle_command_with_rest_success(self, amperity_client_stub):
        """Test successful bug report submission using rest parameter."""
        import tempfile
        from chuck_data.config import ConfigManager, set_amperity_token
        
        # Use real config system with token set
        with tempfile.NamedTemporaryFile() as tmp:
            config_manager = ConfigManager(tmp.name)
            
            with mock.patch("chuck_data.config._config_manager", config_manager):
                # Set amperity token using real config
                set_amperity_token("test-token")
                
                # Mock only the external client instantiation
                with mock.patch("chuck_data.commands.bug.AmperityAPIClient") as mock_client_class:
                    mock_client_class.return_value = amperity_client_stub
                    
                    result = handle_command(None, rest="Hi caleb!")

                    assert result.success
                    assert "Bug report submitted successfully" in result.message
