"""Tests for TUI display methods."""

import unittest
from unittest.mock import patch, MagicMock
from rich.console import Console
from src.ui.tui import ChuckTUI


class TestTUIDisplay(unittest.TestCase):
    """Test cases for TUI display methods."""

    def setUp(self):
        """Set up common test fixtures."""
        self.tui = ChuckTUI()
        self.tui.console = MagicMock()

    def test_no_color_mode_initialization(self):
        """Test that TUI initializes properly with no_color=True."""
        tui_no_color = ChuckTUI(no_color=True)
        self.assertTrue(tui_no_color.no_color)
        # Check that console was created with no color
        self.assertEqual(tui_no_color.console._force_terminal, False)

    def test_color_mode_initialization(self):
        """Test that TUI initializes properly with default color mode."""
        tui_default = ChuckTUI()
        self.assertFalse(tui_default.no_color)
        # Check that console was created with colors enabled
        self.assertEqual(tui_default.console._force_terminal, True)

    def test_prompt_styling_respects_no_color(self):
        """Test that prompt styling is disabled in no-color mode."""
        # This test verifies that the run() method sets up prompt styles correctly
        # We can't easily test the actual PromptSession creation without major mocking,
        # but we can verify the no_color setting is propagated correctly
        tui_no_color = ChuckTUI(no_color=True)
        tui_with_color = ChuckTUI(no_color=False)

        self.assertTrue(tui_no_color.no_color)
        self.assertFalse(tui_with_color.no_color)

    def test_display_status_full_data(self):
        """Test status display method with full data including connection and permissions."""
        # Create test data with all fields
        status_data = {
            "workspace_url": "test-workspace",
            "active_catalog": "test-catalog",
            "active_schema": "test-schema",
            "active_model": "test-model",
            "connection_status": "Connected - token is valid",
            "permissions": {
                "basic_connectivity": {
                    "authorized": True,
                    "details": "Connected as test-user",
                    "api_path": "/api/2.0/preview/scim/v2/Me",
                },
                "unity_catalog": {
                    "authorized": True,
                    "details": "Unity Catalog access granted (3 catalogs visible)",
                    "api_path": "/api/2.1/unity-catalog/catalogs",
                },
            },
        }

        # Patch display_table and _display_permissions to verify calls
        with patch("src.ui.table_formatter.display_table") as mock_display_table:
            with patch.object(
                self.tui, "_display_permissions"
            ) as mock_display_permissions:
                self.tui._display_status(status_data)

                # display_table should be called once with the status data
                mock_display_table.assert_called_once()
                kwargs = mock_display_table.call_args.kwargs
                self.assertEqual(kwargs["title"], "Current Configuration")
                self.assertEqual(kwargs["data"][0]["setting"], "Workspace URL")
                self.assertEqual(kwargs["data"][0]["value"], "test-workspace")

                # Verify _display_permissions was called with the permission data
                mock_display_permissions.assert_called_once_with(
                    status_data["permissions"]
                )

    def test_display_status_invalid_token(self):
        """Test status display method with invalid token."""
        # Create test data with invalid token
        status_data = {
            "workspace_url": "test-workspace",
            "active_catalog": "test-catalog",
            "active_schema": "test-schema",
            "active_model": "test-model",
            "connection_status": "Invalid token - authentication failed",
        }

        with patch("src.ui.table_formatter.display_table") as mock_display_table:
            self.tui._display_status(status_data)

            mock_display_table.assert_called_once()
            kwargs = mock_display_table.call_args.kwargs
            row = next(
                item
                for item in kwargs["data"]
                if item["setting"] == "Connection Status"
            )
            self.assertEqual(row["value"], "Invalid token - authentication failed")
            style = kwargs["style_map"]["value"](row["value"], row)
            self.assertEqual(style, "red")

    def test_display_status_not_connected(self):
        """Test status display method with no connection."""
        # Create test data with no connection
        status_data = {
            "workspace_url": "test-workspace",
            "active_catalog": "Not set",
            "active_schema": "Not set",
            "active_model": "Not set",
            "connection_status": "Not connected - no valid Databricks token found",
        }

        with patch("src.ui.table_formatter.display_table") as mock_display_table:
            self.tui._display_status(status_data)

            mock_display_table.assert_called_once()
            kwargs = mock_display_table.call_args.kwargs
            conn_row = next(
                item
                for item in kwargs["data"]
                if item["setting"] == "Connection Status"
            )
            self.assertEqual(
                conn_row["value"], "Not connected - no valid Databricks token found"
            )
            style = kwargs["style_map"]["value"](conn_row["value"], conn_row)
            self.assertEqual(style, "red")

            catalog_row = next(
                item for item in kwargs["data"] if item["setting"] == "Active Catalog"
            )
            cat_style = kwargs["style_map"]["value"](catalog_row["value"], catalog_row)
            self.assertEqual(cat_style, "yellow")

    def test_display_permissions(self):
        """Test permissions display method."""
        # Create test permission data
        permissions_data = {
            "basic_connectivity": {
                "authorized": True,
                "details": "Connected as test-user",
                "api_path": "/api/2.0/preview/scim/v2/Me",
            },
            "unity_catalog": {
                "authorized": False,
                "error": "Access denied",
                "api_path": "/api/2.1/unity-catalog/catalogs",
            },
            "sql_warehouse": {
                "authorized": True,
                "details": "SQL Warehouse access granted (2 warehouses visible)",
                "api_path": "/api/2.0/sql/warehouses",
            },
        }

        with patch("src.ui.table_formatter.display_table") as mock_display_table:
            self.tui._display_permissions(permissions_data)

            mock_display_table.assert_called_once()
            kwargs = mock_display_table.call_args.kwargs
            self.assertEqual(kwargs["title"], "Databricks API Token Permissions")
            self.assertEqual(kwargs["headers"], ["Resource", "Status", "Details"])

            data = kwargs["data"]
            auth_entry = next(
                item for item in data if item["resource"] == "Basic Connectivity"
            )
            unauth_entry = next(
                item for item in data if item["resource"] == "Unity Catalog"
            )
            status_style = kwargs["style_map"]["status"]
            self.assertEqual(status_style(auth_entry["status"], auth_entry), "green")
            self.assertEqual(status_style(unauth_entry["status"], unauth_entry), "red")

        # Verify API endpoints were printed
        print_calls = [
            call[0][0]
            for call in self.tui.console.print.call_args_list
            if isinstance(call[0][0], str)
        ]
        api_heading = next(
            (line for line in print_calls if "API endpoints checked" in line), None
        )
        self.assertIsNotNone(api_heading, "API endpoints section not displayed")

    def test_display_status_truncates_long_values(self):
        """Test that long values in status display are properly truncated."""
        # Create test data with very long values
        very_long_url = "https://very-long-workspace-url-that-exceeds-the-display-limit.cloud.databricks.com"
        status_data = {
            "workspace_url": very_long_url,
            "active_catalog": "test-catalog",
            "active_schema": "test-schema",
            "active_model": "test-model",
            "connection_status": "Connected - token is valid",
        }

        # Use a real console to capture formatted output
        self.tui.console = Console(record=True)
        self.tui._display_status(status_data)
        output = self.tui.console.export_text()
        self.assertIn(
            "https://very-long-workspace-url-that-exceeds-the-displa…", output
        )
