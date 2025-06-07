"""Tests for TUI display methods."""

import unittest
from unittest.mock import patch, MagicMock
from rich.console import Console
from chuck_data.ui.tui import ChuckTUI


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
        with patch("chuck_data.ui.table_formatter.display_table") as mock_display_table:
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

        with patch("chuck_data.ui.table_formatter.display_table") as mock_display_table:
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

        with patch("chuck_data.ui.table_formatter.display_table") as mock_display_table:
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

        with patch("chuck_data.ui.table_formatter.display_table") as mock_display_table:
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

    def test_table_display_field_mapping(self):
        """Test that table display columns match actual data fields from API responses."""
        # This test would have caught the created_at/updated_at field mapping issue

        # Mock realistic table data structure (matching actual API response)
        table_data = {
            "tables": [
                {
                    "name": "ecommerce_profiles",
                    "table_type": "MANAGED",
                    "created_at": 1748473407547,  # Unix timestamp in milliseconds
                    "updated_at": 1748473408383,  # Unix timestamp in milliseconds
                    "row_count": 4387229,  # Large row count for formatting test
                    "columns": [
                        {"name": "system_id", "type_text": "string"},
                        {"name": "last_updated", "type_text": "timestamp"},
                    ],
                },
                {
                    "name": "loyalty_member",
                    "table_type": "MANAGED",
                    "created_at": 1748473412513,
                    "updated_at": 1748473413145,
                    "row_count": 919746,  # Medium row count for formatting test
                    "columns": [{"name": "customer_id", "type_text": "string"}],
                },
            ],
            "catalog_name": "john_test",
            "schema_name": "bronze",
            "total_count": 2,
        }

        with patch("chuck_data.ui.table_formatter.display_table") as mock_display_table:
            # _display_tables raises PaginationCancelled by design
            from chuck_data.exceptions import PaginationCancelled

            with self.assertRaises(PaginationCancelled):
                self.tui._display_tables(table_data)

            # Verify display_table was called
            mock_display_table.assert_called_once()
            kwargs = mock_display_table.call_args.kwargs

            # Verify column names match data fields
            columns = kwargs["columns"]
            data = kwargs["data"]

            # This test would have caught the field name mismatch
            for column in columns:
                if column in ["name", "table_type"]:  # These should always exist
                    continue
                # Verify that display columns exist in the actual data
                self.assertTrue(
                    any(column in row for row in data),
                    f"Display column '{column}' not found in any data row. Available keys: {list(data[0].keys()) if data else 'No data'}",
                )

            # Verify expected columns are present (including new row_count)
            expected_columns = [
                "name",
                "table_type",
                "column_count",
                "row_count",
                "created_at",
                "updated_at",
            ]
            self.assertEqual(columns, expected_columns)

            # Verify data was processed correctly
            self.assertEqual(len(data), 2)
            self.assertEqual(data[0]["name"], "ecommerce_profiles")
            self.assertEqual(data[0]["table_type"], "MANAGED")

            # Verify timestamp fields are present and formatted
            self.assertIn("created_at", data[0])
            self.assertIn("updated_at", data[0])

            # Verify row count fields are present and formatted
            self.assertIn("row_count", data[0])
            self.assertIn("row_count", data[1])

            # Verify row count formatting (4387229 -> 4.4M, 919746 -> 919.7K)
            self.assertEqual(data[0]["row_count"], "4.4M")  # 4387229 formatted
            self.assertEqual(data[1]["row_count"], "919.7K")  # 919746 formatted

    def test_table_timestamp_formatting(self):
        """Test that Unix timestamps are properly converted to readable dates."""

        table_data = {
            "tables": [
                {
                    "name": "test_table",
                    "table_type": "MANAGED",
                    "created_at": 1748473407547,  # Unix timestamp in milliseconds
                    "updated_at": 1748473408383,
                    "columns": [],
                }
            ],
            "catalog_name": "test_catalog",
            "schema_name": "test_schema",
            "total_count": 1,
        }

        with patch("chuck_data.ui.table_formatter.display_table") as mock_display_table:
            # _display_tables raises PaginationCancelled by design
            from chuck_data.exceptions import PaginationCancelled

            with self.assertRaises(PaginationCancelled):
                self.tui._display_tables(table_data)

            kwargs = mock_display_table.call_args.kwargs
            data = kwargs["data"]

            # Verify timestamps were converted to readable format (YYYY-MM-DD)
            created_date = data[0]["created_at"]
            updated_date = data[0]["updated_at"]

            # Should be formatted as YYYY-MM-DD
            self.assertRegex(
                created_date,
                r"^\d{4}-\d{2}-\d{2}$",
                f"created_at should be formatted as YYYY-MM-DD, got: {created_date}",
            )
            self.assertRegex(
                updated_date,
                r"^\d{4}-\d{2}-\d{2}$",
                f"updated_at should be formatted as YYYY-MM-DD, got: {updated_date}",
            )

            # Verify the actual date conversion (1748473407547 ms = 2025-05-28)
            self.assertEqual(created_date, "2025-05-28")
            self.assertEqual(updated_date, "2025-05-28")

    def test_table_display_with_missing_timestamps(self):
        """Test table display handles missing timestamp fields gracefully."""

        table_data = {
            "tables": [
                {
                    "name": "table_no_timestamps",
                    "table_type": "VIEW",
                    # No created_at or updated_at fields
                    "columns": [],
                }
            ],
            "catalog_name": "test_catalog",
            "schema_name": "test_schema",
            "total_count": 1,
        }

        with patch("chuck_data.ui.table_formatter.display_table") as mock_display_table:
            # _display_tables raises PaginationCancelled by design, not an error
            from chuck_data.exceptions import PaginationCancelled

            with self.assertRaises(PaginationCancelled):
                self.tui._display_tables(table_data)

            kwargs = mock_display_table.call_args.kwargs
            data = kwargs["data"]

            # Verify the table was processed even without timestamps
            self.assertEqual(len(data), 1)
            self.assertEqual(data[0]["name"], "table_no_timestamps")

            # Timestamp fields should be None or empty
            self.assertIsNone(data[0].get("created_at"))
            self.assertIsNone(data[0].get("updated_at"))

    def test_row_count_formatting(self):
        """Test that row counts are properly formatted with K/M/B suffixes."""

        test_cases = [
            {"row_count": 123, "expected": "123"},  # Small numbers stay as-is
            {"row_count": 1234, "expected": "1.2K"},  # Thousands
            {"row_count": 50000, "expected": "50.0K"},  # Tens of thousands
            {"row_count": 1234567, "expected": "1.2M"},  # Millions
            {"row_count": 4387229, "expected": "4.4M"},  # Real example from API
            {"row_count": 1234567890, "expected": "1.2B"},  # Billions
            {"row_count": "-", "expected": "-"},  # Dash for unknown values
        ]

        for i, case in enumerate(test_cases):
            with self.subTest(case=case):
                table_data = {
                    "tables": [
                        {
                            "name": f"test_table_{i}",
                            "table_type": "MANAGED",
                            "row_count": case["row_count"],
                            "columns": [],
                        }
                    ],
                    "catalog_name": "test_catalog",
                    "schema_name": "test_schema",
                    "total_count": 1,
                }

                with patch(
                    "chuck_data.ui.table_formatter.display_table"
                ) as mock_display_table:
                    from chuck_data.exceptions import PaginationCancelled

                    with self.assertRaises(PaginationCancelled):
                        self.tui._display_tables(table_data)

                    kwargs = mock_display_table.call_args.kwargs
                    data = kwargs["data"]

                    # Verify row count was formatted correctly
                    actual_row_count = data[0]["row_count"]
                    self.assertEqual(
                        actual_row_count,
                        case["expected"],
                        f"Row count {case['row_count']} should format to {case['expected']}, got {actual_row_count}",
                    )

    def test_display_warehouses_basic(self):
        """Test basic warehouse display functionality."""
        # Create test warehouse data with type field logic
        warehouse_data = {
            "warehouses": [
                {
                    "id": "warehouse-123",
                    "name": "Test Warehouse 1",
                    "size": "XLARGE",
                    "state": "STOPPED",
                    "enable_serverless_compute": True,
                    "warehouse_type": "PRO",
                },
                {
                    "id": "warehouse-456",
                    "name": "Test Warehouse 2",
                    "size": "SMALL",
                    "state": "RUNNING",
                    "enable_serverless_compute": False,
                    "warehouse_type": "PRO",
                },
            ],
            "current_warehouse_id": "warehouse-123",
        }

        from chuck_data.exceptions import PaginationCancelled

        with patch("chuck_data.ui.table_formatter.display_table") as mock_display_table:
            with self.assertRaises(PaginationCancelled):
                self.tui._display_warehouses(warehouse_data)

            # Verify basic table structure was set up correctly
            mock_display_table.assert_called_once()
            call_args = mock_display_table.call_args.kwargs

            self.assertEqual(call_args["title"], "Available SQL Warehouses")
            self.assertEqual(
                call_args["columns"], ["name", "id", "size", "type", "state"]
            )

            # Verify type field logic: serverless when enable_serverless_compute=true, otherwise warehouse_type
            data = call_args["data"]
            self.assertEqual(len(data), 2)
            self.assertEqual(
                data[0]["type"], "serverless"
            )  # enable_serverless_compute=True
            self.assertEqual(
                data[1]["type"], "pro"
            )  # enable_serverless_compute=False, so show warehouse_type

            # Verify size and state values are lowercased
            self.assertEqual(data[0]["size"], "xlarge")  # Original: "XLARGE"
            self.assertEqual(data[0]["state"], "stopped")  # Original: "STOPPED"
            self.assertEqual(data[1]["size"], "small")  # Original: "SMALL"
            self.assertEqual(data[1]["state"], "running")  # Original: "RUNNING"

    def test_display_warehouses_styling(self):
        """Test warehouse display styling functions."""
        warehouse_data = {
            "warehouses": [
                {
                    "id": "warehouse-active",
                    "name": "Active",
                    "size": "SMALL",
                    "state": "RUNNING",
                    "enable_serverless_compute": True,
                    "warehouse_type": "PRO",
                },
                {
                    "id": "warehouse-inactive",
                    "name": "Inactive",
                    "size": "SMALL",
                    "state": "STOPPED",
                    "enable_serverless_compute": False,
                    "warehouse_type": "PRO",
                },
            ],
            "current_warehouse_id": "warehouse-active",
        }

        from chuck_data.exceptions import PaginationCancelled

        with patch("chuck_data.ui.table_formatter.display_table") as mock_display_table:
            with self.assertRaises(PaginationCancelled):
                self.tui._display_warehouses(warehouse_data)

            style_map = mock_display_table.call_args.kwargs["style_map"]

            # Test ID styling for current warehouse
            id_styler = style_map["id"]
            self.assertEqual(id_styler("warehouse-active"), "bold green")
            self.assertIsNone(id_styler("warehouse-inactive"))

            # Test state styling
            state_styler = style_map["state"]
            self.assertEqual(state_styler("running"), "green")
            self.assertEqual(state_styler("stopped"), "red")
            self.assertEqual(state_styler("starting"), "yellow")

    def test_display_warehouses_empty_list(self):
        """Test warehouse display with empty warehouse list."""
        warehouse_data = {"warehouses": []}

        from chuck_data.exceptions import PaginationCancelled

        with self.assertRaises(PaginationCancelled):
            self.tui._display_warehouses(warehouse_data)

        # Verify warning message was printed
        print_calls = [
            str(call[0][0]) for call in self.tui.console.print.call_args_list
        ]
        self.assertTrue(any("No SQL warehouses found" in msg for msg in print_calls))

    def test_display_warehouses_type_conversion(self):
        """Test that warehouse type logic works correctly."""
        warehouse_data = {
            "warehouses": [
                {
                    "id": "1",
                    "name": "Serverless Warehouse",
                    "size": "SMALL",
                    "state": "STOPPED",
                    "enable_serverless_compute": True,
                    "warehouse_type": "PRO",
                },
                {
                    "id": "2",
                    "name": "Pro Warehouse",
                    "size": "SMALL",
                    "state": "STOPPED",
                    "enable_serverless_compute": False,
                    "warehouse_type": "PRO",
                },
                {
                    "id": "3",
                    "name": "Classic Warehouse",
                    "size": "SMALL",
                    "state": "STOPPED",
                    "enable_serverless_compute": False,
                    "warehouse_type": "CLASSIC",
                },
                {
                    "id": "4",
                    "name": "Missing Fields",
                    "size": "SMALL",
                    "state": "STOPPED",
                },  # Missing both fields
            ],
        }

        from chuck_data.exceptions import PaginationCancelled

        with patch("chuck_data.ui.table_formatter.display_table") as mock_display_table:
            with self.assertRaises(PaginationCancelled):
                self.tui._display_warehouses(warehouse_data)

            data = mock_display_table.call_args.kwargs["data"]

            # Check type field logic
            self.assertEqual(
                data[0]["type"], "serverless"
            )  # enable_serverless_compute=True
            self.assertEqual(
                data[1]["type"], "pro"
            )  # enable_serverless_compute=False, warehouse_type="PRO"
            self.assertEqual(
                data[2]["type"], "classic"
            )  # enable_serverless_compute=False, warehouse_type="CLASSIC"
            self.assertEqual(
                data[3]["type"], ""
            )  # Missing both fields (warehouse_type defaults to empty string)

    def test_display_warehouses_current_warehouse_message(self):
        """Test that current warehouse message is displayed when set."""
        warehouse_data = {
            "warehouses": [
                {
                    "id": "wh-123",
                    "name": "Test",
                    "size": "SMALL",
                    "state": "RUNNING",
                    "enable_serverless_compute": False,
                    "warehouse_type": "PRO",
                }
            ],
            "current_warehouse_id": "wh-123",
        }

        from chuck_data.exceptions import PaginationCancelled

        with patch("chuck_data.ui.table_formatter.display_table"):
            with self.assertRaises(PaginationCancelled):
                self.tui._display_warehouses(warehouse_data)

        # Check current warehouse message was printed
        print_calls = [
            str(call[0][0]) for call in self.tui.console.print.call_args_list
        ]
        self.assertTrue(
            any(
                "Current SQL warehouse ID:" in msg and "wh-123" in msg
                for msg in print_calls
            )
        )
