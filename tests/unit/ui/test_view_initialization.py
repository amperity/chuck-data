"""Tests for view initialization and registration."""

import unittest
from chuck_data.ui.view_registry import get_view


class TestViewInitialization(unittest.TestCase):
    """Test that views are properly registered."""

    def test_tables_view_registration(self):
        """Test that the TablesTableView is properly registered."""
        view_cls = get_view("list-tables")
        self.assertIsNotNone(view_cls, "TablesTableView should be registered under 'list-tables'")
        self.assertEqual(view_cls.__name__, "TablesTableView", 
                         "Registered view should be TablesTableView")
        
        # Check alternate name registration
        view_cls_alt = get_view("tables")
        self.assertIsNotNone(view_cls_alt, "TablesTableView should be registered under 'tables'")
        self.assertIs(view_cls, view_cls_alt, 
                      "Both 'list-tables' and 'tables' should reference the same view class")

    def test_all_views_registration(self):
        """Test that all critical views are registered."""
        # Test each view is properly registered
        expected_views = {
            "list-catalogs": "CatalogsTableView",
            "catalogs": "CatalogsTableView",
            "list-schemas": "SchemasTableView", 
            "schemas": "SchemasTableView",
            "list-tables": "TablesTableView",
            "tables": "TablesTableView",
            "list-models": "ModelsTableView",
            "models": "ModelsTableView",
            "list-warehouses": "WarehousesTableView",
            "warehouses": "WarehousesTableView",
            "list-volumes": "VolumesTableView",
            "volumes": "VolumesTableView",
            "status": "StatusTableView"
        }
        
        for key, expected_classname in expected_views.items():
            view_cls = get_view(key)
            self.assertIsNotNone(view_cls, f"{expected_classname} should be registered under '{key}'")
            self.assertEqual(view_cls.__name__, expected_classname, 
                           f"Registered view for '{key}' should be {expected_classname}")