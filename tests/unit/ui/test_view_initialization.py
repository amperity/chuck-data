"""
Tests that views are properly imported and registered with the view registry.
"""


def test_views_are_properly_registered():
    """Verify all views are properly registered with the view registry."""
    from chuck_data.ui import view_registry

    # Import these to ensure registration happens
    import chuck_data.ui.views  # noqa: F401

    # List of expected views and their registrations
    expected_views = [
        ("list-tables", "TablesTableView"),
        ("tables", "TablesTableView"),
        ("list-schemas", "SchemasTableView"),
        ("schemas", "SchemasTableView"),
        ("list-catalogs", "CatalogsTableView"),
        ("catalogs", "CatalogsTableView"),
        ("list-warehouses", "WarehousesTableView"),
        ("warehouses", "WarehousesTableView"),
        ("list-volumes", "VolumesTableView"),
        ("volumes", "VolumesTableView"),
        ("list-models", "ModelsTableView"),
        ("models", "ModelsTableView"),
        ("status", "StatusTableView"),
    ]

    for view_name, expected_class_name in expected_views:
        view_class = view_registry.get_view(view_name)
        assert view_class is not None, f"View {view_name} not registered"
        assert (
            view_class.__name__ == expected_class_name
        ), f"View {view_name} registered as wrong class"

    # Ensure import system registers the views
    import importlib

    importlib.reload(chuck_data.ui.views)

    # Check again after reload
    for view_name, expected_class_name in expected_views:
        view_class = view_registry.get_view(view_name)
        assert view_class is not None, f"View {view_name} not registered after reload"
