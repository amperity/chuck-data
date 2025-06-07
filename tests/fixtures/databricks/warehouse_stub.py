"""Warehouse operations mixin for DatabricksClientStub."""


class WarehouseStubMixin:
    """Mixin providing warehouse operations for DatabricksClientStub."""

    def __init__(self):
        self.warehouses = []

    def list_warehouses(self, **kwargs):
        """List available warehouses."""
        return self.warehouses

    def get_warehouse(self, warehouse_id):
        """Get a specific warehouse by ID."""
        warehouse = next((w for w in self.warehouses if w["id"] == warehouse_id), None)
        if not warehouse:
            raise Exception(f"Warehouse {warehouse_id} not found")
        return warehouse

    def start_warehouse(self, warehouse_id):
        """Start a warehouse."""
        warehouse = self.get_warehouse(warehouse_id)
        warehouse["state"] = "STARTING"
        return warehouse

    def stop_warehouse(self, warehouse_id):
        """Stop a warehouse."""
        warehouse = self.get_warehouse(warehouse_id)
        warehouse["state"] = "STOPPING"
        return warehouse

    def add_warehouse(
        self,
        warehouse_id=None,
        name="Test Warehouse",
        state="RUNNING",
        size="SMALL",
        enable_serverless_compute=False,
        warehouse_type="PRO",
        creator_name="test.user@example.com",
        auto_stop_mins=60,
        **kwargs,
    ):
        """Add a warehouse to the test data."""
        if warehouse_id is None:
            warehouse_id = f"warehouse_{len(self.warehouses)}"

        warehouse = {
            "id": warehouse_id,
            "name": name,
            "state": state,
            "size": size,  # Use size instead of cluster_size for the main field
            "cluster_size": size,  # Keep cluster_size for backward compatibility
            "enable_serverless_compute": enable_serverless_compute,
            "warehouse_type": warehouse_type,
            "creator_name": creator_name,
            "auto_stop_mins": auto_stop_mins,
            "jdbc_url": f"jdbc:databricks://test.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=/sql/1.0/warehouses/{warehouse_id}",
            **kwargs,
        }
        self.warehouses.append(warehouse)
        return warehouse
