"""Warehouse operations mixin for DatabricksClientStub."""


class WarehouseStubMixin:
    """Mixin providing warehouse operations for DatabricksClientStub."""

    def __init__(self):
        self.warehouses = []
        self.create_warehouse_calls = []  # Track create_warehouse calls
        self.create_warehouse_error = None  # Exception to raise on create_warehouse

    def list_warehouses(self, **kwargs):
        """List available warehouses."""
        return self.warehouses

    def get_warehouse(self, warehouse_id):
        """Get a specific warehouse by ID."""
        warehouse = next((w for w in self.warehouses if w["id"] == warehouse_id), None)
        return warehouse  # Return None if not found

    def start_warehouse(self, warehouse_id):
        """Start a warehouse."""
        warehouse = self.get_warehouse(warehouse_id)
        if not warehouse:
            raise Exception(f"Warehouse {warehouse_id} not found")
        warehouse["state"] = "STARTING"
        return warehouse

    def stop_warehouse(self, warehouse_id):
        """Stop a warehouse."""
        warehouse = self.get_warehouse(warehouse_id)
        if not warehouse:
            raise Exception(f"Warehouse {warehouse_id} not found")
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

    def create_warehouse(self, warehouse_config):
        """Create a new warehouse."""
        # Track the call for verification
        self.create_warehouse_calls.append(warehouse_config)

        # Raise error if configured
        if self.create_warehouse_error:
            raise self.create_warehouse_error

        # Generate new warehouse ID
        new_id = f"warehouse_{len(self.warehouses) + 1}"

        # Create warehouse from config
        created_warehouse = {
            "id": new_id,
            "name": warehouse_config.get("name", "New Warehouse"),
            "size": warehouse_config.get("size", "Small"),
            "cluster_size": warehouse_config.get("size", "Small"),
            "auto_stop_mins": warehouse_config.get("auto_stop_mins", 120),
            "state": "STARTING",
            "warehouse_type": "PRO",
            "enable_serverless_compute": False,
            "creator_name": "test.user@example.com",
            "jdbc_url": f"jdbc:databricks://test.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=/sql/1.0/warehouses/{new_id}",
        }

        # Add optional cluster config if provided
        if warehouse_config.get("min_num_clusters") is not None:
            created_warehouse["min_num_clusters"] = warehouse_config["min_num_clusters"]
        if warehouse_config.get("max_num_clusters") is not None:
            created_warehouse["max_num_clusters"] = warehouse_config["max_num_clusters"]

        # Add to warehouses list
        self.warehouses.append(created_warehouse)
        return created_warehouse

    def set_create_warehouse_error(self, error):
        """Configure create_warehouse to raise an error."""
        self.create_warehouse_error = error

    def clear_create_warehouse_error(self):
        """Clear any configured error."""
        self.create_warehouse_error = None
