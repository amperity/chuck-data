"""Volume operations mixin for DatabricksClientStub."""


class VolumeStubMixin:
    """Mixin providing volume operations for DatabricksClientStub."""

    def __init__(self):
        self.volumes = {}  # (catalog_name, schema_name) -> [volumes]
        self.list_volumes_error = None
        self.create_volume_failure = False
        self.list_volumes_calls = []

    def list_volumes(self, catalog_name, schema_name, **kwargs):
        """List volumes in a schema."""
        self.list_volumes_calls.append((catalog_name, schema_name, kwargs))
        if self.list_volumes_error:
            raise self.list_volumes_error
        key = (catalog_name, schema_name)
        return {"volumes": self.volumes.get(key, [])}

    def create_volume(
        self, catalog_name, schema_name, volume_name, volume_type="MANAGED", **kwargs
    ):
        """Create a volume."""
        if self.create_volume_failure:
            return None

        key = (catalog_name, schema_name)
        if key not in self.volumes:
            self.volumes[key] = []

        volume = {
            "name": volume_name,
            "full_name": f"{catalog_name}.{schema_name}.{volume_name}",
            "volume_type": volume_type,
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            **kwargs,
        }
        self.volumes[key].append(volume)
        return volume

    def set_list_volumes_error(self, error):
        """Configure list_volumes to raise error."""
        self.list_volumes_error = error

    def set_create_volume_failure(self, should_fail=True):
        """Configure create_volume to return None."""
        self.create_volume_failure = should_fail

    def add_volume(
        self, catalog_name, schema_name, volume_name, volume_type="MANAGED", **kwargs
    ):
        """Add a volume to the test data."""
        key = (catalog_name, schema_name)
        if key not in self.volumes:
            self.volumes[key] = []

        volume = {
            "name": volume_name,
            "full_name": f"{catalog_name}.{schema_name}.{volume_name}",
            "volume_type": volume_type,
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            **kwargs,
        }
        self.volumes[key].append(volume)
        return volume
