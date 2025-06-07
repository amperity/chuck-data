"""Volume operations mixin for DatabricksClientStub."""


class VolumeStubMixin:
    """Mixin providing volume operations for DatabricksClientStub."""

    def __init__(self):
        self.volumes = {}  # catalog_name -> [volumes]
        self.list_volumes_error = None
        self.create_volume_failure = False

    def list_volumes(self, catalog_name, **kwargs):
        """List volumes in a catalog."""
        if self.list_volumes_error:
            raise self.list_volumes_error
        return {"volumes": self.volumes.get(catalog_name, [])}

    def create_volume(
        self, catalog_name, schema_name, volume_name, volume_type="MANAGED", **kwargs
    ):
        """Create a volume."""
        if self.create_volume_failure:
            return None

        key = catalog_name
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
        key = catalog_name
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
