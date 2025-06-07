"""Main DatabricksClientStub that combines all functionality mixins."""

from .catalog_stub import CatalogStubMixin
from .schema_stub import SchemaStubMixin
from .table_stub import TableStubMixin
from .model_stub import ModelStubMixin
from .warehouse_stub import WarehouseStubMixin
from .volume_stub import VolumeStubMixin
from .sql_stub import SQLStubMixin
from .job_stub import JobStubMixin
from .pii_stub import PIIStubMixin
from .connection_stub import ConnectionStubMixin
from .file_stub import FileStubMixin


class DatabricksClientStub(
    CatalogStubMixin,
    SchemaStubMixin,
    TableStubMixin,
    ModelStubMixin,
    WarehouseStubMixin,
    VolumeStubMixin,
    SQLStubMixin,
    JobStubMixin,
    PIIStubMixin,
    ConnectionStubMixin,
    FileStubMixin,
):
    """Comprehensive stub for DatabricksAPIClient with predictable responses.

    This stub combines all functionality mixins to provide a complete test double
    for the Databricks API client.
    """

    def __init__(self):
        # Initialize all mixins
        CatalogStubMixin.__init__(self)
        SchemaStubMixin.__init__(self)
        TableStubMixin.__init__(self)
        ModelStubMixin.__init__(self)
        WarehouseStubMixin.__init__(self)
        VolumeStubMixin.__init__(self)
        SQLStubMixin.__init__(self)
        JobStubMixin.__init__(self)
        PIIStubMixin.__init__(self)
        ConnectionStubMixin.__init__(self)
        FileStubMixin.__init__(self)

    def reset(self):
        """Reset all data to initial state."""
        self.catalogs = []
        self.schemas = {}
        self.tables = {}
        self.models = []
        self.warehouses = []
        self.volumes = {}
        self.connection_status = "connected"
        self.permissions = {}
        self.token_validation_result = True
        self.sql_results = {}
        self.pii_scan_results = {}

        # Reset call tracking
        self.create_stitch_notebook_calls = []
        self.list_catalogs_calls = []
        self.get_catalog_calls = []
        self.list_schemas_calls = []
        self.get_schema_calls = []
        self.list_tables_calls = []
        self.get_table_calls = []
