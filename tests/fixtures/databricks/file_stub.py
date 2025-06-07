"""File operations mixin for DatabricksClientStub."""


class FileStubMixin:
    """Mixin providing file operations for DatabricksClientStub."""

    def upload_file(self, file_path, destination_path):
        """Upload a file."""
        return {
            "source_path": file_path,
            "destination_path": destination_path,
            "status": "uploaded",
            "size_bytes": 1024,
        }
