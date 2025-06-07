"""File operations mixin for DatabricksClientStub."""


class FileStubMixin:
    """Mixin providing file operations for DatabricksClientStub."""

    def __init__(self):
        self.upload_file_failure = False

    def upload_file(self, file_path, destination_path):
        """Upload a file."""
        if self.upload_file_failure:
            return False
        return True

    def set_upload_file_failure(self, should_fail=True):
        """Configure upload_file to fail."""
        self.upload_file_failure = should_fail

    def fetch_amperity_job_init(self, amperity_token=None):
        """Fetch Amperity job initialization script."""
        if hasattr(self, "_fetch_amperity_error"):
            raise self._fetch_amperity_error
        return {"cluster-init": "echo 'Amperity init script'"}

    def set_fetch_amperity_error(self, error):
        """Configure fetch_amperity_job_init to raise error."""
        self._fetch_amperity_error = error
