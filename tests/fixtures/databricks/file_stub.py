"""File operations mixin for DatabricksClientStub."""


class FileStubMixin:
    """Mixin providing file operations for DatabricksClientStub."""

    def __init__(self):
        self.upload_file_failure = False
        self.upload_file_calls = []  # Track upload_file calls
        self.store_dbfs_file_calls = []  # Track store_dbfs_file calls
        self.upload_file_error = None  # Exception to raise on upload_file
        self.store_dbfs_file_error = None  # Exception to raise on store_dbfs_file
        self.uploaded_files = {}  # Simulate uploaded files: path -> content

    def upload_file(
        self, path=None, file_path=None, content=None, overwrite=False, **kwargs
    ):
        """Upload a file to volumes."""
        # Track the call for verification
        call_info = {
            "path": path,
            "file_path": file_path,
            "content": content,
            "overwrite": overwrite,
            **kwargs,
        }
        self.upload_file_calls.append(call_info)

        # Raise error if configured
        if self.upload_file_error:
            raise self.upload_file_error

        # Return failure if configured
        if self.upload_file_failure:
            return False

        # Simulate successful upload
        destination_path = path
        if file_path:
            # Simulate reading file (for testing, we just store the file_path)
            self.uploaded_files[destination_path] = f"file_content_from_{file_path}"
        elif content:
            self.uploaded_files[destination_path] = content

        return True

    def store_dbfs_file(self, path, contents, overwrite=False, **kwargs):
        """Store a file in DBFS."""
        # Track the call for verification
        call_info = {
            "path": path,
            "contents": contents,
            "overwrite": overwrite,
            **kwargs,
        }
        self.store_dbfs_file_calls.append(call_info)

        # Raise error if configured
        if self.store_dbfs_file_error:
            raise self.store_dbfs_file_error

        # Simulate successful DBFS storage
        self.uploaded_files[path] = contents
        return True

    def set_upload_file_failure(self, should_fail=True):
        """Configure upload_file to fail."""
        self.upload_file_failure = should_fail

    def set_upload_file_error(self, error):
        """Configure upload_file to raise an error."""
        self.upload_file_error = error

    def set_store_dbfs_file_error(self, error):
        """Configure store_dbfs_file to raise an error."""
        self.store_dbfs_file_error = error

    def clear_upload_errors(self):
        """Clear any configured errors."""
        self.upload_file_error = None
        self.store_dbfs_file_error = None
        self.upload_file_failure = False

    def fetch_amperity_job_init(self, amperity_token=None):
        """Fetch Amperity job initialization script."""
        if hasattr(self, "_fetch_amperity_error"):
            raise self._fetch_amperity_error
        return {"cluster-init": "echo 'Amperity init script'"}

    def set_fetch_amperity_error(self, error):
        """Configure fetch_amperity_job_init to raise error."""
        self._fetch_amperity_error = error
