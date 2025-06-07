"""HTTP operations mixin for DatabricksClientStub."""

from unittest.mock import MagicMock


class HTTPStubMixin:
    """Mixin providing raw HTTP operations for DatabricksClientStub."""

    def __init__(self):
        # Create mock objects for post and get methods
        self.post = MagicMock()
        self.get = MagicMock()
        
        # Set up default responses
        self._setup_default_responses()

    def _setup_default_responses(self):
        """Set up default HTTP responses."""
        # Default successful responses
        self.post.return_value = {"success": True}
        self.get.return_value = {"status": "success"}