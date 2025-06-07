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

        # Storage for configured responses
        self._get_responses = {}
        self._get_errors = {}

    def set_get_response(self, url, response):
        """Configure a specific response for a GET request to a URL."""
        self._get_responses[url] = response

        # Update the get mock to use our response mapping
        def get_side_effect(url_arg):
            if url_arg in self._get_errors:
                raise self._get_errors[url_arg]
            if url_arg in self._get_responses:
                return self._get_responses[url_arg]
            return {"status": "success"}  # Default response

        self.get.side_effect = get_side_effect

    def set_get_error(self, url, error):
        """Configure a specific error for a GET request to a URL."""
        self._get_errors[url] = error

        # Update the get mock to use our error mapping
        def get_side_effect(url_arg):
            if url_arg in self._get_errors:
                raise self._get_errors[url_arg]
            if url_arg in self._get_responses:
                return self._get_responses[url_arg]
            return {"status": "success"}  # Default response

        self.get.side_effect = get_side_effect
