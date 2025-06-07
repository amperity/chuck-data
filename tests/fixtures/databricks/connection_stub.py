"""Connection operations mixin for DatabricksClientStub."""


class ConnectionStubMixin:
    """Mixin providing connection operations for DatabricksClientStub."""

    def __init__(self):
        self.connection_status = "connected"
        self.permissions = {}

    def test_connection(self):
        """Test the connection."""
        if self.connection_status == "connected":
            return {"status": "success", "workspace": "test-workspace"}
        else:
            raise Exception("Connection failed")

    def get_current_user(self):
        """Get current user information."""
        return {"userName": "test.user@example.com", "displayName": "Test User"}

    def set_connection_status(self, status):
        """Set the connection status for testing."""
        self.connection_status = status
