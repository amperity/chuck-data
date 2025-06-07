"""Connection operations mixin for DatabricksClientStub."""


class ConnectionStubMixin:
    """Mixin providing connection operations for DatabricksClientStub."""

    def __init__(self):
        self.connection_status = "connected"
        self.permissions = {}
        self.token_validation_result = True

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

    def validate_token(self):
        """Validate the token."""
        if self.token_validation_result is True:
            return True
        elif self.token_validation_result is False:
            return False
        else:
            # If it's an exception, raise it
            raise self.token_validation_result

    def set_token_validation_result(self, result):
        """Set the token validation result for testing.

        Args:
            result: True for valid token, False for invalid token,
                   or Exception instance to raise an exception
        """
        self.token_validation_result = result
