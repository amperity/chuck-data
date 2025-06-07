"""Amperity client fixtures."""


class AmperityClientStub:
    """Comprehensive stub for AmperityAPIClient with predictable responses."""

    def __init__(self):
        self.base_url = "chuck.amperity.com"
        self.nonce = None
        self.token = None
        self.state = "pending"
        self.auth_thread = None

        # Test configuration
        self.should_fail_auth_start = False
        self.should_fail_auth_completion = False
        self.should_fail_metrics = False
        self.should_fail_bug_report = False
        self.should_raise_exception = False
        self.auth_completion_delay = 0

        # Track method calls for testing
        self.metrics_calls = []

    def start_auth(self) -> tuple[bool, str]:
        """Start the authentication process."""
        if self.should_fail_auth_start:
            return False, "Failed to start auth: 500 - Server Error"

        self.nonce = "test-nonce-123"
        self.state = "started"
        return True, "Authentication started. Please log in via the browser."

    def get_auth_status(self) -> dict:
        """Return the current authentication status."""
        return {"state": self.state, "nonce": self.nonce, "has_token": bool(self.token)}

    def wait_for_auth_completion(
        self, poll_interval: int = 1, timeout: int = None
    ) -> tuple[bool, str]:
        """Wait for authentication to complete in a blocking manner."""
        if not self.nonce:
            return False, "Authentication not started"

        if self.should_fail_auth_completion:
            self.state = "error"
            return False, "Authentication failed: error"

        # Simulate successful authentication
        self.state = "success"
        self.token = "test-auth-token-456"
        return True, "Authentication completed successfully."

    def submit_metrics(self, payload: dict, token: str) -> bool:
        """Send usage metrics to the Amperity API."""
        # Track the call
        self.metrics_calls.append((payload, token))

        if self.should_raise_exception:
            raise Exception("Test exception")

        if self.should_fail_metrics:
            return False

        # Validate basic payload structure
        if not isinstance(payload, dict):
            return False

        if not token:
            return False

        return True

    def submit_bug_report(self, payload: dict, token: str) -> tuple[bool, str]:
        """Send a bug report to the Amperity API."""
        if self.should_fail_bug_report:
            return False, "Failed to submit bug report: 500"

        # Validate basic payload structure
        if not isinstance(payload, dict):
            return False, "Invalid payload format"

        if not token:
            return False, "Authentication token required"

        return True, "Bug report submitted successfully"

    def _poll_auth_state(self) -> None:
        """Poll the auth state endpoint until authentication is complete."""
        # In stub, this is a no-op since we control state directly
        pass

    # Helper methods for test configuration
    def set_auth_start_failure(self, should_fail: bool = True):
        """Configure whether start_auth should fail."""
        self.should_fail_auth_start = should_fail

    def set_auth_completion_failure(self, should_fail: bool = True):
        """Configure whether wait_for_auth_completion should fail."""
        self.should_fail_auth_completion = should_fail

    def set_metrics_failure(self, should_fail: bool = True):
        """Configure whether submit_metrics should fail."""
        self.should_fail_metrics = should_fail

    def set_bug_report_failure(self, should_fail: bool = True):
        """Configure whether submit_bug_report should fail."""
        self.should_fail_bug_report = should_fail

    def reset(self):
        """Reset all state to initial values."""
        self.nonce = None
        self.token = None
        self.state = "pending"
        self.auth_thread = None
        self.should_fail_auth_start = False
        self.should_fail_auth_completion = False
        self.should_fail_metrics = False
        self.should_fail_bug_report = False
        self.auth_completion_delay = 0
