"""Metrics collector and related fixtures."""


class MetricsCollectorStub:
    """Comprehensive stub for MetricsCollector with predictable responses."""

    def __init__(self):
        # Track method calls for testing
        self.track_event_calls = []

        # Test configuration
        self.should_fail_track_event = False
        self.should_return_false = False

    def track_event(
        self,
        prompt=None,
        tools=None,
        conversation_history=None,
        error=None,
        additional_data=None,
    ):
        """Track an event (simulate metrics collection)."""
        call_info = {
            "prompt": prompt,
            "tools": tools,
            "conversation_history": conversation_history,
            "error": error,
            "additional_data": additional_data,
        }
        self.track_event_calls.append(call_info)

        if self.should_fail_track_event:
            raise Exception("Metrics collection failed")

        return not self.should_return_false

    def set_track_event_failure(self, should_fail=True):
        """Configure track_event to fail."""
        self.should_fail_track_event = should_fail

    def set_return_false(self, should_return_false=True):
        """Configure track_event to return False."""
        self.should_return_false = should_return_false


class ConfigManagerStub:
    """Comprehensive stub for ConfigManager with predictable responses."""

    def __init__(self):
        self.config = ConfigStub()

    def get_config(self):
        """Return the config stub."""
        return self.config


class ConfigStub:
    """Comprehensive stub for Config objects with predictable responses."""

    def __init__(self):
        # Default config values
        self.workspace_url = "https://test.databricks.com"
        self.active_catalog = "test_catalog"
        self.active_schema = "test_schema"
        self.active_model = "test_model"
        self.usage_tracking_consent = True

        # Additional config properties as needed
        self.databricks_token = "test-token"
        self.host = "test.databricks.com"
