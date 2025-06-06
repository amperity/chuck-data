"""
Tests for the metrics collector.
"""

import unittest
from unittest.mock import patch

from chuck_data.metrics_collector import MetricsCollector, get_metrics_collector
from tests.fixtures import AmperityClientStub, ConfigManagerStub


class TestMetricsCollector(unittest.TestCase):
    """Test cases for MetricsCollector."""

    def setUp(self):
        """Set up test fixtures."""
        self.config_manager_stub = ConfigManagerStub()
        self.config_stub = self.config_manager_stub.config

        # Create the metrics collector with mocked config and AmperityClientStub
        self.amperity_client_stub = AmperityClientStub()
        with patch(
            "chuck_data.metrics_collector.get_config_manager",
            return_value=self.config_manager_stub,
        ):
            with patch(
                "chuck_data.metrics_collector.AmperityAPIClient",
                return_value=self.amperity_client_stub,
            ):
                self.metrics_collector = MetricsCollector()

    def test_should_track_with_consent(self):
        """Test that metrics are tracked when consent is given."""
        self.config_stub.usage_tracking_consent = True
        result = self.metrics_collector._should_track()
        self.assertTrue(result)

    def test_should_track_without_consent(self):
        """Test that metrics are not tracked when consent is not given."""
        self.config_stub.usage_tracking_consent = False
        result = self.metrics_collector._should_track()
        self.assertFalse(result)

    def test_get_chuck_configuration(self):
        """Test that configuration is retrieved correctly."""
        self.config_stub.workspace_url = "test-workspace"
        self.config_stub.active_catalog = "test-catalog"
        self.config_stub.active_schema = "test-schema"
        self.config_stub.active_model = "test-model"

        result = self.metrics_collector._get_chuck_configuration_for_metric()

        self.assertEqual(
            result,
            {
                "workspace_url": "test-workspace",
                "active_catalog": "test-catalog",
                "active_schema": "test-schema",
                "active_model": "test-model",
            },
        )

    @patch("chuck_data.metrics_collector.get_amperity_token", return_value="test-token")
    def test_track_event_no_consent(self, mock_get_token):
        """Test that tracking is skipped when consent is not given."""
        self.config_stub.usage_tracking_consent = False

        # Reset stub metrics call count
        self.amperity_client_stub.metrics_calls = []

        result = self.metrics_collector.track_event(prompt="test prompt")

        self.assertFalse(result)
        # Ensure submit_metrics is not called
        self.assertEqual(len(self.amperity_client_stub.metrics_calls), 0)

    @patch("chuck_data.metrics_collector.get_amperity_token", return_value="test-token")
    @patch("chuck_data.metrics_collector.MetricsCollector.send_metric")
    def test_track_event_with_all_fields(self, mock_send_metric, mock_get_token):
        """Test tracking with all fields provided."""
        self.config_stub.usage_tracking_consent = True
        mock_send_metric.return_value = True

        # Prepare test data
        prompt = "test prompt"
        tools = [{"name": "test_tool", "arguments": {"arg1": "value1"}}]
        conversation_history = [{"role": "assistant", "content": "test response"}]
        error = "test error"
        additional_data = {"event_context": "test_context"}

        # Call track_event
        result = self.metrics_collector.track_event(
            prompt=prompt,
            tools=tools,
            conversation_history=conversation_history,
            error=error,
            additional_data=additional_data,
        )

        # Assert results
        self.assertTrue(result)
        mock_send_metric.assert_called_once()

        # Check payload content
        payload = mock_send_metric.call_args[0][0]
        self.assertEqual(payload["event"], "USAGE")
        self.assertEqual(payload["prompt"], prompt)
        self.assertEqual(payload["tools"], tools)
        self.assertEqual(payload["conversation_history"], conversation_history)
        self.assertEqual(payload["error"], error)
        self.assertEqual(payload["additional_data"], additional_data)

    @patch("chuck_data.metrics_collector.get_amperity_token", return_value="test-token")
    def test_send_metric_successful(self, mock_get_token):
        """Test successful metrics sending."""
        payload = {"event": "USAGE", "prompt": "test prompt"}

        # Reset stub metrics call count
        self.amperity_client_stub.metrics_calls = []

        result = self.metrics_collector.send_metric(payload)

        self.assertTrue(result)
        self.assertEqual(len(self.amperity_client_stub.metrics_calls), 1)
        self.assertEqual(
            self.amperity_client_stub.metrics_calls[0], (payload, "test-token")
        )

    @patch("chuck_data.metrics_collector.get_amperity_token", return_value="test-token")
    def test_send_metric_failure(self, mock_get_token):
        """Test handling of metrics sending failure."""
        # Configure stub to simulate failure
        self.amperity_client_stub.should_fail_metrics = True
        self.amperity_client_stub.metrics_calls = []

        payload = {"event": "USAGE", "prompt": "test prompt"}

        result = self.metrics_collector.send_metric(payload)

        self.assertFalse(result)
        self.assertEqual(len(self.amperity_client_stub.metrics_calls), 1)
        self.assertEqual(
            self.amperity_client_stub.metrics_calls[0], (payload, "test-token")
        )

    @patch("chuck_data.metrics_collector.get_amperity_token", return_value="test-token")
    def test_send_metric_exception(self, mock_get_token):
        """Test handling of exceptions during metrics sending."""
        # Configure stub to raise exception
        self.amperity_client_stub.should_raise_exception = True
        self.amperity_client_stub.metrics_calls = []

        payload = {"event": "USAGE", "prompt": "test prompt"}

        result = self.metrics_collector.send_metric(payload)

        self.assertFalse(result)
        self.assertEqual(len(self.amperity_client_stub.metrics_calls), 1)
        self.assertEqual(
            self.amperity_client_stub.metrics_calls[0], (payload, "test-token")
        )

    @patch("chuck_data.metrics_collector.get_amperity_token", return_value=None)
    def test_send_metric_no_token(self, mock_get_token):
        """Test that metrics are not sent when no token is available."""
        # Reset stub metrics call count
        self.amperity_client_stub.metrics_calls = []

        payload = {"event": "USAGE", "prompt": "test prompt"}

        result = self.metrics_collector.send_metric(payload)

        self.assertFalse(result)
        self.assertEqual(len(self.amperity_client_stub.metrics_calls), 0)

    def test_get_metrics_collector(self):
        """Test that get_metrics_collector returns the singleton instance."""
        with patch("chuck_data.metrics_collector._metrics_collector") as mock_collector:
            collector = get_metrics_collector()
            self.assertEqual(collector, mock_collector)
