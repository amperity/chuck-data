"""
Tests for the metrics collector.
"""

import pytest
from unittest.mock import patch

from chuck_data.metrics_collector import MetricsCollector, get_metrics_collector
from tests.fixtures.collectors import ConfigManagerStub


@pytest.fixture
def metrics_collector_with_stubs(amperity_client_stub):
    """Create a MetricsCollector with stubbed dependencies."""
    config_manager_stub = ConfigManagerStub()
    config_stub = config_manager_stub.config

    # Create the metrics collector with mocked config and AmperityClientStub
    with patch(
        "chuck_data.metrics_collector.get_config_manager",
        return_value=config_manager_stub,
    ):
        with patch(
            "chuck_data.metrics_collector.AmperityAPIClient",
            return_value=amperity_client_stub,
        ):
            metrics_collector = MetricsCollector()

    return metrics_collector, config_stub, amperity_client_stub


def test_should_track_with_consent(metrics_collector_with_stubs):
    """Test that metrics are tracked when consent is given."""
    metrics_collector, config_stub, _ = metrics_collector_with_stubs
    config_stub.usage_tracking_consent = True
    result = metrics_collector._should_track()
    assert result


def test_should_track_without_consent(metrics_collector_with_stubs):
    """Test that metrics are not tracked when consent is not given."""
    metrics_collector, config_stub, _ = metrics_collector_with_stubs
    config_stub.usage_tracking_consent = False
    result = metrics_collector._should_track()
    assert not result


def test_get_chuck_configuration(metrics_collector_with_stubs):
    """Test that configuration is retrieved correctly."""
    metrics_collector, config_stub, _ = metrics_collector_with_stubs
    config_stub.workspace_url = "test-workspace"
    config_stub.active_catalog = "test-catalog"
    config_stub.active_schema = "test-schema"
    config_stub.active_model = "test-model"

    result = metrics_collector._get_chuck_configuration_for_metric()

    assert result == {
        "workspace_url": "test-workspace",
        "active_catalog": "test-catalog",
        "active_schema": "test-schema",
        "active_model": "test-model",
    }


def test_track_event_no_consent(metrics_collector_with_stubs):
    """Test that tracking is skipped when consent is not given."""
    import tempfile
    from chuck_data.config import ConfigManager, set_amperity_token

    # Use real config system
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            # Set amperity token using real config
            set_amperity_token("test-token")

            metrics_collector, config_stub, amperity_client_stub = (
                metrics_collector_with_stubs
            )
            config_stub.usage_tracking_consent = False

            # Reset stub metrics call count
            amperity_client_stub.metrics_calls = []

            result = metrics_collector.track_event(prompt="test prompt")

            assert not result
            # Ensure submit_metrics is not called
            assert len(amperity_client_stub.metrics_calls) == 0


@patch("chuck_data.metrics_collector.MetricsCollector.send_metric")
def test_track_event_with_all_fields(mock_send_metric, metrics_collector_with_stubs):
    """Test tracking with all fields provided."""
    import tempfile
    from chuck_data.config import ConfigManager, set_amperity_token

    # Use real config system
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            # Set amperity token using real config
            set_amperity_token("test-token")

            metrics_collector, config_stub, _ = metrics_collector_with_stubs
            config_stub.usage_tracking_consent = True
            mock_send_metric.return_value = True

            # Prepare test data
            prompt = "test prompt"
            tools = [{"name": "test_tool", "arguments": {"arg1": "value1"}}]
            conversation_history = [{"role": "assistant", "content": "test response"}]
            error = "test error"
            additional_data = {"event_context": "test_context"}

            # Call track_event
            result = metrics_collector.track_event(
                prompt=prompt,
                tools=tools,
                conversation_history=conversation_history,
                error=error,
                additional_data=additional_data,
            )

            # Assert results
            assert result
            mock_send_metric.assert_called_once()

            # Check payload content
            payload = mock_send_metric.call_args[0][0]
            assert payload["event"] == "USAGE"
            assert payload["prompt"] == prompt
            assert payload["tools"] == tools
            assert payload["conversation_history"] == conversation_history
            assert payload["error"] == error
            assert payload["additional_data"] == additional_data


def test_send_metric_successful(metrics_collector_with_stubs):
    """Test successful metrics sending."""
    import tempfile
    from chuck_data.config import ConfigManager, set_amperity_token

    # Use real config system
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            # Set amperity token using real config
            set_amperity_token("test-token")

            metrics_collector, _, amperity_client_stub = metrics_collector_with_stubs
            payload = {"event": "USAGE", "prompt": "test prompt"}

            # Reset stub metrics call count
            amperity_client_stub.metrics_calls = []

            result = metrics_collector.send_metric(payload)

            assert result
            assert len(amperity_client_stub.metrics_calls) == 1
            assert amperity_client_stub.metrics_calls[0] == (payload, "test-token")


def test_send_metric_failure(metrics_collector_with_stubs):
    """Test handling of metrics sending failure."""
    import tempfile
    from chuck_data.config import ConfigManager, set_amperity_token

    # Use real config system
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            # Set amperity token using real config
            set_amperity_token("test-token")

            metrics_collector, _, amperity_client_stub = metrics_collector_with_stubs

            # Configure stub to simulate failure
            amperity_client_stub.should_fail_metrics = True
            amperity_client_stub.metrics_calls = []

            payload = {"event": "USAGE", "prompt": "test prompt"}

            result = metrics_collector.send_metric(payload)

            assert not result
            assert len(amperity_client_stub.metrics_calls) == 1
            assert amperity_client_stub.metrics_calls[0] == (payload, "test-token")


def test_send_metric_exception(metrics_collector_with_stubs):
    """Test handling of exceptions during metrics sending."""
    import tempfile
    from chuck_data.config import ConfigManager, set_amperity_token

    # Use real config system
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            # Set amperity token using real config
            set_amperity_token("test-token")

            metrics_collector, _, amperity_client_stub = metrics_collector_with_stubs

            # Configure stub to raise exception
            amperity_client_stub.should_raise_exception = True
            amperity_client_stub.metrics_calls = []

            payload = {"event": "USAGE", "prompt": "test prompt"}

            result = metrics_collector.send_metric(payload)

            assert not result
            assert len(amperity_client_stub.metrics_calls) == 1
            assert amperity_client_stub.metrics_calls[0] == (payload, "test-token")


def test_send_metric_no_token(metrics_collector_with_stubs):
    """Test that metrics are not sent when no token is available."""
    import tempfile
    from chuck_data.config import ConfigManager

    # Use real config system with no token set
    with tempfile.NamedTemporaryFile() as tmp:
        config_manager = ConfigManager(tmp.name)

        with patch("chuck_data.config._config_manager", config_manager):
            # Don't set any amperity token (should be None by default)

            metrics_collector, _, amperity_client_stub = metrics_collector_with_stubs

            # Reset stub metrics call count
            amperity_client_stub.metrics_calls = []

            payload = {"event": "USAGE", "prompt": "test prompt"}

            result = metrics_collector.send_metric(payload)

            assert not result
            assert len(amperity_client_stub.metrics_calls) == 0


def test_get_metrics_collector():
    """Test that get_metrics_collector returns the singleton instance."""
    with patch("chuck_data.metrics_collector._metrics_collector") as mock_collector:
        collector = get_metrics_collector()
        assert collector == mock_collector
