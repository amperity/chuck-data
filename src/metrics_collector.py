"""
Metrics collection service for tracking usage events.

This module provides functionality to collect and send metrics about usage
of the application to help improve its features and performance.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Union
from src.clients.amperity import AmperityAPIClient

from src.config import get_config_manager, get_amperity_token


class MetricsCollector:
    """Collects and sends usage metrics to the Amperity API."""

    def __init__(self):
        """Initialize the metrics collector."""
        self.config_manager = get_config_manager()
        self._client = AmperityAPIClient()

    def _should_track(self) -> bool:
        """
        Determine if metrics should be tracked based on user consent.

        Returns:
            bool: True if user has provided consent, False otherwise.
        """
        return self.config_manager.get_config().usage_tracking_consent

    def _get_chuck_configuration_for_metric(self) -> Dict[str, Any]:
        """
        Get the configuration settings relevant for metrics.

        Returns:
            Dict[str, Any]: Dictionary of configuration values.
        """
        config = self.config_manager.get_config()
        return {
            "workspace_url": config.workspace_url,
            "active_catalog": config.active_catalog,
            "active_schema": config.active_schema,
            "active_model": config.active_model,
        }

    def track_event(
        self,
        prompt: Optional[str] = None,
        tools: Optional[Union[List[Dict[str, Any]], Dict[str, Any]]] = None,
        conversation_history: Optional[List[Dict[str, Any]]] = None,
        error: Optional[str] = None,
        additional_data: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Track a usage event with provided data.

        Args:
            prompt: The user prompt or query that triggered this event
            tools: Tool usage information for this event
            conversation_history: Previous conversation messages
            error: Error information if this is an error event
            additional_data: Any additional context-specific data

        Returns:
            bool: True if the metrics were sent successfully, False otherwise.
        """
        if not self._should_track():
            logging.debug("Metrics tracking skipped - user has not provided consent")
            return False

        try:
            # Convert tools to list if it's a dict
            if tools and isinstance(tools, dict):
                tools = [tools]

            # Prepare the payload
            payload = {
                "event": "USAGE",  # All events are USAGE events
                "configuration": self._get_chuck_configuration_for_metric(),
            }

            # Add optional fields if provided
            if prompt:
                payload["prompt"] = prompt
            if tools:
                payload["tools"] = tools
            if conversation_history:
                payload["conversation_history"] = conversation_history
            if error:
                payload["error"] = error
            if additional_data:
                payload["additional_data"] = additional_data

            # Send the metric
            return self.send_metric(payload)
        except Exception as e:
            logging.debug(f"Error tracking metrics: {e}", exc_info=True)
            return False

    def send_metric(self, payload: Dict[str, Any]) -> bool:
        """
        Send the collected metric to the Amperity API.

        Args:
            payload: The data payload to send

        Returns:
            bool: True if sent successfully, False otherwise.
        """
        try:

            token = get_amperity_token()
            if not token:
                logging.debug("Cannot send metrics - no authentication token available")
                return False

            # Convert the payload to a JSON string for logging
            payload_str = json.dumps(payload)
            logging.debug(f"Sending metric: {payload_str[:100]}...")

            return self._client.submit_metrics(payload, token)
        except Exception as e:
            logging.debug(f"Error sending metrics: {e}", exc_info=True)
            return False


# Global metrics collector instance
_metrics_collector = MetricsCollector()


def get_metrics_collector() -> MetricsCollector:
    """
    Get the global metrics collector instance.

    Returns:
        MetricsCollector: The global metrics collector instance.
    """
    return _metrics_collector
