"""
Tests for agent command handler.

This module contains tests for the agent command handler.
"""

import unittest
from unittest.mock import patch, MagicMock


# Create mocks at module level to avoid importing problematic classes
class MockAgentManagerClass:
    def __init__(self, *args, **kwargs):
        self.api_client = args[0] if args else None
        self.tool_output_callback = kwargs.get("tool_output_callback")
        self.conversation_history = [
            {"role": "user", "content": "Test question"},
            {"role": "assistant", "content": "Test response"},
        ]

    def process_query(self, query):
        return f"Processed query: {query}"

    def process_pii_detection(self, table_name, catalog_name=None, schema_name=None):
        return f"PII detection for {table_name}"

    def process_bulk_pii_scan(self, catalog_name=None, schema_name=None):
        return f"Bulk PII scan for {catalog_name}.{schema_name}"

    def process_setup_stitch(self, catalog_name=None, schema_name=None):
        return f"Stitch setup for {catalog_name}.{schema_name}"


# Directly apply the mock to avoid importing the actual class
with patch("src.agent.manager.AgentManager", MockAgentManagerClass):
    from src.commands.agent import handle_command


class TestAgentCommand(unittest.TestCase):
    """Tests for agent command handler."""

    def test_missing_query(self):
        """Test handling when query parameter is not provided."""
        result = handle_command(None)
        self.assertFalse(result.success)
        self.assertIn("Please provide a query", result.message)

    @patch("src.agent.AgentManager", MockAgentManagerClass)
    @patch("src.config.get_agent_history", return_value=[])
    @patch("src.config.set_agent_history")
    @patch("src.commands.agent.get_metrics_collector")
    def test_general_query_mode(
        self, mock_get_metrics_collector, mock_set_history, mock_get_history
    ):
        """Test processing a general query."""
        mock_client = MagicMock()
        mock_metrics_collector = MagicMock()
        mock_get_metrics_collector.return_value = mock_metrics_collector

        # Call function
        result = handle_command(mock_client, query="What tables are available?")

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(
            result.data["response"], "Processed query: What tables are available?"
        )
        mock_set_history.assert_called_once()

        # Verify metrics collection
        mock_metrics_collector.track_event.assert_called_once()
        # Check that the right parameters were passed
        call_args = mock_metrics_collector.track_event.call_args[1]
        self.assertEqual(call_args["prompt"], "What tables are available?")
        self.assertEqual(
            call_args["tools"],
            [
                {
                    "name": "general_query",
                    "arguments": {"query": "What tables are available?"},
                }
            ],
        )
        self.assertIn(
            {"role": "assistant", "content": "Test response"},
            call_args["conversation_history"],
        )
        self.assertEqual(
            call_args["additional_data"],
            {"event_context": "agent_interaction", "agent_mode": "general"},
        )

    @patch("src.agent.AgentManager", MockAgentManagerClass)
    @patch("src.config.get_agent_history", return_value=[])
    @patch("src.config.set_agent_history")
    @patch("src.commands.agent.get_metrics_collector")
    def test_pii_detection_mode(
        self, mock_get_metrics_collector, mock_set_history, mock_get_history
    ):
        """Test processing a PII detection query."""
        mock_client = MagicMock()
        mock_metrics_collector = MagicMock()
        mock_get_metrics_collector.return_value = mock_metrics_collector

        # Call function
        result = handle_command(
            mock_client,
            query="customers",
            mode="pii",
            catalog_name="test_catalog",
            schema_name="test_schema",
        )

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(result.data["response"], "PII detection for customers")
        mock_set_history.assert_called_once()

        # Verify metrics collection
        mock_metrics_collector.track_event.assert_called_once()
        # Check that the right parameters were passed
        call_args = mock_metrics_collector.track_event.call_args[1]
        self.assertEqual(call_args["prompt"], "customers")
        self.assertEqual(
            call_args["tools"],
            [{"name": "pii_detection", "arguments": {"table": "customers"}}],
        )
        self.assertIn(
            {"role": "assistant", "content": "Test response"},
            call_args["conversation_history"],
        )
        self.assertEqual(
            call_args["additional_data"],
            {"event_context": "agent_interaction", "agent_mode": "pii"},
        )

    @patch("src.agent.AgentManager", MockAgentManagerClass)
    @patch("src.config.get_agent_history", return_value=[])
    @patch("src.config.set_agent_history")
    def test_bulk_pii_scan_mode(self, mock_set_history, mock_get_history):
        """Test processing a bulk PII scan."""
        mock_client = MagicMock()

        # Call function
        result = handle_command(
            mock_client,
            query="Scan all tables",
            mode="bulk_pii",
            catalog_name="test_catalog",
            schema_name="test_schema",
        )

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(
            result.data["response"], "Bulk PII scan for test_catalog.test_schema"
        )
        mock_set_history.assert_called_once()

    @patch("src.agent.AgentManager", MockAgentManagerClass)
    @patch("src.config.get_agent_history", return_value=[])
    @patch("src.config.set_agent_history")
    def test_stitch_setup_mode(self, mock_set_history, mock_get_history):
        """Test processing a stitch setup request."""
        mock_client = MagicMock()

        # Call function
        result = handle_command(
            mock_client,
            query="Set up stitch",
            mode="stitch",
            catalog_name="test_catalog",
            schema_name="test_schema",
        )

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(
            result.data["response"], "Stitch setup for test_catalog.test_schema"
        )
        mock_set_history.assert_called_once()

    @patch("src.agent.AgentManager", side_effect=Exception("Agent error"))
    def test_agent_exception(self, mock_agent_manager):
        """Test agent with unexpected exception."""
        # Call function
        result = handle_command(None, query="This will fail")

        # Verify results
        self.assertFalse(result.success)
        self.assertIn("Failed to process query", result.message)
        self.assertEqual(str(result.error), "Agent error")

    @patch("src.agent.AgentManager", MockAgentManagerClass)
    @patch("src.config.get_agent_history", return_value=[])
    @patch("src.config.set_agent_history")
    def test_query_from_rest_parameter(self, mock_set_history, mock_get_history):
        """Test processing a query from the rest parameter."""
        mock_client = MagicMock()

        # Call function with rest parameter instead of query
        result = handle_command(mock_client, rest="What tables are available?")

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(
            result.data["response"], "Processed query: What tables are available?"
        )
        mock_set_history.assert_called_once()

    @patch("src.agent.AgentManager", MockAgentManagerClass)
    @patch("src.config.get_agent_history", return_value=[])
    @patch("src.config.set_agent_history")
    def test_query_from_raw_args_parameter(self, mock_set_history, mock_get_history):
        """Test processing a query from the raw_args parameter."""
        mock_client = MagicMock()

        # Call function with raw_args parameter
        raw_args = ["What", "tables", "are", "available?"]
        result = handle_command(mock_client, raw_args=raw_args)

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(
            result.data["response"], "Processed query: What tables are available?"
        )
        mock_set_history.assert_called_once()

    @patch("src.agent.AgentManager", MockAgentManagerClass)
    @patch("src.config.get_agent_history", return_value=[])
    @patch("src.config.set_agent_history")
    def test_callback_parameter_passed(self, mock_set_history, mock_get_history):
        """Test that tool_output_callback is properly passed to AgentManager."""
        mock_client = MagicMock()
        mock_callback = MagicMock()

        # Call function with callback
        result = handle_command(
            mock_client,
            query="What tables are available?",
            tool_output_callback=mock_callback,
        )

        # Verify results
        self.assertTrue(result.success)
        self.assertEqual(
            result.data["response"], "Processed query: What tables are available?"
        )
        mock_set_history.assert_called_once()
