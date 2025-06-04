"""Unit tests for the models module."""

import unittest
from src.models import list_models, get_model
from tests.fixtures import (
    EXPECTED_MODEL_LIST,
    DatabricksClientStub,
)


class TestModels(unittest.TestCase):
    """Test cases for the models module."""

    def test_list_models_success(self):
        """Test successful retrieval of model list."""
        # Create a client stub
        client_stub = DatabricksClientStub()
        # Configure stub to return expected model list
        client_stub.models = EXPECTED_MODEL_LIST

        models = list_models(client_stub)

        self.assertEqual(models, EXPECTED_MODEL_LIST)

    def test_list_models_empty(self):
        """Test retrieval with empty model list."""
        # Create a client stub
        client_stub = DatabricksClientStub()
        # Configure stub to return empty list
        client_stub.models = []

        models = list_models(client_stub)
        self.assertEqual(models, [])

    def test_list_models_http_error(self):
        """Test failure with HTTP error."""
        # Create a client stub
        client_stub = DatabricksClientStub()
        # Configure stub to raise ValueError
        client_stub.set_list_models_error(
            ValueError("HTTP error occurred: 404 Not Found")
        )

        with self.assertRaises(ValueError) as context:
            list_models(client_stub)
        self.assertIn("Model serving API error", str(context.exception))

    def test_list_models_connection_error(self):
        """Test failure due to connection error."""
        # Create a client stub
        client_stub = DatabricksClientStub()
        # Configure stub to raise ConnectionError
        client_stub.set_list_models_error(ConnectionError("Connection failed"))

        with self.assertRaises(ConnectionError) as context:
            list_models(client_stub)
        self.assertIn("Failed to connect to serving endpoint", str(context.exception))

    def test_get_model_success(self):
        """Test successful retrieval of a specific model."""
        # Create client stub and configure model detail
        client_stub = DatabricksClientStub()
        model_detail = {
            "name": "databricks-llama-4-maverick",
            "creator": "user@example.com",
            "creation_timestamp": 1645123456789,
            "state": "READY",
        }
        client_stub.add_model(
            "databricks-llama-4-maverick",
            status="READY",
            creator="user@example.com",
            creation_timestamp=1645123456789,
        )

        # Call the function
        result = get_model(client_stub, "databricks-llama-4-maverick")

        # Verify results
        self.assertEqual(result["name"], model_detail["name"])
        self.assertEqual(result["creator"], model_detail["creator"])

    def test_get_model_not_found(self):
        """Test retrieval of a non-existent model."""
        # Create client stub that returns None for not found models
        client_stub = DatabricksClientStub()
        # No model added, so get_model will return None

        # Call the function
        result = get_model(client_stub, "nonexistent-model")

        # Verify result is None
        self.assertIsNone(result)

    def test_get_model_error(self):
        """Test retrieval with a non-404 error."""
        # Create client stub that raises a 500 error
        client_stub = DatabricksClientStub()
        client_stub.set_get_model_error(
            ValueError("HTTP error occurred: 500 Internal Server Error")
        )

        # Call the function and expect an exception
        with self.assertRaises(ValueError) as context:
            get_model(client_stub, "error-model")

        # Verify error handling
        self.assertIn("Model serving API error", str(context.exception))

    def test_get_model_connection_error(self):
        """Test retrieval with connection error."""
        # Create client stub that raises a connection error
        client_stub = DatabricksClientStub()
        client_stub.set_get_model_error(ConnectionError("Connection failed"))

        # Call the function and expect an exception
        with self.assertRaises(ConnectionError) as context:
            get_model(client_stub, "network-error-model")

        # Verify error handling
        self.assertIn("Failed to connect to serving endpoint", str(context.exception))
