"""Tests for the Amperity API client."""

from unittest.mock import patch

import requests

from chuck_data.clients.amperity import AmperityAPIClient


@patch("chuck_data.clients.amperity.time.sleep")
@patch("chuck_data.clients.amperity.requests.get")
def test_poll_auth_state_stops_on_4xx(mock_get, mock_sleep):
    """Ensure polling stops when the API returns a 4xx response."""

    client = AmperityAPIClient()
    client.nonce = "nonce"

    resp = requests.Response()
    resp.status_code = 401
    mock_get.return_value = resp

    client._poll_auth_state()

    assert client.state == "error"
    mock_get.assert_called_once()
