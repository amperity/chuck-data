"""
Reusable Databricks API client for authentication and requests.
"""

import logging
import requests


class APIClient:
    """Reusable Databricks API client for authentication and requests."""

    def __init__(self, workspace_url, token):
        """
        Initialize the API client.

        Args:
            workspace_url: Databricks workspace URL
            token: Databricks API token
        """
        # Initialize with workspace URL and token
        self.workspace_url = workspace_url
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "User-Agent": "amperity",
        }

    def get(self, endpoint):
        """
        Send a GET request to the Databricks API.

        Args:
            endpoint: API endpoint (starting with /)

        Returns:
            JSON response from the API

        Raises:
            ValueError: If an HTTP error occurs
            ConnectionError: If a connection error occurs
        """
        url = f"{self.workspace_url}{endpoint}"
        # Construct the full URL for the API request

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error: {e}, Response: {response.text}")
            raise ValueError(f"HTTP error occurred: {e}, Response: {response.text}")
        except requests.RequestException as e:
            logging.error(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    def post(self, endpoint, data):
        """
        Send a POST request to the Databricks API.

        Args:
            endpoint: API endpoint (starting with /)
            data: JSON data to send in the request body

        Returns:
            JSON response from the API

        Raises:
            ValueError: If an HTTP error occurs
            ConnectionError: If a connection error occurs
        """
        url = f"{self.workspace_url}{endpoint}"
        logging.debug(f"POST request to: {url}")

        try:
            response = requests.post(url, headers=self.headers, json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error: {e}, Response: {response.text}")
            raise ValueError(f"HTTP error occurred: {e}, Response: {response.text}")
        except requests.RequestException as e:
            logging.error(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")

    def upload_file(self, path, file_path=None, content=None, overwrite=False):
        """
        Upload a file using the /api/2.0/fs/files endpoint.

        Args:
            path: The destination path (e.g., "/Volumes/my-catalog/my-schema/my-volume/file.txt")
            file_path: Local file path to upload (mutually exclusive with content)
            content: String content to upload (mutually exclusive with file_path)
            overwrite: Whether to overwrite an existing file

        Returns:
            True if successful (API returns no content on success)

        Raises:
            ValueError: If both file_path and content are provided or neither is provided
            ValueError: If an HTTP error occurs
            ConnectionError: If a connection error occurs
        """
        if (file_path and content) or (not file_path and not content):
            raise ValueError("Exactly one of file_path or content must be provided")

        # URL encode the path and make sure it starts with a slash
        import urllib.parse

        if not path.startswith("/"):
            path = f"/{path}"

        # Remove duplicate slashes if any
        while "//" in path:
            path = path.replace("//", "/")

        # URL encode path components but preserve the slashes
        encoded_path = "/".join(
            urllib.parse.quote(component) for component in path.split("/") if component
        )
        encoded_path = f"/{encoded_path}"

        url = f"{self.workspace_url}/api/2.0/fs/files{encoded_path}"

        if overwrite:
            url += "?overwrite=true"

        logging.debug(f"File upload request to: {url}")

        headers = self.headers.copy()
        headers.update({"Content-Type": "application/octet-stream"})

        # Get binary data to upload
        if file_path:
            with open(file_path, "rb") as f:
                binary_data = f.read()
        else:
            # Convert string content to bytes
            binary_data = content.encode("utf-8")

        try:
            # Use PUT request with raw binary data in the body
            response = requests.put(url, headers=headers, data=binary_data)
            response.raise_for_status()
            # API returns 204 No Content on success
            return True
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error: {e}, Response: {response.text}")
            raise ValueError(f"HTTP error occurred: {e}, Response: {response.text}")
        except requests.RequestException as e:
            logging.error(f"Connection error: {e}")
            raise ConnectionError(f"Connection error occurred: {e}")
