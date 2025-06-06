"""Amperity API client for authentication."""

import logging
import os
import requests
import threading
import time
import webbrowser
import readchar
import json
from rich.console import Console

from chuck_data.config import set_amperity_token
from chuck_data.ui.theme import (
    SUCCESS_STYLE,
    INFO_STYLE,
)

# Default Amperity base domain used when environment variable is not provided
DEFAULT_AMPERITY_URL = "chuck.amperity.com"


def get_amperity_url() -> str:
    """Return the Amperity base URL, using env var override if set.

    Strips any protocol prefix to ensure clean domain-only return.
    """
    url = os.getenv("CHUCK_AMPERITY_URL", DEFAULT_AMPERITY_URL)
    # Remove protocol if present
    if url.startswith("https://"):
        url = url[8:]
    elif url.startswith("http://"):
        url = url[7:]
    return url


class AmperityAPIClient:
    """Client for handling Amperity authentication flow."""

    def __init__(self) -> None:
        self.base_url = get_amperity_url()
        self.nonce: str | None = None
        self.token: str | None = None
        self.state = "pending"
        self.auth_thread: threading.Thread | None = None

    def start_auth(self) -> tuple[bool, str]:
        """Start the authentication process."""
        try:
            resp = requests.post(f"https://{self.base_url}/api/auth/start", timeout=30)
            if resp.status_code != 200:
                return False, f"Failed to start auth: {resp.status_code} - {resp.text}"
            auth_data = resp.json()
            self.nonce = auth_data.get("nonce")
            if not self.nonce:
                return False, "Failed to get nonce from Amperity auth server"

            console = Console()
            console.print(
                f"Amperity Nonce: [{SUCCESS_STYLE}]{self.nonce}[/{SUCCESS_STYLE}]"
            )
            console.print(
                f"[{INFO_STYLE}]Press any key to open the login page in your browser...[/{INFO_STYLE}]"
            )
            readchar.readchar()
            login_url = f"https://{self.base_url}/login?nonce={self.nonce}"
            try:
                webbrowser.open(login_url)
            except Exception as e:  # pragma: no cover - cannot trigger in tests
                logging.error("Failed to open browser: %s", e)
                return False, f"Failed to open browser: {e}"

            self.auth_thread = threading.Thread(
                target=self._poll_auth_state, daemon=True
            )
            self.auth_thread.start()
            return True, "Authentication started. Please log in via the browser."
        except Exception as e:  # pragma: no cover - network issues
            logging.error("Auth start error: %s", e)
            return False, f"Authentication error: {e}"

    def _poll_auth_state(self) -> None:
        """Poll the auth state endpoint until authentication is complete."""
        while self.state not in {"success", "error"}:
            try:
                state_url = f"https://{self.base_url}/api/auth/state/{self.nonce}"
                resp = requests.get(state_url)
                if resp.status_code == 200:
                    state_data = resp.json()
                    self.state = state_data.get("state", "unknown")
                    if self.state == "success":
                        self.token = state_data.get("token")
                        if self.token:
                            set_amperity_token(self.token)
                        break
                    if self.state == "error":
                        logging.error("Authentication failed")
                        break
                elif 400 <= resp.status_code < 500:
                    logging.error(
                        "Authentication state polling received %s", resp.status_code
                    )
                    self.state = "error"
                    break
            except Exception as e:  # pragma: no cover - network issues
                logging.error("Error polling auth state: %s", e)
                self.state = "error"
                break
            time.sleep(2)

    def get_auth_status(self) -> dict:
        """Return the current authentication status."""
        return {"state": self.state, "nonce": self.nonce, "has_token": bool(self.token)}

    def wait_for_auth_completion(
        self, poll_interval: int = 1, timeout: int = None
    ) -> tuple[bool, str]:
        """Wait for authentication to complete in a blocking manner."""
        if not self.nonce:
            return False, "Authentication not started"
        console = Console()
        console.print(
            f"[{INFO_STYLE}]Waiting for authentication to complete...[/{INFO_STYLE}]"
        )
        console.print("[dim]Press Ctrl+C to cancel[/dim]")

        elapsed = 0
        try:
            while True:
                status = self.get_auth_status()
                if status["state"] == "success":
                    print("\n")
                    return True, "Authentication completed successfully."
                if status["state"] in {"error", "timeout"}:
                    print("\n")
                    return False, f"Authentication failed: {status['state']}"

                import sys

                # Show elapsed time and helpful message after 30 seconds
                if elapsed > 30:
                    sys.stdout.write(
                        f"\r[{elapsed}s] Still waiting... Please complete authentication in your browser"
                    )
                else:
                    sys.stdout.write(f"\r[{elapsed}s] Waiting for authentication...")
                sys.stdout.flush()

                time.sleep(poll_interval)
                elapsed += poll_interval

        except KeyboardInterrupt:
            print("\n")
            return False, "Authentication cancelled by user"

    def submit_metrics(self, payload: dict, token: str) -> bool:
        """Send usage metrics to the Amperity API.

        Args:
            payload: The data payload to send
            token: The authentication token

        Returns:
            bool: True if metrics were sent successfully, False otherwise.
        """
        try:
            url = f"https://{self.base_url}/api/usage"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            }

            response = requests.post(
                url,
                headers=headers,
                data=json.dumps(payload),
                timeout=10,  # 10 seconds timeout
            )

            if response.status_code == 200 or response.status_code == 201:
                logging.debug(f"Metrics sent successfully: {response.status_code}")
                return True
            else:
                logging.debug(
                    f"Failed to send metrics: {response.status_code} - {response.text}"
                )
                return False

        except Exception as e:
            logging.error(f"Error sending metrics: {e}", exc_info=True)
            return False

    def submit_bug_report(self, payload: dict, token: str) -> tuple[bool, str]:
        """Send a bug report to the Amperity API.

        Args:
            payload: The bug report payload to send.
            token: The authentication token.

        Returns:
            tuple[bool, str]: Success flag and response message.
        """
        try:
            url = f"https://{self.base_url}/api/usage"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            }

            response = requests.post(
                url,
                headers=headers,
                data=json.dumps(payload),
                timeout=10,
            )

            if response.status_code in (200, 201, 202, 204):
                logging.debug(
                    f"Bug report submitted successfully: {response.status_code}"
                )
                return True, "Bug report submitted successfully"

            logging.debug(
                f"Failed to submit bug report: {response.status_code} - {response.text}"
            )
            return False, f"Failed to submit bug report: {response.status_code}"

        except Exception as e:  # pragma: no cover - network issues
            logging.error(f"Error submitting bug report: {e}", exc_info=True)
            return False, str(e)
