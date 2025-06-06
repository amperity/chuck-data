"""Utilities for handling Databricks workspace URLs."""

from __future__ import annotations

from typing import Optional, Tuple
from urllib.parse import urlparse

# Mapping of cloud provider to base domain
DATABRICKS_DOMAIN_MAP = {
    "AWS": "cloud.databricks.com",
    "Azure": "azuredatabricks.net",
    "GCP": "gcp.databricks.com",
    "Generic": "databricks.com",
}

# Reverse map of domain to provider for validation/detection
DATABRICKS_DOMAINS = {v: k for k, v in DATABRICKS_DOMAIN_MAP.items()}

# URL validation regex patterns
# Databricks workspace IDs are typically numeric or alphanumeric
# Common formats: long numbers, or letters followed by numbers/hyphens
WORKSPACE_ID_PATTERN = r"^([0-9]{10,}|[a-z0-9][a-z0-9\-]*[a-z0-9]|[a-z0-9]{3,})$"


def normalize_workspace_url(url: str) -> str:
    """Return just the workspace identifier portion of a URL."""
    if not url:
        return ""

    to_parse = url if "://" in url else f"https://{url}"
    parsed = urlparse(to_parse)
    host = parsed.hostname or ""

    for domain in DATABRICKS_DOMAIN_MAP.values():
        if host.endswith(domain):
            host = host[: -(len(domain) + 1)]
            break

    return host


def validate_workspace_url(url: str) -> Tuple[bool, Optional[str]]:
    """Validate that ``url`` is a plausible Databricks workspace URL."""
    if not url:
        return False, "Workspace URL cannot be empty"

    if not isinstance(url, str):
        return False, "Workspace URL must be a string"

    # Basic validation - just check it's reasonable input, let API calls handle validity
    url_clean = url.strip()

    # Should be reasonable length
    if len(url_clean) < 1 or len(url_clean) > 200:
        return False, "Workspace URL should be between 1-200 characters"

    # No whitespace allowed
    if " " in url_clean:
        return False, "Workspace URL cannot contain spaces"

    return True, None


def get_full_workspace_url(workspace_id: str, cloud_provider: str = "AWS") -> str:
    """Return the full workspace URL for ``workspace_id``."""
    domain = DATABRICKS_DOMAIN_MAP.get(cloud_provider, DATABRICKS_DOMAIN_MAP["AWS"])
    return f"https://{workspace_id}.{domain}"


def detect_cloud_provider(url: str) -> str:
    """Infer the cloud provider from ``url``."""
    if not url:
        return "AWS"  # Default to AWS if no URL provided

    for domain, provider in DATABRICKS_DOMAINS.items():
        if domain in url:
            return provider
    return "AWS"


def format_workspace_url_for_display(
    workspace_id: str, cloud_provider: str = "AWS"
) -> str:
    """Format a workspace URL for display to users."""
    return get_full_workspace_url(workspace_id, cloud_provider)
