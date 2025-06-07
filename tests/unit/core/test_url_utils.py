"""Tests for the url_utils module."""

from chuck_data.databricks.url_utils import (
    normalize_workspace_url,
    detect_cloud_provider,
    get_full_workspace_url,
    validate_workspace_url,
    DATABRICKS_DOMAIN_MAP,
)


def test_normalize_workspace_url():
    """Test URL normalization function."""
    test_cases = [
        # Basic cases
        ("workspace", "workspace"),
        ("https://workspace", "workspace"),
        ("http://workspace", "workspace"),
        # AWS cases
        ("workspace.cloud.databricks.com", "workspace"),
        ("https://workspace.cloud.databricks.com", "workspace"),
        ("dbc-12345-ab.cloud.databricks.com", "dbc-12345-ab"),
        # Azure cases - the problematic one from the issue
        ("adb-3856707039489412.12.azuredatabricks.net", "adb-3856707039489412.12"),
        (
            "https://adb-3856707039489412.12.azuredatabricks.net",
            "adb-3856707039489412.12",
        ),
        # Another Azure case from user error
        (
            "https://adb-8924977320831502.2.azuredatabricks.net",
            "adb-8924977320831502.2",
        ),
        ("workspace.azuredatabricks.net", "workspace"),
        ("https://workspace.azuredatabricks.net", "workspace"),
        # GCP cases
        ("workspace.gcp.databricks.com", "workspace"),
        ("https://workspace.gcp.databricks.com", "workspace"),
        # Generic cases
        ("workspace.databricks.com", "workspace"),
        ("https://workspace.databricks.com", "workspace"),
    ]

    for input_url, expected_url in test_cases:
        result = normalize_workspace_url(input_url)
        assert result == expected_url, f"Failed for input: {input_url}"


def test_detect_cloud_provider():
    """Test cloud provider detection."""
    test_cases = [
        # AWS cases
        ("workspace.cloud.databricks.com", "AWS"),
        ("https://workspace.cloud.databricks.com", "AWS"),
        ("dbc-12345-ab.cloud.databricks.com", "AWS"),
        # Azure cases
        ("adb-3856707039489412.12.azuredatabricks.net", "Azure"),
        ("https://adb-3856707039489412.12.azuredatabricks.net", "Azure"),
        ("workspace.azuredatabricks.net", "Azure"),
        # GCP cases
        ("workspace.gcp.databricks.com", "GCP"),
        ("https://workspace.gcp.databricks.com", "GCP"),
        # Generic cases
        ("workspace.databricks.com", "Generic"),
        ("https://workspace.databricks.com", "Generic"),
        # Default to AWS for unknown
        ("some-workspace", "AWS"),
        ("unknown.domain.com", "AWS"),
    ]

    for input_url, expected_provider in test_cases:
        result = detect_cloud_provider(input_url)
        assert result == expected_provider, f"Failed for input: {input_url}"


def test_get_full_workspace_url():
    """Test full workspace URL generation."""
    test_cases = [
        ("workspace", "AWS", "https://workspace.cloud.databricks.com"),
        ("workspace", "Azure", "https://workspace.azuredatabricks.net"),
        ("workspace", "GCP", "https://workspace.gcp.databricks.com"),
        ("workspace", "Generic", "https://workspace.databricks.com"),
        ("adb-123456789", "Azure", "https://adb-123456789.azuredatabricks.net"),
        # Default to AWS for unknown provider
        ("workspace", "Unknown", "https://workspace.cloud.databricks.com"),
    ]

    for workspace_id, cloud_provider, expected_url in test_cases:
        result = get_full_workspace_url(workspace_id, cloud_provider)
        assert result == expected_url, f"Failed for {workspace_id}/{cloud_provider}"


def test_validate_workspace_url():
    """Test workspace URL validation."""
    # Valid cases
    valid_cases = [
        "workspace",
        "dbc-12345-ab",
        "adb-123456789",
        "workspace.cloud.databricks.com",
        "workspace.azuredatabricks.net",
        "workspace.gcp.databricks.com",
        "https://workspace.cloud.databricks.com",
        "https://workspace.azuredatabricks.net",
    ]

    for url in valid_cases:
        is_valid, error_msg = validate_workspace_url(url)
        assert is_valid, f"URL should be valid: {url}, error: {error_msg}"
        assert error_msg is None

    # Invalid cases
    invalid_cases = [
        ("", "Workspace URL cannot be empty"),
        (None, "Workspace URL cannot be empty"),
        (123, "Workspace URL must be a string"),
    ]

    for url, expected_error_fragment in invalid_cases:
        is_valid, error_msg = validate_workspace_url(url)
        assert not is_valid, f"URL should be invalid: {url}"
        assert error_msg is not None
        if expected_error_fragment:
            assert expected_error_fragment in error_msg


def test_domain_map_consistency():
    """Ensure the shared domain map is used for URL generation."""
    for provider, domain in DATABRICKS_DOMAIN_MAP.items():
        full_url = get_full_workspace_url("myws", provider)
        assert full_url == f"https://myws.{domain}"
