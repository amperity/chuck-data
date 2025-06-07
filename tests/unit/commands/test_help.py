"""
Tests for help command handler.

Following approved testing patterns:
- Use real internal business logic (get_user_commands, format_help_text)
- No external boundaries to mock in this simple command
- Test end-to-end help command behavior
"""

from chuck_data.commands.help import handle_command


def test_help_command_success_real_logic():
    """Test successful help command execution with real logic."""
    # Test real help command with no mocking - it should work end-to-end
    result = handle_command(None)

    # Verify real command execution
    assert result.success
    assert result.data is not None
    assert "help_text" in result.data
    assert isinstance(result.data["help_text"], str)
    assert len(result.data["help_text"]) > 0

    # Real help text should contain expected command information
    help_text = result.data["help_text"]
    assert "Commands" in help_text or "help" in help_text.lower()


def test_help_command_with_client_real_logic(databricks_client_stub):
    """Test help command with client provided (should work the same)."""
    # Help command doesn't use the client, should work the same
    result = handle_command(databricks_client_stub)

    # Should succeed with real logic regardless of client
    assert result.success
    assert result.data is not None
    assert "help_text" in result.data
    assert isinstance(result.data["help_text"], str)
    assert len(result.data["help_text"]) > 0


def test_help_command_content_real_logic():
    """Test that help command returns real content from the command registry."""
    result = handle_command(None)

    assert result.success
    help_text = result.data["help_text"]

    # Real help should contain information about actual commands
    # These are commands we know exist in the system
    expected_content_indicators = [
        "help",  # Help command itself
        "status",  # Status command
        "Commands",  # Section header
        "/",  # TUI command indicators
    ]

    # At least some of these should be present in real help text
    found_indicators = [
        indicator
        for indicator in expected_content_indicators
        if indicator.lower() in help_text.lower()
    ]

    assert (
        len(found_indicators) > 0
    ), f"Expected to find command indicators in help text: {help_text[:200]}..."


def test_help_command_real_formatting():
    """Test that help command uses real formatting logic."""
    result = handle_command(None)

    assert result.success
    help_text = result.data["help_text"]

    # Real formatting should produce structured text
    assert isinstance(help_text, str)
    assert len(help_text.strip()) > 10  # Should be substantial content

    # Real help formatting should include some structure
    # (exact structure depends on implementation, but should be non-trivial)
    lines = help_text.split("\n")
    assert len(lines) > 1, "Help text should be multi-line"


def test_help_command_idempotent_real_logic():
    """Test that help command produces consistent results."""
    # Call multiple times and verify consistency
    result1 = handle_command(None)
    result2 = handle_command(None)

    assert result1.success
    assert result2.success

    # Real logic should produce identical results
    assert result1.data["help_text"] == result2.data["help_text"]


def test_help_command_no_side_effects_real_logic():
    """Test that help command has no side effects with real logic."""
    # Store initial state (this is a read-only command)
    result_before = handle_command(None)

    # Call help command
    result = handle_command(None)

    # Call again to verify no state changes
    result_after = handle_command(None)

    # All should succeed and produce identical results
    assert result_before.success
    assert result.success
    assert result_after.success

    assert result_before.data["help_text"] == result_after.data["help_text"]
