"""
Tests for the interactive context module.
"""

from chuck_data.interactive_context import InteractiveContext


def test_command_name_normalization():
    """Test that the context normalizes command names by removing leading slashes."""
    context = InteractiveContext()

    # Clean up any existing data for this test
    if "test_cmd" in context._active_contexts:
        del context._active_contexts["test_cmd"]
    if "/test_cmd" in context._active_contexts:
        del context._active_contexts["/test_cmd"]

    # Test set_active_context with slash
    context.set_active_context("/test_cmd")

    # The original command name should be preserved in current_command
    assert context.current_command == "/test_cmd"

    # But internally it should be stored without the slash
    assert "test_cmd" in context._active_contexts
    assert "/test_cmd" not in context._active_contexts

    # Test storing data with slash
    context.store_context_data("/test_cmd", "key1", "value1")

    # Data should be retrievable with or without slash
    assert context.get_context_data("/test_cmd").get("key1") == "value1"
    assert context.get_context_data("test_cmd").get("key1") == "value1"

    # Test storing data without slash
    context.store_context_data("test_cmd", "key2", "value2")

    # Data should be retrievable with or without slash
    assert context.get_context_data("/test_cmd").get("key2") == "value2"
    assert context.get_context_data("test_cmd").get("key2") == "value2"

    # Test clear_active_context with slash
    context.clear_active_context("/test_cmd")

    # Context should be gone
    assert "test_cmd" not in context._active_contexts
    assert context.current_command is None

    # Now test with slash stored first, then access without slash
    context.set_active_context("/another_test")
    context.store_context_data("/another_test", "key", "value")

    # Access with no slash
    assert context.get_context_data("another_test").get("key") == "value"

    # Clear with no slash
    context.clear_active_context("another_test")
    assert "another_test" not in context._active_contexts
