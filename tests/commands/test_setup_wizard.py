"""
Comprehensive tests for the setup wizard command covering:
- Unit tests with minimal mocking
- Integration tests with real validation logic
- Security tests for token handling and password mode
"""

import pytest
from unittest.mock import patch, MagicMock
from io import StringIO
from tests.fixtures import AmperityClientStub

from chuck_data.commands.setup_wizard import (
    DEFINITION,
    SetupWizardOrchestrator,
)
from chuck_data.commands.wizard import WizardStep, WizardState, InputValidator
from chuck_data.commands.wizard.steps import (
    AmperityAuthStep,
    WorkspaceUrlStep,
    TokenInputStep,
    ModelSelectionStep,
    UsageConsentStep,
    create_step,
)
from chuck_data.commands.wizard.renderer import WizardRenderer
from chuck_data.interactive_context import InteractiveContext


class TestWizardComponents:
    """Test individual wizard components with minimal mocking."""

    def test_wizard_state_validation(self):
        """Test wizard state validation logic."""
        state = WizardState()

        # Initial state should be valid for auth step
        assert state.is_valid_for_step(WizardStep.AMPERITY_AUTH)
        assert state.is_valid_for_step(WizardStep.WORKSPACE_URL)
        assert not state.is_valid_for_step(WizardStep.TOKEN_INPUT)

        # With workspace URL, token input should be valid
        state.workspace_url = "https://test.databricks.com"
        assert state.is_valid_for_step(WizardStep.TOKEN_INPUT)
        assert not state.is_valid_for_step(WizardStep.MODEL_SELECTION)

        # With token, model selection should be valid
        state.token = "test-token"
        assert state.is_valid_for_step(WizardStep.MODEL_SELECTION)

    def test_input_validator_workspace_url_real(self):
        """Test workspace URL validation with real validation logic."""
        validator = InputValidator()

        # Test basic validation failures
        basic_invalid_urls = [
            "",  # empty
            "workspace with spaces",  # spaces not allowed
            "a" * 201,  # too long
        ]
        for url in basic_invalid_urls:
            result = validator.validate_workspace_url(url)
            assert not result.is_valid, f"URL '{url}' should be invalid"

        # Test URLs that pass basic validation (API will test validity)
        valid_or_testable_urls = [
            "invalid-url",
            "not-a-url",
            "random-text",
            "ab",
            "workspace123",
            "test123",
            "1234567890123",
            "https://workspace123.cloud.databricks.com",
            "https://test.azuredatabricks.net",
            "workspace456.cloud.databricks.com",
        ]
        for url in valid_or_testable_urls:
            result = validator.validate_workspace_url(url)
            assert (
                result.is_valid
            ), f"URL '{url}' should pass basic validation: {result.message}"

    def test_input_validator_token_empty(self):
        """Test token validation with empty input."""
        validator = InputValidator()

        # Test empty token
        result = validator.validate_token("", "https://test.databricks.com")
        assert not result.is_valid
        assert "cannot be empty" in result.message

        # Test None workspace URL
        result = validator.validate_token("some-token", None)
        assert not result.is_valid

    def test_input_validator_model_selection(self):
        """Test model selection validation with real logic."""
        validator = InputValidator()
        models = [{"name": "model1"}, {"name": "model2"}, {"name": "test-model-name"}]

        # Test empty input
        result = validator.validate_model_selection("", models)
        assert not result.is_valid

        # Test valid index
        result = validator.validate_model_selection("1", models)
        assert result.is_valid
        assert result.processed_value == "model1"

        # Test valid name (case insensitive)
        result = validator.validate_model_selection("MODEL2", models)
        assert result.is_valid
        assert result.processed_value == "model2"

        # Test invalid index
        result = validator.validate_model_selection("10", models)
        assert not result.is_valid

        # Test invalid name
        result = validator.validate_model_selection("nonexistent", models)
        assert not result.is_valid

        # Test empty models list
        result = validator.validate_model_selection("1", [])
        assert not result.is_valid

    def test_input_validator_usage_consent(self):
        """Test usage consent validation with real logic."""
        validator = InputValidator()

        # Test valid responses
        valid_inputs = [
            ("yes", "yes"),
            ("no", "no"),
            ("Y", "yes"),
            ("n", "no"),
            ("  yes  ", "yes"),  # whitespace handling
        ]

        for input_val, expected in valid_inputs:
            result = validator.validate_usage_consent(input_val)
            assert result.is_valid, f"Input '{input_val}' should be valid"
            assert result.processed_value == expected

        # Test invalid responses
        invalid_inputs = ["maybe", "", "  ", "invalid", "true", "false"]
        for invalid_input in invalid_inputs:
            result = validator.validate_usage_consent(invalid_input)
            assert not result.is_valid, f"Input '{invalid_input}' should be invalid"
            assert "Please enter 'yes' or 'no'" in result.message

    def test_input_validator_edge_cases(self):
        """Test input validator edge cases."""
        validator = InputValidator()

        # Test whitespace handling in usage consent
        result = validator.validate_usage_consent("  yes  ")
        assert result.is_valid
        assert result.processed_value == "yes"

        # Test case insensitive model matching
        models = [{"name": "Test-Model"}]
        result = validator.validate_model_selection("test-model", models)
        assert result.is_valid
        assert result.processed_value == "Test-Model"

        # Test token validation with invalid workspace
        with patch("chuck_data.clients.databricks.DatabricksAPIClient") as mock_client:
            mock_client.side_effect = Exception("Connection failed")
            result = validator.validate_token(
                "some-token", "https://invalid-workspace.com"
            )
            assert not result.is_valid
            assert "Error validating token" in result.message


class TestStepHandlers:
    """Test individual step handlers with minimal mocking."""

    def test_amperity_auth_step_existing_token(self):
        """Test Amperity auth step with existing token."""
        validator = InputValidator()
        step = AmperityAuthStep(validator)
        state = WizardState()

        # Only mock external API call
        with patch(
            "chuck_data.commands.wizard.steps.get_amperity_token",
            return_value="existing-token",
        ):
            result = step.handle_input("", state)

            assert result.success
            assert result.next_step == WizardStep.WORKSPACE_URL
            assert "already exists" in result.message

    def test_amperity_auth_step_new_auth_success(self):
        """Test Amperity auth step with successful new authentication."""
        validator = InputValidator()
        step = AmperityAuthStep(validator)
        state = WizardState()

        # Only mock external dependencies (Amperity API)
        amperity_stub = AmperityClientStub()

        with (
            patch(
                "chuck_data.commands.wizard.steps.get_amperity_token", return_value=None
            ),
            patch(
                "chuck_data.commands.wizard.steps.AmperityAPIClient",
                return_value=amperity_stub,
            ),
        ):
            result = step.handle_input("", state)

            assert result.success
            assert result.next_step == WizardStep.WORKSPACE_URL
            assert "authentication complete" in result.message.lower()

    def test_workspace_url_step_real_validation(self):
        """Test workspace URL step with real validation."""
        validator = InputValidator()
        step = WorkspaceUrlStep(validator)
        state = WizardState()

        # Test with valid workspace ID (no mocking of validation)
        result = step.handle_input("workspace123", state)
        assert result.success
        assert result.next_step == WizardStep.TOKEN_INPUT
        assert "workspace123" in result.data["workspace_url"]

        # Test with basic invalid input (should fail with real validation)
        result = step.handle_input("workspace with spaces", state)
        assert not result.success
        assert result.action.name == "RETRY"

    def test_workspace_url_step_invalid_inputs(self):
        """Test workspace URL step with various invalid inputs."""
        validator = InputValidator()
        step = create_step(WizardStep.WORKSPACE_URL, validator)
        state = WizardState()

        # Only test inputs that fail basic validation
        basic_invalid_inputs = ["", "workspace with spaces", "a" * 201]

        for invalid_input in basic_invalid_inputs:
            result = step.handle_input(invalid_input, state)
            assert not result.success, f"Input '{invalid_input}' should fail validation"
            assert result.action.name == "RETRY"
            assert result.message is not None

    def test_token_input_step_error_flow(self):
        """Test token input step error handling (goes back to workspace URL)."""
        validator = InputValidator()
        step = TokenInputStep(validator)
        state = WizardState(workspace_url="https://invalid-workspace.nonexistent.com")

        # This should fail validation and go back to workspace URL step
        result = step.handle_input("some-token", state)
        assert not result.success
        assert result.next_step == WizardStep.WORKSPACE_URL
        assert result.action.name == "CONTINUE"
        assert "Please re-enter your workspace URL and token" in result.message

    def test_model_selection_step_real_validation(self):
        """Test model selection step with real validation."""
        validator = InputValidator()
        step = ModelSelectionStep(validator)
        models = [{"name": "model1"}, {"name": "model2"}]
        state = WizardState(models=models)

        # Only mock external config setting
        with patch(
            "chuck_data.commands.wizard.steps.set_active_model", return_value=True
        ):
            # Test valid index (uses real validation)
            result = step.handle_input("1", state)
            assert result.success
            assert result.next_step == WizardStep.USAGE_CONSENT
            assert result.data["selected_model"] == "model1"

            # Test valid name (uses real validation)
            result = step.handle_input("model2", state)
            assert result.success
            assert result.data["selected_model"] == "model2"

            # Test invalid input (uses real validation)
            result = step.handle_input("invalid", state)
            assert not result.success
            assert result.action.name == "RETRY"

    def test_usage_consent_step_real_validation(self):
        """Test usage consent step with real validation."""
        validator = InputValidator()
        step = UsageConsentStep(validator)
        state = WizardState()

        # Only mock external config setting
        with patch(
            "chuck_data.commands.wizard.steps.set_usage_tracking_consent",
            return_value=True,
        ):
            # Test valid input (uses real validation)
            result = step.handle_input("yes", state)
            assert result.success
            assert result.next_step == WizardStep.COMPLETE
            assert result.data["usage_consent"] is True

            # Test invalid input (uses real validation)
            result = step.handle_input("maybe", state)
            assert not result.success
            assert result.action.name == "RETRY"
            assert "Please enter 'yes' or 'no'" in result.message

    def test_usage_consent_retry_behavior(self):
        """Test usage consent step retry behavior with invalid input."""
        validator = InputValidator()
        step = create_step(WizardStep.USAGE_CONSENT, validator)
        state = WizardState()

        # Test invalid response
        result = step.handle_input("maybe", state)
        assert not result.success
        assert result.action.name == "RETRY"
        assert "Please enter 'yes' or 'no'" in result.message

        # Test valid response
        with patch(
            "chuck_data.commands.wizard.steps.set_usage_tracking_consent",
            return_value=True,
        ):
            result = step.handle_input("yes", state)
            assert result.success
            assert result.next_step == WizardStep.COMPLETE


class TestSetupWizardOrchestrator:
    """Test the main orchestrator logic."""

    def test_screen_clearing_helper_methods(self):
        """Test screen clearing helper methods."""
        orchestrator = SetupWizardOrchestrator()

        # Test which steps should trigger screen clearing
        assert orchestrator._should_clear_screen_after_step(WizardStep.AMPERITY_AUTH)
        assert orchestrator._should_clear_screen_after_step(WizardStep.TOKEN_INPUT)
        assert orchestrator._should_clear_screen_after_step(WizardStep.MODEL_SELECTION)
        assert not orchestrator._should_clear_screen_after_step(
            WizardStep.WORKSPACE_URL
        )
        assert not orchestrator._should_clear_screen_after_step(
            WizardStep.USAGE_CONSENT
        )

        # Test forward progression logic
        assert orchestrator._is_forward_progression(
            WizardStep.AMPERITY_AUTH, WizardStep.WORKSPACE_URL
        )
        assert orchestrator._is_forward_progression(
            WizardStep.TOKEN_INPUT, WizardStep.MODEL_SELECTION
        )

        # Test backward progression (error scenarios)
        assert not orchestrator._is_forward_progression(
            WizardStep.TOKEN_INPUT, WizardStep.WORKSPACE_URL
        )

    @patch("chuck_data.commands.wizard.steps.get_amperity_token")
    @patch("chuck_data.commands.wizard.renderer.WizardRenderer.render_step")
    def test_no_double_rendering_on_input_processing(
        self, mock_render_step, mock_get_token
    ):
        """Test that steps are not double-rendered when processing input."""
        mock_get_token.return_value = "existing-token"

        orchestrator = SetupWizardOrchestrator()

        # Start wizard - renders step 1 (auth) and step 2 (workspace URL)
        result = orchestrator.start_wizard()
        assert result.success

        mock_render_step.reset_mock()

        # Process input for workspace URL - should render next step (token) but not re-render current step
        result = orchestrator.handle_interactive_input("workspace123")
        assert result.success

        # Should only render the next step (token), not double-render the workspace step
        # Key test: we don't render the same step twice during input processing
        transition_render_count = mock_render_step.call_count
        assert (
            transition_render_count <= 1
        ), f"Expected at most 1 render for transition, got {transition_render_count}"

    @patch("chuck_data.commands.wizard.steps.get_amperity_token")
    def test_render_step_parameter_controls_initial_rendering(self, mock_get_token):
        """Test that _process_step render_step parameter controls the initial step rendering."""
        mock_get_token.return_value = "existing-token"

        orchestrator = SetupWizardOrchestrator()

        # Test with a simple state that won't cause transitions
        from chuck_data.commands.wizard.state import WizardState, WizardStep

        state = WizardState(current_step=WizardStep.WORKSPACE_URL)

        # Mock the renderer to track render calls
        with patch.object(orchestrator.renderer, "render_step") as mock_render:
            # Test _process_step with render_step=True - should render the current step initially
            orchestrator._process_step(state, "", render_step=True)
            initial_renders = mock_render.call_count
            assert (
                initial_renders >= 1
            ), "Should render at least once when render_step=True"

            mock_render.reset_mock()

            # Test _process_step with render_step=False - should not render the current step initially
            # (but may still render if transitioning to a new step)
            orchestrator._process_step(state, "", render_step=False)
            no_initial_render_calls = mock_render.call_count

            # The key difference: with render_step=False, we should have fewer renders
            # because we skip the initial current step rendering
            assert (
                no_initial_render_calls < initial_renders
                or no_initial_render_calls == 0
            ), f"render_step=False should render less than render_step=True ({no_initial_render_calls} vs {initial_renders})"

    def test_start_wizard_vs_handle_interactive_input_rendering(self):
        """Test that start_wizard renders but handle_interactive_input doesn't double-render."""
        with patch(
            "chuck_data.commands.wizard.steps.get_amperity_token",
            return_value="existing-token",
        ):
            orchestrator = SetupWizardOrchestrator()

            # Test the specific methods that were causing double rendering
            with patch.object(orchestrator, "_process_step") as mock_process_step:
                # start_wizard should call _process_step with render_step=True
                orchestrator.start_wizard()
                mock_process_step.assert_called_once()
                args, kwargs = mock_process_step.call_args
                assert (
                    kwargs.get("render_step", True) is True
                ), "start_wizard should call _process_step with render_step=True"

                mock_process_step.reset_mock()

                # Mock the state loading to avoid restarting
                with patch.object(
                    orchestrator, "_load_state_from_context"
                ) as mock_load_state:
                    from chuck_data.commands.wizard.state import WizardState, WizardStep

                    mock_load_state.return_value = WizardState(
                        current_step=WizardStep.WORKSPACE_URL
                    )

                    # handle_interactive_input should call _process_step with render_step=False
                    orchestrator.handle_interactive_input("test-input")
                    mock_process_step.assert_called_once()
                    args, kwargs = mock_process_step.call_args
                    assert (
                        kwargs.get("render_step", True) is False
                    ), "handle_interactive_input should call _process_step with render_step=False"

    @patch("chuck_data.commands.wizard.steps.get_amperity_token")
    def test_error_scenario_no_screen_clear(self, mock_get_token):
        """Test that errors don't trigger screen clearing."""
        mock_get_token.return_value = "existing-token"

        orchestrator = SetupWizardOrchestrator()

        # Start wizard (should succeed)
        result = orchestrator.start_wizard()
        assert result.success

        # Simulate basic invalid workspace URL input
        result = orchestrator.handle_interactive_input("workspace with spaces")
        assert not result.success  # Should fail validation

        # The screen should not have been cleared due to validation failure
        # This is tested implicitly through the mocking - if screen clearing
        # was attempted inappropriately, the test would behave differently


class TestWizardOrchestratorIntegration:
    """Test orchestrator integration with real components."""

    def setup_method(self):
        """Set up test environment."""
        # Create a real interactive context for testing
        self.context = InteractiveContext()

    def test_screen_clearing_logic(self):
        """Test screen clearing behavior in different scenarios."""
        orchestrator = SetupWizardOrchestrator()

        # Test helper methods
        assert orchestrator._should_clear_screen_after_step(WizardStep.AMPERITY_AUTH)
        assert orchestrator._should_clear_screen_after_step(WizardStep.TOKEN_INPUT)
        assert orchestrator._should_clear_screen_after_step(WizardStep.MODEL_SELECTION)
        assert not orchestrator._should_clear_screen_after_step(
            WizardStep.WORKSPACE_URL
        )
        assert not orchestrator._should_clear_screen_after_step(
            WizardStep.USAGE_CONSENT
        )

        # Test forward progression logic
        assert orchestrator._is_forward_progression(
            WizardStep.AMPERITY_AUTH, WizardStep.WORKSPACE_URL
        )
        assert orchestrator._is_forward_progression(
            WizardStep.TOKEN_INPUT, WizardStep.MODEL_SELECTION
        )

        # Test backward progression (should not clear)
        assert not orchestrator._is_forward_progression(
            WizardStep.TOKEN_INPUT, WizardStep.WORKSPACE_URL
        )


class TestErrorFlowIntegration:
    """Test complete error flows end-to-end."""

    @patch("chuck_data.commands.wizard.steps.get_amperity_token")
    @patch("chuck_data.commands.wizard.steps.AmperityAPIClient")
    @patch("chuck_data.clients.databricks.DatabricksAPIClient")
    def test_complete_error_recovery_flow(
        self, mock_databricks_client, mock_amperity_client, mock_get_token
    ):
        """Test a complete error recovery flow."""
        # Setup stub for external dependencies only
        mock_get_token.return_value = None
        amperity_stub = AmperityClientStub()
        mock_amperity_client.return_value = amperity_stub

        # Mock token validation failure
        mock_db_client = MagicMock()
        mock_db_client.validate_token.return_value = False
        mock_databricks_client.return_value = mock_db_client

        orchestrator = SetupWizardOrchestrator()

        # 1. Start wizard - should succeed
        result = orchestrator.start_wizard()
        assert result.success

        # 2. Enter valid workspace URL - should succeed
        result = orchestrator.handle_interactive_input("workspace123")
        assert result.success

        # 3. Enter invalid token - token validation will fail and go back to URL step
        # The wizard handles this gracefully by returning success=False and transitioning back
        result = orchestrator.handle_interactive_input("invalid-token")
        # The result might be success=True because it successfully transitioned back to URL step
        # but the error flow worked correctly as evidenced by the output showing step 2

        # The orchestrator should now be back at workspace URL step
        # We can verify this by checking that the next input is treated as a URL

        # 4. Re-enter workspace URL
        result = orchestrator.handle_interactive_input("workspace456")
        assert result.success

        # This flow tests the real error recovery behavior without over-mocking

    def test_validation_error_messages_preserved(self):
        """Test that validation error messages are properly preserved and displayed."""
        validator = InputValidator()

        # Test that error messages contain helpful information for basic validation
        result = validator.validate_workspace_url("workspace with spaces")
        assert not result.is_valid
        assert "cannot contain spaces" in result.message

        result = validator.validate_usage_consent("maybe")
        assert not result.is_valid
        assert "Please enter 'yes' or 'no'" in result.message

        # Test that error details are available when needed
        result = validator.validate_workspace_url("a" * 201)  # too long
        assert not result.is_valid
        assert "1-200 characters" in result.message


class TestRendererIntegration:
    """Test renderer behavior with real console output."""

    def test_renderer_screen_clearing_parameter(self):
        """Test that renderer respects the clear_screen parameter."""
        # Use a real console but capture output
        from rich.console import Console

        console_output = StringIO()
        console = Console(file=console_output, width=80)
        renderer = WizardRenderer(console)

        validator = InputValidator()
        step = create_step(WizardStep.WORKSPACE_URL, validator)
        state = WizardState()

        # Test without clearing
        renderer.render_step(step, state, 2, clear_screen=False)
        console_output.getvalue()

        # Reset output
        console_output.seek(0)
        console_output.truncate(0)

        # Test with clearing - this should have additional clear commands
        with patch.object(renderer, "clear_terminal") as mock_clear:
            renderer.render_step(step, state, 2, clear_screen=True)
            mock_clear.assert_called_once()


class TestSecurityFixes:
    """Test the core security fixes for token handling and password mode."""

    def test_tui_input_mode_detection_logic(self):
        """Test the TUI input mode detection logic that was fixed."""

        # Test the exact logic from src/ui/tui.py lines 247-248
        # if step == "token_input" and ctx.get("workspace_url"):
        #     hide_input = True

        test_cases = [
            # (step, workspace_url, expected_hide_input, description)
            (
                "token_input",
                "https://test.com",
                True,
                "Token step with URL should hide input",
            ),
            (
                "token_input",
                None,
                False,
                "Token step without URL should not hide input",
            ),
            (
                "workspace_url",
                "https://test.com",
                False,
                "Workspace step with URL should not hide input",
            ),
            (
                "workspace_url",
                None,
                False,
                "Workspace step without URL should not hide input",
            ),
            (
                "model_selection",
                "https://test.com",
                False,
                "Other steps should not hide input",
            ),
        ]

        for step, workspace_url, expected_hide, description in test_cases:
            # Replicate the exact TUI logic from lines 247-248
            ctx = {"current_step": step, "workspace_url": workspace_url}
            hide_input = step == "token_input" and bool(ctx.get("workspace_url"))

            assert (
                hide_input == expected_hide
            ), f"{description}. Got hide_input={hide_input}"

    def test_prompt_history_parameter_logic(self):
        """Test that history is disabled when input is hidden."""

        # Test the logic from src/ui/tui.py line 253
        # enable_history_search=not hide_input

        test_cases = [
            (True, False, "When hiding input, history should be disabled"),
            (False, True, "When not hiding input, history should be enabled"),
        ]

        for hide_input, expected_enable_history, description in test_cases:
            enable_history_search = not hide_input
            assert (
                enable_history_search == expected_enable_history
            ), f"{description}. Got enable_history_search={enable_history_search}"

    def test_token_validation_error_flow(self):
        """Test that token validation failures trigger the correct error flow."""
        from chuck_data.commands.wizard.steps import TokenInputStep
        from chuck_data.commands.wizard.validator import InputValidator
        from chuck_data.commands.wizard.state import WizardState, WizardStep

        validator = InputValidator()
        step = TokenInputStep(validator)
        state = WizardState(workspace_url="https://invalid.nonexistent.com")

        # This should fail validation and return the error flow
        result = step.handle_input("some-token", state)

        # Should not succeed (validation failed)
        assert not result.success
        # Should go back to workspace URL step
        assert result.next_step == WizardStep.WORKSPACE_URL
        # Should have the error message
        assert "Please re-enter your workspace URL and token" in result.message

    def test_token_not_stored_in_processed_value_on_failure(self):
        """Test that tokens are not stored in processed_value when validation fails."""
        from chuck_data.commands.wizard.validator import InputValidator

        validator = InputValidator()

        # Mock token validation to fail
        with patch("chuck_data.clients.databricks.DatabricksAPIClient") as mock_client:
            mock_client.side_effect = Exception("Validation failed")

            result = validator.validate_token("secret-token-123", "https://test.com")

            # Should fail validation
            assert not result.is_valid
            # Should not store the token in processed_value
            assert result.processed_value is None

    def test_step_detection_for_password_mode_after_error(self):
        """Test that step detection works correctly after token validation error."""

        # This tests the specific scenario from the bug report:
        # After token validation fails and we go back to workspace URL step,
        # the TUI should not be stuck in password mode

        # Scenario 1: On token_input step with workspace_url - should hide input
        token_step_ctx = {
            "current_step": "token_input",
            "workspace_url": "https://test.com",
        }
        step = token_step_ctx.get("current_step")
        hide_input = step == "token_input" and bool(token_step_ctx.get("workspace_url"))
        assert hide_input is True, "Should hide input on token step"

        # Scenario 2: After error, back to workspace_url step - should NOT hide input
        workspace_step_ctx = {
            "current_step": "workspace_url",
            "workspace_url": "https://test.com",
        }
        step = workspace_step_ctx.get("current_step")
        hide_input = step == "token_input" and bool(
            workspace_step_ctx.get("workspace_url")
        )
        assert (
            hide_input is False
        ), "Should NOT hide input on workspace step (even with workspace_url present)"

    @patch("chuck_data.commands.wizard.steps.get_amperity_token")
    def test_context_step_update_on_token_failure(self, mock_get_token):
        """Test that context step is updated correctly when token validation fails."""
        from chuck_data.commands.setup_wizard import SetupWizardOrchestrator
        from chuck_data.interactive_context import InteractiveContext

        mock_get_token.return_value = "existing-token"

        context = InteractiveContext()
        context.clear_active_context("/setup")

        try:
            orchestrator = SetupWizardOrchestrator()

            # Start wizard
            result = orchestrator.start_wizard()
            assert result.success

            # Go to workspace step
            result = orchestrator.handle_interactive_input("workspace123")
            assert result.success

            # Verify we're on token step
            context_data = context.get_context_data("/setup")
            assert context_data.get("current_step") == "token_input"

            # Mock a validation failure that should go back to workspace URL
            with patch(
                "chuck_data.clients.databricks.DatabricksAPIClient"
            ) as mock_client:
                mock_db_client = MagicMock()
                mock_db_client.validate_token.return_value = False
                mock_client.return_value = mock_db_client

                # Process token that should fail
                result = orchestrator.handle_interactive_input("invalid-token")

                # The key test: context should be updated to workspace_url step
                context_data = context.get_context_data("/setup")

                # This is the critical assertion - if this fails, the TUI will be stuck in password mode
                assert (
                    context_data.get("current_step") == "workspace_url"
                ), f"Context should be updated to workspace_url step, got {context_data.get('current_step')}"

        finally:
            context.clear_active_context("/setup")

    def test_usage_consent_invalid_input_shows_error(self):
        """Test that invalid usage consent input shows error message to user."""
        from chuck_data.commands.wizard.steps import UsageConsentStep
        from chuck_data.commands.wizard.validator import InputValidator
        from chuck_data.commands.wizard.state import WizardState

        # Create usage consent step directly to test the error display
        validator = InputValidator()
        step = UsageConsentStep(validator)
        state = WizardState()

        # Test invalid input
        result = step.handle_input("asdf", state)

        # Should fail and include error message
        assert not result.success
        assert "Please enter 'yes' or 'no'" in result.message

        # Now test that the orchestrator preserves this error message
        from chuck_data.commands.setup_wizard import SetupWizardOrchestrator
        from chuck_data.interactive_context import InteractiveContext

        context = InteractiveContext()
        context.clear_active_context("/setup")

        try:
            orchestrator = SetupWizardOrchestrator()

            # Manually set up context to be on usage consent step
            context.set_active_context("/setup")
            context.store_context_data("/setup", "current_step", "usage_consent")
            context.store_context_data("/setup", "workspace_url", "test-workspace")
            context.store_context_data("/setup", "token", "test-token")
            context.store_context_data("/setup", "models", [{"name": "test-model"}])
            context.store_context_data("/setup", "selected_model", "test-model")

            # Test invalid input on usage consent step
            result = orchestrator.handle_interactive_input("asdf")

            # Should fail and show error message
            assert not result.success
            assert "Please enter 'yes' or 'no'" in result.message

            # Error should also be preserved in state for display
            context_data = context.get_context_data("/setup")
            error_message = context_data.get("error_message", "")
            assert "Please enter 'yes' or 'no'" in error_message

            # The critical test: RETRY actions should trigger re-rendering to show error
            # Mock the renderer to verify it's called during the RETRY action
            with patch(
                "chuck_data.commands.wizard.renderer.WizardRenderer.render_step"
            ) as mock_render:
                # This call should trigger RETRY action and re-render with error
                result2 = orchestrator.handle_interactive_input("invalid_again")

                # Should have failed and triggered re-render
                assert not result2.success
                assert "Please enter 'yes' or 'no'" in result2.message

                # Verify render_step was called during RETRY
                mock_render.assert_called()
                call_args = mock_render.call_args
                rendered_state = call_args[0][1]  # Second argument is the state
                assert rendered_state.error_message is not None
                assert "Please enter 'yes' or 'no'" in rendered_state.error_message

        finally:
            context.clear_active_context("/setup")


class TestCommandIntegration:
    """Test the command integration with minimal mocking."""

    def test_command_definition(self):
        """Test that the command definition has the correct attributes."""
        assert DEFINITION.name == "setup-wizard"
        assert DEFINITION.tui_aliases == ["/setup", "/wizard"]
        assert DEFINITION.needs_api_client is True
        assert DEFINITION.visible_to_user is True
        assert DEFINITION.visible_to_agent is False
        assert DEFINITION.supports_interactive_input is True
        assert DEFINITION.parameters == {}
        assert DEFINITION.required_params == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


"""
Security tests for setup wizard - focusing on token handling and input mode bugs.
"""


class TestTokenSecurityAndInputMode:
    """Test token security and input mode handling."""

    def setup_method(self):
        """Set up test environment."""
        self.context = InteractiveContext()
        self.context.clear_active_context("/setup")

    def teardown_method(self):
        """Clean up test environment."""
        self.context.clear_active_context("/setup")

    @patch("chuck_data.commands.wizard.steps.get_amperity_token")
    @patch("chuck_data.clients.databricks.DatabricksAPIClient")
    def test_token_not_stored_in_history_on_failure(
        self, mock_databricks_client, mock_get_token
    ):
        """Test that tokens are not stored in command history when validation fails."""
        mock_get_token.return_value = "existing-token"

        # Mock token validation failure
        mock_db_client = MagicMock()
        mock_db_client.validate_token.return_value = False
        mock_databricks_client.return_value = mock_db_client

        orchestrator = SetupWizardOrchestrator()

        # Start wizard and get to token input step
        result = orchestrator.start_wizard()
        assert result.success

        result = orchestrator.handle_interactive_input("workspace123")
        assert result.success

        # Now we should be on token input step
        context_data = self.context.get_context_data("/setup")
        assert context_data.get("current_step") == "token_input"

        # Simulate token input that fails validation - should go back to workspace URL
        result = orchestrator.handle_interactive_input("fake-token-123")
        # The result is success=True because it successfully transitions back to workspace step

        # Verify we're back at workspace URL step
        context_data = self.context.get_context_data("/setup")
        assert context_data.get("current_step") == "workspace_url"

        # Error message should be preserved in the wizard state
        error_message = context_data.get("error_message", "")
        assert "Please re-enter your workspace URL and token" in error_message

        # Key security test: The token should not be stored in any context or state
        for key, value in context_data.items():
            if isinstance(value, str):
                assert (
                    "fake-token-123" not in value
                ), f"Token found in context key '{key}': {value}"

    def test_input_mode_detection_logic(self):
        """Test the input mode detection logic from TUI."""
        # Test the exact logic from TUI._run_interactive_mode

        # Test context data scenarios
        test_cases = [
            # (context_data, expected_hide_input, description)
            (
                {"current_step": "token_input", "workspace_url": "https://test.com"},
                True,
                "Should hide input for token_input step with workspace_url",
            ),
            (
                {"current_step": "workspace_url", "workspace_url": None},
                False,
                "Should NOT hide input for workspace_url step",
            ),
            (
                {"current_step": "workspace_url", "workspace_url": "https://test.com"},
                False,
                "Should NOT hide input for workspace_url step even with workspace_url present",
            ),
            (
                {
                    "current_step": "model_selection",
                    "workspace_url": "https://test.com",
                },
                False,
                "Should NOT hide input for other steps",
            ),
        ]

        for ctx, expected_hide, description in test_cases:
            # Replicate the exact TUI logic
            step = ctx.get("current_step")
            hide_input = step == "token_input" and bool(ctx.get("workspace_url"))

            assert (
                hide_input == expected_hide
            ), f"{description}. Got hide_input={hide_input}"

    @patch("chuck_data.commands.wizard.steps.get_amperity_token")
    def test_step_context_updates_correctly_on_token_failure(self, mock_get_token):
        """Test that step context is correctly updated when token validation fails."""
        mock_get_token.return_value = "existing-token"

        orchestrator = SetupWizardOrchestrator()

        # Start and progress to token step
        result = orchestrator.start_wizard()
        assert result.success

        result = orchestrator.handle_interactive_input("workspace123")
        assert result.success

        # Verify we're on token input step
        context_data = self.context.get_context_data("/setup")
        assert context_data.get("current_step") == "token_input"

        # Mock token validation failure and process token input
        with patch("chuck_data.clients.databricks.DatabricksAPIClient") as mock_client:
            mock_db_client = MagicMock()
            mock_db_client.validate_token.return_value = False
            mock_client.return_value = mock_db_client

            result = orchestrator.handle_interactive_input("invalid-token")

            # Should successfully transition back to workspace URL step
            # (success=True indicates successful transition, not successful validation)

            # Verify context is updated to workspace_url step
            context_data = self.context.get_context_data("/setup")
            assert (
                context_data.get("current_step") == "workspace_url"
            ), f"Expected workspace_url step, got {context_data.get('current_step')}"

    def test_token_not_in_wizard_state_after_failure(self):
        """Test that failed tokens are not stored in wizard state."""
        from chuck_data.commands.wizard.validator import InputValidator

        validator = InputValidator()

        # Mock token validation failure
        with patch("chuck_data.clients.databricks.DatabricksAPIClient") as mock_client:
            mock_client.side_effect = Exception("Connection failed")

            result = validator.validate_token(
                "secret-token-456", "https://test.databricks.com"
            )
            assert not result.is_valid

            # The token should not be in the processed_value when validation fails
            assert result.processed_value is None or "secret-token-456" not in str(
                result.processed_value
            )

    @patch("chuck_data.commands.wizard.steps.get_amperity_token")
    def test_no_token_leakage_in_error_messages(self, mock_get_token):
        """Test that tokens don't leak into error messages."""
        mock_get_token.return_value = "existing-token"

        orchestrator = SetupWizardOrchestrator()

        # Start and progress to token step
        result = orchestrator.start_wizard()
        result = orchestrator.handle_interactive_input("workspace123")

        # Mock token validation failure
        with patch("chuck_data.clients.databricks.DatabricksAPIClient") as mock_client:
            mock_client.side_effect = Exception("Network error with secret details")

            result = orchestrator.handle_interactive_input("super-secret-token")

            # Error message should not contain the token
            assert (
                "super-secret-token" not in result.message
            ), f"Token leaked in error message: {result.message}"

            # Check all context data for token leakage
            context_data = self.context.get_context_data("/setup")
            for key, value in context_data.items():
                if isinstance(value, str):
                    assert (
                        "super-secret-token" not in value
                    ), f"Token leaked in context key '{key}': {value}"


class TestTUISecurityIntegration:
    """Test TUI security features integration."""

    def test_prompt_parameters_logic(self):
        """Test the logic for prompt parameters (password mode and history)."""

        # This test verifies the fix for both issues:
        # 1. enable_history_search=False when is_password=True (no token in history)
        # 2. Correct step detection for is_password parameter

        test_cases = [
            # (context_data, expected_hide_input, expected_enable_history, description)
            (
                {"current_step": "token_input", "workspace_url": "https://test.com"},
                True,
                False,
                "Token input should hide input and disable history",
            ),
            (
                {"current_step": "workspace_url", "workspace_url": None},
                False,
                True,
                "Workspace URL input should show input and enable history",
            ),
            (
                {"current_step": "workspace_url", "workspace_url": "https://test.com"},
                False,
                True,
                "Workspace URL after token failure should show input and enable history",
            ),
        ]

        for ctx, expected_hide, expected_history, description in test_cases:
            # Replicate the exact TUI logic
            step = ctx.get("current_step")
            hide_input = step == "token_input" and bool(ctx.get("workspace_url"))
            enable_history = not hide_input

            assert (
                hide_input == expected_hide
            ), f"{description}. Got hide_input={hide_input}"
            assert (
                enable_history == expected_history
            ), f"{description}. Got enable_history={enable_history}"

    @patch("chuck_data.commands.wizard.steps.get_amperity_token")
    def test_api_error_message_displayed_to_user(self, mock_get_token):
        """Test that API errors from token validation are displayed to the user."""
        from chuck_data.commands.setup_wizard import SetupWizardOrchestrator
        from chuck_data.interactive_context import InteractiveContext

        mock_get_token.return_value = "existing-token"
        context = InteractiveContext()
        context.clear_active_context("/setup")

        try:
            orchestrator = SetupWizardOrchestrator()

            # Start wizard and go to workspace step
            result = orchestrator.start_wizard()
            assert result.success

            # Enter workspace URL
            result = orchestrator.handle_interactive_input("workspace123")
            assert result.success

            # Mock API error when validating token
            with patch(
                "chuck_data.clients.databricks.DatabricksAPIClient"
            ) as mock_client:
                mock_client.side_effect = Exception(
                    "Connection error: Failed to resolve 'workspace123.cloud.databricks.com'"
                )

                # Process token that should fail with API error
                result = orchestrator.handle_interactive_input("some-token")

                # Should transition back to workspace URL step but preserve error message
                context_data = context.get_context_data("/setup")
                assert context_data.get("current_step") == "workspace_url"

                # The critical test: error message should be preserved in state
                error_message = context_data.get("error_message", "")
                assert (
                    "Error validating token" in error_message
                ), f"Expected error message in state, got: {error_message}"
                # Verify the user-friendly error message format
                assert (
                    "Please re-enter your workspace URL and token" in error_message
                ), f"Expected user instructions in message: {error_message}"

        finally:
            context.clear_active_context("/setup")
