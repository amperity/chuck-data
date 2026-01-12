"""
Unit tests for wizard renderer.
"""

import pytest
from io import StringIO
from rich.console import Console
from chuck_data.commands.wizard.renderer import WizardRenderer
from chuck_data.commands.wizard.state import WizardState, WizardStep


class TestWizardRenderer:
    """Tests for WizardRenderer."""

    @pytest.fixture
    def console(self):
        """Create a test console."""
        return Console(file=StringIO(), force_terminal=True, width=100)

    @pytest.fixture
    def renderer(self, console):
        """Create a test renderer."""
        return WizardRenderer(console)

    def test_get_step_number_from_state(self, renderer):
        """Test that get_step_number reads from state."""
        state = WizardState(step_number=1)
        assert renderer.get_step_number(state) == 1

        state = WizardState(step_number=5)
        assert renderer.get_step_number(state) == 5

        state = WizardState(step_number=15)
        assert renderer.get_step_number(state) == 15

    def test_get_step_number_different_steps_same_number(self, renderer):
        """Test that step number comes from state, not step type."""
        # Different steps can have same number depending on path
        state1 = WizardState(current_step=WizardStep.WORKSPACE_URL, step_number=3)
        assert renderer.get_step_number(state1) == 3

        state2 = WizardState(current_step=WizardStep.AWS_PROFILE_INPUT, step_number=3)
        assert renderer.get_step_number(state2) == 3

    def test_get_step_number_dynamic_numbering(self, renderer):
        """Test that step numbers are dynamic based on state."""
        # In Databricks-only path, LLM_PROVIDER_SELECTION might be step 5
        state_databricks = WizardState(
            current_step=WizardStep.LLM_PROVIDER_SELECTION, step_number=5
        )
        assert renderer.get_step_number(state_databricks) == 5

        # In Redshift path, LLM_PROVIDER_SELECTION might be step 14
        state_redshift = WizardState(
            current_step=WizardStep.LLM_PROVIDER_SELECTION, step_number=14
        )
        assert renderer.get_step_number(state_redshift) == 14

    def test_render_step_header(self, renderer):
        """Test that render_step_header uses step number correctly."""
        # Capture output
        output = StringIO()
        renderer.console = Console(file=output, force_terminal=False, width=100)

        renderer.render_step_header(5, "Test Step", clear_screen=False)

        result = output.getvalue()
        assert "Step 5: Test Step" in result
        assert "=" * 50 in result

    def test_render_step_header_with_different_numbers(self, renderer):
        """Test render_step_header with various step numbers."""
        for step_num in [1, 5, 10, 15]:
            output = StringIO()
            renderer.console = Console(file=output, force_terminal=False, width=100)

            renderer.render_step_header(step_num, "Test Title", clear_screen=False)

            result = output.getvalue()
            assert f"Step {step_num}: Test Title" in result
