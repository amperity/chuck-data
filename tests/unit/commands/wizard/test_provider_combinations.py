"""
Tests for valid data provider + compute provider combinations.
"""

import pytest
from unittest.mock import patch, Mock
from chuck_data.commands.wizard.state import WizardState, WizardStep
from chuck_data.commands.wizard.steps import (
    ComputeProviderSelectionStep,
    VALID_PROVIDER_COMBINATIONS,
)
from chuck_data.commands.wizard.validator import InputValidator


class TestProviderCombinations:
    """Tests for enforcing valid data + compute provider combinations."""

    @pytest.fixture
    def step(self):
        """Create compute provider selection step."""
        validator = InputValidator()
        return ComputeProviderSelectionStep(validator)

    def test_valid_combinations_defined(self):
        """Test that valid combinations are properly defined."""
        assert "databricks" in VALID_PROVIDER_COMBINATIONS
        assert "aws_redshift" in VALID_PROVIDER_COMBINATIONS

        # Databricks data → Databricks compute only
        assert VALID_PROVIDER_COMBINATIONS["databricks"] == ["databricks"]

        # Redshift data → Databricks or EMR compute
        assert set(VALID_PROVIDER_COMBINATIONS["aws_redshift"]) == {
            "databricks",
            "aws_emr",
        }

    def test_databricks_data_shows_only_databricks_compute(self, step):
        """Test that Databricks data provider only shows Databricks compute option."""
        state = WizardState(
            data_provider="databricks",
            workspace_url="https://test.databricks.com",
            token="test-token",
        )

        prompt = step.get_prompt_message(state)

        # Should only show option 1 (Databricks)
        assert "1. Databricks" in prompt
        # Should NOT show option 2
        assert "2." not in prompt
        assert "AWS EMR" not in prompt

    def test_redshift_data_shows_both_compute_options(self, step):
        """Test that Redshift data provider shows both Databricks and EMR options."""
        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
            s3_bucket="test-bucket",
            iam_role="arn:aws:iam::123456789012:role/RedshiftRole",
        )

        prompt = step.get_prompt_message(state)

        # Should show both options
        assert "1. Databricks (default)" in prompt
        assert "2. AWS EMR" in prompt

    @patch("chuck_data.config.get_config_manager")
    def test_databricks_data_accepts_databricks_compute(
        self, mock_config_manager, step
    ):
        """Test that Databricks data + Databricks compute is accepted."""
        mock_manager = Mock()
        mock_manager.update.return_value = True
        mock_config_manager.return_value = mock_manager

        state = WizardState(
            data_provider="databricks",
            workspace_url="https://test.databricks.com",
            token="test-token",
        )

        result = step.handle_input("1", state)

        assert result.success is True
        assert result.data["compute_provider"] == "databricks"
        mock_manager.update.assert_called_once_with(compute_provider="databricks")

    def test_databricks_data_rejects_emr_compute(self, step):
        """Test that Databricks data + EMR compute is rejected."""
        state = WizardState(
            data_provider="databricks",
            workspace_url="https://test.databricks.com",
            token="test-token",
        )

        result = step.handle_input("2", state)

        assert result.success is False
        assert "Invalid combination" in result.message
        assert (
            "databricks data provider does not support aws_emr compute provider"
            in result.message
        )

    @patch("chuck_data.config.get_config_manager")
    def test_redshift_data_accepts_databricks_compute(self, mock_config_manager, step):
        """Test that Redshift data + Databricks compute is accepted."""
        mock_manager = Mock()
        mock_manager.update.return_value = True
        mock_config_manager.return_value = mock_manager

        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
            s3_bucket="test-bucket",
            iam_role="arn:aws:iam::123456789012:role/RedshiftRole",
        )

        result = step.handle_input("1", state)

        assert result.success is True
        assert result.data["compute_provider"] == "databricks"

    @patch("chuck_data.config.get_config_manager")
    def test_redshift_data_accepts_emr_compute(self, mock_config_manager, step):
        """Test that Redshift data + EMR compute is accepted."""
        mock_manager = Mock()
        mock_manager.update.return_value = True
        mock_config_manager.return_value = mock_manager

        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
            s3_bucket="test-bucket",
            iam_role="arn:aws:iam::123456789012:role/RedshiftRole",
        )

        result = step.handle_input("2", state)

        assert result.success is True
        assert result.data["compute_provider"] == "aws_emr"

    def test_databricks_data_name_input_accepts_databricks(self, step):
        """Test that typing 'databricks' works for Databricks data provider."""
        with patch("chuck_data.config.get_config_manager") as mock_config_manager:
            mock_manager = Mock()
            mock_manager.update.return_value = True
            mock_config_manager.return_value = mock_manager

            state = WizardState(
                data_provider="databricks",
                workspace_url="https://test.databricks.com",
                token="test-token",
            )

            result = step.handle_input("databricks", state)

            assert result.success is True
            assert result.data["compute_provider"] == "databricks"

    def test_databricks_data_name_input_rejects_emr(self, step):
        """Test that typing 'emr' is rejected for Databricks data provider."""
        state = WizardState(
            data_provider="databricks",
            workspace_url="https://test.databricks.com",
            token="test-token",
        )

        result = step.handle_input("emr", state)

        assert result.success is False
        assert "Invalid combination" in result.message

    def test_redshift_data_name_input_accepts_emr(self, step):
        """Test that typing 'emr' works for Redshift data provider."""
        with patch("chuck_data.config.get_config_manager") as mock_config_manager:
            mock_manager = Mock()
            mock_manager.update.return_value = True
            mock_config_manager.return_value = mock_manager

            state = WizardState(
                data_provider="aws_redshift",
                aws_profile="default",
                aws_region="us-west-2",
                s3_bucket="test-bucket",
                iam_role="arn:aws:iam::123456789012:role/RedshiftRole",
            )

            result = step.handle_input("aws_emr", state)

            assert result.success is True
            assert result.data["compute_provider"] == "aws_emr"


class TestComputeProviderFlowForDatabricks:
    """Test complete flow for Databricks data provider."""

    @pytest.fixture
    def step(self):
        """Create compute provider selection step."""
        validator = InputValidator()
        return ComputeProviderSelectionStep(validator)

    @patch("chuck_data.config.get_config_manager")
    def test_databricks_only_flow_proceeds_to_llm_selection(
        self, mock_config_manager, step
    ):
        """Test that Databricks → Databricks goes to LLM selection."""
        mock_manager = Mock()
        mock_manager.update.return_value = True
        mock_config_manager.return_value = mock_manager

        state = WizardState(
            data_provider="databricks",
            workspace_url="https://test.databricks.com",
            token="test-token",
        )

        result = step.handle_input("1", state)

        assert result.success is True
        assert result.next_step == WizardStep.LLM_PROVIDER_SELECTION


class TestComputeProviderFlowForRedshift:
    """Test complete flow for Redshift data provider."""

    @pytest.fixture
    def step(self):
        """Create compute provider selection step."""
        validator = InputValidator()
        return ComputeProviderSelectionStep(validator)

    @patch("chuck_data.config.get_config_manager")
    def test_redshift_databricks_requires_instance_profile(
        self, mock_config_manager, step
    ):
        """Test that Redshift → Databricks requires instance profile input."""
        mock_manager = Mock()
        mock_manager.update.return_value = True
        mock_config_manager.return_value = mock_manager

        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
            s3_bucket="test-bucket",
            iam_role="arn:aws:iam::123456789012:role/RedshiftRole",
        )

        result = step.handle_input("1", state)

        assert result.success is True
        assert result.next_step == WizardStep.INSTANCE_PROFILE_INPUT
        assert "Instance Profile ARN" in result.message

    @patch("chuck_data.config.get_config_manager")
    def test_redshift_emr_proceeds_to_cluster_id(self, mock_config_manager, step):
        """Test that Redshift → EMR goes to EMR cluster ID input."""
        mock_manager = Mock()
        mock_manager.update.return_value = True
        mock_config_manager.return_value = mock_manager

        state = WizardState(
            data_provider="aws_redshift",
            aws_profile="default",
            aws_region="us-west-2",
            s3_bucket="test-bucket",
            iam_role="arn:aws:iam::123456789012:role/RedshiftRole",
        )

        result = step.handle_input("2", state)

        assert result.success is True
        assert result.next_step == WizardStep.EMR_CLUSTER_ID_INPUT
        assert "EMR cluster ID" in result.message
