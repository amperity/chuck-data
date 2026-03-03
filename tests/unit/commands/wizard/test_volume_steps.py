"""Tests for the Databricks Volume location wizard steps.

Covers DatabricksVolumeCatalogInputStep, DatabricksVolumeSchemaInputStep,
the TokenInputStep routing change for Snowflake, and the wizard state
transitions added for DATABRICKS_VOLUME_CATALOG_INPUT /
DATABRICKS_VOLUME_SCHEMA_INPUT.
"""

from unittest.mock import Mock, patch

import pytest

from chuck_data.commands.wizard.state import (
    WizardAction,
    WizardState,
    WizardStateMachine,
    WizardStep,
)
from chuck_data.commands.setup_wizard import SetupWizardOrchestrator
from chuck_data.commands.wizard.steps import (
    DatabricksVolumeCatalogInputStep,
    DatabricksVolumeSchemaInputStep,
    create_step,
)
from chuck_data.commands.wizard.validator import InputValidator


@pytest.fixture
def validator():
    return InputValidator()


@pytest.fixture
def snowflake_databricks_state():
    """A WizardState mid-setup: Snowflake data, Databricks compute, token saved."""
    state = WizardState()
    state.data_provider = "snowflake"
    state.compute_provider = "databricks"
    state.workspace_url = "https://my.azuredatabricks.net"
    state.token = "dapi-abc123"
    return state


# ---------------------------------------------------------------------------
# WizardState: validity and transitions
# ---------------------------------------------------------------------------


class TestWizardStateVolumeSteps:
    """New WizardStep values are wired correctly into the state machine."""

    def test_volume_catalog_step_valid_when_token_set(self, snowflake_databricks_state):
        assert snowflake_databricks_state.is_valid_for_step(
            WizardStep.DATABRICKS_VOLUME_CATALOG_INPUT
        )

    def test_volume_catalog_step_invalid_without_token(self):
        state = WizardState()
        state.data_provider = "snowflake"
        state.compute_provider = "databricks"
        assert not state.is_valid_for_step(WizardStep.DATABRICKS_VOLUME_CATALOG_INPUT)

    def test_volume_catalog_step_invalid_for_non_snowflake(self):
        state = WizardState()
        state.data_provider = "databricks"
        state.compute_provider = "databricks"
        state.token = "dapi-abc"
        assert not state.is_valid_for_step(WizardStep.DATABRICKS_VOLUME_CATALOG_INPUT)

    def test_volume_schema_step_valid_when_catalog_set(
        self, snowflake_databricks_state
    ):
        snowflake_databricks_state.volume_catalog = "my_catalog"
        assert snowflake_databricks_state.is_valid_for_step(
            WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT
        )

    def test_volume_schema_step_invalid_without_catalog(
        self, snowflake_databricks_state
    ):
        assert not snowflake_databricks_state.is_valid_for_step(
            WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT
        )

    def test_get_next_step_after_token_for_snowflake(self, snowflake_databricks_state):
        sm = WizardStateMachine()
        next_step = sm.get_next_step(WizardStep.TOKEN_INPUT, snowflake_databricks_state)
        assert next_step == WizardStep.DATABRICKS_VOLUME_CATALOG_INPUT

    def test_get_next_step_after_token_for_redshift(self):
        state = WizardState()
        state.data_provider = "aws_redshift"
        state.compute_provider = "databricks"
        sm = WizardStateMachine()
        next_step = sm.get_next_step(WizardStep.TOKEN_INPUT, state)
        assert next_step == WizardStep.LLM_PROVIDER_SELECTION

    def test_get_next_step_after_catalog_input(self, snowflake_databricks_state):
        sm = WizardStateMachine()
        next_step = sm.get_next_step(
            WizardStep.DATABRICKS_VOLUME_CATALOG_INPUT, snowflake_databricks_state
        )
        assert next_step == WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT

    def test_get_next_step_after_schema_input(self, snowflake_databricks_state):
        sm = WizardStateMachine()
        next_step = sm.get_next_step(
            WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT, snowflake_databricks_state
        )
        assert next_step == WizardStep.LLM_PROVIDER_SELECTION

    def test_valid_transition_token_to_catalog(self):
        sm = WizardStateMachine()
        assert sm.can_transition(
            WizardStep.TOKEN_INPUT, WizardStep.DATABRICKS_VOLUME_CATALOG_INPUT
        )

    def test_valid_transition_catalog_to_schema(self):
        sm = WizardStateMachine()
        assert sm.can_transition(
            WizardStep.DATABRICKS_VOLUME_CATALOG_INPUT,
            WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT,
        )

    def test_valid_transition_schema_to_llm(self):
        sm = WizardStateMachine()
        assert sm.can_transition(
            WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT, WizardStep.LLM_PROVIDER_SELECTION
        )


# ---------------------------------------------------------------------------
# TokenInputStep routing for Snowflake
# ---------------------------------------------------------------------------


class TestTokenInputStepSnowflakeRouting:
    """TokenInputStep sends Snowflake users to the catalog step, not LLM."""

    def _valid_validation(self):
        v = Mock()
        v.is_valid = True
        v.processed_value = "dapi-abc123"
        return v

    @patch("chuck_data.commands.wizard.steps.get_chuck_service")
    def test_snowflake_routes_to_volume_catalog(
        self, mock_service, validator, snowflake_databricks_state
    ):
        step = create_step(WizardStep.TOKEN_INPUT, validator)

        with patch.object(
            step.validator, "validate_token", return_value=self._valid_validation()
        ):
            with patch(
                "chuck_data.commands.wizard.steps.set_workspace_url", return_value=True
            ):
                with patch(
                    "chuck_data.commands.wizard.steps.set_databricks_token",
                    return_value=True,
                ):
                    mock_service.return_value = None
                    result = step.handle_input(
                        "dapi-abc123", snowflake_databricks_state
                    )

        assert result.success
        assert result.next_step == WizardStep.DATABRICKS_VOLUME_CATALOG_INPUT

    @patch("chuck_data.commands.wizard.steps.get_chuck_service")
    def test_redshift_databricks_routes_to_llm(self, mock_service, validator):
        state = WizardState()
        state.data_provider = "aws_redshift"
        state.compute_provider = "databricks"
        state.workspace_url = "https://ws.azuredatabricks.net"
        step = create_step(WizardStep.TOKEN_INPUT, validator)

        with patch.object(
            step.validator, "validate_token", return_value=self._valid_validation()
        ):
            with patch(
                "chuck_data.commands.wizard.steps.set_workspace_url", return_value=True
            ):
                with patch(
                    "chuck_data.commands.wizard.steps.set_databricks_token",
                    return_value=True,
                ):
                    mock_service.return_value = None
                    result = step.handle_input("dapi-abc123", state)

        assert result.success
        assert result.next_step == WizardStep.LLM_PROVIDER_SELECTION


# ---------------------------------------------------------------------------
# DatabricksVolumeCatalogInputStep
# ---------------------------------------------------------------------------


class TestDatabricksVolumeCatalogInputStep:
    """Tests for the catalog input step."""

    def test_saves_catalog_and_advances(self, validator, snowflake_databricks_state):
        step = create_step(WizardStep.DATABRICKS_VOLUME_CATALOG_INPUT, validator)

        with patch("chuck_data.config.get_config_manager") as mock_cfg:
            mock_cfg.return_value.update.return_value = True
            result = step.handle_input("my_catalog", snowflake_databricks_state)

        assert result.success
        assert result.next_step == WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT
        assert result.action == WizardAction.CONTINUE
        assert result.data["volume_catalog"] == "my_catalog"

    def test_whitespace_is_stripped(self, validator, snowflake_databricks_state):
        step = create_step(WizardStep.DATABRICKS_VOLUME_CATALOG_INPUT, validator)

        with patch("chuck_data.config.get_config_manager") as mock_cfg:
            mock_cfg.return_value.update.return_value = True
            result = step.handle_input("  my_catalog  ", snowflake_databricks_state)

        assert result.data["volume_catalog"] == "my_catalog"

    def test_empty_input_is_rejected(self, validator, snowflake_databricks_state):
        step = create_step(WizardStep.DATABRICKS_VOLUME_CATALOG_INPUT, validator)
        result = step.handle_input("   ", snowflake_databricks_state)

        assert not result.success
        assert result.action == WizardAction.RETRY

    def test_config_save_error_returns_retry(
        self, validator, snowflake_databricks_state
    ):
        step = create_step(WizardStep.DATABRICKS_VOLUME_CATALOG_INPUT, validator)

        with patch("chuck_data.config.get_config_manager") as mock_cfg:
            mock_cfg.return_value.update.side_effect = Exception("disk full")
            result = step.handle_input("my_catalog", snowflake_databricks_state)

        assert not result.success
        assert result.action == WizardAction.RETRY

    def test_step_title(self, validator):
        step = create_step(WizardStep.DATABRICKS_VOLUME_CATALOG_INPUT, validator)
        assert "Catalog" in step.get_step_title()

    def test_prompt_mentions_volume(self, validator, snowflake_databricks_state):
        step = create_step(WizardStep.DATABRICKS_VOLUME_CATALOG_INPUT, validator)
        prompt = step.get_prompt_message(snowflake_databricks_state)
        assert "chuck" in prompt.lower() or "volume" in prompt.lower()


# ---------------------------------------------------------------------------
# DatabricksVolumeSchemaInputStep
# ---------------------------------------------------------------------------


class TestDatabricksVolumeSchemaInputStep:
    """Tests for the schema input step."""

    def _state_with_catalog(self, catalog="my_catalog"):
        state = WizardState()
        state.data_provider = "snowflake"
        state.compute_provider = "databricks"
        state.token = "dapi-abc"
        state.volume_catalog = catalog
        return state

    def _mock_service_with_volumes(self, volume_names):
        """Build a mock chuck service whose Databricks client has the given volumes."""
        mock_dbx = Mock()
        mock_dbx.list_volumes.return_value = {
            "volumes": [{"name": n} for n in volume_names]
        }
        mock_dbx.create_volume.return_value = {"name": "chuck"}
        mock_svc = Mock()
        mock_svc.client = mock_dbx
        return mock_svc, mock_dbx

    def test_saves_schema_and_advances_to_llm(self, validator):
        step = create_step(WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT, validator)
        state = self._state_with_catalog()
        mock_svc, _ = self._mock_service_with_volumes(["chuck"])

        with patch(
            "chuck_data.commands.wizard.steps.get_chuck_service", return_value=mock_svc
        ):
            with patch("chuck_data.config.get_config_manager") as mock_cfg:
                mock_cfg.return_value.update.return_value = True
                result = step.handle_input("my_schema", state)

        assert result.success
        assert result.next_step == WizardStep.LLM_PROVIDER_SELECTION
        assert result.action == WizardAction.CONTINUE
        assert result.data["volume_schema"] == "my_schema"

    def test_whitespace_is_stripped(self, validator):
        step = create_step(WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT, validator)
        state = self._state_with_catalog()
        mock_svc, _ = self._mock_service_with_volumes(["chuck"])

        with patch(
            "chuck_data.commands.wizard.steps.get_chuck_service", return_value=mock_svc
        ):
            with patch("chuck_data.config.get_config_manager") as mock_cfg:
                mock_cfg.return_value.update.return_value = True
                result = step.handle_input("  my_schema  ", state)

        assert result.data["volume_schema"] == "my_schema"

    def test_empty_input_is_rejected(self, validator):
        step = create_step(WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT, validator)
        result = step.handle_input("  ", self._state_with_catalog())
        assert not result.success
        assert result.action == WizardAction.RETRY

    def test_creates_chuck_volume_when_missing(self, validator):
        step = create_step(WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT, validator)
        state = self._state_with_catalog()
        mock_svc, mock_dbx = self._mock_service_with_volumes([])  # no chuck volume

        with patch(
            "chuck_data.commands.wizard.steps.get_chuck_service", return_value=mock_svc
        ):
            with patch("chuck_data.config.get_config_manager") as mock_cfg:
                mock_cfg.return_value.update.return_value = True
                result = step.handle_input("my_schema", state)

        assert result.success
        mock_dbx.create_volume.assert_called_once_with(
            catalog_name="my_catalog", schema_name="my_schema", name="chuck"
        )

    def test_skips_volume_creation_when_chuck_already_exists(self, validator):
        step = create_step(WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT, validator)
        state = self._state_with_catalog()
        mock_svc, mock_dbx = self._mock_service_with_volumes(["chuck", "other"])

        with patch(
            "chuck_data.commands.wizard.steps.get_chuck_service", return_value=mock_svc
        ):
            with patch("chuck_data.config.get_config_manager") as mock_cfg:
                mock_cfg.return_value.update.return_value = True
                step.handle_input("my_schema", state)

        mock_dbx.create_volume.assert_not_called()

    def test_volume_creation_failure_is_non_fatal(self, validator):
        """A failure to create the volume should not block setup."""
        step = create_step(WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT, validator)
        state = self._state_with_catalog()
        mock_svc, mock_dbx = self._mock_service_with_volumes([])
        mock_dbx.create_volume.side_effect = Exception("403 Forbidden")

        with patch(
            "chuck_data.commands.wizard.steps.get_chuck_service", return_value=mock_svc
        ):
            with patch("chuck_data.config.get_config_manager") as mock_cfg:
                mock_cfg.return_value.update.return_value = True
                result = step.handle_input("my_schema", state)

        assert result.success  # setup proceeds despite volume creation failure
        assert result.data["volume_schema"] == "my_schema"

    def test_no_active_service_still_saves_config(self, validator):
        """If no chuck service is running, config is still saved."""
        step = create_step(WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT, validator)
        state = self._state_with_catalog()

        with patch(
            "chuck_data.commands.wizard.steps.get_chuck_service", return_value=None
        ):
            with patch("chuck_data.config.get_config_manager") as mock_cfg:
                mock_cfg.return_value.update.return_value = True
                result = step.handle_input("my_schema", state)

        assert result.success
        assert result.data["volume_schema"] == "my_schema"

    def test_missing_catalog_exits_wizard(self, validator):
        """If volume_catalog was never set, the step exits cleanly."""
        step = create_step(WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT, validator)
        state = WizardState()  # no volume_catalog set
        result = step.handle_input("my_schema", state)
        assert not result.success
        assert result.action == WizardAction.EXIT

    def test_config_save_error_returns_retry(self, validator):
        step = create_step(WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT, validator)
        state = self._state_with_catalog()
        mock_svc, _ = self._mock_service_with_volumes(["chuck"])

        with patch(
            "chuck_data.commands.wizard.steps.get_chuck_service", return_value=mock_svc
        ):
            with patch("chuck_data.config.get_config_manager") as mock_cfg:
                mock_cfg.return_value.update.side_effect = Exception("disk full")
                result = step.handle_input("my_schema", state)

        assert not result.success
        assert result.action == WizardAction.RETRY

    def test_prompt_includes_catalog_name(self, validator):
        step = create_step(WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT, validator)
        state = self._state_with_catalog("prod_catalog")
        prompt = step.get_prompt_message(state)
        assert "prod_catalog" in prompt

    def test_step_title(self, validator):
        step = create_step(WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT, validator)
        assert "Schema" in step.get_step_title()


# ---------------------------------------------------------------------------
# Step factory registration
# ---------------------------------------------------------------------------


class TestStepFactoryIncludesVolumeSteps:
    def test_factory_creates_volume_catalog_step(self, validator):
        step = create_step(WizardStep.DATABRICKS_VOLUME_CATALOG_INPUT, validator)
        assert isinstance(step, DatabricksVolumeCatalogInputStep)

    def test_factory_creates_volume_schema_step(self, validator):
        step = create_step(WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT, validator)
        assert isinstance(step, DatabricksVolumeSchemaInputStep)


# ---------------------------------------------------------------------------
# State persistence: setup_wizard.py save/load round-trip
# ---------------------------------------------------------------------------


class TestVolumeStateRoundTrip:
    """volume_catalog/volume_schema survive the context save → load cycle.

    The bug: _save_state_to_context used a manual allowlist that omitted
    volume_catalog and volume_schema, so _load_state_from_context would
    always restore them as None, causing 'Catalog not set' in the schema step.
    """

    def test_volume_catalog_survives_save_load_roundtrip(self):
        """volume_catalog written in one turn is present in the next."""
        orchestrator = SetupWizardOrchestrator()

        state = WizardState(current_step=WizardStep.DATABRICKS_VOLUME_CATALOG_INPUT)
        state.data_provider = "snowflake"
        state.compute_provider = "databricks"
        state.token = "dapi-abc"
        state.volume_catalog = "my_catalog"

        orchestrator._save_state_to_context(state)
        loaded = orchestrator._load_state_from_context()

        assert loaded is not None
        assert loaded.volume_catalog == "my_catalog"

    def test_volume_schema_survives_save_load_roundtrip(self):
        """volume_schema written in one turn is present in the next."""
        orchestrator = SetupWizardOrchestrator()

        state = WizardState(current_step=WizardStep.DATABRICKS_VOLUME_SCHEMA_INPUT)
        state.data_provider = "snowflake"
        state.compute_provider = "databricks"
        state.token = "dapi-abc"
        state.volume_catalog = "my_catalog"
        state.volume_schema = "my_schema"

        orchestrator._save_state_to_context(state)
        loaded = orchestrator._load_state_from_context()

        assert loaded is not None
        assert loaded.volume_catalog == "my_catalog"
        assert loaded.volume_schema == "my_schema"

    def test_volume_fields_default_to_none_when_not_set(self):
        """Absent volume fields load as None without error."""
        orchestrator = SetupWizardOrchestrator()

        state = WizardState(current_step=WizardStep.TOKEN_INPUT)
        state.data_provider = "databricks"
        # volume_catalog / volume_schema never set

        orchestrator._save_state_to_context(state)
        loaded = orchestrator._load_state_from_context()

        assert loaded is not None
        assert loaded.volume_catalog is None
        assert loaded.volume_schema is None
