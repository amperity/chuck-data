"""Tests for the Snowflake Stitch job launch path in setup_stitch.py.

Covers _snowflake_execute_job_launch: metadata correctness, volume location
resolution via get_volume_catalog/get_volume_schema, and error paths.
"""

from unittest.mock import Mock, patch

import pytest

from chuck_data.commands.setup_stitch import (
    _handle_snowflake_stitch_setup,
    _snowflake_execute_job_launch,
)
from chuck_data.command_output import CommandResult

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def console():
    return Mock()


@pytest.fixture
def snowflake_client():
    client = Mock()
    client.account = "myorg-myaccount"
    return client


@pytest.fixture
def sample_manifest():
    return {
        "name": "stitch-snowflake-mydb-myschema",
        "tables": [{"path": "mydb.myschema.customers", "fields": []}],
        "settings": {
            "data_provider": "snowflake",
            "compute_provider": "databricks",
            "snowflake_config": {
                "account": "myorg-myaccount",
                "user": "stitch_user",
                "database": "mydb",
                "schema": "myschema",
                "warehouse": "stitch_wh",
            },
            "output_database_name": "mydb",
            "output_schema_name": "stitch_outputs",
        },
    }


@pytest.fixture
def init_data():
    return {"cluster-init": "#!/bin/bash\necho init", "job-id": "chuck-job-123"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VOLUME_UPLOAD_PATH = "/Volumes/vol_cat/vol_sch/chuck/manifest.json"


def _run_databricks_launch(
    console,
    snowflake_client,
    sample_manifest,
    init_data,
    mock_get_token,
    upload_path=VOLUME_UPLOAD_PATH,
):
    """Call _snowflake_execute_job_launch with DatabricksVolumeStorage and capture prep."""
    from chuck_data.storage_providers.databricks import DatabricksVolumeStorage

    mock_get_token.return_value = "amp-token"

    mock_storage = Mock(spec=DatabricksVolumeStorage)
    mock_storage.upload_file.return_value = True

    compute_provider = Mock()
    compute_provider.storage_provider = mock_storage

    captured = {}

    def capture_launch(prep):
        captured.update(prep)
        return {"success": True, "run_id": "dbx-run-42"}

    compute_provider.launch_stitch_job.side_effect = capture_launch

    with patch("chuck_data.clients.amperity.AmperityAPIClient") as mock_amp:
        mock_amp.return_value.fetch_amperity_job_init.return_value = init_data
        mock_amp.return_value.record_job_submission.return_value = None

        result = _snowflake_execute_job_launch(
            console=console,
            client=snowflake_client,
            database="mydb",
            schema="myschema",
            manifest=sample_manifest,
            manifest_path="/tmp/manifest.json",
            upload_path=upload_path,
            timestamp="20240101_120000",
            compute_provider=compute_provider,
            compute_provider_name="databricks",
        )

    return result, captured, compute_provider


# ---------------------------------------------------------------------------
# Databricks compute + Snowflake data
# ---------------------------------------------------------------------------


class TestSnowflakeLaunchDatabricksCompute:
    """_snowflake_execute_job_launch with DatabricksVolumeStorage."""

    @patch("chuck_data.commands.setup_stitch.get_amperity_token")
    @patch(
        "chuck_data.commands.setup_stitch.get_volume_catalog", return_value="vol_cat"
    )
    @patch("chuck_data.commands.setup_stitch.get_volume_schema", return_value="vol_sch")
    def test_metadata_has_config_file_path(
        self,
        _schema,
        _catalog,
        mock_token,
        console,
        snowflake_client,
        sample_manifest,
        init_data,
    ):
        """config_file_path equals upload_path (required by DatabricksComputeProvider)."""
        _, captured, _ = _run_databricks_launch(
            console,
            snowflake_client,
            sample_manifest,
            init_data,
            mock_token,
        )
        assert captured["metadata"]["config_file_path"] == VOLUME_UPLOAD_PATH

    @patch("chuck_data.commands.setup_stitch.get_amperity_token")
    @patch(
        "chuck_data.commands.setup_stitch.get_volume_catalog", return_value="vol_cat"
    )
    @patch("chuck_data.commands.setup_stitch.get_volume_schema", return_value="vol_sch")
    def test_target_catalog_and_schema_come_from_volume_config(
        self,
        _schema,
        _catalog,
        mock_token,
        console,
        snowflake_client,
        sample_manifest,
        init_data,
    ):
        """target_catalog/schema come from get_volume_catalog/schema, not get_active_catalog."""
        _, captured, _ = _run_databricks_launch(
            console,
            snowflake_client,
            sample_manifest,
            init_data,
            mock_token,
        )
        assert captured["metadata"]["target_catalog"] == "vol_cat"
        assert captured["metadata"]["target_schema"] == "vol_sch"

    @patch("chuck_data.commands.setup_stitch.get_amperity_token")
    @patch(
        "chuck_data.commands.setup_stitch.get_volume_catalog", return_value="vol_cat"
    )
    @patch("chuck_data.commands.setup_stitch.get_volume_schema", return_value="vol_sch")
    def test_init_script_stored_under_same_volume_location(
        self,
        _schema,
        _catalog,
        mock_token,
        console,
        snowflake_client,
        sample_manifest,
        init_data,
    ):
        """Init script path uses the same catalog/schema as the manifest."""
        _, captured, _ = _run_databricks_launch(
            console,
            snowflake_client,
            sample_manifest,
            init_data,
            mock_token,
        )
        init_path = captured["metadata"]["init_script_path"]
        assert init_path.startswith("/Volumes/vol_cat/vol_sch/chuck/")

    @patch("chuck_data.commands.setup_stitch.get_amperity_token")
    @patch(
        "chuck_data.commands.setup_stitch.get_volume_catalog", return_value="vol_cat"
    )
    @patch("chuck_data.commands.setup_stitch.get_volume_schema", return_value="vol_sch")
    def test_metadata_has_init_script_content(
        self,
        _schema,
        _catalog,
        mock_token,
        console,
        snowflake_client,
        sample_manifest,
        init_data,
    ):
        """init_script_content is present (DatabricksComputeProvider re-uploads it)."""
        _, captured, _ = _run_databricks_launch(
            console,
            snowflake_client,
            sample_manifest,
            init_data,
            mock_token,
        )
        assert captured["metadata"]["init_script_content"] == "#!/bin/bash\necho init"

    @patch("chuck_data.commands.setup_stitch.get_amperity_token")
    @patch(
        "chuck_data.commands.setup_stitch.get_volume_catalog", return_value="vol_cat"
    )
    @patch("chuck_data.commands.setup_stitch.get_volume_schema", return_value="vol_sch")
    def test_metadata_has_pii_scan_output(
        self,
        _schema,
        _catalog,
        mock_token,
        console,
        snowflake_client,
        sample_manifest,
        init_data,
    ):
        """pii_scan_output is present with a message key."""
        _, captured, _ = _run_databricks_launch(
            console,
            snowflake_client,
            sample_manifest,
            init_data,
            mock_token,
        )
        pii = captured["metadata"]["pii_scan_output"]
        assert isinstance(pii, dict)
        assert "message" in pii

    @patch("chuck_data.commands.setup_stitch.get_amperity_token")
    @patch(
        "chuck_data.commands.setup_stitch.get_volume_catalog", return_value="vol_cat"
    )
    @patch("chuck_data.commands.setup_stitch.get_volume_schema", return_value="vol_sch")
    def test_metadata_has_s3_config_path_alias(
        self,
        _schema,
        _catalog,
        mock_token,
        console,
        snowflake_client,
        sample_manifest,
        init_data,
    ):
        """s3_config_path equals upload_path so EMRComputeProvider also works."""
        _, captured, _ = _run_databricks_launch(
            console,
            snowflake_client,
            sample_manifest,
            init_data,
            mock_token,
        )
        assert captured["metadata"]["s3_config_path"] == VOLUME_UPLOAD_PATH
        assert captured["metadata"]["config_file_path"] == VOLUME_UPLOAD_PATH

    @patch("chuck_data.commands.setup_stitch.get_amperity_token")
    @patch(
        "chuck_data.commands.setup_stitch.get_volume_catalog", return_value="vol_cat"
    )
    @patch("chuck_data.commands.setup_stitch.get_volume_schema", return_value="vol_sch")
    def test_returns_success_with_run_id(
        self,
        _schema,
        _catalog,
        mock_token,
        console,
        snowflake_client,
        sample_manifest,
        init_data,
    ):
        """Returns a successful CommandResult containing the run_id."""
        result, _, _ = _run_databricks_launch(
            console,
            snowflake_client,
            sample_manifest,
            init_data,
            mock_token,
        )
        assert isinstance(result, CommandResult)
        assert result.success is True
        assert result.data["run_id"] == "dbx-run-42"


# ---------------------------------------------------------------------------
# EMR compute + Snowflake data
# ---------------------------------------------------------------------------


class TestSnowflakeLaunchEMRCompute:
    """_snowflake_execute_job_launch with EMRComputeProvider."""

    @patch("chuck_data.commands.setup_stitch.get_amperity_token")
    @patch("chuck_data.commands.setup_stitch.get_s3_bucket", return_value="my-bucket")
    def test_metadata_has_s3_config_path_for_emr(
        self,
        _bucket,
        mock_token,
        console,
        snowflake_client,
        sample_manifest,
        init_data,
    ):
        """s3_config_path is set to upload_path (required by EMRComputeProvider)."""
        mock_token.return_value = "amp-token"

        from chuck_data.storage_providers.s3 import S3Storage
        from chuck_data.compute_providers.emr import EMRComputeProvider

        mock_storage = Mock(spec=S3Storage)
        mock_storage.upload_file.return_value = True

        compute_provider = Mock(spec=EMRComputeProvider)
        compute_provider.storage_provider = mock_storage

        captured = {}

        def capture_launch(prep):
            captured.update(prep)
            return {
                "success": True,
                "step_id": "s-EMR123",
                "monitoring_url": "https://aws/emr",
            }

        compute_provider.launch_stitch_job.side_effect = capture_launch
        upload_path = "s3://my-bucket/chuck/manifests/manifest.json"

        with patch("chuck_data.clients.amperity.AmperityAPIClient") as mock_amp:
            mock_amp.return_value.fetch_amperity_job_init.return_value = init_data

            _snowflake_execute_job_launch(
                console=console,
                client=snowflake_client,
                database="mydb",
                schema="myschema",
                manifest=sample_manifest,
                manifest_path="/tmp/manifest.json",
                upload_path=upload_path,
                timestamp="20240101_120000",
                compute_provider=compute_provider,
                compute_provider_name="emr",
            )

        assert captured["metadata"]["s3_config_path"] == upload_path

    @patch("chuck_data.commands.setup_stitch.get_amperity_token")
    @patch("chuck_data.commands.setup_stitch.get_s3_bucket", return_value="my-bucket")
    def test_emr_target_catalog_falls_back_to_snowflake_database(
        self,
        _bucket,
        mock_token,
        console,
        snowflake_client,
        sample_manifest,
        init_data,
    ):
        """For S3 storage the target_catalog/schema fall back to the Snowflake db/schema."""
        mock_token.return_value = "amp-token"

        from chuck_data.storage_providers.s3 import S3Storage
        from chuck_data.compute_providers.emr import EMRComputeProvider

        mock_storage = Mock(spec=S3Storage)
        mock_storage.upload_file.return_value = True

        compute_provider = Mock(spec=EMRComputeProvider)
        compute_provider.storage_provider = mock_storage

        captured = {}

        def capture_launch(prep):
            captured.update(prep)
            return {"success": True, "step_id": "s-EMR123"}

        compute_provider.launch_stitch_job.side_effect = capture_launch

        with patch("chuck_data.clients.amperity.AmperityAPIClient") as mock_amp:
            mock_amp.return_value.fetch_amperity_job_init.return_value = init_data

            _snowflake_execute_job_launch(
                console=console,
                client=snowflake_client,
                database="mydb",
                schema="myschema",
                manifest=sample_manifest,
                manifest_path="/tmp/manifest.json",
                upload_path="s3://my-bucket/chuck/manifests/manifest.json",
                timestamp="20240101_120000",
                compute_provider=compute_provider,
                compute_provider_name="emr",
            )

        assert captured["metadata"]["target_catalog"] == "mydb"
        assert captured["metadata"]["target_schema"] == "myschema"


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestSnowflakeLaunchErrors:
    """Error-path tests for _snowflake_execute_job_launch."""

    def test_missing_amperity_token_returns_error(
        self,
        console,
        snowflake_client,
        sample_manifest,
    ):
        """Returns a failure when the Amperity token is missing."""
        compute_provider = Mock()
        compute_provider.storage_provider = Mock()

        with patch(
            "chuck_data.commands.setup_stitch.get_amperity_token", return_value=None
        ):
            result = _snowflake_execute_job_launch(
                console=console,
                client=snowflake_client,
                database="mydb",
                schema="myschema",
                manifest=sample_manifest,
                manifest_path="/tmp/manifest.json",
                upload_path=VOLUME_UPLOAD_PATH,
                timestamp="20240101_120000",
                compute_provider=compute_provider,
                compute_provider_name="databricks",
            )

        assert isinstance(result, CommandResult)
        assert result.success is False
        assert "amp_login" in result.message

    @patch("chuck_data.commands.setup_stitch.get_amperity_token")
    @patch("chuck_data.commands.setup_stitch.get_volume_catalog", return_value=None)
    @patch("chuck_data.commands.setup_stitch.get_volume_schema", return_value=None)
    def test_missing_volume_config_returns_error(
        self,
        _schema,
        _catalog,
        mock_token,
        console,
        snowflake_client,
        sample_manifest,
        init_data,
    ):
        """Returns a failure when volume_catalog/schema are not configured."""
        mock_token.return_value = "amp-token"

        from chuck_data.storage_providers.databricks import DatabricksVolumeStorage

        mock_storage = Mock(spec=DatabricksVolumeStorage)
        compute_provider = Mock()
        compute_provider.storage_provider = mock_storage

        with patch("chuck_data.clients.amperity.AmperityAPIClient") as mock_amp:
            mock_amp.return_value.fetch_amperity_job_init.return_value = init_data

            result = _snowflake_execute_job_launch(
                console=console,
                client=snowflake_client,
                database="mydb",
                schema="myschema",
                manifest=sample_manifest,
                manifest_path="/tmp/manifest.json",
                upload_path=VOLUME_UPLOAD_PATH,
                timestamp="20240101_120000",
                compute_provider=compute_provider,
                compute_provider_name="databricks",
            )

        assert isinstance(result, CommandResult)
        assert result.success is False
        assert "volume" in result.message.lower() or "setup" in result.message.lower()

    @patch("chuck_data.commands.setup_stitch.get_amperity_token")
    @patch(
        "chuck_data.commands.setup_stitch.get_volume_catalog", return_value="vol_cat"
    )
    @patch("chuck_data.commands.setup_stitch.get_volume_schema", return_value="vol_sch")
    def test_upload_init_script_failure_returns_error(
        self,
        _schema,
        _catalog,
        mock_token,
        console,
        snowflake_client,
        sample_manifest,
        init_data,
    ):
        """Returns failure when init script upload raises."""
        mock_token.return_value = "amp-token"

        from chuck_data.storage_providers.databricks import DatabricksVolumeStorage

        mock_storage = Mock(spec=DatabricksVolumeStorage)
        mock_storage.upload_file.side_effect = Exception("Volumes write failed")

        compute_provider = Mock()
        compute_provider.storage_provider = mock_storage

        with patch("chuck_data.clients.amperity.AmperityAPIClient") as mock_amp:
            mock_amp.return_value.fetch_amperity_job_init.return_value = init_data

            result = _snowflake_execute_job_launch(
                console=console,
                client=snowflake_client,
                database="mydb",
                schema="myschema",
                manifest=sample_manifest,
                manifest_path="/tmp/manifest.json",
                upload_path=VOLUME_UPLOAD_PATH,
                timestamp="20240101_120000",
                compute_provider=compute_provider,
                compute_provider_name="databricks",
            )

        assert isinstance(result, CommandResult)
        assert result.success is False
        assert "Failed to upload init script" in result.message

    @patch("chuck_data.commands.setup_stitch.get_amperity_token")
    @patch(
        "chuck_data.commands.setup_stitch.get_volume_catalog", return_value="vol_cat"
    )
    @patch("chuck_data.commands.setup_stitch.get_volume_schema", return_value="vol_sch")
    def test_launch_job_failure_propagates_error(
        self,
        _schema,
        _catalog,
        mock_token,
        console,
        snowflake_client,
        sample_manifest,
        init_data,
    ):
        """Returns failure when launch_stitch_job returns an error."""
        mock_token.return_value = "amp-token"

        from chuck_data.storage_providers.databricks import DatabricksVolumeStorage

        mock_storage = Mock(spec=DatabricksVolumeStorage)
        mock_storage.upload_file.return_value = True

        compute_provider = Mock()
        compute_provider.storage_provider = mock_storage
        compute_provider.launch_stitch_job.return_value = {
            "success": False,
            "error": "Databricks cluster unavailable",
        }

        with patch("chuck_data.clients.amperity.AmperityAPIClient") as mock_amp:
            mock_amp.return_value.fetch_amperity_job_init.return_value = init_data

            result = _snowflake_execute_job_launch(
                console=console,
                client=snowflake_client,
                database="mydb",
                schema="myschema",
                manifest=sample_manifest,
                manifest_path="/tmp/manifest.json",
                upload_path=VOLUME_UPLOAD_PATH,
                timestamp="20240101_120000",
                compute_provider=compute_provider,
                compute_provider_name="databricks",
            )

        assert isinstance(result, CommandResult)
        assert result.success is False
        assert "Databricks cluster unavailable" in result.message


# ---------------------------------------------------------------------------
# _handle_snowflake_stitch_setup: active_schema vs volume_schema guard
# ---------------------------------------------------------------------------


class TestSnowflakeSchemaConflictGuard:
    """When active_schema is set to the Databricks volume schema instead of
    the Snowflake data schema, stitch setup should fail with a clear message
    rather than trying to read PII tags from the wrong schema."""

    def _make_snowflake_client(self):
        client = Mock()
        client.account = "myorg-myaccount"
        return client

    def _make_compute_provider(self):
        from chuck_data.storage_providers.databricks import DatabricksVolumeStorage

        mock_storage = Mock(spec=DatabricksVolumeStorage)
        provider = Mock()
        provider.storage_provider = mock_storage
        return provider

    @patch("chuck_data.commands.setup_stitch.get_volume_schema", return_value="vol_sch")
    @patch("chuck_data.commands.setup_stitch.get_active_schema", return_value="vol_sch")
    @patch("chuck_data.commands.setup_stitch.get_active_database", return_value="mydb")
    def test_returns_error_when_active_schema_equals_volume_schema(
        self, _db, _schema, _vol_schema
    ):
        """If active_schema == volume_schema the user is targeting the Databricks
        volume location instead of their Snowflake data schema."""
        result = _handle_snowflake_stitch_setup(
            client=self._make_snowflake_client(),
            compute_provider=self._make_compute_provider(),
            compute_provider_name="databricks",
            interactive_input=None,
            auto_confirm=False,
        )

        assert isinstance(result, CommandResult)
        assert result.success is False
        assert "vol_sch" in result.message
        assert (
            "Databricks volume schema" in result.message
            or "volume schema" in result.message
        )

    @patch("chuck_data.commands.setup_stitch.get_volume_schema", return_value="vol_sch")
    @patch(
        "chuck_data.commands.setup_stitch.get_active_schema",
        return_value="snowflake_sch",
    )
    @patch("chuck_data.commands.setup_stitch.get_active_database", return_value="mydb")
    @patch(
        "chuck_data.commands.setup_stitch.get_volume_catalog", return_value="vol_cat"
    )
    def test_proceeds_when_active_schema_differs_from_volume_schema(
        self, _vol_cat, _db, _schema, _vol_schema
    ):
        """If active_schema is different from volume_schema, setup proceeds to
        the manifest preparation step (no schema-conflict error)."""
        snowflake_client = self._make_snowflake_client()
        # Make read_snowflake_semantic_tags fail quickly so we don't need to
        # mock the full Stitch pipeline — we just need to confirm the guard
        # doesn't fire and that Phase 1 is entered.
        snowflake_client.read_snowflake_semantic_tags.return_value = {
            "success": False,
            "error": "expected-test-stop",
        }

        result = _handle_snowflake_stitch_setup(
            client=snowflake_client,
            compute_provider=self._make_compute_provider(),
            compute_provider_name="databricks",
            interactive_input=None,
            auto_confirm=False,
        )

        # The guard did not fire; Phase 1 was entered and stopped at tag reading
        assert isinstance(result, CommandResult)
        assert result.success is False
        assert "expected-test-stop" in result.message
