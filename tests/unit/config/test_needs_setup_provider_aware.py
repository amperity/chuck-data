"""
Tests for provider-aware needs_setup() logic.

This tests that the needs_setup() function only checks for configs
relevant to the currently configured data provider.
"""

from unittest.mock import Mock, patch

from chuck_data.config import ConfigManager, ChuckConfig


class TestNeedsSetupProviderAware:
    """Test suite for provider-aware needs_setup() logic."""

    @patch("chuck_data.config.ChuckConfig")
    def test_needs_setup_no_amperity_token(self, mock_config_class):
        """Test that needs_setup returns True when amperity_token is missing."""
        mock_config = Mock(spec=ChuckConfig)
        mock_config.amperity_token = None
        mock_config.active_model = "amazon.nova-pro-v1:0"
        mock_config.data_provider = "aws_redshift"

        config_manager = ConfigManager()
        config_manager.load = Mock(return_value=mock_config)

        assert config_manager.needs_setup() is True

    @patch("chuck_data.config.ChuckConfig")
    def test_needs_setup_no_provider(self, mock_config_class):
        """Test that needs_setup returns True when no provider is set."""
        mock_config = Mock(spec=ChuckConfig)
        mock_config.amperity_token = "valid-token"
        mock_config.active_model = "amazon.nova-pro-v1:0"
        mock_config.data_provider = None

        config_manager = ConfigManager()
        config_manager.load = Mock(return_value=mock_config)

        assert config_manager.needs_setup() is True

    @patch("chuck_data.config.ChuckConfig")
    def test_needs_setup_no_model(self, mock_config_class):
        """Test that needs_setup returns True when no model is set."""
        mock_config = Mock(spec=ChuckConfig)
        mock_config.amperity_token = "valid-token"
        mock_config.active_model = None
        mock_config.data_provider = "aws_redshift"

        config_manager = ConfigManager()
        config_manager.load = Mock(return_value=mock_config)

        assert config_manager.needs_setup() is True

    @patch("chuck_data.config.ChuckConfig")
    def test_needs_setup_databricks_complete(self, mock_config_class):
        """Test that needs_setup returns False for complete Databricks config."""
        mock_config = Mock(spec=ChuckConfig)
        mock_config.amperity_token = "valid-token"
        mock_config.active_model = "amazon.nova-pro-v1:0"
        mock_config.data_provider = "databricks"
        mock_config.workspace_url = "https://my-workspace.cloud.databricks.com"
        mock_config.databricks_token = "dapi12345"

        config_manager = ConfigManager()
        config_manager.load = Mock(return_value=mock_config)

        assert config_manager.needs_setup() is False

    @patch("chuck_data.config.ChuckConfig")
    def test_needs_setup_databricks_missing_token(self, mock_config_class):
        """Test that needs_setup returns True when Databricks token is missing."""
        mock_config = Mock(spec=ChuckConfig)
        mock_config.amperity_token = "valid-token"
        mock_config.active_model = "amazon.nova-pro-v1:0"
        mock_config.data_provider = "databricks"
        mock_config.workspace_url = "https://my-workspace.cloud.databricks.com"
        mock_config.databricks_token = None

        config_manager = ConfigManager()
        config_manager.load = Mock(return_value=mock_config)

        assert config_manager.needs_setup() is True

    @patch("chuck_data.config.ChuckConfig")
    def test_needs_setup_databricks_missing_workspace(self, mock_config_class):
        """Test that needs_setup returns True when Databricks workspace is missing."""
        mock_config = Mock(spec=ChuckConfig)
        mock_config.amperity_token = "valid-token"
        mock_config.active_model = "amazon.nova-pro-v1:0"
        mock_config.data_provider = "databricks"
        mock_config.workspace_url = None
        mock_config.databricks_token = "dapi12345"

        config_manager = ConfigManager()
        config_manager.load = Mock(return_value=mock_config)

        assert config_manager.needs_setup() is True

    @patch("chuck_data.config.ChuckConfig")
    def test_needs_setup_redshift_complete_with_workgroup(self, mock_config_class):
        """Test that needs_setup returns False for complete Redshift config with workgroup."""
        mock_config = Mock(spec=ChuckConfig)
        mock_config.amperity_token = "valid-token"
        mock_config.active_model = "amazon.nova-pro-v1:0"
        mock_config.data_provider = "aws_redshift"
        mock_config.aws_region = "us-west-2"
        mock_config.redshift_workgroup_name = "my-workgroup"
        mock_config.redshift_cluster_identifier = None

        config_manager = ConfigManager()
        config_manager.load = Mock(return_value=mock_config)

        assert config_manager.needs_setup() is False

    @patch("chuck_data.config.ChuckConfig")
    def test_needs_setup_redshift_complete_with_cluster(self, mock_config_class):
        """Test that needs_setup returns False for complete Redshift config with cluster."""
        mock_config = Mock(spec=ChuckConfig)
        mock_config.amperity_token = "valid-token"
        mock_config.active_model = "amazon.nova-pro-v1:0"
        mock_config.data_provider = "aws_redshift"
        mock_config.aws_region = "us-west-2"
        mock_config.redshift_workgroup_name = None
        mock_config.redshift_cluster_identifier = "my-cluster"

        config_manager = ConfigManager()
        config_manager.load = Mock(return_value=mock_config)

        assert config_manager.needs_setup() is False

    @patch("chuck_data.config.ChuckConfig")
    def test_needs_setup_redshift_missing_region(self, mock_config_class):
        """Test that needs_setup returns True when Redshift region is missing."""
        mock_config = Mock(spec=ChuckConfig)
        mock_config.amperity_token = "valid-token"
        mock_config.active_model = "amazon.nova-pro-v1:0"
        mock_config.data_provider = "aws_redshift"
        mock_config.aws_region = None
        mock_config.redshift_workgroup_name = "my-workgroup"
        mock_config.redshift_cluster_identifier = None

        config_manager = ConfigManager()
        config_manager.load = Mock(return_value=mock_config)

        assert config_manager.needs_setup() is True

    @patch("chuck_data.config.ChuckConfig")
    def test_needs_setup_redshift_missing_cluster_and_workgroup(
        self, mock_config_class
    ):
        """Test that needs_setup returns True when both cluster and workgroup are missing."""
        mock_config = Mock(spec=ChuckConfig)
        mock_config.amperity_token = "valid-token"
        mock_config.active_model = "amazon.nova-pro-v1:0"
        mock_config.data_provider = "aws_redshift"
        mock_config.aws_region = "us-west-2"
        mock_config.redshift_workgroup_name = None
        mock_config.redshift_cluster_identifier = None

        config_manager = ConfigManager()
        config_manager.load = Mock(return_value=mock_config)

        assert config_manager.needs_setup() is True

    @patch("chuck_data.config.ChuckConfig")
    def test_needs_setup_redshift_ignores_databricks_tokens(self, mock_config_class):
        """Test that Redshift setup doesn't check for Databricks tokens."""
        mock_config = Mock(spec=ChuckConfig)
        mock_config.amperity_token = "valid-token"
        mock_config.active_model = "amazon.nova-pro-v1:0"
        mock_config.data_provider = "aws_redshift"
        mock_config.aws_region = "us-west-2"
        mock_config.redshift_workgroup_name = "my-workgroup"
        mock_config.redshift_cluster_identifier = None
        # Databricks fields are not set
        mock_config.databricks_token = None
        mock_config.workspace_url = None

        config_manager = ConfigManager()
        config_manager.load = Mock(return_value=mock_config)

        # Should not need setup even though Databricks fields are missing
        assert config_manager.needs_setup() is False

    @patch("chuck_data.config.ChuckConfig")
    def test_needs_setup_databricks_ignores_redshift_config(self, mock_config_class):
        """Test that Databricks setup doesn't check for Redshift configs."""
        mock_config = Mock(spec=ChuckConfig)
        mock_config.amperity_token = "valid-token"
        mock_config.active_model = "amazon.nova-pro-v1:0"
        mock_config.data_provider = "databricks"
        mock_config.workspace_url = "https://my-workspace.cloud.databricks.com"
        mock_config.databricks_token = "dapi12345"
        # Redshift fields are not set
        mock_config.aws_region = None
        mock_config.redshift_workgroup_name = None
        mock_config.redshift_cluster_identifier = None

        config_manager = ConfigManager()
        config_manager.load = Mock(return_value=mock_config)

        # Should not need setup even though Redshift fields are missing
        assert config_manager.needs_setup() is False

    @patch("chuck_data.config.ChuckConfig")
    def test_needs_setup_unknown_provider(self, mock_config_class):
        """Test that needs_setup returns True for unknown provider."""
        mock_config = Mock(spec=ChuckConfig)
        mock_config.amperity_token = "valid-token"
        mock_config.active_model = "amazon.nova-pro-v1:0"
        mock_config.data_provider = "unknown_provider"

        config_manager = ConfigManager()
        config_manager.load = Mock(return_value=mock_config)

        assert config_manager.needs_setup() is True
