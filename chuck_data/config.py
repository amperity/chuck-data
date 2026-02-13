"""
Configuration management for Chuck TUI using Pydantic for schema validation.
"""

import json
import os
import logging
import tempfile
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from chuck_data.databricks.url_utils import validate_workspace_url


class ChuckConfig(BaseModel):
    """Pydantic model for Chuck configuration"""

    workspace_url: Optional[str] = Field(
        default=None, description="Databricks workspace URL"
    )
    active_model: Optional[str] = Field(
        default=None, description="Currently active model name"
    )
    warehouse_id: Optional[str] = Field(
        default=None, description="SQL warehouse ID for table operations"
    )
    active_catalog: Optional[str] = Field(
        default=None, description="Currently active Unity Catalog"
    )
    active_schema: Optional[str] = Field(
        default=None, description="Currently active schema"
    )
    amperity_token: Optional[str] = Field(
        default=None, description="Amperity authentication token"
    )
    databricks_token: Optional[str] = Field(
        default=None, description="Databricks API token for authentication"
    )
    usage_tracking_consent: Optional[bool] = Field(
        default=False, description="User consent for usage tracking"
    )
    llm_provider: Optional[str] = Field(
        default="databricks",
        description="LLM provider to use (databricks, aws_bedrock, openai, anthropic)",
    )
    llm_provider_config: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description="Provider-specific configuration (nested dict by provider name)",
    )
    data_provider: Optional[str] = Field(
        default=None,
        description="Data provider type (databricks, aws_redshift)",
    )
    compute_provider: Optional[str] = Field(
        default=None,
        description="Compute provider type (databricks, emr)",
    )

    # No validator - use defaults instead of failing

    model_config = {
        # Allow extra fields for backward compatibility
        "extra": "allow"
    }


class ConfigManager:
    """Configuration manager for Chuck"""

    # Class variable for singleton pattern
    _instance = None
    # Track instances by config path to support testing with different paths
    _instances_by_path = {}

    @classmethod
    def _resolve_config_path(cls, config_path: Optional[str] = None) -> str:
        """Resolve configuration file path with precedence: parameter > CHUCK_CONFIG_PATH > default.

        Args:
            config_path: Explicit path provided by caller

        Returns:
            Resolved configuration file path
        """
        if config_path is not None:
            return config_path

        # Check for environment variable first
        env_path = os.getenv("CHUCK_CONFIG_PATH")
        if env_path is not None:
            return env_path

        # Fall back to default
        return os.path.join(os.path.expanduser("~"), ".chuck_config.json")

    def __new__(cls, config_path: Optional[str] = None):
        """Singleton pattern that also respects different config paths for testing.

        Configuration file path resolution precedence:
        1. config_path parameter (highest priority)
        2. CHUCK_CONFIG_PATH environment variable
        3. ~/.chuck_config.json (default)
        """
        config_path = cls._resolve_config_path(config_path)

        # For testing, allow different instances with different paths
        if config_path in cls._instances_by_path:
            return cls._instances_by_path[config_path]

        if cls._instance is None or config_path not in cls._instances_by_path:
            instance = super(ConfigManager, cls).__new__(cls)

            # Only set as main instance if we don't have one yet
            if cls._instance is None:
                cls._instance = instance

            # Track by path for testing support
            cls._instances_by_path[config_path] = instance
            instance._initialized = False
            return instance

        return cls._instance

    def __init__(self, config_path: Optional[str] = None):
        """Initialize config manager with optional custom path"""
        if getattr(self, "_initialized", False):
            return

        self.config_path = self._resolve_config_path(config_path)

        self._config: Optional[ChuckConfig] = None
        self._initialized = True

    def load(self) -> ChuckConfig:
        """Load configuration from file or create default"""
        # Don't cache in tests (always reload)
        if not self.config_path.startswith(tempfile.gettempdir()):
            if self._config:
                return self._config

        config_data = {}
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, "r") as f:
                    config_data = json.load(f)
                    logging.debug(f"Loaded configuration from {self.config_path}")
            except json.JSONDecodeError:
                logging.error("Config file is corrupted. Using default config.")
            except Exception as e:
                logging.error(f"Error loading config: {e}")

        # Override with environment variables if available
        env_mappings = {
            "workspace_url": ["CHUCK_WORKSPACE_URL"],
            "active_model": ["CHUCK_ACTIVE_MODEL"],
            "warehouse_id": ["CHUCK_WAREHOUSE_ID"],
            "active_catalog": ["CHUCK_ACTIVE_CATALOG"],
            "active_schema": ["CHUCK_ACTIVE_SCHEMA"],
            "amperity_token": ["CHUCK_AMPERITY_TOKEN"],
            "databricks_token": ["CHUCK_DATABRICKS_TOKEN"],
            "usage_tracking_consent": ["CHUCK_USAGE_TRACKING_CONSENT"],
            "llm_provider": ["CHUCK_LLM_PROVIDER"],
        }

        for field, env_vars in env_mappings.items():
            for env_var in env_vars:
                value = os.getenv(env_var)
                if value is not None:
                    # Handle boolean conversion for usage_tracking_consent
                    if field == "usage_tracking_consent":
                        config_data[field] = value.lower() in ("true", "1", "yes", "on")
                    else:
                        config_data[field] = value
                    logging.debug(f"Using {field} from environment variable {env_var}")
                    break

        # Create Pydantic model instance
        self._config = ChuckConfig(**config_data)
        return self._config

    def save(self) -> bool:
        """Save configuration to file"""
        if not self._config:
            return False

        try:
            # Ensure directory exists
            directory = os.path.dirname(self.config_path)
            if directory:  # Check if directory is not empty
                os.makedirs(directory, exist_ok=True)

            # Write config
            with open(self.config_path, "w") as f:
                json.dump(self._config.model_dump(), f, indent=2)
            logging.debug(f"Saved configuration to {self.config_path}")
            return True
        except Exception as e:
            logging.error(f"Error saving config: {e}")
            return False

    def get_config(self) -> ChuckConfig:
        """Get configuration object"""
        return self.load()

    def needs_setup(self) -> bool:
        """Check if first-time setup is needed based on missing critical configuration.

        This function is provider-aware and only checks for configs relevant to the
        configured data provider. If no provider is set, it returns True (needs setup).
        """
        config = self.load()

        # FIRST: Check if logged in (amperity_token)
        # This is required regardless of provider
        if not config.amperity_token or config.amperity_token == "":
            return True

        # Always check for model
        if not config.active_model:
            return True

        # If no data provider is set, we definitely need setup
        provider = config.data_provider
        if not provider:
            return True

        # Check provider-specific configs
        if provider == "databricks":
            # Databricks requires workspace_url and databricks_token
            workspace_url = getattr(config, "workspace_url", None)
            databricks_token = getattr(config, "databricks_token", None)

            critical_configs = [workspace_url, databricks_token]
            return any(item is None or item == "" for item in critical_configs)

        elif provider == "aws_redshift":
            # Redshift requires AWS configs and (optionally) amperity_token
            # Check required AWS configs
            aws_region = getattr(config, "aws_region", None)
            if not aws_region or aws_region == "":
                return True

            # Either cluster_identifier or workgroup_name must be set
            cluster_id = getattr(config, "redshift_cluster_identifier", None)
            workgroup = getattr(config, "redshift_workgroup_name", None)

            has_cluster_or_workgroup = (cluster_id and cluster_id != "") or (
                workgroup and workgroup != ""
            )
            if not has_cluster_or_workgroup:
                return True

            return False

        # Unknown provider - needs setup
        return True

    def update(self, **kwargs) -> bool:
        """Update configuration values"""
        config = self.load()

        # Handle workspace_url validation using the new utility
        if "workspace_url" in kwargs:
            is_valid, _ = validate_workspace_url(kwargs["workspace_url"])
            if not is_valid:
                logging.warning("Invalid workspace_url provided, using default instead")
                # Remove invalid workspace_url so default is maintained
                kwargs.pop("workspace_url")

        # Set values
        # Note: We use setattr directly instead of checking hasattr because the model
        # has extra="allow", which allows dynamic fields like aws_account_id, aws_region, etc.
        for key, value in kwargs.items():
            setattr(config, key, value)

        return self.save()


# Global config manager instance
_config_manager = ConfigManager()

# API functions for backward compatibility


def get_workspace_url():
    return _config_manager.get_config().workspace_url


def set_workspace_url(workspace_url):
    """Set the workspace URL in config after validation and normalization.

    Args:
        workspace_url: URL of the Databricks workspace

    Returns:
        True if successful, False otherwise
    """
    is_valid, error_message = validate_workspace_url(workspace_url)
    if not is_valid:
        logging.error(f"Invalid workspace URL: {error_message}")
        return False

    # Normalize the URL before saving

    return _config_manager.update(workspace_url=workspace_url)


def get_amperity_token():
    """Get the Amperity token from config or environment."""
    token = _config_manager.get_config().amperity_token

    # Fall back to environment variable
    if not token:
        token = os.getenv("CHUCK_AMPERITY_TOKEN")
        if token:
            logging.debug("Using Amperity token from environment variable")

    return token


def set_amperity_token(token):
    """Set the Amperity token in config."""
    return _config_manager.update(amperity_token=token)


def get_active_model():
    """Get the active model from config."""
    return _config_manager.get_config().active_model


def set_active_model(model_name):
    """Set the active model in config and clear agent history when changed."""
    current_model = get_active_model()
    result = _config_manager.update(active_model=model_name)
    if current_model != model_name:
        clear_agent_history()
    return result


def get_llm_provider():
    """Get the LLM provider from config."""
    return _config_manager.get_config().llm_provider


def set_llm_provider(provider_name):
    """Set the LLM provider in config."""
    return _config_manager.update(llm_provider=provider_name)


def get_warehouse_id():
    """Get the warehouse ID from config."""
    return _config_manager.get_config().warehouse_id


def set_warehouse_id(warehouse_id):
    """Set the warehouse ID in config."""
    return _config_manager.update(warehouse_id=warehouse_id)


def get_active_catalog():
    """Get the active catalog from config."""
    return _config_manager.get_config().active_catalog


def set_active_catalog(catalog_name):
    """Set the active catalog in config."""
    return _config_manager.update(active_catalog=catalog_name)


def get_active_schema():
    """Get the active schema from config."""
    return _config_manager.get_config().active_schema


def set_active_schema(schema_name):
    """Set the active schema in config."""
    return _config_manager.update(active_schema=schema_name)


def get_databricks_token():
    """Get the Databricks token from config."""
    return _config_manager.get_config().databricks_token


def set_databricks_token(token):
    """Set the Databricks token in config."""
    return _config_manager.update(databricks_token=token)


def get_active_database():
    """Get the active Redshift database from config."""
    config = _config_manager.get_config()
    return getattr(config, "redshift_database", None)


def set_active_database(database_name):
    """Set the active Redshift database in config."""
    return _config_manager.update(redshift_database=database_name)


def get_aws_region():
    """Get AWS region from config."""
    return getattr(_config_manager.get_config(), "aws_region", None)


def get_aws_account_id():
    """Get AWS Account ID from config."""
    return getattr(_config_manager.get_config(), "aws_account_id", None)


def set_aws_account_id(account_id):
    """Set AWS Account ID in config."""
    return _config_manager.update(aws_account_id=account_id)


def get_redshift_cluster_identifier():
    """Get Redshift cluster identifier from config."""
    return getattr(_config_manager.get_config(), "redshift_cluster_identifier", None)


def get_redshift_workgroup_name():
    """Get Redshift Serverless workgroup name from config."""
    return getattr(_config_manager.get_config(), "redshift_workgroup_name", None)


def get_redshift_iam_role():
    """Get Redshift IAM role ARN from config."""
    return getattr(_config_manager.get_config(), "redshift_iam_role", None)


def get_redshift_s3_temp_dir():
    """Get S3 temp directory for Redshift operations from config."""
    return getattr(_config_manager.get_config(), "redshift_s3_temp_dir", None)


def get_databricks_instance_profile_arn():
    """Get Databricks instance profile ARN for AWS access from config."""
    return getattr(
        _config_manager.get_config(), "databricks_instance_profile_arn", None
    )


def get_data_provider():
    """Get the data provider from config (databricks, aws_redshift, etc.)."""
    config = _config_manager.get_config()
    return getattr(config, "data_provider", None)


def set_data_provider(provider: str):
    """Set the data provider and reset related configuration.

    When switching data providers, automatically clears provider-specific config:
    - Switching to Databricks: clears redshift-specific config, sets default schema
    - Switching to Redshift: clears databricks-specific config (catalogs), sets public schema

    Args:
        provider: Data provider type ("databricks", "aws_redshift")
    """
    current_provider = get_data_provider()

    # If provider hasn't changed, just update it
    if current_provider == provider:
        return _config_manager.update(data_provider=provider)

    # Provider is changing - reset related configuration
    updates = {"data_provider": provider}

    if provider == "databricks":
        # Switching to Databricks - clear Redshift-specific config
        updates.update(
            {
                "active_schema": None,  # Let user select from actual schemas
                # Keep active_catalog as user may have set it for Databricks
            }
        )
        # Clear any Redshift-specific config attributes
        config = _config_manager.get_config()
        redshift_attrs = [
            "redshift_cluster_identifier",
            "redshift_workgroup_name",
            "redshift_database",
            "redshift_iam_role",
            "redshift_s3_temp_dir",
            "aws_region",
            "aws_access_key_id",
            "aws_secret_access_key",
        ]
        for attr in redshift_attrs:
            if hasattr(config, attr):
                updates[attr] = None

    elif provider == "aws_redshift":
        # Switching to Redshift - clear Databricks-specific config
        updates.update(
            {
                "active_catalog": None,  # Redshift doesn't use Unity Catalog
                "active_schema": None,  # Let user select from actual schemas
                "warehouse_id": None,  # Different warehouse concept
            }
        )

    return _config_manager.update(**updates)


def get_compute_provider():
    """Get the compute provider from config (databricks, emr, etc.)."""
    config = _config_manager.get_config()
    return getattr(config, "compute_provider", None)


def set_compute_provider(provider: str):
    """Set the compute provider in config.

    Args:
        provider: Compute provider type ("databricks", "emr")
    """
    return _config_manager.update(compute_provider=provider)


# For direct access to config manager
def get_config_manager():
    """Get the global config manager instance"""
    return _config_manager


# ---- Agent conversation history management ----
_agent_history = []


def get_agent_history():
    """Get current agent conversation history."""
    return _agent_history.copy()


def set_agent_history(history):
    """Set the agent conversation history."""
    global _agent_history
    _agent_history = history


def clear_agent_history():
    """Clear the agent conversation history."""
    global _agent_history
    _agent_history = []


# ---- Usage tracking consent management ----


def get_usage_tracking_consent():
    """Get the usage tracking consent status."""
    return _config_manager.get_config().usage_tracking_consent


def set_usage_tracking_consent(consent: bool):
    """Set the usage tracking consent status.

    Args:
        consent: Boolean indicating whether user consents to usage tracking

    Returns:
        True if successful, False otherwise
    """
    return _config_manager.update(usage_tracking_consent=consent)


def get_s3_bucket():
    """Get the S3 bucket from config."""
    config = _config_manager.get_config()
    # Access via __dict__ since s3_bucket is not in the Pydantic schema but allowed via "extra": "allow"
    return getattr(config, "s3_bucket", None)


def get_emr_cluster_id():
    """Get the EMR cluster ID from config."""
    config = _config_manager.get_config()
    return getattr(config, "emr_cluster_id", None)


def get_aws_profile():
    """Get the AWS profile name from config."""
    config = _config_manager.get_config()
    return getattr(config, "aws_profile", None)
