"""
Commands for user authentication with Amperity, Databricks, and Redshift.
"""

import logging
from typing import Any, Optional

from chuck_data.clients.amperity import AmperityAPIClient
from chuck_data.clients.databricks import DatabricksAPIClient
from chuck_data.clients.redshift import RedshiftAPIClient
from chuck_data.commands.base import CommandResult
from chuck_data.command_registry import CommandDefinition
from chuck_data.config import (
    set_databricks_token,
    set_redshift_connection,
    clear_redshift_connection,
)


def handle_amperity_login(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """Handle the login command for Amperity."""
    auth_client = AmperityAPIClient()
    success, message = auth_client.start_auth()
    if not success:
        return CommandResult(False, message=f"Login failed: {message}")

    # Wait for auth completion
    success, message = auth_client.wait_for_auth_completion()
    if not success:
        return CommandResult(False, message=f"Login failed: {message}")

    return CommandResult(True, message=message)


def handle_databricks_login(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """Handle the login command for Databricks."""
    token = kwargs.get("token")
    if not token:
        return CommandResult(False, message="Token parameter is required")

    # Save token to config
    try:
        set_databricks_token(token)
        logging.info("Databricks token set successfully")
        return CommandResult(True, message="Databricks token set successfully")
    except Exception as e:
        logging.error("Failed to set Databricks token: %s", e)
        return CommandResult(False, message=f"Failed to set token: {e}")


def handle_redshift_login(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """
    Handle the login command for Redshift.

    Sets up Redshift connection configuration including credentials and
    IAM role for Spark-Redshift connector.
    """
    # Get required parameters
    region = kwargs.get("region")
    cluster_identifier = kwargs.get("cluster_identifier")
    workgroup_name = kwargs.get("workgroup_name")
    user = kwargs.get("user")
    password = kwargs.get("password")
    database = kwargs.get("database", "dev")
    iam_role = kwargs.get("iam_role")
    s3_temp_dir = kwargs.get("s3_temp_dir")
    port = kwargs.get("port", 5439)

    # Validate required parameters
    if not region:
        return CommandResult(False, message="region parameter is required")

    if not cluster_identifier and not workgroup_name:
        return CommandResult(
            False,
            message="Either cluster_identifier or workgroup_name parameter is required",
        )

    if not user:
        return CommandResult(False, message="user parameter is required")

    if not iam_role:
        return CommandResult(
            False,
            message="iam_role parameter is required (for Spark-Redshift connector)",
        )

    if not s3_temp_dir:
        return CommandResult(
            False,
            message="s3_temp_dir parameter is required (for Spark-Redshift staging)",
        )

    try:
        # Determine host from cluster_identifier or workgroup_name
        if cluster_identifier:
            # Provisioned cluster
            host = f"{cluster_identifier}.{region}.redshift.amazonaws.com"
        else:
            # Serverless workgroup
            # Get workgroup details to construct endpoint
            # For now, user can provide host directly, or we construct it
            host = f"{workgroup_name}.{region}.redshift-serverless.amazonaws.com"

        # Test connection by creating client and validating
        test_client = RedshiftAPIClient(
            region=region,
            cluster_identifier=cluster_identifier,
            workgroup_name=workgroup_name,
            database=database,
        )

        if not test_client.validate_connection():
            return CommandResult(
                False,
                message="Failed to validate Redshift connection. Please check your credentials and network access.",
            )

        # Save configuration
        set_redshift_connection(
            region=region,
            cluster_identifier=cluster_identifier,
            workgroup_name=workgroup_name,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            iam_role=iam_role,
            s3_temp_dir=s3_temp_dir,
        )

        logging.info("Redshift connection configured successfully")

        connection_type = "cluster" if cluster_identifier else "serverless workgroup"
        auth_method = "IAM authentication" if not password else "username/password"
        message = (
            f"Redshift connection configured successfully\n"
            f"  Type: {connection_type}\n"
            f"  Host: {host}\n"
            f"  Region: {region}\n"
            f"  Database: {database}\n"
            f"  User: {user}\n"
            f"  Auth: {auth_method}\n"
            f"  IAM Role: {iam_role}\n"
            f"  S3 Temp Dir: {s3_temp_dir}"
        )

        return CommandResult(True, message=message)

    except Exception as e:
        logging.error("Failed to configure Redshift connection: %s", e)
        return CommandResult(
            False, message=f"Failed to configure Redshift connection: {e}"
        )


def handle_logout(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """Handle the logout command for Amperity, Databricks, or Redshift."""
    service = kwargs.get("service", "amperity")

    if service in ["all", "databricks"]:
        try:
            set_databricks_token("")
            logging.info("Databricks token cleared")
        except Exception as e:
            logging.error("Error clearing Databricks token: %s", e)
            return CommandResult(False, message=f"Error clearing Databricks token: {e}")

    if service in ["all", "redshift"]:
        try:
            clear_redshift_connection()
            logging.info("Redshift connection cleared")
        except Exception as e:
            logging.error("Error clearing Redshift connection: %s", e)
            return CommandResult(
                False, message=f"Error clearing Redshift connection: {e}"
            )

    if service in ["all", "amperity"]:
        from chuck_data.config import set_amperity_token

        try:
            set_amperity_token("")
            logging.info("Amperity token cleared")
        except Exception as e:
            logging.error("Error clearing Amperity token: %s", e)
            return CommandResult(False, message=f"Error clearing Amperity token: {e}")

    return CommandResult(True, message=f"Successfully logged out from {service}")


DEFINITION = [
    CommandDefinition(
        name="amperity-login",
        description="Log in to Amperity",
        handler=handle_amperity_login,
        parameters={},
        required_params=[],
        tui_aliases=["/login", "/amperity-login"],
        needs_api_client=False,
        visible_to_user=True,
        visible_to_agent=False,
    ),
    CommandDefinition(
        name="databricks-login",
        description="Set Databricks API token",
        handler=handle_databricks_login,
        parameters={
            "token": {"type": "string", "description": "Your Databricks API token"}
        },
        required_params=["token"],
        tui_aliases=["/databricks-login", "/set-token"],
        needs_api_client=False,
        visible_to_user=True,
        visible_to_agent=False,
    ),
    CommandDefinition(
        name="redshift-login",
        description="Configure Redshift connection with credentials and IAM role for Spark-Redshift connector. Supports both provisioned clusters and serverless.",
        handler=handle_redshift_login,
        parameters={
            "region": {
                "type": "string",
                "description": "AWS region (e.g., 'us-west-2')",
            },
            "cluster_identifier": {
                "type": "string",
                "description": "Redshift cluster identifier (for provisioned clusters). Either this or workgroup_name is required.",
            },
            "workgroup_name": {
                "type": "string",
                "description": "Redshift Serverless workgroup name. Either this or cluster_identifier is required.",
            },
            "user": {"type": "string", "description": "Redshift username"},
            "password": {
                "type": "string",
                "description": "Redshift password (optional - uses IAM auth if not provided)",
            },
            "database": {
                "type": "string",
                "description": "Default database name (default: 'dev')",
            },
            "iam_role": {
                "type": "string",
                "description": "IAM role ARN for Spark-Redshift connector (e.g., 'arn:aws:iam::123:role/RedshiftRole')",
            },
            "s3_temp_dir": {
                "type": "string",
                "description": "S3 temp directory for Spark-Redshift staging (e.g., 's3://bucket/temp/')",
            },
            "port": {"type": "integer", "description": "Redshift port (default: 5439)"},
        },
        required_params=["region", "user", "iam_role", "s3_temp_dir"],
        tui_aliases=["/redshift-login"],
        needs_api_client=False,
        visible_to_user=True,
        visible_to_agent=False,
    ),
    CommandDefinition(
        name="logout",
        description="Log out from Amperity (default), Databricks, Redshift, or all services",
        handler=handle_logout,
        parameters={
            "service": {
                "type": "string",
                "description": "Service to log out from (amperity, databricks, redshift, or all)",
                "default": "amperity",
            }
        },
        required_params=[],
        tui_aliases=["/logout"],
        needs_api_client=False,
        visible_to_user=True,
        visible_to_agent=False,
    ),
]
