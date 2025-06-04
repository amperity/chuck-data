"""
Command for uploading files to Databricks volumes or DBFS.
"""

from typing import Optional, Any
from src.clients.databricks import DatabricksAPIClient
from src.commands.base import CommandResult
from src.command_registry import CommandDefinition
import os
import logging


def handle_command(
    client: Optional[DatabricksAPIClient], **kwargs: Any
) -> CommandResult:
    """
    Upload a file to Databricks volumes or DBFS.

    Args:
        client: DatabricksAPIClient instance for API calls
        **kwargs: Command parameters
            - local_path: Path to local file to upload
            - destination_path: Path in Databricks where the file should be uploaded
            - overwrite: Whether to overwrite existing files (optional)
            - use_dbfs: Whether to use DBFS API instead of volumes API (optional)
            - contents: String content to upload instead of a file (optional, mutually exclusive with local_path)

    Returns:
        CommandResult with upload status if successful
    """
    if not client:
        return CommandResult(
            False,
            message="No Databricks client available. Please set up your workspace first.",
        )

    # Extract parameters
    local_path = kwargs.get("local_path")
    destination_path = kwargs.get("destination_path")
    overwrite = kwargs.get("overwrite", False)
    use_dbfs = kwargs.get("use_dbfs", False)
    contents = kwargs.get("contents")

    # Validation
    if local_path and contents:
        return CommandResult(
            False,
            message="You cannot specify both local_path and contents. Choose one method to provide file content.",
        )

    if not local_path and not contents:
        return CommandResult(
            False, message="You must provide either local_path or contents to upload."
        )

    if local_path and not os.path.isfile(local_path):
        return CommandResult(False, message=f"Local file not found: {local_path}")

    try:
        # Choose the appropriate upload method based on parameters
        if use_dbfs:
            if contents:
                client.store_dbfs_file(
                    path=destination_path, contents=contents, overwrite=overwrite
                )
            else:
                with open(local_path, "r") as file:
                    file_contents = file.read()
                client.store_dbfs_file(
                    path=destination_path, contents=file_contents, overwrite=overwrite
                )

            upload_type = "DBFS"
        else:
            # Upload to volumes
            if contents:
                client.upload_file(
                    path=destination_path, content=contents, overwrite=overwrite
                )
            else:
                client.upload_file(
                    path=destination_path, file_path=local_path, overwrite=overwrite
                )

            upload_type = "volumes"

        source = local_path if local_path else "provided content"
        return CommandResult(
            True,
            message=f"Successfully uploaded {source} to {upload_type} path: {destination_path}",
        )
    except Exception as e:
        logging.error(f"Error uploading file: {str(e)}")
        return CommandResult(False, message=f"Failed to upload file: {str(e)}", error=e)


DEFINITION = CommandDefinition(
    name="upload-file",
    description="Upload a file to Databricks volumes or DBFS.",
    handler=handle_command,
    parameters={
        "local_path": {
            "type": "string",
            "description": "Path to local file to upload.",
        },
        "destination_path": {
            "type": "string",
            "description": "Path in Databricks where the file should be uploaded.",
        },
        "contents": {
            "type": "string",
            "description": "String content to upload instead of a file (mutually exclusive with local_path).",
        },
        "overwrite": {
            "type": "boolean",
            "description": "Whether to overwrite existing files.",
            "default": False,
        },
        "use_dbfs": {
            "type": "boolean",
            "description": "Whether to use DBFS API instead of volumes API.",
            "default": False,
        },
    },
    required_params=["destination_path"],
    tui_aliases=["/upload", "/upload-file"],
    needs_api_client=True,
    visible_to_user=True,
    visible_to_agent=True,
    usage_hint="Usage: /upload --local_path <file_path> --destination_path <dbx_path> [--overwrite true|false] [--use_dbfs true|false]\n"
    + 'Or:     /upload --contents "file content" --destination_path <dbx_path> [--overwrite true|false] [--use_dbfs true|false]',
)
