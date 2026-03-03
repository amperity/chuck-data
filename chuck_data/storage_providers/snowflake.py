"""Snowflake Storage Provider.

Uploads artifacts (manifests, init scripts) to a Snowflake internal stage using
the Snowflake PUT command. This is the natural storage mechanism for data stored
in Snowflake — the stage lives alongside the data in the same Snowflake account.

Stage path format: @{database}.{schema}.CHUCK_STITCH_STAGE/{path}

The stitch-standalone JAR reads the manifest from the stage using the Snowflake
connector at job startup (see manifest_io.clj snowflake stage handling).
"""

import logging
import os
import tempfile
from typing import Optional

logger = logging.getLogger(__name__)

# Name of the Snowflake internal stage created/used for Stitch artifacts
CHUCK_STAGE_NAME = "CHUCK_STITCH_STAGE"


class SnowflakeStorageProvider:
    """Upload job artifacts to a Snowflake internal stage.

    Creates a named internal stage in the Snowflake account (once, idempotently)
    and uploads files using Snowflake's PUT command. The stage path returned is
    in the format:
        @{database}.{schema}.CHUCK_STITCH_STAGE/{relative_path}

    This path is passed to stitch-standalone's generic_main as the manifest
    location. The JAR reads the manifest via the Snowflake connector at startup.
    """

    def __init__(
        self,
        account: str,
        user: str,
        database: str,
        schema: str,
        warehouse: str,
        role: Optional[str] = None,
        password: Optional[str] = None,
        private_key_path: Optional[str] = None,
    ):
        if not account or not user:
            raise ValueError(
                "SnowflakeStorageProvider requires 'account' and 'user'. "
                "Run /setup to configure Snowflake credentials."
            )
        if not database or not schema:
            raise ValueError(
                "SnowflakeStorageProvider requires 'database' and 'schema' for the stage location. "
                "Use /select-database and /select-schema, or pass --targets DB.SCHEMA to setup_stitch."
            )

        self.account = account
        self.user = user
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.role = role
        self._password = password
        self._private_key_path = private_key_path
        self._stage_fqn = f"{database}.{schema}.{CHUCK_STAGE_NAME}"
        self._stage_ensured = False

    def _get_connection(self):
        """Open a Snowflake connector connection."""
        import snowflake.connector

        params = {
            "account": self.account,
            "user": self.user,
            "database": self.database,
            "schema": self.schema,
            "warehouse": self.warehouse,
        }
        if self.role:
            params["role"] = self.role
        if self._password:
            params["password"] = self._password
        elif self._private_key_path:
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.backends import default_backend

            with open(self._private_key_path, "rb") as f:
                pk = serialization.load_pem_private_key(
                    f.read(), password=None, backend=default_backend()
                )
            params["private_key"] = pk.private_bytes(
                serialization.Encoding.DER,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        return snowflake.connector.connect(**params)

    def _ensure_stage(self, conn):
        """Create the Snowflake internal stage if it doesn't already exist."""
        if self._stage_ensured:
            return
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE STAGE IF NOT EXISTS {self._stage_fqn} "
                f"COMMENT = 'Chuck Stitch artifacts (manifests, init scripts)'"
            )
        self._stage_ensured = True
        logger.info("Ensured Snowflake stage exists: %s", self._stage_fqn)

    def upload_file(self, content: str, path: str, overwrite: bool = True) -> bool:
        """Upload content to the Snowflake internal stage.

        Args:
            content: File content as a string
            path: Destination path. Can be:
                  - A bare filename:          "manifest.json"
                  - A sub-path:               "chuck/manifests/manifest.json"
                  - A full stage path:        "@db.schema.STAGE/chuck/manifest.json"
            overwrite: Whether to overwrite if the file already exists

        Returns:
            True on success.  The upload path accessible to stitch-standalone is:
                @{self._stage_fqn}/{normalised_path}
        """
        # Normalise the path — strip leading @stage/ prefix if present
        relative = path
        if relative.startswith("@"):
            # e.g. @db.schema.CHUCK_STITCH_STAGE/chuck/manifest.json
            slash = relative.find("/")
            relative = relative[slash + 1 :] if slash != -1 else ""
        relative = relative.lstrip("/")

        conn = self._get_connection()
        try:
            self._ensure_stage(conn)

            # Write content to a temporary local file for the PUT command
            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=os.path.splitext(relative)[-1] or ".json",
                delete=False,
                prefix="chuck_stitch_",
            ) as tmp:
                tmp.write(content)
                tmp_path = tmp.name

            try:
                stage_dest = (
                    f"@{self._stage_fqn}/{os.path.dirname(relative)}"
                    if os.path.dirname(relative)
                    else f"@{self._stage_fqn}"
                )
                overwrite_flag = "TRUE" if overwrite else "FALSE"
                with conn.cursor() as cur:
                    # PUT uploads a local file to the stage
                    cur.execute(
                        f"PUT file://{tmp_path} {stage_dest} "
                        f"OVERWRITE={overwrite_flag} AUTO_COMPRESS=FALSE"
                    )
                logger.info(
                    "Uploaded %s to Snowflake stage %s", relative, self._stage_fqn
                )
                return True
            finally:
                os.unlink(tmp_path)

        except Exception as e:
            logger.error(
                "Failed to upload %s to Snowflake stage %s: %s",
                relative,
                self._stage_fqn,
                e,
                exc_info=True,
            )
            raise Exception(
                f"Failed to upload to Snowflake stage {self._stage_fqn}/{relative}: {e}"
            ) from e
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def stage_path(self, relative: str) -> str:
        """Return the fully-qualified stage path for a relative path.

        This is the path passed to stitch-standalone as the manifest location.
        stitch-standalone's manifest_io.clj handles @stage/ paths.
        """
        return f"@{self._stage_fqn}/{relative.lstrip('/')}"
