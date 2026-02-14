"""Dropbox API client with path-restricted writes.

Based on zoom-transcript-agent/src/dropbox_client.py.
Extended with download_text() for read-modify-write pattern.
"""
import dropbox
from dropbox.files import WriteMode
import logging
from typing import Optional

logger = logging.getLogger(__name__)

ALLOWED_ROOT = "/приложения/remotely-save/vault_copy"


class DropboxPathError(Exception):
    """Write attempt outside allowed folder."""
    pass


class DropboxClient:
    """Dropbox API client with write path restriction."""

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        refresh_token: str,
        allowed_root: str = ALLOWED_ROOT,
    ):
        self.dbx = dropbox.Dropbox(
            app_key=app_key,
            app_secret=app_secret,
            oauth2_refresh_token=refresh_token,
        )
        self.allowed_root = allowed_root.rstrip("/").lower()

        # Force token refresh on init — without this,
        # first API call fails with invalid_access_token
        try:
            self.dbx.check_and_refresh_access_token()
            account = self.dbx.users_get_current_account()
            logger.info(
                f"Dropbox connected as {account.name.display_name} "
                f"(writes restricted to: {self.allowed_root})"
            )
        except Exception as e:
            logger.error(f"Dropbox auth failed: {e}")
            raise

    def _validate_path(self, path: str) -> None:
        normalized = path.lower().rstrip("/")
        if not normalized.startswith(self.allowed_root):
            raise DropboxPathError(
                f"Write forbidden! Path '{path}' outside '{self.allowed_root}'"
            )

    def upload_file(
        self, content: str, dropbox_path: str, overwrite: bool = True
    ) -> Optional[str]:
        """Upload text file to Dropbox."""
        self._validate_path(dropbox_path)
        try:
            mode = WriteMode.overwrite if overwrite else WriteMode.add
            result = self.dbx.files_upload(
                content.encode("utf-8"), dropbox_path, mode=mode
            )
            logger.info(f"Uploaded: {dropbox_path}")
            return result.path_display
        except dropbox.exceptions.ApiError as e:
            logger.error(f"Dropbox API error: {e}")
            return None
        except Exception as e:
            logger.error(f"Upload error: {e}")
            return None

    def download_text(self, dropbox_path: str) -> Optional[str]:
        """Download text file from Dropbox. Returns None if not found."""
        try:
            _, response = self.dbx.files_download(dropbox_path)
            return response.content.decode("utf-8")
        except dropbox.exceptions.ApiError as e:
            if e.error.is_path() and e.error.get_path().is_not_found():
                return None
            logger.error(f"Dropbox download error: {e}")
            return None
        except Exception as e:
            logger.error(f"Download error: {e}")
            return None

    def file_exists(self, dropbox_path: str) -> bool:
        try:
            self.dbx.files_get_metadata(dropbox_path)
            return True
        except dropbox.exceptions.ApiError:
            return False

    def create_folder_if_not_exists(self, folder_path: str) -> bool:
        self._validate_path(folder_path)
        try:
            self.dbx.files_create_folder_v2(folder_path)
            logger.info(f"Created folder: {folder_path}")
            return True
        except dropbox.exceptions.ApiError as e:
            if hasattr(e.error, "is_path") and e.error.is_path():
                return True
            logger.error(f"Folder creation error: {e}")
            return False
