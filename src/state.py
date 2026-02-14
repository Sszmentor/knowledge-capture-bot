"""JSON-based state tracker, persisted to Dropbox.

Tracks last synced message IDs for Telegram sources
and content hashes for LMS pages.
"""
import json
import logging
from datetime import datetime
from typing import Optional

from src.clients.dropbox_client import DropboxClient

logger = logging.getLogger(__name__)


class SyncState:
    """State tracker for incremental sync, stored as JSON in Dropbox."""

    def __init__(self, dropbox_client: DropboxClient, vault_path: str, state_file: str):
        self.dbx = dropbox_client
        self.dropbox_path = f"{vault_path}/{state_file}"
        self._data: dict = {}
        self._load()

    def _load(self) -> None:
        """Load state from Dropbox."""
        content = self.dbx.download_text(self.dropbox_path)
        if content:
            try:
                self._data = json.loads(content)
                logger.info(f"State loaded: {len(self._data)} sources tracked")
            except json.JSONDecodeError:
                logger.warning("Corrupted state file, starting fresh")
                self._data = {}
        else:
            logger.info("No state file found, starting fresh")
            self._data = {}

    def _save(self) -> None:
        """Save state to Dropbox."""
        content = json.dumps(self._data, ensure_ascii=False, indent=2)
        self.dbx.upload_file(content, self.dropbox_path, overwrite=True)

    def get_last_msg_id(self, source_key: str) -> int:
        """Get last synced message ID for a source."""
        return self._data.get(source_key, {}).get("last_msg_id", 0)

    def get_content_hash(self, source_key: str) -> Optional[str]:
        """Get stored content hash for LMS page."""
        return self._data.get(source_key, {}).get("content_hash")

    def get_obsidian_path(self, source_key: str) -> Optional[str]:
        """Get Obsidian file path for a source."""
        return self._data.get(source_key, {}).get("obsidian_path")

    def update_telegram(
        self,
        source_key: str,
        name: str,
        last_msg_id: int,
        messages_added: int,
        obsidian_path: str,
    ) -> None:
        """Update state after Telegram sync."""
        if source_key not in self._data:
            self._data[source_key] = {"messages_total": 0}

        entry = self._data[source_key]
        entry["name"] = name
        entry["last_msg_id"] = last_msg_id
        entry["messages_total"] = entry.get("messages_total", 0) + messages_added
        entry["last_check"] = datetime.now().isoformat()
        entry["obsidian_path"] = obsidian_path
        self._save()

    def update_lms(
        self,
        source_key: str,
        content_hash: str,
        obsidian_path: str,
    ) -> None:
        """Update state after LMS sync."""
        if source_key not in self._data:
            self._data[source_key] = {}

        entry = self._data[source_key]
        entry["content_hash"] = content_hash
        entry["last_check"] = datetime.now().isoformat()
        entry["obsidian_path"] = obsidian_path
        self._save()

    def get_all(self) -> dict:
        """Return full state for status endpoint."""
        return self._data.copy()
