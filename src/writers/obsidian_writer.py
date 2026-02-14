"""Obsidian writer: creates/updates markdown files in Dropbox vault.

Pattern: download existing → append new messages → upload back.
For new files: generate full document with header.
"""
import logging
from typing import Optional

from src.clients.dropbox_client import DropboxClient
from src.sources.telegram_source import TopicMessages
from src.writers.md_formatter import (
    format_full_document,
    format_messages_block,
    get_message_count_from_header,
    update_stats_in_header,
)

logger = logging.getLogger(__name__)


# Topic name → Obsidian file name mapping
# For forum topics, maps topic title to a readable file name
TOPIC_NAME_MAP = {
    "general": "w26 General",
    "General": "w26 General",
    "support": "w26 Support",
    "Support": "w26 Support",
    "intro": "w26 Intro",
    "Intro": "w26 Intro",
    "materials/org": "w26 Materials-Org",
    "Materials/Org": "w26 Materials-Org",
    "Учимся проектировать ПО": "w26 Учимся проектировать ПО",
    "Корпоративный ИИ для команды": "w26 Корпоративный ИИ для команде",
}

# Topic descriptions for new file creation
TOPIC_DESCRIPTIONS = {
    "general": "Основной топик обсуждений лаборатории AI Mindset {w26}.",
    "General": "Основной топик обсуждений лаборатории AI Mindset {w26}.",
    "support": "Техническая поддержка и вопросы по платформе.",
    "Support": "Техническая поддержка и вопросы по платформе.",
    "intro": "Представления участников лаборатории.",
    "Intro": "Представления участников лаборатории.",
    "materials/org": "Организационный топик: расписание, ссылки, материалы.",
    "Materials/Org": "Организационный топик: расписание, ссылки, материалы.",
    "Учимся проектировать ПО": "Топик о проектировании программного обеспечения.",
    "Корпоративный ИИ для команды": "Топик о внедрении ИИ в корпоративные процессы.",
}


def make_safe_filename(title: str) -> str:
    """Convert topic/chat title to a safe filename."""
    return title.replace("/", "-").replace("\\", "-")


class ObsidianWriter:
    """Writes/updates Obsidian markdown files via Dropbox API."""

    def __init__(self, dropbox_client: DropboxClient, vault_path: str, chats_folder: str):
        self.dbx = dropbox_client
        self.vault_path = vault_path.rstrip("/")
        self.chats_folder = chats_folder

    def _get_dropbox_path(self, filename: str) -> str:
        """Build full Dropbox path for a chat file."""
        return f"{self.vault_path}/{self.chats_folder}/{filename}.md"

    def get_obsidian_filename(
        self, source_key: str, topic_title: str
    ) -> str:
        """Determine Obsidian filename for a topic.

        Uses TOPIC_NAME_MAP if available, otherwise generates from title.
        """
        # Check mapping first
        if topic_title in TOPIC_NAME_MAP:
            return TOPIC_NAME_MAP[topic_title]

        # Generate from source key and title
        safe = make_safe_filename(topic_title)

        # For flat chats, use chat name as-is
        if ":" not in source_key or source_key.count(":") == 1:
            return safe

        return safe

    def write_topic(
        self,
        topic: TopicMessages,
        source_key: str,
        source_name: str,
    ) -> Optional[str]:
        """Write or append messages for a topic to Obsidian vault.

        Returns the Obsidian path used (relative to vault), or None on error.
        """
        if not topic.messages:
            return None

        filename = self.get_obsidian_filename(source_key, topic.topic_title)
        dropbox_path = self._get_dropbox_path(filename)
        relative_path = f"{self.chats_folder}/{filename}.md"

        logger.info(
            f"Writing {len(topic.messages)} messages to {filename}"
        )

        # Try to download existing file
        existing = self.dbx.download_text(dropbox_path)

        if existing:
            # APPEND mode: add new messages to existing file
            content = self._append_messages(existing, topic)
        else:
            # CREATE mode: generate full document
            description = TOPIC_DESCRIPTIONS.get(topic.topic_title, "")
            title = f"W26 Чат — {topic.topic_title}"
            if topic.topic_id is None:
                # Flat chat — use source name
                title = source_name

            content = format_full_document(
                messages=topic.messages,
                title=title,
                description=description,
            )

        # Upload
        result = self.dbx.upload_file(content, dropbox_path, overwrite=True)
        if result:
            logger.info(f"Uploaded: {dropbox_path}")
            return relative_path
        else:
            logger.error(f"Failed to upload: {dropbox_path}")
            return None

    def _append_messages(
        self, existing_content: str, topic: TopicMessages
    ) -> str:
        """Append new messages to existing file content."""
        # Update stats in header
        old_count = get_message_count_from_header(existing_content)
        new_total = old_count + len(topic.messages)

        # Get end date from last new message
        last_msg = topic.messages[-1]
        new_end_date = last_msg.date[:10] if last_msg.date else "?"

        content = update_stats_in_header(
            existing_content, new_total, new_end_date
        )

        # Append new messages
        new_block = format_messages_block(topic.messages)
        content = content.rstrip() + "\n" + new_block

        return content
