"""Application configuration via environment variables."""
import json
from functools import lru_cache
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings


class TelegramSourceConfig:
    """Config for a single Telegram source."""

    def __init__(self, key: str, chat_id: int, source_type: str):
        self.key = key
        self.chat_id = chat_id
        self.type = source_type  # "forum", "chat", "channel"


class Settings(BaseSettings):
    # Telethon (user account)
    telegram_api_id: int = Field(default=37231632)
    telegram_api_hash: str = Field(default="")
    telegram_session_string: str = Field(default="")

    # Dropbox
    dropbox_app_key: str = Field(default="")
    dropbox_app_secret: str = Field(default="")
    dropbox_refresh_token: str = Field(default="")
    dropbox_vault_path: str = Field(
        default="/Приложения/remotely-save/vault_copy"
    )

    # LMS
    lms_base_url: str = Field(default="https://learn.aimindset.org")
    lms_auth_token: str = Field(default="")

    # Telegram sources (JSON array)
    tg_sources: str = Field(default='[]')

    # Vault paths (relative to dropbox_vault_path)
    obsidian_chats_folder: str = Field(
        default="20 Projects/AI_Mindset/W26 Winter 2026/Чаты"
    )
    obsidian_lms_folder: str = Field(
        default="20 Projects/AI_Mindset/W26 Winter 2026"
    )
    obsidian_sprints_folder: str = Field(
        default="20 Projects/AI_Mindset/W26 Winter 2026/Sprints"
    )
    obsidian_materials_folder: str = Field(
        default="20 Projects/AI_Mindset/W26 Winter 2026/База знаний"
    )
    obsidian_kb_folder: str = Field(
        default="20 Projects/AI_Mindset/W26 Winter 2026/База знаний"
    )

    # State file path (relative to dropbox_vault_path)
    state_file_path: str = Field(
        default=".knowledge-capture-state.json"
    )

    # Polling intervals (seconds)
    telegram_poll_interval: int = Field(default=7200)  # 2 hours
    lms_poll_interval: int = Field(default=21600)  # 6 hours

    # Pipeline (topic extraction)
    anthropic_api_key: str = Field(default="")
    pipeline_folder: str = Field(
        default="20 Projects/AI_Agents/Pipeline"
    )
    topic_extraction_enabled: bool = Field(default=True)
    min_messages_for_extraction: int = Field(default=10)

    # Digest notifications (via Tvorets bot)
    tvorets_bot_token: str = Field(default="")
    digest_admin_id: int = Field(default=1075126)
    digest_morning_hour: int = Field(default=9)   # Novosibirsk time
    digest_evening_hour: int = Field(default=21)   # Novosibirsk time
    digest_enabled: bool = Field(default=True)

    # Logging
    log_level: str = Field(default="INFO")

    def get_tg_sources(self) -> list[TelegramSourceConfig]:
        """Parse TG_SOURCES JSON into list of source configs."""
        try:
            sources_raw = json.loads(self.tg_sources)
            return [
                TelegramSourceConfig(
                    key=s["key"],
                    chat_id=s["chat_id"],
                    source_type=s["type"],
                )
                for s in sources_raw
            ]
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Invalid TG_SOURCES config: {e}")

    def validate_required(self) -> list[str]:
        """Check required fields, return list of errors."""
        errors = []
        if not self.telegram_api_hash:
            errors.append("TELEGRAM_API_HASH is required")
        if not self.telegram_session_string:
            errors.append("TELEGRAM_SESSION_STRING is required")
        if not self.dropbox_app_key:
            errors.append("DROPBOX_APP_KEY is required")
        if not self.dropbox_app_secret:
            errors.append("DROPBOX_APP_SECRET is required")
        if not self.dropbox_refresh_token:
            errors.append("DROPBOX_REFRESH_TOKEN is required")
        return errors

    model_config = {"env_file": ".env", "case_sensitive": False}


@lru_cache()
def get_settings() -> Settings:
    return Settings()
