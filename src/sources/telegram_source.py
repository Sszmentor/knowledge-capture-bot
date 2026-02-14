"""Telegram source: incremental message export via Telethon.

Supports forum supergroups (per-topic export), regular chats, and channels.
Uses min_id for incremental sync — only fetches messages newer than last seen.
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetForumTopicsRequest
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument

logger = logging.getLogger(__name__)


@dataclass
class TelegramMessage:
    """Serialized Telegram message."""

    id: int
    date: Optional[str]
    sender: str
    sender_username: str
    text: str
    media: Optional[str]
    reply_to: Optional[int]
    forward_from: Optional[str]
    views: Optional[int]

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "date": self.date,
            "sender": self.sender,
            "sender_username": self.sender_username,
            "text": self.text,
            "media": self.media,
            "reply_to": self.reply_to,
            "forward_from": self.forward_from,
            "views": self.views,
        }


@dataclass
class TopicMessages:
    """Messages from a single forum topic or flat chat."""

    topic_id: Optional[int]
    topic_title: str
    messages: list[TelegramMessage] = field(default_factory=list)
    max_msg_id: int = 0


@dataclass
class SyncResult:
    """Result of a single source sync."""

    source_key: str
    source_name: str
    topics: list[TopicMessages]
    total_new: int = 0


def serialize_msg(msg) -> TelegramMessage:
    """Convert a Telethon Message to TelegramMessage dataclass."""
    sender_name = ""
    if msg.sender:
        sender_name = (
            getattr(msg.sender, "title", None)
            or f"{getattr(msg.sender, 'first_name', '') or ''} "
            f"{getattr(msg.sender, 'last_name', '') or ''}".strip()
        )
    sender_username = getattr(msg.sender, "username", "") if msg.sender else ""

    media_type = None
    if msg.media:
        if isinstance(msg.media, MessageMediaPhoto):
            media_type = "photo"
        elif isinstance(msg.media, MessageMediaDocument):
            doc = msg.media.document
            if doc:
                for attr in doc.attributes:
                    if hasattr(attr, "voice") and attr.voice:
                        media_type = "voice"
                        break
                    elif hasattr(attr, "round_message") and attr.round_message:
                        media_type = "video_note"
                        break
                    elif hasattr(attr, "file_name"):
                        media_type = f"file:{attr.file_name}"
                        break
                if not media_type:
                    media_type = "document"
        else:
            media_type = type(msg.media).__name__

    reply_to_id = None
    if msg.reply_to:
        reply_to_id = getattr(msg.reply_to, "reply_to_msg_id", None)

    fwd_from = None
    if msg.forward:
        fwd_name = ""
        if msg.forward.sender:
            fwd_name = (
                getattr(msg.forward.sender, "title", None)
                or f"{getattr(msg.forward.sender, 'first_name', '') or ''} "
                f"{getattr(msg.forward.sender, 'last_name', '') or ''}".strip()
            )
        fwd_from = fwd_name or str(getattr(msg.forward, "from_id", ""))

    return TelegramMessage(
        id=msg.id,
        date=msg.date.isoformat() if msg.date else None,
        sender=sender_name,
        sender_username=sender_username,
        text=msg.text or "",
        media=media_type,
        reply_to=reply_to_id,
        forward_from=fwd_from,
        views=getattr(msg, "views", None),
    )


class TelegramSource:
    """Telethon-based Telegram source with incremental sync."""

    def __init__(self, api_id: int, api_hash: str, session_string: str):
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_string = session_string
        self._client: Optional[TelegramClient] = None

    async def connect(self) -> None:
        """Connect to Telegram."""
        self._client = TelegramClient(
            StringSession(self.session_string),
            self.api_id,
            self.api_hash,
        )
        await self._client.connect()

        if not await self._client.is_user_authorized():
            raise RuntimeError(
                "Telegram session expired or invalid. "
                "Regenerate TELEGRAM_SESSION_STRING."
            )

        me = await self._client.get_me()
        logger.info(f"Telegram connected as {me.first_name} (@{me.username})")

    async def disconnect(self) -> None:
        """Disconnect from Telegram."""
        if self._client:
            await self._client.disconnect()
            self._client = None

    @property
    def client(self) -> TelegramClient:
        if not self._client:
            raise RuntimeError("TelegramSource not connected. Call connect() first.")
        return self._client

    async def _get_forum_topics(self, chat_id: int) -> list[dict]:
        """Get all forum topics for a supergroup."""
        entity = await self.client.get_entity(chat_id)
        topics = []
        offset_date = 0
        offset_id = 0
        offset_topic = 0

        while True:
            result = await self.client(
                GetForumTopicsRequest(
                    peer=entity,
                    offset_date=offset_date,
                    offset_id=offset_id,
                    offset_topic=offset_topic,
                    limit=100,
                )
            )
            for topic in result.topics:
                topics.append(
                    {"id": topic.id, "title": topic.title}
                )

            if not result.topics or len(result.topics) < 100:
                break

            last = result.topics[-1]
            offset_date = last.date
            offset_id = last.id
            offset_topic = last.id

        return topics

    async def _fetch_topic_messages(
        self, chat_id: int, topic_id: int, min_id: int = 0
    ) -> list[TelegramMessage]:
        """Fetch messages from a forum topic, newer than min_id."""
        messages = []
        async for msg in self.client.iter_messages(
            chat_id, reply_to=topic_id, min_id=min_id, limit=None
        ):
            if msg.text or msg.media:  # skip service messages
                messages.append(serialize_msg(msg))

        messages.sort(key=lambda m: m.date or "")
        return messages

    async def _fetch_flat_messages(
        self, chat_id: int, min_id: int = 0
    ) -> list[TelegramMessage]:
        """Fetch messages from a regular chat/channel, newer than min_id."""
        messages = []
        async for msg in self.client.iter_messages(
            chat_id, min_id=min_id, limit=None
        ):
            if msg.text or msg.media:
                messages.append(serialize_msg(msg))

        messages.sort(key=lambda m: m.date or "")
        return messages

    async def sync_forum(
        self,
        source_key: str,
        chat_id: int,
        get_last_msg_id: callable,
    ) -> SyncResult:
        """Sync a forum supergroup — each topic separately.

        get_last_msg_id: callable(source_key) -> int
            Returns last synced msg_id for a given source_key.
            For forums, source_key includes topic_id:
            e.g. "tg:-1003680190242:177"
        """
        entity = await self.client.get_entity(chat_id)
        chat_name = getattr(entity, "title", str(chat_id))

        topics = await self._get_forum_topics(chat_id)
        logger.info(f"Forum '{chat_name}': {len(topics)} topics")

        result = SyncResult(
            source_key=source_key,
            source_name=chat_name,
            topics=[],
            total_new=0,
        )

        for topic in topics:
            topic_source_key = f"{source_key}:{topic['id']}"
            last_id = get_last_msg_id(topic_source_key)

            messages = await self._fetch_topic_messages(
                chat_id, topic['id'], min_id=last_id
            )

            if messages:
                max_id = max(m.id for m in messages)
                topic_result = TopicMessages(
                    topic_id=topic['id'],
                    topic_title=topic['title'],
                    messages=messages,
                    max_msg_id=max_id,
                )
                result.topics.append(topic_result)
                result.total_new += len(messages)
                logger.info(
                    f"  [{topic['id']}] {topic['title']}: "
                    f"{len(messages)} new (after #{last_id})"
                )
            else:
                logger.debug(
                    f"  [{topic['id']}] {topic['title']}: no new messages"
                )

        return result

    async def sync_flat(
        self,
        source_key: str,
        chat_id: int,
        last_msg_id: int = 0,
    ) -> SyncResult:
        """Sync a regular chat or channel (no forum topics)."""
        entity = await self.client.get_entity(chat_id)
        chat_name = getattr(entity, "title", str(chat_id))

        messages = await self._fetch_flat_messages(chat_id, min_id=last_msg_id)

        result = SyncResult(
            source_key=source_key,
            source_name=chat_name,
            topics=[],
            total_new=len(messages),
        )

        if messages:
            max_id = max(m.id for m in messages)
            result.topics.append(
                TopicMessages(
                    topic_id=None,
                    topic_title=chat_name,
                    messages=messages,
                    max_msg_id=max_id,
                )
            )
            logger.info(
                f"Chat '{chat_name}': {len(messages)} new (after #{last_msg_id})"
            )
        else:
            logger.info(f"Chat '{chat_name}': no new messages")

        return result
