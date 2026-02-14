"""Topic Extractor — finds interesting blog post topics in chat messages.

Uses Claude Haiku to analyze new Telegram messages and extract
potential topics for the content pipeline.
"""

import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Optional

import anthropic

from src.sources.telegram_source import TelegramMessage

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-20250414"
MAX_MESSAGES_PER_BATCH = 200  # Don't send too many messages at once
MAX_TOPICS_PER_EXTRACTION = 5

EXTRACTION_PROMPT = """\
Ты — аналитик контента. Твоя задача — найти в сообщениях из чата \
темы, которые могут стать интересными блог-постами или постами в Telegram-канал.

## Что ищешь

1. **Инструменты/сервисы** — кто-то попробовал новый инструмент и рассказал о результатах
2. **Техники/подходы** — конкретная методика с результатом (не просто "это круто")
3. **Решения проблем** — что сломалось → как починили → что поняли
4. **Проекты** — кто-то построил что-то конкретное и рассказал как
5. **Инсайты** — неочевидные наблюдения подкреплённые опытом

## Что пропускаешь

- Приветствия, благодарности, оргвопросы
- Просто упоминание инструмента без контекста использования
- Вопросы без ответов
- Реплики короче 2 предложений
- Повторы уже извлечённых тем

## Формат ответа

Верни JSON массив (может быть пустым если ничего интересного нет):

```json
[
  {
    "title": "Краткий заголовок темы (3-8 слов)",
    "summary": "2-3 предложения: о чём тема, почему интересна, какой результат",
    "source_messages": ["Ключевая цитата 1", "Ключевая цитата 2"],
    "tags": ["tag1", "tag2", "tag3"]
  }
]
```

Максимум {max_topics} тем. Лучше 1-2 качественных, чем 5 слабых.
Если ничего стоящего нет — верни пустой массив `[]`.

Отвечай ТОЛЬКО JSON, без комментариев.
"""


@dataclass
class ExtractedTopic:
    """A topic extracted from chat messages."""

    id: str
    title: str
    summary: str
    source_messages: list[str]
    source_chat: str
    source_date: str
    tags: list[str] = field(default_factory=list)
    status: str = "new"  # new → picked → written → published
    created_at: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

    @classmethod
    def from_dict(cls, data: dict) -> "ExtractedTopic":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


class TopicExtractor:
    """Extracts blog post topics from Telegram messages using Claude."""

    def __init__(self, api_key: str):
        self._client = anthropic.AsyncAnthropic(api_key=api_key)

    async def extract_topics(
        self,
        messages: list[TelegramMessage],
        source_chat: str,
        existing_titles: Optional[list[str]] = None,
    ) -> list[ExtractedTopic]:
        """Extract topics from a batch of messages.

        Args:
            messages: New messages to analyze
            source_chat: Chat name for attribution
            existing_titles: Already extracted topic titles (for dedup)

        Returns:
            List of extracted topics (may be empty)
        """
        if not messages:
            return []

        # Format messages for the prompt
        formatted = self._format_messages(messages[:MAX_MESSAGES_PER_BATCH])

        # Build dedup context
        dedup_note = ""
        if existing_titles:
            titles_str = "\n".join(f"- {t}" for t in existing_titles[-20:])
            dedup_note = (
                f"\n\n## Уже извлечённые темы (не дублируй):\n{titles_str}"
            )

        prompt = EXTRACTION_PROMPT.format(max_topics=MAX_TOPICS_PER_EXTRACTION)

        try:
            response = await self._client.messages.create(
                model=MODEL,
                max_tokens=2000,
                system=prompt + dedup_note,
                messages=[
                    {
                        "role": "user",
                        "content": (
                            f"Чат: {source_chat}\n"
                            f"Период: {self._get_date_range(messages)}\n"
                            f"Сообщений: {len(messages)}\n\n"
                            f"{formatted}"
                        ),
                    }
                ],
            )
        except Exception as e:
            logger.error(f"Claude API error during topic extraction: {e}")
            return []

        # Parse response
        raw_text = response.content[0].text.strip()
        return self._parse_response(raw_text, source_chat, messages)

    def _format_messages(self, messages: list[TelegramMessage]) -> str:
        """Format messages for the prompt."""
        lines = []
        for msg in messages:
            if not msg.text or len(msg.text.strip()) < 20:
                continue
            date_str = msg.date[:10] if msg.date else ""
            sender = msg.sender or "?"
            lines.append(f"[{date_str}] {sender}: {msg.text[:500]}")
        return "\n".join(lines)

    def _get_date_range(self, messages: list[TelegramMessage]) -> str:
        """Get date range string from messages."""
        dates = [m.date[:10] for m in messages if m.date]
        if not dates:
            return "неизвестно"
        return f"{min(dates)} — {max(dates)}"

    def _parse_response(
        self,
        raw_text: str,
        source_chat: str,
        messages: list[TelegramMessage],
    ) -> list[ExtractedTopic]:
        """Parse Claude response into ExtractedTopic objects."""
        # Extract JSON from response (handle markdown code blocks)
        json_text = raw_text
        if "```" in json_text:
            # Extract content between ``` markers
            parts = json_text.split("```")
            for part in parts:
                stripped = part.strip()
                if stripped.startswith("json"):
                    stripped = stripped[4:].strip()
                if stripped.startswith("["):
                    json_text = stripped
                    break

        try:
            topics_raw = json.loads(json_text)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse topic extraction response: {raw_text[:200]}")
            return []

        if not isinstance(topics_raw, list):
            return []

        now = datetime.now().isoformat()
        date_range = self._get_date_range(messages)
        source_date = date_range.split(" — ")[-1] if " — " in date_range else date_range

        topics = []
        for item in topics_raw[:MAX_TOPICS_PER_EXTRACTION]:
            if not isinstance(item, dict):
                continue
            title = item.get("title", "").strip()
            if not title:
                continue

            topic = ExtractedTopic(
                id=uuid.uuid4().hex[:12],
                title=title,
                summary=item.get("summary", ""),
                source_messages=item.get("source_messages", []),
                source_chat=source_chat,
                source_date=source_date,
                tags=item.get("tags", []),
                status="new",
                created_at=now,
            )
            topics.append(topic)

        logger.info(
            f"Extracted {len(topics)} topics from {len(messages)} messages "
            f"in {source_chat}"
        )
        return topics
