"""Daily digest notifications via Telegram Bot API.

Accumulates sync results between digests, then generates
a summary with Claude analysis and sends it to the admin
via the Tvorets bot.
"""

import json
import logging
import zoneinfo
from datetime import datetime, timedelta
from typing import Optional

import anthropic
import httpx

logger = logging.getLogger(__name__)

NOVOSIBIRSK = zoneinfo.ZoneInfo("Asia/Novosibirsk")
MODEL = "claude-haiku-4-5-20251001"

# ── Accumulators ─────────────────────────────────────────────
_tg_sync_results: list[dict] = []
_lms_sync_results: list[dict] = []
_extracted_topics: list[dict] = []


def record_tg_sync(result: dict) -> None:
    """Record Telegram sync result for digest. Called from main.py."""
    if result.get("total_new_messages", 0) > 0:
        _tg_sync_results.append(result)


def record_lms_sync(result: dict) -> None:
    """Record LMS sync result for digest. Called from main.py."""
    if result.get("updated", 0) > 0:
        _lms_sync_results.append(result)


def record_topics(topics: list[dict]) -> None:
    """Record extracted topics for digest. Called from main.py."""
    _extracted_topics.extend(topics)


def _consume_accumulated() -> tuple[list[dict], list[dict], list[dict]]:
    """Consume and clear all accumulated data. Returns (tg, lms, topics)."""
    global _tg_sync_results, _lms_sync_results, _extracted_topics

    tg = _tg_sync_results[:]
    lms = _lms_sync_results[:]
    topics = _extracted_topics[:]

    _tg_sync_results = []
    _lms_sync_results = []
    _extracted_topics = []

    return tg, lms, topics


# ── Claude prompt ────────────────────────────────────────────

DIGEST_ANALYSIS_PROMPT = """\
Ты — персональный AI-ассистент Сергея, который помогает ему \
отслеживать активность в лаборатории AI Mindset.

## Контекст проектов Сергея
- **Tvorets bot** (@tvoretsvzavyazke) — Telegram бот для DeFi канала с самоулучшением
- **Zoom transcript agent** — автоматические транскрипции Zoom в Obsidian
- **Blog pipeline** — конвейер создания контента для блога
- **TG Monitor** — мониторинг и аналитика Telegram каналов (крипто + студенты)
- **Knowledge Capture Bot** — архивация контента AI Mindset в Obsidian
- Сергей — инструктор AI Mindset (AI агенты, Claude Code, автоматизация, DeFi)

## Задача
Проанализируй данные о новой активности в чатах AI Mindset за последние часы \
и напиши 2-4 предложения: что самое интересное/полезное и как это может быть \
релевантно проектам Сергея.

Если есть конкретные обсуждения инструментов, техник или проектов — упомяни их.
Если нет ничего особо интересного — так и скажи коротко.

Отвечай по-русски, без markdown, только plain text.
"""


# ── DigestNotifier ───────────────────────────────────────────

class DigestNotifier:
    """Generates and sends daily digest notifications."""

    def __init__(
        self,
        bot_token: str,
        admin_chat_id: int,
        anthropic_api_key: str = "",
    ):
        self._bot_token = bot_token
        self._admin_chat_id = admin_chat_id
        self._anthropic: Optional[anthropic.AsyncAnthropic] = None

        if anthropic_api_key:
            self._anthropic = anthropic.AsyncAnthropic(api_key=anthropic_api_key)

    async def send_digest(self) -> bool:
        """Build and send digest. Returns True if sent, False if nothing new."""
        tg_data, lms_data, topics = _consume_accumulated()

        # Build aggregated summary
        digest_data = self._build_digest_data(tg_data, lms_data, topics)
        if digest_data is None:
            logger.info("Digest: nothing new, skipping")
            return False

        # Generate Claude insight (optional — degrade gracefully)
        insight = ""
        if self._anthropic:
            try:
                insight = await self._generate_insight(digest_data)
            except Exception as e:
                logger.warning(f"Digest insight generation failed: {e}")

        # Determine period label
        now = datetime.now(NOVOSIBIRSK)
        period = "утро" if now.hour < 15 else "вечер"

        text = self._format_digest_html(digest_data, insight, period)

        # Truncate if needed (Telegram limit 4096)
        if len(text) > 4000:
            text = text[:3950] + "\n\n<i>(обрезано)</i>"

        # Send
        sent = await self._send_telegram(text)

        if not sent:
            # Retry once after 60s
            logger.info("Digest: retrying in 60s...")
            import asyncio
            await asyncio.sleep(60)
            sent = await self._send_telegram(text)

        logger.info(
            f"Digest {period}: sent={sent}, "
            f"tg_messages={digest_data['total_tg_messages']}"
        )
        return sent

    def _build_digest_data(
        self,
        tg_data: list[dict],
        lms_data: list[dict],
        topics: list[dict],
    ) -> Optional[dict]:
        """Aggregate accumulated sync data into digest input."""
        if not tg_data and not lms_data:
            return None

        # Telegram: merge all sync results
        tg_summary: dict[str, dict] = {}
        total_tg_messages = 0
        for result in tg_data:
            for src_key, src_data in result.get("sources", {}).items():
                if isinstance(src_data, dict) and "error" not in src_data:
                    if src_key not in tg_summary:
                        tg_summary[src_key] = {
                            "chat_name": src_data.get("chat_name", src_key),
                            "total_new": 0,
                        }
                    new_msgs = src_data.get("new_messages", 0)
                    tg_summary[src_key]["total_new"] += new_msgs
                    total_tg_messages += new_msgs

        # LMS: collect updated sessions
        lms_updates: list[str] = []
        for result in lms_data:
            for sid, detail in result.get("details", {}).items():
                if isinstance(detail, dict) and detail.get("status") == "updated":
                    title = detail.get("title", sid)
                    if title not in lms_updates:
                        lms_updates.append(title)

        # Topics extracted
        topic_titles = [t.get("title", "") for t in topics if t.get("title")]

        return {
            "tg_summary": tg_summary,
            "total_tg_messages": total_tg_messages,
            "lms_updates": lms_updates,
            "topic_titles": topic_titles,
            "topics_data": topics,
        }

    async def _generate_insight(self, digest_data: dict) -> str:
        """Ask Claude to analyze digest data and generate a relevance insight."""
        # Build a concise summary for Claude
        summary_for_claude = {
            "telegram_messages": digest_data["total_tg_messages"],
            "active_chats": {
                k: v["total_new"]
                for k, v in digest_data["tg_summary"].items()
                if v["total_new"] > 0
            },
            "lms_updates": digest_data["lms_updates"],
            "extracted_topics": [
                {"title": t.get("title", ""), "summary": t.get("summary", "")}
                for t in digest_data.get("topics_data", [])
            ],
        }

        response = await self._anthropic.messages.create(
            model=MODEL,
            max_tokens=500,
            system=DIGEST_ANALYSIS_PROMPT,
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"Данные за последний период:\n"
                        f"{json.dumps(summary_for_claude, ensure_ascii=False, indent=2)}"
                    ),
                }
            ],
        )
        return response.content[0].text.strip()

    def _format_digest_html(
        self,
        digest_data: dict,
        insight: str,
        period: str,
    ) -> str:
        """Format digest as Telegram HTML message."""
        lines: list[str] = []
        lines.append(f"<b>AI Mindset Digest</b> ({period})\n")

        # Telegram activity
        tg = digest_data["tg_summary"]
        total = digest_data["total_tg_messages"]
        if tg and total > 0:
            lines.append(f"<b>Telegram:</b> {total} новых сообщений")
            for key, data in tg.items():
                count = data["total_new"]
                name = data["chat_name"]
                if count > 0:
                    lines.append(f"  {name}: {count}")
            lines.append("")

        # LMS updates
        lms = digest_data.get("lms_updates", [])
        if lms:
            lines.append(f"<b>LMS:</b> {len(lms)} обновлений")
            for title in lms:
                lines.append(f"  {title}")
            lines.append("")

        # Extracted topics
        topics = digest_data.get("topic_titles", [])
        if topics:
            lines.append(f"<b>Темы для контента:</b> {len(topics)}")
            for t in topics:
                lines.append(f"  {t}")
            lines.append("")

        # Claude insight
        if insight:
            lines.append(f"<b>Что интересного</b>")
            lines.append(insight)

        return "\n".join(lines)

    async def _send_telegram(self, text: str) -> bool:
        """Send message to admin via Tvorets bot."""
        url = f"https://api.telegram.org/bot{self._bot_token}/sendMessage"
        payload = {
            "chat_id": self._admin_chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(url, json=payload)
                data = resp.json()
                if not data.get("ok"):
                    logger.error(f"Telegram API error: {data}")
                    return False
                logger.info(f"Digest sent to admin {self._admin_chat_id}")
                return True
        except Exception as e:
            logger.error(f"Failed to send digest: {e}")
            return False


# ── Periodic task ────────────────────────────────────────────

async def periodic_digest(
    notifier: DigestNotifier,
    morning_hour: int = 9,
    evening_hour: int = 21,
) -> None:
    """Periodic digest task. Sends at target hours in Novosibirsk timezone."""
    import asyncio

    # Wait 2 minutes after startup to let first syncs complete
    await asyncio.sleep(120)

    while True:
        now = datetime.now(NOVOSIBIRSK)

        # Find next target time
        today_morning = now.replace(
            hour=morning_hour, minute=0, second=0, microsecond=0
        )
        today_evening = now.replace(
            hour=evening_hour, minute=0, second=0, microsecond=0
        )

        candidates = []
        for target in [today_morning, today_evening]:
            if target > now:
                candidates.append(target)

        # Also check tomorrow morning
        tomorrow_morning = today_morning + timedelta(days=1)
        candidates.append(tomorrow_morning)

        next_time = min(candidates)
        sleep_seconds = (next_time - now).total_seconds()

        logger.info(
            f"Digest: next at {next_time.strftime('%H:%M')} Novosibirsk "
            f"(sleeping {sleep_seconds / 3600:.1f}h)"
        )
        await asyncio.sleep(sleep_seconds)

        try:
            result = await notifier.send_digest()
            logger.info(f"Digest result: {result}")
        except Exception as e:
            logger.exception(f"Digest send failed: {e}")
