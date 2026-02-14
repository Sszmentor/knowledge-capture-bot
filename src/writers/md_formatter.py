"""Markdown formatter for Telegram messages → Obsidian.

Formats messages grouped by day with metadata header and stats.
Based on tg_to_obsidian.py patterns.
"""
from collections import Counter
from datetime import datetime
from typing import Optional

from src.sources.telegram_source import TelegramMessage


def format_message(msg: TelegramMessage) -> str:
    """Format a single message as markdown line."""
    date = msg.date[:16].replace("T", " ") if msg.date else "???"
    sender = msg.sender or "unknown"
    username = f" @{msg.sender_username}" if msg.sender_username else ""

    # Media indicator
    media_str = ""
    if msg.media:
        media_map = {
            "photo": " [фото]",
            "voice": " [голосовое]",
            "video_note": " [кружок]",
            "document": " [документ]",
        }
        if msg.media in media_map:
            media_str = media_map[msg.media]
        elif msg.media.startswith("file:"):
            media_str = f" [файл: {msg.media[5:]}]"
        else:
            media_str = f" [{msg.media}]"

    # Forward indicator
    fwd_str = ""
    if msg.forward_from:
        fwd_str = f" *(переслано от {msg.forward_from})*"

    # Reply indicator
    reply_str = ""
    if msg.reply_to:
        reply_str = f" ↩️ #{msg.reply_to}"

    lines = [f"**{sender}**{username} — {date}{media_str}{fwd_str}{reply_str}"]
    if msg.text:
        lines.append(msg.text)
    lines.append("")  # blank line between messages

    return "\n".join(lines)


def format_day_separator(date_str: str) -> str:
    """Format a day separator with date header."""
    return f"\n---\n### {date_str}\n"


def format_messages_block(messages: list[TelegramMessage]) -> str:
    """Format a block of messages with day grouping.

    Returns only the message content (no header/stats) —
    suitable for appending to existing files.
    """
    lines = []
    current_date = None

    for msg in messages:
        if msg.date:
            msg_date = msg.date[:10]
            if msg_date != current_date:
                current_date = msg_date
                dt = datetime.fromisoformat(msg.date)
                day_str = dt.strftime("%Y-%m-%d (%a)")
                lines.append(format_day_separator(day_str))

        lines.append(format_message(msg))

    return "\n".join(lines)


def format_full_document(
    messages: list[TelegramMessage],
    title: str,
    description: str = "",
    tags: Optional[list[str]] = None,
) -> str:
    """Format a complete Obsidian document with header, stats, and messages.

    Used for initial file creation (not appending).
    """
    if tags is None:
        tags = ["type/chat-export", "project/ai-mindset"]

    # Calculate stats
    senders: Counter = Counter()
    for m in messages:
        senders[m.sender or "unknown"] += 1
    top_senders = senders.most_common(10)
    unique_senders = len(senders)

    date_start = messages[0].date[:10] if messages else "?"
    date_end = messages[-1].date[:10] if messages else "?"

    today = datetime.now()
    created = today.strftime("%y.%m.%d")

    # Build header
    tags_str = ", ".join(tags)
    header = f"""---
tags: [{tags_str}]
created: {created}
---

# {title}

{description}

**Сообщений:** {len(messages)}
**Период:** {date_start} — {date_end}
**Участников:** {unique_senders}

**Топ отправителей:**
"""
    for sender, count in top_senders:
        header += f"- {sender}: {count}\n"

    # Build body
    body = format_messages_block(messages)

    return header + "\n" + body


def update_stats_in_header(
    existing_content: str,
    new_total: int,
    new_end_date: str,
) -> str:
    """Update message count and end date in existing file header.

    Parses existing header and updates:
    - **Сообщений:** N → new_total
    - **Период:** start — end → start — new_end_date
    """
    lines = existing_content.split("\n")
    updated_lines = []

    for line in lines:
        if line.startswith("**Сообщений:**"):
            updated_lines.append(f"**Сообщений:** {new_total}")
        elif line.startswith("**Период:**"):
            # Extract start date, replace end date
            parts = line.split("—")
            if len(parts) == 2:
                start = parts[0].replace("**Период:**", "").strip()
                updated_lines.append(f"**Период:** {start} — {new_end_date}")
            else:
                updated_lines.append(line)
        else:
            updated_lines.append(line)

    return "\n".join(updated_lines)


def get_message_count_from_header(content: str) -> int:
    """Extract current message count from file header."""
    for line in content.split("\n"):
        if line.startswith("**Сообщений:**"):
            try:
                return int(line.replace("**Сообщений:**", "").strip())
            except ValueError:
                return 0
    return 0
