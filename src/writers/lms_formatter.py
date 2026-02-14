"""Markdown formatter for LMS sessions → Obsidian.

Converts LmsSession data into structured Obsidian markdown documents
with YAML frontmatter, chapters, tools, quotes, and other metadata.
"""
from datetime import datetime

from src.sources.lms_source import LmsSession


def _format_duration(minutes: int) -> str:
    """Format duration as human-readable string."""
    if minutes >= 60:
        h = minutes // 60
        m = minutes % 60
        return f"{h}ч {m}мин" if m else f"{h}ч"
    return f"{minutes} мин"


def _format_chapters(chapters: list[dict], video_id: str) -> str:
    """Format chapters as markdown with optional YouTube timestamps."""
    if not chapters:
        return ""

    lines = ["## Главы\n"]
    for ch in chapters:
        start = ch.get("start", 0)
        title = ch.get("title", "")
        # Format as HH:MM:SS
        h, remainder = divmod(start, 3600)
        m, s = divmod(remainder, 60)
        if h:
            timestamp = f"{h}:{m:02d}:{s:02d}"
        else:
            timestamp = f"{m}:{s:02d}"

        if video_id:
            yt_link = f"https://youtu.be/{video_id}?t={start}"
            lines.append(f"- [{timestamp}]({yt_link}) {title}")
        else:
            lines.append(f"- `{timestamp}` {title}")

    return "\n".join(lines) + "\n"


def _format_tools(tools: list) -> str:
    """Format tools list (can be list of dicts or strings)."""
    if not tools:
        return ""

    lines = ["## Инструменты\n"]
    for tool in tools:
        if isinstance(tool, dict):
            name = tool.get("name", "")
            desc = tool.get("description", "")
            if desc:
                lines.append(f"- **{name}** — {desc}")
            else:
                lines.append(f"- **{name}**")
        else:
            lines.append(f"- {tool}")

    return "\n".join(lines) + "\n"


def _format_quotes(quotes: list) -> str:
    """Format quotes as blockquotes (can be list of dicts or strings)."""
    if not quotes:
        return ""

    lines = ["## Цитаты\n"]
    for q in quotes:
        if isinstance(q, dict):
            text = q.get("text", "")
            speaker = q.get("speaker", "")
            if speaker:
                lines.append(f"> {text}\n> — *{speaker}*\n")
            else:
                lines.append(f"> {text}\n")
        else:
            lines.append(f"> {q}\n")

    return "\n".join(lines) + "\n"


def _format_homework(homework: list) -> str:
    """Format homework items (can be list of dicts or strings)."""
    if not homework:
        return ""

    lines = ["## Домашнее задание\n"]
    for item in homework:
        if isinstance(item, dict):
            title = item.get("title", "")
            desc = item.get("description", "")
            if desc:
                lines.append(f"- [ ] **{title}** — {desc}")
            else:
                lines.append(f"- [ ] {title}")
        else:
            lines.append(f"- [ ] {item}")

    return "\n".join(lines) + "\n"


def _format_resources(resources: list) -> str:
    """Format external resources/links (can be list of dicts or strings)."""
    if not resources:
        return ""

    lines = ["## Ресурсы\n"]
    for r in resources:
        if isinstance(r, dict):
            title = r.get("title", r.get("name", ""))
            url = r.get("url", r.get("link", ""))
            desc = r.get("description", "")
            if url:
                line = f"- [{title}]({url})"
            else:
                line = f"- {title}"
            if desc:
                line += f" — {desc}"
            lines.append(line)
        else:
            lines.append(f"- {r}")

    return "\n".join(lines) + "\n"


def _format_prompts(prompts: list) -> str:
    """Format prompt examples (can be list of dicts or strings)."""
    if not prompts:
        return ""

    lines = ["## Промпты\n"]
    for p in prompts:
        if isinstance(p, dict):
            title = p.get("title", p.get("name", ""))
            text = p.get("text", p.get("content", ""))
            if title:
                lines.append(f"### {title}\n")
            if text:
                lines.append(f"```\n{text}\n```\n")
        else:
            lines.append(f"```\n{p}\n```\n")

    return "\n".join(lines) + "\n"


def _session_type_label(session_id: str) -> str:
    """Return human-readable session type."""
    if session_id.startswith("ws"):
        return "workshop"
    elif session_id.startswith("at"):
        return "advanced"
    elif session_id.startswith("bonus"):
        return "bonus"
    elif session_id.startswith("oh"):
        return "office-hours"
    elif session_id.startswith("fs"):
        return "focus-session"
    elif session_id.startswith("fos"):
        return "founder-os"
    return "session"


def format_lms_session(session: LmsSession) -> str:
    """Format a complete LMS session as Obsidian markdown document.

    Returns full document with YAML frontmatter.
    """
    today = datetime.now().strftime("%y.%m.%d")
    session_type = _session_type_label(session.id)

    # Speakers
    speakers_str = session.speaker
    if session.speakers:
        speakers_str = ", ".join(session.speakers)

    # Tags
    tags = [
        f"type/{session_type}",
        "project/ai-mindset",
        "source/lms",
    ]

    # YAML frontmatter
    tags_yaml = ", ".join(tags)
    parts = [f"""---
tags: [{tags_yaml}]
created: {today}
session_id: {session.id}
date: {session.date}
speaker: {speakers_str}
duration: {session.duration}
status: {session.status}
---

# {session.title}
"""]

    if session.subtitle:
        parts.append(f"*{session.subtitle}*\n")

    # Meta line
    meta_parts = []
    if session.date:
        meta_parts.append(f"{session.date}")
    if session.duration:
        meta_parts.append(_format_duration(session.duration))
    if speakers_str:
        meta_parts.append(speakers_str)
    if session.status:
        meta_parts.append(session.status)
    if meta_parts:
        parts.append(" · ".join(meta_parts) + "\n")

    # Links
    links = []
    if session.video:
        links.append(
            f"[YouTube](https://youtu.be/{session.video})"
        )
    if session.slides:
        url = session.slides
        if not url.startswith("http"):
            url = f"{session.slides}"
        links.append(f"[Слайды]({url})")
    if links:
        parts.append(" | ".join(links) + "\n")

    # TL;DR
    if session.tldr:
        parts.append(f"\n## TL;DR\n\n{session.tldr}\n")

    # Summary/Description
    if session.summary:
        parts.append(f"\n## Описание\n\n{session.summary}\n")

    # Key Topics
    if session.key_topics:
        parts.append("\n## Ключевые темы\n")
        for topic in session.key_topics:
            parts.append(f"- {topic}")
        parts.append("")

    # Chapters
    chapters_md = _format_chapters(session.chapters, session.video)
    if chapters_md:
        parts.append(f"\n{chapters_md}")

    # Tools
    tools_md = _format_tools(session.tools)
    if tools_md:
        parts.append(f"\n{tools_md}")

    # Prompts
    prompts_md = _format_prompts(session.prompts)
    if prompts_md:
        parts.append(f"\n{prompts_md}")

    # Quotes
    quotes_md = _format_quotes(session.quotes)
    if quotes_md:
        parts.append(f"\n{quotes_md}")

    # Homework
    hw_md = _format_homework(session.homework)
    if hw_md:
        parts.append(f"\n{hw_md}")

    # Resources
    res_md = _format_resources(session.resources)
    if res_md:
        parts.append(f"\n{res_md}")

    # Participant Feedback
    if session.participant_feedback:
        parts.append(
            f"\n## Обратная связь участников\n\n{session.participant_feedback}\n"
        )

    # Sharing Session
    if session.sharing_session:
        parts.append(
            f"\n## Sharing Session\n\n{session.sharing_session}\n"
        )

    # Key Takeaways
    if session.key_takeaways:
        parts.append("\n## Ключевые выводы\n")
        for takeaway in session.key_takeaways:
            parts.append(f"- {takeaway}")
        parts.append("")

    return "\n".join(parts)


def get_session_filename(session: LmsSession) -> str:
    """Generate Obsidian filename for a session.

    Format: SESSION_ID Title
    Example: WS01 Prompt Engineering
    """
    import re

    title = session.title
    # Remove patterns like "WS01: ", "AT03: ", "Session 0: "
    title = re.sub(r"^[A-Za-z]+\s*\d+:\s*", "", title)
    # Remove characters unsafe for filenames
    title = title.replace("/", "-").replace("\\", "-")
    return f"{session.id.upper()} {title}"
