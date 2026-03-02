"""Markdown formatter for LMS sessions → Obsidian.

Converts LmsSession data into structured Obsidian markdown documents
with YAML frontmatter, chapters, tools, quotes, and other metadata.

Also provides formatters for sprints, materials reference pages
(tools/prompts/metaphors), and knowledge base articles.
"""
import re
from datetime import datetime
from typing import Union

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


def _format_transcript_section(transcript: str) -> str:
    """Format transcript field — URL link or inline text."""
    if not transcript:
        return ""
    if transcript.startswith("http"):
        return f"\n## Транскрипт\n\n[Открыть транскрипт]({transcript})\n"
    return f"\n## Транскрипт\n\n{transcript}\n"


def _make_youtube_url(video: str) -> str:
    """Guard for full/partial YouTube URLs."""
    if not video:
        return ""
    if video.startswith("http"):
        return video
    return f"https://youtu.be/{video}"


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

    # Video callout (prominent, right after title)
    lines = []
    if session.video:
        video_url = _make_youtube_url(session.video)
        lines.append("")
        lines.append("> [!video] 🎥 Запись")
        lines.append(f"> [Смотреть на YouTube]({video_url})")
        if session.slides:
            slides_url = session.slides if session.slides.startswith("http") else f"https://{session.slides}"
            lines.append(f"> [Слайды]({slides_url})")
        lines.append("")

    if lines:
        parts.extend(lines)

    # Transcript
    if hasattr(session, 'transcript') and session.transcript:
        parts.append(_format_transcript_section(session.transcript))
    elif session.video:
        parts.append("")
        parts.append("## Транскрипт")
        parts.append("")
        parts.append("- [ ] Требует транскрипции")
        parts.append("")

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
    title = session.title
    # Remove patterns like "WS01: ", "AT03: ", "Session 0: "
    title = re.sub(r"^[A-Za-z]+\s*\d+:\s*", "", title)
    # Remove characters unsafe for filenames
    title = title.replace("/", "-").replace("\\", "-")
    return f"{session.id.upper()} {title}"


# ─────────────────────────────────────────────────────────────────────────────
# New formatters for chunks and materials
# ─────────────────────────────────────────────────────────────────────────────


def format_sprint(data: dict) -> str:
    """Format a sprint for Obsidian.

    Produces a markdown document with YAML frontmatter and structured
    sections derived from the sprint data dict.
    """
    today = datetime.now().strftime("%y.%m.%d")

    sprint_id = data.get("id", "")
    title = data.get("title", sprint_id.upper() if sprint_id else "Sprint")
    description = data.get("description", data.get("desc", ""))
    date_label = data.get("dateLabel", data.get("date", ""))
    duration = data.get("duration", "")
    difficulty = data.get("difficulty", "")
    speakers = data.get("speakers", [])
    if isinstance(speakers, str):
        speakers = [speakers]
    tools_list = data.get("tools", [])
    link_details = data.get("linkDetails", data.get("link", ""))
    status = data.get("status", "confirmed")
    tags_extra = data.get("tags", [])

    # Build tags
    tags = ["type/sprint", "project/ai-mindset", "source/lms"]
    if difficulty:
        tags.append(f"difficulty/{difficulty}")
    for t in tags_extra:
        if t and f"tag/{t}" not in tags:
            tags.append(f"tag/{t}")

    tags_yaml = ", ".join(tags)

    # YAML frontmatter
    speakers_yaml = ", ".join(speakers) if speakers else ""
    parts = [f"""---
tags: [{tags_yaml}]
created: {today}
sprint_id: {sprint_id}
status: {status}
difficulty: {difficulty}
---

# {title}
"""]

    if description:
        parts.append(f"*{description}*\n")

    # Meta block
    meta_lines = []
    if date_label:
        meta_lines.append(f"Дата: {date_label}")
    if duration:
        meta_lines.append(f"Длительность: {duration}")
    if speakers_yaml:
        meta_lines.append(f"Спикеры: {speakers_yaml}")
    if difficulty:
        meta_lines.append(f"Сложность: {difficulty}")

    if meta_lines:
        parts.append("\n".join(meta_lines) + "\n")

    # Tools section
    if tools_list:
        parts.append("\n## Инструменты\n")
        for tool in tools_list:
            if isinstance(tool, dict):
                name = tool.get("name", str(tool))
                parts.append(f"- {name}")
            else:
                parts.append(f"- {tool}")
        parts.append("")

    # Modules / sessions inside the sprint
    modules = data.get("modules", data.get("sessions", data.get("content", [])))
    if modules and isinstance(modules, list):
        parts.append("\n## Модули\n")
        for mod in modules:
            if isinstance(mod, dict):
                mod_title = mod.get("title", mod.get("name", ""))
                mod_desc = mod.get("description", mod.get("desc", ""))
                if mod_title:
                    if mod_desc:
                        parts.append(f"- **{mod_title}** — {mod_desc}")
                    else:
                        parts.append(f"- **{mod_title}**")
            else:
                parts.append(f"- {mod}")
        parts.append("")

    # Links
    if link_details:
        parts.append(f"\n## Ссылки\n\n- [Подробнее]({link_details})\n")

    return "\n".join(parts)


def get_sprint_filename(data: dict) -> str:
    """Generate Obsidian filename for a sprint.

    Format: SPRINT_ID
    Example: POS
    """
    sprint_id = data.get("id", "sprint")
    return sprint_id.upper()


def get_sprint_folder_name(data: dict) -> str:
    """Generate subfolder name for a sprint: 'SPRINT_ID {sprint}'."""
    sprint_id = data.get("id", "sprint")
    return f"{sprint_id.upper()} {{sprint}}"


def format_materials_page(content_type: str, items: list[dict]) -> str:
    """Format a materials reference page for Obsidian.

    Supports content_type: 'tools', 'prompts', 'metaphors', 'speakers'.

    For tools: groups by tier (Essential / Power / Pro / Other) and
    renders as a Markdown table.
    For prompts: renders as titled code-block sections grouped by category.
    For metaphors: renders as a numbered list.
    For speakers: renders as a simple bio list.
    """
    today = datetime.now().strftime("%y.%m.%d")
    total = len(items)

    if content_type == "tools":
        return _format_tools_page(items, today, total)
    elif content_type == "prompts":
        return _format_prompts_page(items, today, total)
    elif content_type == "metaphors":
        return _format_metaphors_page(items, today, total)
    elif content_type == "speakers":
        return _format_speakers_page(items, today, total)
    else:
        # Generic fallback
        parts = [f"""---
tags: [type/reference, project/ai-mindset, source/lms]
created: {today}
---

# {content_type.capitalize()} ({total})
"""]
        for item in items:
            if isinstance(item, dict):
                name = item.get("name", item.get("title", str(item)))
                parts.append(f"- {name}")
            else:
                parts.append(f"- {item}")
        return "\n".join(parts) + "\n"


def _format_tools_page(items: list[dict], today: str, total: int) -> str:
    """Format tools as a tiered reference table."""
    # Group by tier
    tiers: dict[str, list[dict]] = {}
    tier_order = ["essential", "power", "pro"]
    for item in items:
        tier = item.get("tier", "other").lower()
        tiers.setdefault(tier, []).append(item)

    # Build ordered list of tiers present
    ordered_tiers = [t for t in tier_order if t in tiers]
    for t in tiers:
        if t not in ordered_tiers:
            ordered_tiers.append(t)

    parts = [f"""---
tags: [type/reference, project/ai-mindset, source/lms]
created: {today}
---

# Инструменты W26

> {total} инструментов, упоминаемых в программе

"""]

    for tier in ordered_tiers:
        tier_items = tiers[tier]
        tier_label = tier.capitalize()
        parts.append(f"## {tier_label}\n")
        parts.append("| Инструмент | Категория | Модель оплаты |")
        parts.append("|---|---|---|")
        for tool in tier_items:
            name = tool.get("name", "")
            category = tool.get("category", "")
            pricing = tool.get("pricing", tool.get("price", ""))
            parts.append(f"| {name} | {category} | {pricing} |")
        parts.append("")

    return "\n".join(parts) + "\n"


def _format_prompts_page(items: list[dict], today: str, total: int) -> str:
    """Format prompts grouped by category."""
    # Group by category
    categories: dict[str, list[dict]] = {}
    for item in items:
        cat = item.get("category", item.get("type", "General"))
        categories.setdefault(cat, []).append(item)

    parts = [f"""---
tags: [type/reference, project/ai-mindset, source/lms]
created: {today}
---

# Промпты W26

> {total} промптов из программы

"""]

    for cat, cat_items in categories.items():
        parts.append(f"## {cat}\n")
        for p in cat_items:
            title = p.get("title", p.get("name", ""))
            text = p.get("text", p.get("content", p.get("prompt", "")))
            description = p.get("description", "")
            if title:
                parts.append(f"### {title}\n")
            if description:
                parts.append(f"{description}\n")
            if text:
                parts.append(f"```\n{text}\n```\n")

    return "\n".join(parts) + "\n"


def _format_metaphors_page(items: list[dict], today: str, total: int) -> str:
    """Format metaphors as a numbered reference list."""
    parts = [f"""---
tags: [type/reference, project/ai-mindset, source/lms]
created: {today}
---

# Метафоры W26

> {total} метафор из программы

"""]

    for i, item in enumerate(items, start=1):
        if isinstance(item, dict):
            title = item.get("title", item.get("name", ""))
            description = item.get(
                "description", item.get("text", item.get("content", ""))
            )
            context = item.get("context", "")
            parts.append(f"### {i}. {title}\n")
            if description:
                parts.append(f"{description}\n")
            if context:
                parts.append(f"*Контекст: {context}*\n")
        else:
            parts.append(f"{i}. {item}\n")

    return "\n".join(parts) + "\n"


def _format_speakers_page(items: list[dict], today: str, total: int) -> str:
    """Format speakers as a bio list."""
    parts = [f"""---
tags: [type/reference, project/ai-mindset, source/lms]
created: {today}
---

# Спикеры W26

> {total} спикеров программы

"""]

    for item in items:
        if isinstance(item, dict):
            name = item.get("name", item.get("title", ""))
            role = item.get("role", item.get("title", ""))
            bio = item.get("bio", item.get("description", ""))
            photo = item.get("photo", item.get("image", ""))
            parts.append(f"## {name}\n")
            if role:
                parts.append(f"*{role}*\n")
            if bio:
                parts.append(f"{bio}\n")
            if photo:
                parts.append(f"![]({photo})\n")
        else:
            parts.append(f"- {item}\n")

    return "\n".join(parts) + "\n"


def format_kb_article(data: dict) -> str:
    """Format a knowledge base article (vibe-coding-kb chapters) for Obsidian.

    Produces a markdown document with YAML frontmatter. The data dict
    may represent either a top-level KB section or an individual chapter.
    """
    today = datetime.now().strftime("%y.%m.%d")

    article_id = data.get("id", "")
    title = data.get("title", article_id or "KB Article")
    description = data.get("description", data.get("desc", ""))
    content_text = data.get("content", data.get("text", ""))
    section = data.get("section", data.get("category", ""))
    tags_extra = data.get("tags", [])
    order = data.get("order", data.get("index", ""))
    difficulty = data.get("difficulty", "")
    examples = data.get("examples", [])
    tips = data.get("tips", [])
    links = data.get("links", data.get("resources", []))
    chapters = data.get("chapters", [])

    # Build tags
    tags = ["type/kb-article", "project/ai-mindset", "source/lms", "topic/vibe-coding"]
    if section:
        clean_section = re.sub(r"[^\w-]", "-", section.lower())
        tags.append(f"section/{clean_section}")
    for t in tags_extra:
        if t:
            tags.append(f"tag/{t}")

    tags_yaml = ", ".join(tags)

    parts = [f"""---
tags: [{tags_yaml}]
created: {today}
kb_id: {article_id}
section: {section}
---

# {title}
"""]

    if description:
        parts.append(f"*{description}*\n")

    # Main content body
    if content_text:
        parts.append(f"\n{content_text}\n")

    # Sub-chapters (if this is a section with chapters)
    if chapters:
        parts.append("\n## Разделы\n")
        for ch in chapters:
            if isinstance(ch, dict):
                ch_title = ch.get("title", ch.get("name", ""))
                ch_desc = ch.get("description", ch.get("desc", ""))
                if ch_title:
                    if ch_desc:
                        parts.append(f"### {ch_title}\n\n{ch_desc}\n")
                    else:
                        parts.append(f"### {ch_title}\n")
            else:
                parts.append(f"- {ch}")

    # Examples
    if examples:
        parts.append("\n## Примеры\n")
        for ex in examples:
            if isinstance(ex, dict):
                ex_title = ex.get("title", ex.get("name", ""))
                ex_code = ex.get("code", ex.get("content", ex.get("text", "")))
                ex_desc = ex.get("description", "")
                if ex_title:
                    parts.append(f"### {ex_title}\n")
                if ex_desc:
                    parts.append(f"{ex_desc}\n")
                if ex_code:
                    lang = ex.get("lang", ex.get("language", ""))
                    parts.append(f"```{lang}\n{ex_code}\n```\n")
            else:
                parts.append(f"```\n{ex}\n```\n")

    # Tips
    if tips:
        parts.append("\n## Советы\n")
        for tip in tips:
            if isinstance(tip, dict):
                tip_text = tip.get("text", tip.get("content", str(tip)))
                parts.append(f"- {tip_text}")
            else:
                parts.append(f"- {tip}")
        parts.append("")

    # Links/Resources
    if links:
        parts.append("\n## Ссылки\n")
        for link in links:
            if isinstance(link, dict):
                link_title = link.get("title", link.get("name", ""))
                link_url = link.get("url", link.get("href", link.get("link", "")))
                link_desc = link.get("description", "")
                if link_url:
                    line = f"- [{link_title}]({link_url})"
                else:
                    line = f"- {link_title}"
                if link_desc:
                    line += f" — {link_desc}"
                parts.append(line)
            else:
                parts.append(f"- {link}")
        parts.append("")

    return "\n".join(parts) + "\n"


def get_kb_article_filename(data: dict) -> str:
    """Generate Obsidian filename for a KB article.

    Format: KB_ID
    Example: claude-code
    """
    article_id = data.get("id", "kb")
    return article_id
