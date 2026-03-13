"""Fetch book/course content from LMS chunk files.

LMS stores structured courses (Automation 101, Vibe-coding 101) as:
1. A data chunk (book-HASH.js) with block/chapter metadata
2. A page chunk (BookChapterPage-HASH.js / VibeCodingChapterPage-HASH.js)
   with Object.assign mapping file paths → lazy JS imports
3. Individual chapter chunks (e.g., "101 What is automations-HASH.js")
   each exporting a markdown string as default

This module discovers and fetches all chapters for a given book.
"""
import logging
import re
import time
from dataclasses import dataclass, field

import httpx

logger = logging.getLogger(__name__)

LMS_BASE = "https://learn.aimindset.org"


@dataclass
class BookChapter:
    """A single chapter with its content."""
    id: str
    title: str
    block_id: str
    order: int
    description: str = ""
    content: str = ""  # markdown content


@dataclass
class BookBlock:
    """A block (section) of a book."""
    id: str
    title: str
    subtitle: str = ""
    description: str = ""
    order: int = 0
    chapters: list[BookChapter] = field(default_factory=list)


async def fetch_automation_book(bundle_content: str) -> list[BookBlock]:
    """Fetch all Automation 101 blocks and chapters from LMS.

    Args:
        bundle_content: The main JS bundle content (to find chunk filenames)

    Returns:
        List of BookBlock objects with chapter content populated.
    """
    # Step 1: Find BookChapterPage chunk filename
    chapter_page_match = re.search(
        r'import\("\./(BookChapterPage-[A-Za-z0-9_-]+\.js)"\)', bundle_content
    )
    if not chapter_page_match:
        logger.warning("BookChapterPage chunk not found in bundle")
        return []

    chapter_page_filename = chapter_page_match.group(1)
    logger.info(f"Found BookChapterPage chunk: {chapter_page_filename}")

    # Step 2: Find book data chunk filename
    # BookChapterPage imports it as: from"./book-HASH.js" or import{...}from"./book-HASH.js"
    book_chunk_match = re.search(
        r'["\./](book-[A-Za-z0-9_-]+\.js)["\)]', bundle_content
    )
    if not book_chunk_match:
        # Also try searching in the chapter page itself
        logger.warning("book data chunk not found in bundle, will search in chapter page")
        book_chunk_filename = None
    else:
        book_chunk_filename = book_chunk_match.group(1)

    async with httpx.AsyncClient(timeout=30) as client:
        # Step 3: Fetch book data chunk (block/chapter structure)
        try:
            resp = await client.get(f"{LMS_BASE}/assets/{book_chunk_filename}")
            resp.raise_for_status()
            book_data_js = resp.text
        except Exception as e:
            logger.error(f"Failed to fetch book data chunk: {e}")
            return []

        # Step 4: Fetch BookChapterPage chunk (import mappings)
        try:
            resp = await client.get(f"{LMS_BASE}/assets/{chapter_page_filename}")
            resp.raise_for_status()
            chapter_page_js = resp.text
        except Exception as e:
            logger.error(f"Failed to fetch BookChapterPage chunk: {e}")
            return []

    # Step 5: Parse block/chapter structure from book data
    blocks = _parse_book_structure(book_data_js)
    if not blocks:
        logger.warning("Could not parse book structure")
        return []

    logger.info(
        f"Automation 101: {len(blocks)} blocks, "
        f"{sum(len(b.chapters) for b in blocks)} chapters"
    )

    # Step 6: Build file→chunk mapping from BookChapterPage
    file_to_chunk = _parse_chapter_imports(chapter_page_js)
    logger.info(f"Found {len(file_to_chunk)} chapter import mappings")

    # Step 7: Fetch each chapter's content
    async with httpx.AsyncClient(timeout=30) as client:
        for block in blocks:
            for chapter in block.chapters:
                file_path = chapter.description  # temporarily stored here
                chunk_filename = file_to_chunk.get(file_path)
                if not chunk_filename:
                    # Try with /content/automation-book/ prefix
                    full_path = f"/content/automation-book/{file_path}"
                    chunk_filename = file_to_chunk.get(full_path)

                if not chunk_filename:
                    logger.debug(
                        f"No chunk mapping for chapter {chapter.id}: {file_path}"
                    )
                    continue

                try:
                    resp = await client.get(
                        f"{LMS_BASE}/assets/{chunk_filename}"
                    )
                    resp.raise_for_status()
                    content = _extract_markdown_from_chunk(resp.text)
                    if content:
                        chapter.content = content
                        logger.debug(
                            f"Fetched chapter {chapter.id}: "
                            f"{len(content)} chars"
                        )
                    time.sleep(0.5)  # rate limit
                except Exception as e:
                    logger.warning(
                        f"Failed to fetch chapter {chapter.id}: {e}"
                    )

                # Clear temporary storage
                chapter.description = ""

    total_content = sum(
        len(ch.content) for b in blocks for ch in b.chapters
    )
    logger.info(f"Automation 101: fetched {total_content:,} chars of content")

    return blocks


async def fetch_vibe_coding_book(bundle_content: str) -> list[BookBlock]:
    """Fetch Vibe-coding 101 chapters from LMS.

    Similar to automation book but uses VibeCodingChapterPage chunk.
    """
    # Find VibeCodingChapterPage chunk
    match = re.search(
        r'import\("\./(VibeCodingChapterPage-[A-Za-z0-9_-]+\.js)"\)',
        bundle_content,
    )
    if not match:
        logger.warning("VibeCodingChapterPage chunk not found")
        return []

    page_filename = match.group(1)

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(f"{LMS_BASE}/assets/{page_filename}")
            resp.raise_for_status()
            page_js = resp.text
        except Exception as e:
            logger.error(f"Failed to fetch VibeCodingChapterPage: {e}")
            return []

    # Parse import mappings
    file_to_chunk = _parse_chapter_imports(page_js)
    if not file_to_chunk:
        logger.warning("No vibe-coding chapter imports found")
        return []

    logger.info(f"Vibe-coding 101: {len(file_to_chunk)} chapter mappings")

    # Group chapters by directory prefix for blocks
    chapters_by_section: dict[str, list[tuple[str, str]]] = {}
    for file_path, chunk_name in file_to_chunk.items():
        # e.g. "/public/kb/vibe-coding/case-01-intro.md"
        filename = file_path.rsplit("/", 1)[-1] if "/" in file_path else file_path
        section = "vibe-coding"
        chapters_by_section.setdefault(section, []).append(
            (filename, chunk_name)
        )

    # Fetch all chapters
    block = BookBlock(
        id="vibe-coding",
        title="Vibe-coding 101",
        subtitle="Введение в vibe-coding",
        order=1,
    )

    async with httpx.AsyncClient(timeout=30) as client:
        for filename, chunk_name in sorted(chapters_by_section.get("vibe-coding", [])):
            try:
                resp = await client.get(f"{LMS_BASE}/assets/{chunk_name}")
                resp.raise_for_status()
                content = _extract_markdown_from_chunk(resp.text)
                if content:
                    chapter_id = filename.replace(".md", "")
                    block.chapters.append(BookChapter(
                        id=chapter_id,
                        title=chapter_id.replace("-", " ").title(),
                        block_id="vibe-coding",
                        order=len(block.chapters) + 1,
                        content=content,
                    ))
                time.sleep(0.5)
            except Exception as e:
                logger.warning(f"Failed to fetch vibe-coding chapter {filename}: {e}")

    logger.info(
        f"Vibe-coding 101: fetched {len(block.chapters)} chapters, "
        f"{sum(len(ch.content) for ch in block.chapters):,} chars"
    )

    return [block] if block.chapters else []


def _parse_book_structure(book_js: str) -> list[BookBlock]:
    """Parse block/chapter structure from book data chunk.

    The chunk contains: const r=[{id:"basics",title:...,chapters:[...]}]
    """
    import json
    import subprocess
    import tempfile
    import os

    # Find the main array
    match = re.search(r'const\s+\w+=(\[)', book_js)
    if not match:
        return []

    start = match.start(1)
    bracket_count = 0
    end = start
    for i in range(start, len(book_js)):
        if book_js[i] == "[":
            bracket_count += 1
        elif book_js[i] == "]":
            bracket_count -= 1
            if bracket_count == 0:
                end = i + 1
                break

    raw = book_js[start:end]

    # Parse via Node.js
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".js", delete=False, encoding="utf-8"
        ) as f:
            f.write(f"const arr = {raw};\n")
            f.write("process.stdout.write(JSON.stringify(arr));\n")
            tmp_path = f.name

        proc = subprocess.run(
            ["node", tmp_path],
            capture_output=True, text=True, timeout=15, encoding="utf-8",
        )
        os.unlink(tmp_path)

        if proc.returncode != 0:
            logger.warning(f"Node.js parse failed: {proc.stderr[:200]}")
            return []

        data = json.loads(proc.stdout)
    except Exception as e:
        logger.warning(f"Failed to parse book structure: {e}")
        return []

    blocks = []
    for block_data in data:
        block = BookBlock(
            id=block_data.get("id", ""),
            title=block_data.get("title", ""),
            subtitle=block_data.get("subtitle", ""),
            description=block_data.get("description", ""),
            order=block_data.get("order", 0),
        )
        for ch_data in block_data.get("chapters", []):
            chapter = BookChapter(
                id=ch_data.get("id", ""),
                title=ch_data.get("title", ""),
                block_id=block.id,
                order=ch_data.get("order", 0),
                # Temporarily store file path in description for lookup
                description=ch_data.get("file", ""),
            )
            block.chapters.append(chapter)
        blocks.append(block)

    return blocks


def _parse_chapter_imports(page_js: str) -> dict[str, str]:
    """Parse Object.assign mapping from chapter page chunk.

    Extracts: "/content/automation-book/1 Basics/101.md" → "101-HASH.js"
    """
    # Pattern: "PATH.md":()=>...import("./CHUNK.js")
    pattern = re.compile(
        r'"([^"]+\.md)":\(\)=>[^"]*import\("\./([\w .()-]+\.js)"\)'
    )
    return dict(pattern.findall(page_js))


def _extract_markdown_from_chunk(chunk_js: str) -> str:
    """Extract markdown string from a chapter chunk.

    Chapter chunks contain: const n=`---\ntitle: ...\n---\n...`; export {n as default}
    """
    # Find backtick-enclosed string
    match = re.search(r"const\s+\w+\s*=\s*`", chunk_js)
    if not match:
        return ""

    start = match.end()  # position after opening backtick
    # Find closing backtick (not escaped)
    i = start
    while i < len(chunk_js):
        if chunk_js[i] == "`" and (i == 0 or chunk_js[i - 1] != "\\"):
            return chunk_js[start:i]
        i += 1

    return ""


def format_book_for_obsidian(
    title: str,
    description: str,
    blocks: list[BookBlock],
) -> str:
    """Format a complete book as a single Obsidian markdown document."""
    from datetime import datetime

    today = datetime.now().strftime("%y.%m.%d")
    total_chapters = sum(len(b.chapters) for b in blocks)

    parts = [f"""---
tags: [type/reference, project/ai-mindset, source/lms, type/course]
created: {today}
---

# {title}

> {description}
> {len(blocks)} блоков, {total_chapters} глав

"""]

    for block in blocks:
        parts.append(f"## {block.title}\n")
        if block.subtitle:
            parts.append(f"> {block.subtitle}\n")
        if block.description:
            parts.append(f"{block.description}\n")
        parts.append("")

        for chapter in block.chapters:
            if chapter.content:
                # Remove YAML frontmatter from chapter content
                content = chapter.content
                if content.startswith("---"):
                    end = content.find("---", 3)
                    if end > 0:
                        content = content[end + 3:].strip()

                parts.append(f"### {chapter.title}\n")
                parts.append(content)
                parts.append("")
            else:
                parts.append(f"### {chapter.title}\n")
                parts.append(f"*Контент главы недоступен*\n")

    return "\n".join(parts) + "\n"
