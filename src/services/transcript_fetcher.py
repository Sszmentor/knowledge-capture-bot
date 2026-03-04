"""Fetch and parse transcripts from LMS and YouTube (fallback).

Primary source: LMS markdown files at learn.aimindset.org/transcripts/*.md
Fallback: YouTube transcript API (if LMS transcript unavailable).

Two LMS transcript formats:
1. VTT-style: numbered cues with HH:MM:SS.mmm --> HH:MM:SS.mmm timestamps
2. Markdown: **Speaker Name:** text (already readable)
"""
import logging
import re

import httpx

logger = logging.getLogger(__name__)

LMS_BASE = "https://learn.aimindset.org"


def fetch_lms_transcript(transcript_path: str) -> str:
    """Fetch transcript .md file from LMS.

    Args:
        transcript_path: path from LMS data, e.g. "/transcripts/ws01-transcript"
                         or "/w26/advanced/at01/transcript"

    Returns:
        Cleaned transcript text, or empty string on failure.
    """
    if not transcript_path:
        return ""

    # Ensure path starts with /
    if not transcript_path.startswith("/"):
        transcript_path = f"/transcripts/{transcript_path}"

    # Try with .md extension first, then without
    urls_to_try = []
    base_url = f"{LMS_BASE}{transcript_path}"
    if not base_url.endswith(".md"):
        urls_to_try.append(f"{base_url}.md")
    urls_to_try.append(base_url)

    for url in urls_to_try:
        try:
            resp = httpx.get(url, timeout=30, follow_redirects=True)
            if resp.status_code == 200 and len(resp.text) > 200:
                text = parse_transcript_md(resp.text)
                if text:
                    logger.info(
                        f"LMS transcript fetched: {transcript_path} "
                        f"({len(text)} chars)"
                    )
                    return text
        except Exception as e:
            logger.warning(f"LMS transcript fetch failed for {url}: {e}")

    logger.info(f"No LMS transcript found at {transcript_path}")
    return ""


def parse_transcript_md(raw: str) -> str:
    """Parse LMS transcript markdown into clean Obsidian-ready text.

    Detects format automatically:
    1. VTT-style: numbered cues with timestamp arrows (-->)
    2. Markdown: already readable, minimal cleaning
    """
    if not raw or len(raw) < 100:
        return ""

    # Detect VTT format by looking for timestamp arrows in first 5000 chars
    if "-->" in raw[:5000]:
        return _parse_vtt_transcript(raw)
    else:
        return _clean_markdown_transcript(raw)


def _parse_vtt_transcript(raw: str) -> str:
    """Convert VTT-style cues to clean text with 5-minute timestamp blocks."""
    lines = raw.split("\n")
    segments = []  # (start_seconds, text)

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        # Match timestamp line: 00:00:00.000 --> 00:00:05.000
        ts_match = re.match(
            r"(\d{1,2}):(\d{2}):(\d{2})\.\d+\s*-->\s*", line
        )
        if ts_match:
            h = int(ts_match.group(1))
            m = int(ts_match.group(2))
            s = int(ts_match.group(3))
            start_sec = h * 3600 + m * 60 + s
            # Collect text lines until empty line or next cue number
            i += 1
            text_parts = []
            while i < len(lines):
                content = lines[i].strip()
                if not content:
                    # Empty line = end of cue
                    i += 1
                    break
                if content.isdigit():
                    # Next cue number — don't advance, let outer loop handle
                    break
                text_parts.append(content)
                i += 1
            if text_parts:
                segments.append((start_sec, " ".join(text_parts)))
        else:
            i += 1

    return _group_into_blocks(segments)


def _group_into_blocks(segments: list, block_seconds: int = 300) -> str:
    """Group timestamped segments into 5-minute blocks with timestamp headers."""
    if not segments:
        return ""

    blocks = []
    current_block_start = 0
    current_texts = []

    for start_sec, text in segments:
        if start_sec >= current_block_start + block_seconds and current_texts:
            blocks.append((current_block_start, " ".join(current_texts)))
            current_block_start = (start_sec // block_seconds) * block_seconds
            current_texts = [text]
        else:
            current_texts.append(text)

    if current_texts:
        blocks.append((current_block_start, " ".join(current_texts)))

    # Format with timestamps
    lines = []
    for ts, text in blocks:
        m, s = divmod(ts, 60)
        h, m = divmod(m, 60)
        label = f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"
        lines.append(f"**[{label}]** {text}\n")

    return "\n".join(lines)


def _clean_markdown_transcript(raw: str) -> str:
    """Clean markdown transcript — already readable, minimal processing."""
    text = raw

    # Remove frontmatter if present
    if text.startswith("---"):
        end = text.find("---", 3)
        if end > 0:
            text = text[end + 3:].strip()

    # Remove title line (# Транскрипт: ...)
    lines = text.split("\n")
    if lines and lines[0].startswith("# "):
        lines = lines[1:]

    return "\n".join(lines).strip()


def is_full_transcript_text(transcript: str) -> bool:
    """Check if transcript field contains actual text vs a path or URL."""
    if not transcript:
        return False
    return (
        len(transcript) > 500
        and not transcript.startswith("/")
        and not transcript.startswith("http")
    )
