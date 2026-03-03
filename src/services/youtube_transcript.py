"""YouTube transcript fallback — used when LMS transcript is unavailable.

Uses youtube-transcript-api to fetch auto/manual captions.
Priority: ru manual → en manual → ru auto → en auto → any available.
"""
import logging
import re

logger = logging.getLogger(__name__)


def extract_video_id(video_url: str) -> str:
    """Extract YouTube video ID from various URL formats."""
    if not video_url:
        return ""

    # Already just an ID (11 chars, alphanumeric + - _)
    if re.match(r"^[\w-]{11}$", video_url):
        return video_url

    # youtu.be/VIDEO_ID
    m = re.search(r"youtu\.be/([\w-]{11})", video_url)
    if m:
        return m.group(1)

    # youtube.com/watch?v=VIDEO_ID
    m = re.search(r"[?&]v=([\w-]{11})", video_url)
    if m:
        return m.group(1)

    # youtube.com/embed/VIDEO_ID
    m = re.search(r"embed/([\w-]{11})", video_url)
    if m:
        return m.group(1)

    return ""


def fetch_youtube_transcript(video_id: str) -> str:
    """Fetch YouTube transcript, format with 5-min timestamps and YT links.

    Returns formatted text or empty string on failure.
    """
    if not video_id:
        return ""

    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        from youtube_transcript_api._errors import (
            NoTranscriptFound,
            TranscriptsDisabled,
        )
    except ImportError:
        logger.warning("youtube-transcript-api not installed, skipping YT fallback")
        return ""

    try:
        ytt = YouTubeTranscriptApi()

        # Try fetching with language priority
        snippets = None
        lang_source = ""

        for langs in [["ru"], ["en"], ["ru", "en"]]:
            try:
                snippets = ytt.fetch(video_id, languages=langs)
                lang_source = langs[0]
                break
            except (NoTranscriptFound, Exception):
                continue

        if not snippets:
            # Try any available language
            try:
                snippets = ytt.fetch(video_id)
                lang_source = "auto"
            except Exception:
                pass

        if not snippets:
            logger.info(f"No YouTube transcript found for {video_id}")
            return ""

        return _format_yt_transcript(snippets, video_id, lang_source)

    except TranscriptsDisabled:
        logger.info(f"Transcripts disabled for YouTube video {video_id}")
        return ""
    except Exception as e:
        logger.warning(f"YouTube transcript failed for {video_id}: {e}")
        return ""


def _format_yt_transcript(snippets, video_id: str, lang: str) -> str:
    """Group YouTube snippets into 5-minute blocks with clickable timestamps."""
    blocks = []
    current_block = []
    current_mark = 0  # seconds

    for s in snippets:
        start = s.start if hasattr(s, 'start') else s.get('start', 0)
        text = s.text if hasattr(s, 'text') else s.get('text', '')

        if start >= current_mark + 300:  # new 5-min block
            if current_block:
                blocks.append((current_mark, " ".join(current_block)))
            current_mark = (int(start) // 300) * 300
            current_block = [text]
        else:
            current_block.append(text)

    # Last block
    if current_block:
        blocks.append((current_mark, " ".join(current_block)))

    if not blocks:
        return ""

    # Format as markdown with YouTube links
    lines = []
    for ts, text in blocks:
        m, sec = divmod(ts, 60)
        h, m = divmod(m, 60)
        if h:
            label = f"{h}:{m:02d}:{sec:02d}"
        else:
            label = f"{m}:{sec:02d}"
        yt_link = f"https://youtu.be/{video_id}?t={ts}"
        lines.append(f"**[{label}]({yt_link})** {text}\n")

    return "\n".join(lines)
