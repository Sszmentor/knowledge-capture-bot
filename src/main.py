"""Knowledge Capture Bot — FastAPI + periodic sync tasks.

Polls Telegram chats (2h) and LMS (6h), writes new content
to Obsidian vault via Dropbox API.
"""
import asyncio
import hashlib
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI

from src.clients.dropbox_client import DropboxClient
from src.config import Settings, get_settings
from src.extractors.topic_extractor import ExtractedTopic, TopicExtractor
from src.notifier import (
    DigestNotifier,
    periodic_digest,
    record_lms_sync,
    record_tg_sync,
    record_topics,
)
from src.services.book_fetcher import (
    fetch_automation_book,
    fetch_vibe_coding_book,
    format_book_for_obsidian,
)
from src.services.transcript_fetcher import fetch_lms_transcript, is_full_transcript_text
from src.services.youtube_transcript import extract_video_id, fetch_youtube_transcript
from src.sources.lms_source import LmsSource
from src.sources.telegram_source import TelegramMessage, TelegramSource
from src.state import SyncState
from src.writers.lms_formatter import (
    format_kb_article,
    format_lms_session,
    format_materials_page,
    format_sprint,
    get_kb_article_filename,
    get_session_filename,
    get_sprint_filename,
    get_sprint_folder_name,
)
from src.writers.obsidian_writer import ObsidianWriter

logger = logging.getLogger(__name__)

# Global references for periodic tasks
_telegram_task: Optional[asyncio.Task] = None
_lms_task: Optional[asyncio.Task] = None
_digest_task: Optional[asyncio.Task] = None
_notifier: Optional[DigestNotifier] = None
_telegram_source: Optional[TelegramSource] = None
_lms_source: Optional[LmsSource] = None
_topic_extractor: Optional[TopicExtractor] = None
_state: Optional[SyncState] = None
_writer: Optional[ObsidianWriter] = None
_settings: Optional[Settings] = None
_last_sync: Optional[dict] = None
_last_lms_sync: Optional[dict] = None


def _setup_logging(level: str) -> None:
    """Configure logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Quiet noisy libraries
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("dropbox").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


async def sync_telegram() -> dict:
    """Run full Telegram sync for all configured sources.

    Returns dict with sync results per source.
    """
    global _last_sync

    settings = _settings
    tg = _telegram_source
    state = _state
    writer = _writer

    if not all([settings, tg, state, writer]):
        return {"error": "Not initialized"}

    sources = settings.get_tg_sources()
    if not sources:
        logger.warning("No Telegram sources configured (TG_SOURCES is empty)")
        return {"sources": 0, "warning": "no sources configured"}

    results = {}
    total_new = 0
    all_new_messages: list[TelegramMessage] = []

    for source in sources:
        source_key = f"tg:{source.chat_id}"
        try:
            if source.type == "forum":
                sync_result = await tg.sync_forum(
                    source_key=source_key,
                    chat_id=source.chat_id,
                    get_last_msg_id=state.get_last_msg_id,
                )
            else:
                last_id = state.get_last_msg_id(source_key)
                sync_result = await tg.sync_flat(
                    source_key=source_key,
                    chat_id=source.chat_id,
                    last_msg_id=last_id,
                )

            # Write each topic to Obsidian
            for topic in sync_result.topics:
                if not topic.messages:
                    continue

                obsidian_path = writer.write_topic(
                    topic=topic,
                    source_key=source_key,
                    source_name=sync_result.source_name,
                )

                if obsidian_path:
                    # Build state key: for forums include topic_id
                    if topic.topic_id is not None:
                        state_key = f"{source_key}:{topic.topic_id}"
                    else:
                        state_key = source_key

                    state.update_telegram(
                        source_key=state_key,
                        name=f"{source.key}:{topic.topic_title}",
                        last_msg_id=topic.max_msg_id,
                        messages_added=len(topic.messages),
                        obsidian_path=obsidian_path,
                    )

            # Collect messages for topic extraction
            for topic in sync_result.topics:
                all_new_messages.extend(topic.messages)

            results[source.key] = {
                "chat_name": sync_result.source_name,
                "new_messages": sync_result.total_new,
                "topics": len(sync_result.topics),
            }
            total_new += sync_result.total_new

        except Exception as e:
            logger.exception(f"Error syncing {source.key}: {e}")
            results[source.key] = {"error": str(e)}

    # ── Topic Extraction ──────────────────────────────────────────
    topics_extracted = 0
    if (
        _topic_extractor
        and _settings.topic_extraction_enabled
        and total_new >= _settings.min_messages_for_extraction
    ):
        try:
            topics_extracted = await _extract_and_save_topics(all_new_messages)
        except Exception as e:
            logger.exception(f"Topic extraction failed: {e}")

    _last_sync = {
        "timestamp": datetime.now().isoformat(),
        "total_new_messages": total_new,
        "topics_extracted": topics_extracted,
        "sources": results,
    }

    logger.info(
        f"Telegram sync complete: {total_new} new messages "
        f"from {len(sources)} sources, {topics_extracted} topics extracted"
    )
    return _last_sync


async def sync_lms() -> dict:
    """Run LMS sync — fetch bundle + chunks, parse all content, write to Obsidian.

    Uses content hashing to detect changes per item.
    Syncs: sessions, sprints, tools, prompts, metaphors, vibe-coding-kb.
    Labs and masterclasses are fetched and counted but not written as
    individual files (informational only — extend as needed).

    Returns dict with sync results.
    """
    global _last_lms_sync

    state = _state
    settings = _settings

    if not all([_lms_source, state, _writer]):
        return {"error": "Not initialized"}

    dbx = _writer.dbx
    vault_path = settings.dropbox_vault_path

    # ── Pre-fetch bundle and all chunks once ─────────────────────
    try:
        await _lms_source.fetch_bundle()
    except Exception as e:
        logger.exception(f"Failed to fetch LMS bundle: {e}")
        return {"error": str(e)}

    try:
        await _lms_source.fetch_chunks()
    except Exception as e:
        logger.warning(f"Failed to fetch LMS chunks (continuing): {e}")

    # ── Sessions ─────────────────────────────────────────────────
    session_results = await _sync_lms_sessions(dbx, vault_path, settings)

    # ── Sprints ──────────────────────────────────────────────────
    sprint_results = await _sync_lms_sprints(dbx, vault_path, settings)

    # ── Materials: tools, prompts, metaphors ─────────────────────
    materials_results = await _sync_lms_materials(dbx, vault_path, settings)

    # ── Vibe-coding KB ───────────────────────────────────────────
    kb_results = await _sync_lms_kb(dbx, vault_path, settings)

    # ── Books: Automation 101, Vibe-coding 101 ──────────────────
    books_results = await _sync_lms_books(dbx, vault_path, settings)

    # ── Informational: labs and masterclasses ────────────────────
    info_results = await _sync_lms_info(settings)

    _last_lms_sync = {
        "timestamp": datetime.now().isoformat(),
        "sessions": session_results,
        "sprints": sprint_results,
        "materials": materials_results,
        "kb": kb_results,
        "books": books_results,
        "info": info_results,
    }

    total_updated = (
        session_results.get("updated", 0)
        + sprint_results.get("updated", 0)
        + materials_results.get("updated", 0)
        + kb_results.get("updated", 0)
        + books_results.get("updated", 0)
    )
    logger.info(
        f"LMS sync complete: {total_updated} items updated "
        f"(sessions={session_results.get('updated', 0)}, "
        f"sprints={sprint_results.get('updated', 0)}, "
        f"materials={materials_results.get('updated', 0)}, "
        f"kb={kb_results.get('updated', 0)}, "
        f"books={books_results.get('updated', 0)})"
    )
    return _last_lms_sync


async def _sync_lms_sessions(dbx, vault_path: str, settings: Settings) -> dict:
    """Sync LMS sessions to Лаборатории/w26 {lab}/."""
    state = _state
    lms_folder = f"{settings.obsidian_labs_folder}/{settings.obsidian_lab_name}"

    try:
        sessions = await _lms_source.get_sessions()
    except Exception as e:
        logger.exception(f"Failed to parse LMS sessions: {e}")
        return {"error": str(e)}

    updated = 0
    skipped = 0
    errors = 0
    details = {}

    for session in sessions:
        source_key = f"lms:{session.id}"
        try:
            # ── Transcript enrichment ─────────────────────────────
            # Priority: LMS transcript file → YouTube → keep as-is
            if session.transcript and not is_full_transcript_text(session.transcript):
                lms_text = fetch_lms_transcript(session.transcript)
                if lms_text:
                    session.transcript = lms_text
                    logger.info(f"LMS transcript enriched: {session.id} ({len(lms_text)} chars)")
                elif session.video:
                    vid = extract_video_id(session.video)
                    if vid:
                        yt_text = fetch_youtube_transcript(vid)
                        if yt_text:
                            session.transcript = yt_text
                            logger.info(f"YouTube transcript fallback: {session.id}")
                # Rate limit between LMS requests
                time.sleep(1)
            elif not session.transcript and session.video:
                vid = extract_video_id(session.video)
                if vid:
                    yt_text = fetch_youtube_transcript(vid)
                    if yt_text:
                        session.transcript = yt_text
                        logger.info(f"YouTube transcript (no LMS path): {session.id}")
                time.sleep(1)

            # ── Hash: include transcript in change detection ──────
            old_hash = state.get_content_hash(source_key)
            hash_input = json.dumps(session.raw, sort_keys=True, ensure_ascii=False)
            hash_input += session.transcript or ""
            new_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:16]

            if old_hash == new_hash:
                skipped += 1
                continue

            markdown = format_lms_session(session)
            filename = get_session_filename(session)

            # Determine subfolder within the lab based on session type
            sid = session.id.lower() if session.id else ""
            if sid.startswith("pos-"):
                # POS sprint sessions → Лаборатории/POS {sprint}/
                subfolder = f"{settings.obsidian_labs_folder}/POS {{sprint}}"
            elif sid.startswith("ws") or sid.startswith("bonus"):
                subfolder = f"{lms_folder}/Workshops"
            elif sid.startswith("at"):
                subfolder = f"{lms_folder}/Advanced"
            elif sid.startswith("oh"):
                subfolder = f"{lms_folder}/Office Hours"
            elif sid.startswith("fs") or sid.startswith("fos"):
                subfolder = f"{lms_folder}/Focus Sessions"
            else:
                subfolder = lms_folder

            dropbox_path = f"{vault_path}/{subfolder}/{filename}.md"
            relative_path = f"{subfolder}/{filename}.md"

            result = dbx.upload_file(markdown, dropbox_path, overwrite=True)

            if result:
                state.update_lms(
                    source_key=source_key,
                    content_hash=new_hash,
                    obsidian_path=relative_path,
                )
                updated += 1
                details[session.id] = {
                    "title": session.title,
                    "status": "updated",
                    "path": relative_path,
                }
                logger.info(f"LMS session: updated {session.id} → {filename}")
            else:
                errors += 1
                details[session.id] = {"error": "upload failed"}
                logger.error(f"LMS session: failed to upload {session.id}")

        except Exception as e:
            errors += 1
            details[session.id] = {"error": str(e)}
            logger.exception(f"LMS session: error processing {session.id}: {e}")

    logger.info(
        f"Sessions: {updated} updated, {skipped} unchanged, {errors} errors "
        f"(total {len(sessions)})"
    )
    return {
        "total": len(sessions),
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "details": details,
    }


async def _sync_lms_sprints(dbx, vault_path: str, settings: Settings) -> dict:
    """Sync LMS sprints — each into its own subfolder inside Лаборатории."""
    state = _state

    try:
        sprints = await _lms_source.get_sprints()
    except Exception as e:
        logger.exception(f"Failed to parse LMS sprints: {e}")
        return {"error": str(e), "updated": 0}

    if not sprints:
        logger.info("No sprints found (chunk may not exist yet)")
        return {"total": 0, "updated": 0, "skipped": 0, "errors": 0}

    updated = 0
    skipped = 0
    errors = 0
    details = {}

    for sprint in sprints:
        sprint_id = sprint.get("id", "")
        source_key = f"lms:sprint:{sprint_id}"
        try:
            new_hash = _lms_source.content_hash_for(sprint)
            old_hash = state.get_content_hash(source_key)

            if old_hash == new_hash:
                skipped += 1
                continue

            markdown = format_sprint(sprint)
            sprint_folder_name = get_sprint_folder_name(sprint)
            filename = get_sprint_filename(sprint)
            if sprint_id.lower() == "pos":
                sprint_folder = f"{settings.obsidian_labs_folder}/{sprint_folder_name}"
            else:
                sprint_folder = f"{settings.obsidian_labs_folder}/{settings.obsidian_lab_name}/Sprints/{sprint_folder_name}"
            dropbox_path = f"{vault_path}/{sprint_folder}/{filename}.md"
            relative_path = f"{sprint_folder}/{filename}.md"

            result = dbx.upload_file(markdown, dropbox_path, overwrite=True)

            if result:
                state.update_lms(
                    source_key=source_key,
                    content_hash=new_hash,
                    obsidian_path=relative_path,
                )
                updated += 1
                title = sprint.get("title", sprint_id)
                details[sprint_id] = {
                    "title": title,
                    "status": "updated",
                    "path": relative_path,
                }
                logger.info(f"LMS sprint: updated {sprint_id} → {filename}")
            else:
                errors += 1
                details[sprint_id] = {"error": "upload failed"}
                logger.error(f"LMS sprint: failed to upload {sprint_id}")

        except Exception as e:
            errors += 1
            details[sprint_id] = {"error": str(e)}
            logger.exception(f"LMS sprint: error processing {sprint_id}: {e}")

    logger.info(
        f"Sprints: {updated} updated, {skipped} unchanged, {errors} errors "
        f"(total {len(sprints)})"
    )
    return {
        "total": len(sprints),
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "details": details,
    }


async def _sync_lms_materials(dbx, vault_path: str, settings: Settings) -> dict:
    """Sync tools, prompts, and metaphors as reference pages."""
    state = _state
    materials_folder = settings.obsidian_materials_folder
    updated = 0
    errors = 0
    details = {}

    material_types = [
        ("tools", "lms:tools", "инструменты"),
        ("prompts", "lms:prompts", "промпты"),
        ("metaphors", "lms:metaphors", "метафоры"),
        ("speakers", "lms:speakers", "эксперты"),
    ]

    for content_type, source_key, filename_base in material_types:
        try:
            if content_type == "tools":
                items = await _lms_source.get_tools()
            elif content_type == "prompts":
                items = await _lms_source.get_prompts()
            elif content_type == "metaphors":
                items = await _lms_source.get_metaphors()
            else:
                items = await _lms_source.get_speakers()

            if not items:
                logger.info(f"Materials '{content_type}': no items found")
                details[content_type] = {"total": 0, "status": "empty"}
                continue

            new_hash = _lms_source.content_hash_for(items)
            old_hash = state.get_content_hash(source_key)

            if old_hash == new_hash:
                details[content_type] = {
                    "total": len(items),
                    "status": "unchanged",
                }
                continue

            markdown = format_materials_page(content_type, items)
            dropbox_path = f"{vault_path}/{materials_folder}/{filename_base}.md"
            relative_path = f"{materials_folder}/{filename_base}.md"

            result = dbx.upload_file(markdown, dropbox_path, overwrite=True)

            if result:
                state.update_lms(
                    source_key=source_key,
                    content_hash=new_hash,
                    obsidian_path=relative_path,
                )
                updated += 1
                details[content_type] = {
                    "total": len(items),
                    "status": "updated",
                    "path": relative_path,
                }
                logger.info(
                    f"LMS materials: updated {content_type} "
                    f"({len(items)} items) → {filename_base}.md"
                )
            else:
                errors += 1
                details[content_type] = {"error": "upload failed"}
                logger.error(f"LMS materials: failed to upload {content_type}")

        except Exception as e:
            errors += 1
            details[content_type] = {"error": str(e)}
            logger.exception(
                f"LMS materials: error processing {content_type}: {e}"
            )

    logger.info(
        f"Materials: {updated} updated, {errors} errors"
    )
    return {
        "updated": updated,
        "errors": errors,
        "details": details,
    }


async def _sync_lms_kb(dbx, vault_path: str, settings: Settings) -> dict:
    """Sync vibe-coding knowledge base articles to obsidian_kb_folder."""
    state = _state
    kb_folder = settings.obsidian_kb_folder

    try:
        kb_items = await _lms_source.get_vibe_coding_kb()
    except Exception as e:
        logger.exception(f"Failed to parse vibe-coding-kb: {e}")
        return {"error": str(e), "updated": 0}

    if not kb_items:
        logger.info("No KB articles found (chunk may not exist yet)")
        return {"total": 0, "updated": 0, "skipped": 0, "errors": 0}

    updated = 0
    skipped = 0
    errors = 0
    details = {}

    for article in kb_items:
        article_id = article.get("id", "")
        source_key = f"lms:kb:{article_id}"
        try:
            new_hash = _lms_source.content_hash_for(article)
            old_hash = state.get_content_hash(source_key)

            if old_hash == new_hash:
                skipped += 1
                continue

            markdown = format_kb_article(article)
            filename = get_kb_article_filename(article)
            dropbox_path = f"{vault_path}/{kb_folder}/{filename}.md"
            relative_path = f"{kb_folder}/{filename}.md"

            result = dbx.upload_file(markdown, dropbox_path, overwrite=True)

            if result:
                state.update_lms(
                    source_key=source_key,
                    content_hash=new_hash,
                    obsidian_path=relative_path,
                )
                updated += 1
                title = article.get("title", article_id)
                details[article_id] = {
                    "title": title,
                    "status": "updated",
                    "path": relative_path,
                }
                logger.info(f"LMS KB: updated {article_id} → {filename}")
            else:
                errors += 1
                details[article_id] = {"error": "upload failed"}
                logger.error(f"LMS KB: failed to upload {article_id}")

        except Exception as e:
            errors += 1
            details[article_id] = {"error": str(e)}
            logger.exception(
                f"LMS KB: error processing {article_id}: {e}"
            )

    logger.info(
        f"KB articles: {updated} updated, {skipped} unchanged, {errors} errors "
        f"(total {len(kb_items)})"
    )
    return {
        "total": len(kb_items),
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "details": details,
    }


async def _sync_lms_books(dbx, vault_path: str, settings: Settings) -> dict:
    """Sync structured books (Automation 101, Vibe-coding 101) to KB folder.

    Fetches chapter content from individual JS chunks and writes as
    single comprehensive Obsidian documents.
    """
    state = _state
    kb_folder = settings.obsidian_kb_folder
    updated = 0
    errors = 0
    details = {}

    bundle_content = _lms_source._bundle_content
    if not bundle_content:
        return {"error": "Bundle not loaded", "updated": 0}

    # ── Automation 101 ────────────────────────────────────────────
    try:
        auto_blocks = await fetch_automation_book(bundle_content)
        if auto_blocks:
            total_chars = sum(
                len(ch.content) for b in auto_blocks for ch in b.chapters
            )
            if total_chars > 1000:
                markdown = format_book_for_obsidian(
                    title="Автоматизация 101",
                    description="Введение в автоматизацию — от основ до продвинутых практик",
                    blocks=auto_blocks,
                )
                source_key = "lms:book:automation-101"
                new_hash = hashlib.sha256(markdown.encode()).hexdigest()[:16]
                old_hash = state.get_content_hash(source_key)

                if old_hash != new_hash:
                    path = f"{vault_path}/{kb_folder}/автоматизация 101.md"
                    result = dbx.upload_file(markdown, path, overwrite=True)
                    if result:
                        state.update_lms(
                            source_key=source_key,
                            content_hash=new_hash,
                            obsidian_path=f"{kb_folder}/автоматизация 101.md",
                        )
                        updated += 1
                        details["automation-101"] = {
                            "status": "updated",
                            "blocks": len(auto_blocks),
                            "chapters": sum(len(b.chapters) for b in auto_blocks),
                            "chars": total_chars,
                        }
                        logger.info(
                            f"Automation 101: updated ({total_chars:,} chars, "
                            f"{sum(len(b.chapters) for b in auto_blocks)} chapters)"
                        )
                    else:
                        errors += 1
                        details["automation-101"] = {"error": "upload failed"}
                else:
                    details["automation-101"] = {"status": "unchanged"}
            else:
                logger.warning(f"Automation 101: insufficient content ({total_chars} chars)")
                details["automation-101"] = {"status": "insufficient_content"}
        else:
            logger.info("Automation 101: no blocks found")
            details["automation-101"] = {"status": "not_found"}
    except Exception as e:
        errors += 1
        details["automation-101"] = {"error": str(e)}
        logger.exception(f"Automation 101 sync error: {e}")

    # ── Vibe-coding 101 ──────────────────────────────────────────
    try:
        vc_blocks = await fetch_vibe_coding_book(bundle_content)
        if vc_blocks:
            total_chars = sum(
                len(ch.content) for b in vc_blocks for ch in b.chapters
            )
            if total_chars > 1000:
                markdown = format_book_for_obsidian(
                    title="Vibe-coding 101",
                    description="Введение в vibe-coding — от идеи до прототипа",
                    blocks=vc_blocks,
                )
                source_key = "lms:book:vibe-coding-101"
                new_hash = hashlib.sha256(markdown.encode()).hexdigest()[:16]
                old_hash = state.get_content_hash(source_key)

                if old_hash != new_hash:
                    path = f"{vault_path}/{kb_folder}/vibe-coding 101.md"
                    result = dbx.upload_file(markdown, path, overwrite=True)
                    if result:
                        state.update_lms(
                            source_key=source_key,
                            content_hash=new_hash,
                            obsidian_path=f"{kb_folder}/vibe-coding 101.md",
                        )
                        updated += 1
                        details["vibe-coding-101"] = {
                            "status": "updated",
                            "chapters": sum(len(b.chapters) for b in vc_blocks),
                            "chars": total_chars,
                        }
                        logger.info(
                            f"Vibe-coding 101: updated ({total_chars:,} chars, "
                            f"{sum(len(b.chapters) for b in vc_blocks)} chapters)"
                        )
                    else:
                        errors += 1
                        details["vibe-coding-101"] = {"error": "upload failed"}
                else:
                    details["vibe-coding-101"] = {"status": "unchanged"}
            else:
                logger.warning(f"Vibe-coding 101: insufficient content ({total_chars} chars)")
                details["vibe-coding-101"] = {"status": "insufficient_content"}
        else:
            logger.info("Vibe-coding 101: no blocks found")
            details["vibe-coding-101"] = {"status": "not_found"}
    except Exception as e:
        errors += 1
        details["vibe-coding-101"] = {"error": str(e)}
        logger.exception(f"Vibe-coding 101 sync error: {e}")

    return {"updated": updated, "errors": errors, "details": details}


async def _sync_lms_info(settings: Settings) -> dict:
    """Fetch labs and masterclasses for informational count only.

    These are not written as individual files but their counts are
    included in the sync report. Extend this function to write them
    if needed in the future.
    """
    info: dict = {}

    try:
        labs = await _lms_source.get_labs()
        info["labs"] = {"total": len(labs)}
        if labs:
            logger.info(f"LMS labs (informational): {len(labs)} found")
    except Exception as e:
        logger.warning(f"Could not fetch labs: {e}")
        info["labs"] = {"error": str(e)}

    try:
        masterclasses = await _lms_source.get_masterclasses()
        info["masterclasses"] = {"total": len(masterclasses)}
        if masterclasses:
            logger.info(
                f"LMS masterclasses (informational): {len(masterclasses)} found"
            )
    except Exception as e:
        logger.warning(f"Could not fetch masterclasses: {e}")
        info["masterclasses"] = {"error": str(e)}

    try:
        programs = await _lms_source.get_programs()
        info["programs"] = {"total": len(programs)}
        if programs:
            logger.info(
                f"LMS programs (informational): {len(programs)} found"
            )
    except Exception as e:
        logger.warning(f"Could not fetch programs: {e}")
        info["programs"] = {"error": str(e)}

    return info


async def _extract_and_save_topics(
    messages: list[TelegramMessage],
) -> int:
    """Extract topics from messages and save to Dropbox pipeline folder.

    Returns number of topics extracted.
    """
    settings = _settings
    dbx = _writer.dbx
    vault_path = settings.dropbox_vault_path
    pipeline_folder = settings.pipeline_folder

    # Load existing topic titles for dedup
    existing_titles = await _load_existing_topic_titles()

    # Group messages by source chat for better context
    # (all messages have sender info, use source_chat from sync)
    topics = await _topic_extractor.extract_topics(
        messages=messages,
        source_chat="AI Mindset chats",
        existing_titles=existing_titles,
    )

    if not topics:
        logger.info("No topics extracted from current batch")
        return 0

    # Record topics for digest notifications
    record_topics([t.to_dict() for t in topics])

    # Save each topic as JSON to Dropbox
    today = datetime.now().strftime("%Y-%m-%d")
    saved = 0

    for topic in topics:
        filename = f"{today}-{topic.id}.json"
        dropbox_path = f"{vault_path}/{pipeline_folder}/topics/{filename}"

        try:
            content = topic.to_json()
            result = dbx.upload_file(content, dropbox_path, overwrite=False)
            if result:
                saved += 1
                logger.info(f"Pipeline: saved topic '{topic.title}' → {filename}")
        except Exception as e:
            logger.error(f"Failed to save topic '{topic.title}': {e}")

    return saved


async def _load_existing_topic_titles() -> list[str]:
    """Load titles of already extracted topics from Dropbox."""
    settings = _settings
    dbx = _writer.dbx
    vault_path = settings.dropbox_vault_path
    pipeline_folder = settings.pipeline_folder
    topics_path = f"{vault_path}/{pipeline_folder}/topics"

    titles = []
    try:
        entries = dbx.list_folder(topics_path)
        for entry in entries:
            if not entry.name.endswith(".json"):
                continue
            try:
                content = dbx.download_text(f"{topics_path}/{entry.name}")
                if content:
                    data = json.loads(content)
                    title = data.get("title", "")
                    if title:
                        titles.append(title)
            except Exception:
                continue
    except Exception as e:
        # Folder might not exist yet — that's fine
        logger.debug(f"Could not list existing topics: {e}")

    return titles


async def _periodic_telegram_sync(interval_seconds: int) -> None:
    """Periodic Telegram sync task."""
    # First sync: 30 seconds after startup (let Telethon connect)
    await asyncio.sleep(30)

    while True:
        try:
            logger.info("Periodic sync: starting Telegram check...")
            result = await sync_telegram()
            record_tg_sync(result)
        except Exception as e:
            logger.exception(f"Error in periodic Telegram sync: {e}")

        await asyncio.sleep(interval_seconds)


async def _periodic_lms_sync(interval_seconds: int) -> None:
    """Periodic LMS sync task."""
    # First sync: 60 seconds after startup
    await asyncio.sleep(60)

    while True:
        try:
            logger.info("Periodic sync: starting LMS check...")
            result = await sync_lms()
            record_lms_sync(result)
        except Exception as e:
            logger.exception(f"Error in periodic LMS sync: {e}")

        await asyncio.sleep(interval_seconds)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize clients on startup, cleanup on shutdown."""
    global _telegram_task, _lms_task, _digest_task, _notifier
    global _telegram_source, _lms_source, _topic_extractor
    global _state, _writer, _settings

    settings = get_settings()
    _settings = settings

    # Setup logging
    _setup_logging(settings.log_level)

    # Validate config
    errors = settings.validate_required()
    if errors:
        for err in errors:
            logger.error(f"Config error: {err}")
        logger.error("Cannot start — fix configuration errors above")
        yield
        return

    logger.info("=== Knowledge Capture Bot starting ===")

    # Initialize Dropbox
    dbx = None
    try:
        dbx = DropboxClient(
            app_key=settings.dropbox_app_key,
            app_secret=settings.dropbox_app_secret,
            refresh_token=settings.dropbox_refresh_token,
        )
    except Exception as e:
        logger.error(f"Dropbox init failed: {e}. Sync will not work.")

    if dbx:
        # Initialize state
        _state = SyncState(
            dropbox_client=dbx,
            vault_path=settings.dropbox_vault_path,
            state_file=settings.state_file_path,
        )

        # Initialize writer
        _writer = ObsidianWriter(
            dropbox_client=dbx,
            vault_path=settings.dropbox_vault_path,
            labs_folder=settings.obsidian_labs_folder,
            lab_name=settings.obsidian_lab_name,
        )

    # Initialize Telegram
    _telegram_source = TelegramSource(
        api_id=settings.telegram_api_id,
        api_hash=settings.telegram_api_hash,
        session_string=settings.telegram_session_string,
    )

    try:
        await _telegram_source.connect()
    except Exception as e:
        logger.error(f"Failed to connect to Telegram: {e}")
        _telegram_source = None

    # Initialize LMS source
    _lms_source = LmsSource(base_url=settings.lms_base_url)

    # Initialize Topic Extractor (pipeline)
    if settings.anthropic_api_key and settings.topic_extraction_enabled:
        _topic_extractor = TopicExtractor(api_key=settings.anthropic_api_key)
        logger.info("Topic extraction enabled (pipeline)")
    else:
        logger.info("Topic extraction disabled (no ANTHROPIC_API_KEY or disabled)")

    # Start periodic tasks only if Dropbox is ready
    if _telegram_source and dbx:
        interval = settings.telegram_poll_interval
        logger.info(
            f"Starting periodic Telegram sync (every {interval // 3600}h "
            f"{(interval % 3600) // 60}m)"
        )
        _telegram_task = asyncio.create_task(_periodic_telegram_sync(interval))
    else:
        logger.warning(
            "Periodic Telegram sync NOT started — "
            f"Telegram={'OK' if _telegram_source else 'FAIL'}, "
            f"Dropbox={'OK' if dbx else 'FAIL'}"
        )

    if dbx:
        lms_interval = settings.lms_poll_interval
        logger.info(
            f"Starting periodic LMS sync (every {lms_interval // 3600}h "
            f"{(lms_interval % 3600) // 60}m)"
        )
        _lms_task = asyncio.create_task(_periodic_lms_sync(lms_interval))
    else:
        logger.warning("Periodic LMS sync NOT started — Dropbox not available")

    # Initialize Digest Notifier
    if settings.digest_enabled and settings.tvorets_bot_token:
        _notifier = DigestNotifier(
            bot_token=settings.tvorets_bot_token,
            admin_chat_id=settings.digest_admin_id,
            anthropic_api_key=settings.anthropic_api_key,
        )
        _digest_task = asyncio.create_task(
            periodic_digest(
                notifier=_notifier,
                morning_hour=settings.digest_morning_hour,
                evening_hour=settings.digest_evening_hour,
            )
        )
        logger.info(
            f"Digest notifications enabled "
            f"({settings.digest_morning_hour}:00 + "
            f"{settings.digest_evening_hour}:00 Novosibirsk)"
        )
    else:
        if not settings.tvorets_bot_token:
            logger.info("Digest notifications disabled (no TVORETS_BOT_TOKEN)")
        else:
            logger.info("Digest notifications disabled (DIGEST_ENABLED=false)")

    logger.info("=== Knowledge Capture Bot ready ===")

    yield  # App runs

    # Cleanup
    logger.info("=== Shutting down ===")

    for task in [_telegram_task, _lms_task, _digest_task]:
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    if _telegram_source:
        await _telegram_source.disconnect()

    logger.info("=== Shutdown complete ===")


app = FastAPI(
    title="Knowledge Capture Bot",
    description="Автоматический сбор материалов из Telegram и LMS в Obsidian",
    version="1.7.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    """Railway health check."""
    return {
        "status": "ok",
        "telegram_connected": _telegram_source is not None,
        "dropbox_connected": _state is not None,
        "lms_available": _lms_source is not None,
        "pipeline_enabled": _topic_extractor is not None,
        "digest_enabled": _notifier is not None,
        "uptime": datetime.now().isoformat(),
    }


@app.post("/sync")
async def manual_sync():
    """Manual sync trigger — all sources (Telegram + LMS)."""
    tg_result = await sync_telegram()
    record_tg_sync(tg_result)
    lms_result = await sync_lms()
    record_lms_sync(lms_result)
    return {"telegram": tg_result, "lms": lms_result}


@app.post("/sync/telegram")
async def manual_sync_telegram():
    """Manual sync trigger — Telegram only."""
    result = await sync_telegram()
    record_tg_sync(result)
    return result


@app.post("/sync/lms")
async def manual_sync_lms():
    """Manual sync trigger — LMS only."""
    result = await sync_lms()
    record_lms_sync(result)
    return result


@app.get("/topics")
async def list_topics():
    """List extracted pipeline topics from Dropbox."""
    if not _writer or not _settings:
        return {"error": "Not initialized"}

    dbx = _writer.dbx
    vault_path = _settings.dropbox_vault_path
    topics_path = f"{vault_path}/{_settings.pipeline_folder}/topics"

    topics = []
    try:
        entries = dbx.list_folder(topics_path)
        for entry in entries:
            if not entry.name.endswith(".json"):
                continue
            try:
                content = dbx.download_text(f"{topics_path}/{entry.name}")
                if content:
                    data = json.loads(content)
                    topics.append(data)
            except Exception:
                continue
    except Exception as e:
        return {"error": f"Could not list topics: {e}", "topics": []}

    # Sort by created_at descending
    topics.sort(key=lambda t: t.get("created_at", ""), reverse=True)

    return {
        "total": len(topics),
        "topics": topics,
    }


@app.post("/topics/extract")
async def manual_extract_topics():
    """Manual topic extraction — re-sync telegram then extract."""
    if not _topic_extractor:
        return {"error": "Topic extraction not configured (set ANTHROPIC_API_KEY)"}

    # First sync to get fresh messages
    sync_result = await sync_telegram()
    topics_count = sync_result.get("topics_extracted", 0)

    return {
        "sync": sync_result,
        "topics_extracted": topics_count,
    }


@app.post("/digest")
async def manual_digest():
    """Manual digest trigger — for testing."""
    if not _notifier:
        return {"sent": False, "reason": "digest not configured (no TVORETS_BOT_TOKEN)"}
    try:
        sent = await _notifier.send_digest()
        if sent:
            return {"sent": True}
        else:
            return {"sent": False, "reason": "nothing new since last digest"}
    except Exception as e:
        logger.exception(f"Manual digest failed: {e}")
        return {"sent": False, "reason": str(e)}


@app.get("/status")
async def status():
    """Current sync status."""
    return {
        "last_telegram_sync": _last_sync,
        "last_lms_sync": _last_lms_sync,
        "state": _state.get_all() if _state else {},
        "sources_configured": len(_settings.get_tg_sources()) if _settings else 0,
        "pipeline_enabled": _topic_extractor is not None,
    }


@app.get("/lms/needs-transcription")
async def needs_transcription():
    """List sessions that have video but no transcript."""
    settings = get_settings()
    lms = LmsSource(base_url=settings.lms_base_url)
    sessions = await lms.get_sessions()

    needs = []
    for s in sessions:
        if s.video and not s.transcript:
            needs.append({
                "id": s.id,
                "title": s.title,
                "video": s.video,
            })

    return {"count": len(needs), "sessions": needs}


@app.post("/admin/force-resync-lms")
async def force_resync_lms():
    """Clear LMS content hashes to force full re-sync on next run.

    Use after adding new features (e.g. transcripts) to update all notes.
    """
    state = _state
    if not state:
        return {"error": "Not initialized"}

    all_state = state.get_all()
    cleared = 0
    for key in list(all_state.keys()):
        if key.startswith("lms:"):
            entry = all_state[key]
            if "content_hash" in entry:
                entry["content_hash"] = ""
                cleared += 1

    state._save()
    logger.info(f"Force resync: cleared {cleared} LMS session hashes")

    return {
        "status": "ok",
        "cleared_hashes": cleared,
        "next_step": "POST /sync/lms to apply",
    }


@app.get("/admin/migration-preview")
async def migration_preview():
    """Preview what would be moved without executing."""
    return {
        "info": "Call POST /admin/migrate-structure to execute",
        "plan": {
            "sessions": "W26 Winter 2026/*.md -> Лаборатории/w26 {lab}/",
            "sprints": "W26 Winter 2026/Sprints/* -> Лаборатории/SPRINT {sprint}/",
            "materials": "W26 Winter 2026/База знаний/tools,prompts,metaphors -> Материалы/",
            "kb": "W26 Winter 2026/База знаний/kb-articles -> База знаний/",
            "chats": "W26 Winter 2026/Чаты/* -> Лаборатории/w26 {lab}/Чаты/",
            "preserved": "W26 Winter 2026/W1-W4/ — NOT MOVED (manual notes)",
        }
    }


@app.post("/admin/migrate-structure")
async def migrate_structure():
    """One-time migration: move files from old structure to new."""
    settings = get_settings()
    dbx = DropboxClient(
        settings.dropbox_app_key,
        settings.dropbox_app_secret,
        settings.dropbox_refresh_token,
    )

    vault = settings.dropbox_vault_path
    base = f"{vault}/20 Projects/AI_Mindset"
    old_w26 = f"{base}/W26 Winter 2026"
    new_labs = f"{base}/Лаборатории"
    new_lab = f"{new_labs}/w26 {{lab}}"
    new_materials = f"{base}/Материалы"
    new_kb = f"{base}/База знаний"

    report = {"moved": [], "skipped": [], "errors": [], "deleted": []}

    # Create target folders
    for folder in [new_labs, new_lab, f"{new_lab}/Чаты", new_materials, new_kb]:
        dbx.create_folder(folder)

    # Create sprint subfolders
    sprint_ids = ["pos", "automation", "bos", "vibe-coding", "music",
                  "knowledge", "art", "research", "presentation", "coaching"]
    for sid in sprint_ids:
        dbx.create_folder(f"{new_labs}/{sid.upper()} {{sprint}}")

    # Move sessions (24 files)
    session_patterns = [
        "WS00", "WS01", "WS02", "WS03", "WS04",
        "AT01", "AT02", "AT03", "AT04", "AT05",
        "BONUS01", "BONUS02", "BONUS03", "BONUS04",
        "OH01", "OH02", "OH03", "OH04",
        "FS01", "FS02", "FS03", "FS04",
        "FOS18",
    ]

    # List files in old W26 folder
    old_files = dbx.list_folder(old_w26) or []
    for entry in old_files:
        name = entry.name if hasattr(entry, 'name') else str(entry)
        if not name.endswith('.md'):
            continue
        # Check if it's a session file
        is_session = any(name.startswith(p) for p in session_patterns)
        if is_session:
            old_path = f"{old_w26}/{name}"
            new_path = f"{new_lab}/{name}"
            if not dbx.file_exists(new_path):
                result = dbx.move_file(old_path, new_path)
                if result:
                    report["moved"].append(f"{name} -> Лаборатории/w26 {{lab}}/")
                else:
                    report["errors"].append(f"Failed to move {name}")
            else:
                report["skipped"].append(name)

    # Move sprints (from Sprints/ to individual sprint folders)
    old_sprints = f"{old_w26}/Sprints"
    sprint_files = dbx.list_folder(old_sprints) or []
    for entry in sprint_files:
        name = entry.name if hasattr(entry, 'name') else str(entry)
        if not name.endswith('.md'):
            continue
        # Extract sprint ID from filename (e.g., "POS POS.md" -> "POS")
        sprint_id = name.split()[0] if ' ' in name else name.replace('.md', '')
        old_path = f"{old_sprints}/{name}"
        # New filename without duplication
        new_filename = f"{sprint_id}.md"
        new_path = f"{new_labs}/{sprint_id} {{sprint}}/{new_filename}"
        if not dbx.file_exists(new_path):
            result = dbx.move_file(old_path, new_path)
            if result:
                report["moved"].append(f"Sprints/{name} -> Лаборатории/{sprint_id} {{sprint}}/{new_filename}")
            else:
                report["errors"].append(f"Failed to move sprint {name}")
        else:
            report["skipped"].append(f"Sprint {name}")

    # Move materials (tools, prompts, metaphors, speakers)
    material_moves = {
        "Инструменты.md": "Инструменты.md",
        "Инструменты W26.md": "Инструменты.md",  # stale duplicate
        "Промпты.md": "Промпты.md",
        "Промпты W26.md": "Промпты.md",  # stale duplicate
        "Метафоры.md": "Метафоры.md",
        "Метафоры W26.md": "Метафоры.md",  # stale duplicate
        "Спикеры W26.md": "Эксперты.md",
    }
    old_kb_path = f"{old_w26}/База знаний"
    for old_name, new_name in material_moves.items():
        old_path = f"{old_kb_path}/{old_name}"
        new_path = f"{new_materials}/{new_name}"
        if dbx.file_exists(old_path) and not dbx.file_exists(new_path):
            result = dbx.move_file(old_path, new_path)
            if result:
                report["moved"].append(f"База знаний/{old_name} -> Материалы/{new_name}")
            else:
                report["errors"].append(f"Failed to move material {old_name}")
        elif dbx.file_exists(old_path) and dbx.file_exists(new_path):
            # Delete stale duplicate
            dbx.delete_file(old_path)
            report["deleted"].append(old_name)

    # Move KB articles
    kb_files = dbx.list_folder(old_kb_path) or []
    for entry in kb_files:
        name = entry.name if hasattr(entry, 'name') else str(entry)
        if not name.endswith('.md'):
            continue
        # Skip already-moved materials
        if name in material_moves:
            continue
        old_path = f"{old_kb_path}/{name}"
        new_path = f"{new_kb}/{name}"
        if not dbx.file_exists(new_path):
            result = dbx.move_file(old_path, new_path)
            if result:
                report["moved"].append(f"База знаний/{name} -> База знаний/{name}")
            else:
                report["errors"].append(f"Failed to move KB {name}")
        else:
            report["skipped"].append(f"KB {name}")

    # Move chats into lab
    old_chats = f"{old_w26}/Чаты"
    chat_files = dbx.list_folder(old_chats) or []
    chat_rename = {
        "w26 General.md": "General.md",
        "w26 Support.md": "Support.md",
        "w26 Intro.md": "Intro.md",
        "w26 Materials-Org.md": "Materials-Org.md",
        "w26 Advanced.md": "Advanced.md",
        "w26 Корпоративный ИИ для команды.md": "Корпоративный ИИ для команды.md",
        "w26 Корпоративный ИИ для команде.md": "Корпоративный ИИ для команды.md",
        "w26 Учимся проектировать ПО.md": "Учимся проектировать ПО.md",
    }
    for entry in chat_files:
        name = entry.name if hasattr(entry, 'name') else str(entry)
        if not name.endswith('.md'):
            continue
        new_name = chat_rename.get(name, name)
        old_path = f"{old_chats}/{name}"
        new_path = f"{new_lab}/Чаты/{new_name}"
        if not dbx.file_exists(new_path):
            result = dbx.move_file(old_path, new_path)
            if result:
                report["moved"].append(f"Чаты/{name} -> w26 {{lab}}/Чаты/{new_name}")
            else:
                report["errors"].append(f"Failed to move chat {name}")
        else:
            report["skipped"].append(f"Chat {name}")

    return {
        "status": "completed",
        "summary": {
            "moved": len(report["moved"]),
            "skipped": len(report["skipped"]),
            "deleted": len(report["deleted"]),
            "errors": len(report["errors"]),
        },
        "details": report,
    }
