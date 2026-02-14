"""Knowledge Capture Bot — FastAPI + periodic sync tasks.

Polls Telegram chats (2h) and LMS (6h), writes new content
to Obsidian vault via Dropbox API.
"""
import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI

from src.clients.dropbox_client import DropboxClient
from src.config import Settings, get_settings
from src.extractors.topic_extractor import ExtractedTopic, TopicExtractor
from src.sources.lms_source import LmsSource
from src.sources.telegram_source import TelegramMessage, TelegramSource
from src.state import SyncState
from src.writers.lms_formatter import format_lms_session, get_session_filename
from src.writers.obsidian_writer import ObsidianWriter

logger = logging.getLogger(__name__)

# Global references for periodic tasks
_telegram_task: Optional[asyncio.Task] = None
_lms_task: Optional[asyncio.Task] = None
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
    """Run LMS sync — fetch bundle, parse sessions, write to Obsidian.

    Uses content hashing to detect changes per session.
    Returns dict with sync results.
    """
    global _last_lms_sync

    state = _state
    settings = _settings

    if not all([_lms_source, state, _writer]):
        return {"error": "Not initialized"}

    dbx = _writer.dbx
    lms_folder = settings.obsidian_lms_folder
    vault_path = settings.dropbox_vault_path

    try:
        sessions = await _lms_source.get_sessions()
    except Exception as e:
        logger.exception(f"Failed to fetch LMS sessions: {e}")
        return {"error": str(e)}

    updated = 0
    skipped = 0
    errors = 0
    details = {}

    for session in sessions:
        source_key = f"lms:{session.id}"
        try:
            # Check if content changed
            old_hash = state.get_content_hash(source_key)
            new_hash = session.content_hash

            if old_hash == new_hash:
                skipped += 1
                continue

            # Generate markdown
            markdown = format_lms_session(session)
            filename = get_session_filename(session)

            # Write to Dropbox
            dropbox_path = f"{vault_path}/{lms_folder}/{filename}.md"
            relative_path = f"{lms_folder}/{filename}.md"

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
                logger.info(f"LMS: updated {session.id} → {filename}")
            else:
                errors += 1
                details[session.id] = {"error": "upload failed"}
                logger.error(f"LMS: failed to upload {session.id}")

        except Exception as e:
            errors += 1
            details[session.id] = {"error": str(e)}
            logger.exception(f"LMS: error processing {session.id}: {e}")

    _last_lms_sync = {
        "timestamp": datetime.now().isoformat(),
        "total_sessions": len(sessions),
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "details": details,
    }

    logger.info(
        f"LMS sync complete: {updated} updated, "
        f"{skipped} unchanged, {errors} errors "
        f"(out of {len(sessions)} sessions)"
    )
    return _last_lms_sync


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
            await sync_telegram()
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
            await sync_lms()
        except Exception as e:
            logger.exception(f"Error in periodic LMS sync: {e}")

        await asyncio.sleep(interval_seconds)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize clients on startup, cleanup on shutdown."""
    global _telegram_task, _lms_task
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
            chats_folder=settings.obsidian_chats_folder,
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

    logger.info("=== Knowledge Capture Bot ready ===")

    yield  # App runs

    # Cleanup
    logger.info("=== Shutting down ===")

    for task in [_telegram_task, _lms_task]:
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
    version="1.2.0",
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
        "uptime": datetime.now().isoformat(),
    }


@app.post("/sync")
async def manual_sync():
    """Manual sync trigger — all sources (Telegram + LMS)."""
    tg_result = await sync_telegram()
    lms_result = await sync_lms()
    return {"telegram": tg_result, "lms": lms_result}


@app.post("/sync/telegram")
async def manual_sync_telegram():
    """Manual sync trigger — Telegram only."""
    return await sync_telegram()


@app.post("/sync/lms")
async def manual_sync_lms():
    """Manual sync trigger — LMS only."""
    return await sync_lms()


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
