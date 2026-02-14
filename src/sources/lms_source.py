"""LMS source: extracts session data from AI Mindset knowledge base.

The LMS (learn.aimindset.org) is a React SPA where all content is embedded
in the JS bundle. Strategy:
1. Fetch the index HTML to find the current JS bundle filename
2. Download the JS bundle
3. Extract session objects using regex + Node.js eval
4. Return parsed session data as dicts
"""
import hashlib
import json
import logging
import re
import subprocess
import tempfile
from dataclasses import dataclass, field
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# All known session ID patterns in the LMS bundle
SESSION_PATTERNS = [
    "ws00", "ws01", "ws02", "ws03", "ws04",
    "at01", "at02", "at03", "at04", "at05",
    "bonus01", "bonus02", "bonus03", "bonus04",
    "oh01", "oh02", "oh03", "oh04",
    "fs01", "fs02", "fs03", "fs04",
    "fos18",
]


@dataclass
class LmsSession:
    """Parsed LMS session data."""

    id: str
    title: str
    subtitle: str = ""
    date: str = ""
    time: str = ""
    speaker: str = ""
    speakers: list[str] = field(default_factory=list)
    duration: int = 0
    video: str = ""
    slides: str = ""
    transcript: str = ""
    chat: str = ""
    metaphor: str = ""
    status: str = ""
    summary: str = ""
    tldr: str = ""
    key_topics: list[str] = field(default_factory=list)
    key_takeaways: list[str] = field(default_factory=list)
    tools: list[dict] = field(default_factory=list)
    resources: list[dict] = field(default_factory=list)
    homework: list[dict] = field(default_factory=list)
    quotes: list[dict] = field(default_factory=list)
    prompts: list[dict] = field(default_factory=list)
    chapters: list[dict] = field(default_factory=list)
    participant_feedback: str = ""
    sharing_session: str = ""
    raw: dict = field(default_factory=dict)

    @property
    def content_hash(self) -> str:
        """Hash of the content for change detection."""
        content = json.dumps(self.raw, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    @classmethod
    def from_dict(cls, data: dict) -> "LmsSession":
        return cls(
            id=data.get("id", ""),
            title=data.get("title", ""),
            subtitle=data.get("subtitle", ""),
            date=data.get("date", ""),
            time=data.get("time", ""),
            speaker=data.get("speaker", ""),
            speakers=data.get("speakers", []),
            duration=data.get("duration", 0),
            video=data.get("video", ""),
            slides=data.get("slides", ""),
            transcript=data.get("transcript", ""),
            chat=data.get("chat", ""),
            metaphor=data.get("metaphor", ""),
            status=data.get("status", ""),
            summary=data.get("summary", ""),
            tldr=data.get("tldr", ""),
            key_topics=data.get("keyTopics", []),
            key_takeaways=data.get("keyTakeaways", []),
            tools=data.get("tools", []),
            resources=data.get("resources", []),
            homework=data.get("homework", []),
            quotes=data.get("quotes", []),
            prompts=data.get("prompts", []),
            chapters=data.get("chapters", []),
            participant_feedback=data.get("participantFeedback", ""),
            sharing_session=data.get("sharingSession", ""),
            raw=data,
        )


class LmsSource:
    """Fetches and parses LMS content from learn.aimindset.org."""

    def __init__(self, base_url: str = "https://learn.aimindset.org"):
        self.base_url = base_url.rstrip("/")
        self._bundle_content: Optional[str] = None

    async def fetch_bundle(self) -> str:
        """Download the main JS bundle from the LMS.

        Returns the bundle content as string.
        """
        async with httpx.AsyncClient(timeout=30) as client:
            # Step 1: Get index.html to find bundle filename
            resp = await client.get(f"{self.base_url}/")
            resp.raise_for_status()

            # Find bundle filename: /assets/index-XXXXX.js
            match = re.search(
                r'src="/assets/(index-[^"]+\.js)"', resp.text
            )
            if not match:
                raise RuntimeError("Could not find JS bundle in index.html")

            bundle_name = match.group(1)
            logger.info(f"Found LMS bundle: {bundle_name}")

            # Step 2: Download the bundle
            resp = await client.get(f"{self.base_url}/assets/{bundle_name}")
            resp.raise_for_status()

            self._bundle_content = resp.text
            logger.info(
                f"Downloaded bundle: {len(self._bundle_content):,} chars"
            )
            return self._bundle_content

    def _extract_raw_object(self, content: str, session_id: str) -> Optional[str]:
        """Extract raw JS object literal for a session from bundle."""
        pattern = f"{session_id}:{{id:"
        try:
            start = content.index(pattern)
        except ValueError:
            return None

        obj_start = start + len(session_id) + 1
        brace_count = 0
        for i in range(obj_start, min(obj_start + 50000, len(content))):
            if content[i] == "{":
                brace_count += 1
            elif content[i] == "}":
                brace_count -= 1
                if brace_count == 0:
                    return content[obj_start : i + 1]
        return None

    def _parse_via_node(self, js_objects: dict[str, str]) -> dict[str, dict]:
        """Parse JS object literals to JSON using Node.js.

        This is the most reliable approach since Node.js natively
        understands template literals, single quotes, etc.
        """
        results = {}

        for session_id, raw_js in js_objects.items():
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".js", delete=False
                ) as f:
                    f.write(f"const obj = {raw_js};\n")
                    f.write("process.stdout.write(JSON.stringify(obj));\n")
                    tmp_path = f.name

                proc = subprocess.run(
                    ["node", tmp_path],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )

                if proc.returncode == 0:
                    data = json.loads(proc.stdout)
                    results[session_id] = data
                else:
                    logger.warning(
                        f"Node.js failed for {session_id}: {proc.stderr[:200]}"
                    )
            except (subprocess.TimeoutExpired, json.JSONDecodeError) as e:
                logger.warning(f"Parse error for {session_id}: {e}")

        return results

    async def get_sessions(self) -> list[LmsSession]:
        """Fetch and parse all LMS sessions.

        Returns list of LmsSession objects with parsed data.
        """
        if not self._bundle_content:
            await self.fetch_bundle()

        content = self._bundle_content

        # Extract raw JS objects
        raw_objects = {}
        for sid in SESSION_PATTERNS:
            raw = self._extract_raw_object(content, sid)
            if raw:
                raw_objects[sid] = raw
            else:
                logger.debug(f"Session {sid} not found in bundle")

        logger.info(f"Found {len(raw_objects)} sessions in bundle")

        # Parse via Node.js
        parsed = self._parse_via_node(raw_objects)
        logger.info(f"Successfully parsed {len(parsed)} sessions")

        # Convert to LmsSession objects
        sessions = []
        for sid in SESSION_PATTERNS:
            if sid in parsed:
                sessions.append(LmsSession.from_dict(parsed[sid]))

        return sessions

    async def get_bundle_hash(self) -> str:
        """Get hash of the current bundle for change detection."""
        if not self._bundle_content:
            await self.fetch_bundle()
        return hashlib.sha256(
            self._bundle_content.encode()
        ).hexdigest()[:16]
