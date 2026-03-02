"""LMS source: extracts session data from AI Mindset knowledge base.

The LMS (learn.aimindset.org) is a React SPA where all content is embedded
in the JS bundle. Strategy:
1. Fetch the index HTML to find the current JS bundle filename
2. Download the JS bundle
3. Extract session objects using regex + Node.js eval
4. Discover and download chunk files (sprints, labs, masterclasses, etc.)
5. Return parsed session data as dicts
"""
import hashlib
import json
import logging
import os
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

# Known chunk prefixes to discover dynamically
CHUNK_PREFIXES = [
    "sprints",
    "labs",
    "masterclasses",
    "programs",
    "vibe-coding-kb",
]

# Const array variable names in the main bundle.
# "tools" uses a special non-const pattern: ],tools=[{id:
BUNDLE_CONST_ARRAYS = {
    "tools": "tools",
    "prompts": "PROMPTS",
    "metaphors": "CORE_METAPHORS",
}


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
        # prefix -> content string
        self._chunks: dict[str, str] = {}

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

    async def fetch_chunks(self) -> dict[str, str]:
        """Discover and download lazy-loaded chunk files.

        Scans the main bundle for references to known chunk prefixes,
        then downloads each found chunk. Chunk filename hashes change on
        every rebuild, so we discover them dynamically.

        Returns dict mapping prefix -> chunk content.
        """
        if not self._bundle_content:
            await self.fetch_bundle()

        content = self._bundle_content
        found_chunks: dict[str, str] = {}

        # Patterns to find chunk filenames in the bundle:
        # "./sprints-HASH.js", "/assets/sprints-HASH.js", "sprints-HASH.js"
        async with httpx.AsyncClient(timeout=30) as client:
            for prefix in CHUNK_PREFIXES:
                # Search for patterns like:
                #   "./sprints-CqnLau8F.js"
                #   "/assets/sprints-CqnLau8F.js"
                #   "sprints-CqnLau8F.js"
                # Vite uses hash patterns like: PREFIX-HASH8CHARS.js
                pattern = re.compile(
                    r'["\./](' + re.escape(prefix) + r'-[A-Za-z0-9_]+\.js)["\)]'
                )
                match = pattern.search(content)
                if not match:
                    logger.debug(
                        f"Chunk prefix '{prefix}' not referenced in bundle"
                    )
                    continue

                chunk_filename = match.group(1)
                chunk_url = f"{self.base_url}/assets/{chunk_filename}"
                logger.info(
                    f"Found chunk: {chunk_filename} (prefix={prefix})"
                )

                try:
                    resp = await client.get(chunk_url, timeout=30)
                    resp.raise_for_status()
                    found_chunks[prefix] = resp.text
                    logger.info(
                        f"Downloaded chunk '{prefix}': "
                        f"{len(resp.text):,} chars"
                    )
                except httpx.HTTPError as e:
                    logger.warning(
                        f"Failed to download chunk '{prefix}' "
                        f"({chunk_filename}): {e}"
                    )

        self._chunks = found_chunks
        logger.info(
            f"Chunks downloaded: {list(found_chunks.keys())}"
        )
        return found_chunks

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

    def _extract_const_array(self, content: str, var_name: str) -> Optional[str]:
        """Extract the raw JS array literal for a named variable from a bundle.

        Handles patterns:
          const VAR_NAME=[{...}]
          const VAR_NAME =[{...}]
          ],VAR_NAME=[{...}]     (non-const, part of expression)
          ,VAR_NAME=[{...}]

        Returns the raw JS array string (starting with '['), or None.
        """
        # Escape dollar signs in var_name for regex
        escaped = re.escape(var_name)
        # Try 1: const VAR_NAME spaces* = spaces* [
        pattern = re.compile(r"const\s+" + escaped + r"\s*=\s*(\[)")
        match = pattern.search(content)
        if not match:
            # Try 2: non-const pattern — ],VAR_NAME=[ or ,VAR_NAME=[
            pattern2 = re.compile(r"[\],;]" + escaped + r"=(\[)")
            match = pattern2.search(content)
        if not match:
            logger.debug(f"array '{var_name}' not found in content")
            return None

        # Start bracket is at match.start(1)
        bracket_start = match.start(1)
        bracket_count = 0
        for i in range(bracket_start, min(bracket_start + 500000, len(content))):
            ch = content[i]
            if ch == "[":
                bracket_count += 1
            elif ch == "]":
                bracket_count -= 1
                if bracket_count == 0:
                    return content[bracket_start : i + 1]
        logger.warning(f"Could not find closing bracket for const '{var_name}'")
        return None

    def _extract_array_from_chunk(self, chunk_content: str) -> Optional[str]:
        """Extract the primary exported array from a chunk file.

        Chunks are minified Vite/Rollup output. They typically contain
        one large array as the default export or a standalone const.
        Strategy: find the first top-level '[{' array in the content.

        Returns the raw JS array string or None.
        """
        # Try: export default [{...}]
        match = re.search(r"export\s+default\s+(\[)", chunk_content)
        if match:
            start = match.start(1)
        else:
            # Try: first occurrence of '[{' which is likely the data array
            match = re.search(r"(\[\{)", chunk_content)
            if match:
                start = match.start(1)
            else:
                logger.debug("No array pattern found in chunk")
                return None

        bracket_count = 0
        for i in range(start, min(start + 1000000, len(chunk_content))):
            ch = chunk_content[i]
            if ch == "[":
                bracket_count += 1
            elif ch == "]":
                bracket_count -= 1
                if bracket_count == 0:
                    return chunk_content[start : i + 1]
        logger.warning("Could not find closing bracket for chunk array")
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
                    mode="w", suffix=".js", delete=False, encoding="utf-8"
                ) as f:
                    f.write(f"const obj = {raw_js};\n")
                    f.write("process.stdout.write(JSON.stringify(obj));\n")
                    tmp_path = f.name

                proc = subprocess.run(
                    ["node", tmp_path],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    encoding="utf-8",
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
            finally:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

        return results

    def _parse_array_via_node(self, raw_js_array: str, label: str) -> Optional[list]:
        """Parse a raw JS array literal to a Python list using Node.js.

        Returns list of dicts or None on failure.
        """
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".js", delete=False, encoding="utf-8"
            ) as f:
                f.write(f"const arr = {raw_js_array};\n")
                f.write("process.stdout.write(JSON.stringify(arr));\n")
                tmp_path = f.name

            proc = subprocess.run(
                ["node", tmp_path],
                capture_output=True,
                text=True,
                timeout=15,
                encoding="utf-8",
            )

            if proc.returncode == 0:
                data = json.loads(proc.stdout)
                if isinstance(data, list):
                    return data
                # Might be a single object or wrapped
                if isinstance(data, dict):
                    return [data]
                logger.warning(
                    f"Node.js returned unexpected type for '{label}': {type(data)}"
                )
                return None
            else:
                logger.warning(
                    f"Node.js failed for array '{label}': {proc.stderr[:300]}"
                )
                return None
        except (subprocess.TimeoutExpired, json.JSONDecodeError) as e:
            logger.warning(f"Parse error for array '{label}': {e}")
            return None
        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

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

    async def get_sprints(self) -> list[dict]:
        """Fetch and parse all sprints from the sprints chunk."""
        if not self._chunks:
            await self.fetch_chunks()

        chunk = self._chunks.get("sprints")
        if not chunk:
            logger.warning("Sprints chunk not available")
            return []

        raw_array = self._extract_array_from_chunk(chunk)
        if not raw_array:
            logger.warning("Could not extract array from sprints chunk")
            return []

        result = self._parse_array_via_node(raw_array, "sprints")
        if result is None:
            return []

        logger.info(f"Parsed {len(result)} sprints")
        return result

    async def get_labs(self) -> list[dict]:
        """Fetch and parse all labs from the labs chunk."""
        if not self._chunks:
            await self.fetch_chunks()

        chunk = self._chunks.get("labs")
        if not chunk:
            logger.warning("Labs chunk not available")
            return []

        raw_array = self._extract_array_from_chunk(chunk)
        if not raw_array:
            logger.warning("Could not extract array from labs chunk")
            return []

        result = self._parse_array_via_node(raw_array, "labs")
        if result is None:
            return []

        logger.info(f"Parsed {len(result)} labs")
        return result

    async def get_masterclasses(self) -> list[dict]:
        """Fetch and parse all masterclasses from the masterclasses chunk."""
        if not self._chunks:
            await self.fetch_chunks()

        chunk = self._chunks.get("masterclasses")
        if not chunk:
            logger.warning("Masterclasses chunk not available")
            return []

        raw_array = self._extract_array_from_chunk(chunk)
        if not raw_array:
            logger.warning("Could not extract array from masterclasses chunk")
            return []

        result = self._parse_array_via_node(raw_array, "masterclasses")
        if result is None:
            return []

        logger.info(f"Parsed {len(result)} masterclasses")
        return result

    async def get_programs(self) -> list[dict]:
        """Fetch and parse all programs from the programs chunk."""
        if not self._chunks:
            await self.fetch_chunks()

        chunk = self._chunks.get("programs")
        if not chunk:
            logger.warning("Programs chunk not available")
            return []

        raw_array = self._extract_array_from_chunk(chunk)
        if not raw_array:
            logger.warning("Could not extract array from programs chunk")
            return []

        result = self._parse_array_via_node(raw_array, "programs")
        if result is None:
            return []

        logger.info(f"Parsed {len(result)} programs")
        return result

    async def get_vibe_coding_kb(self) -> list[dict]:
        """Fetch and parse vibe-coding knowledge base from its chunk."""
        if not self._chunks:
            await self.fetch_chunks()

        chunk = self._chunks.get("vibe-coding-kb")
        if not chunk:
            logger.warning("vibe-coding-kb chunk not available")
            return []

        raw_array = self._extract_array_from_chunk(chunk)
        if not raw_array:
            logger.warning("Could not extract array from vibe-coding-kb chunk")
            return []

        result = self._parse_array_via_node(raw_array, "vibe-coding-kb")
        if result is None:
            return []

        logger.info(f"Parsed {len(result)} vibe-coding-kb items")
        return result

    async def get_tools(self) -> list[dict]:
        """Parse tools array (tools$1) from the main bundle."""
        if not self._bundle_content:
            await self.fetch_bundle()

        raw_array = self._extract_const_array(
            self._bundle_content, BUNDLE_CONST_ARRAYS["tools"]
        )
        if not raw_array:
            logger.warning("tools$1 array not found in bundle")
            return []

        result = self._parse_array_via_node(raw_array, "tools")
        if result is None:
            return []

        logger.info(f"Parsed {len(result)} tools")
        return result

    async def get_prompts(self) -> list[dict]:
        """Parse PROMPTS array from the main bundle."""
        if not self._bundle_content:
            await self.fetch_bundle()

        raw_array = self._extract_const_array(
            self._bundle_content, BUNDLE_CONST_ARRAYS["prompts"]
        )
        if not raw_array:
            logger.warning("PROMPTS array not found in bundle")
            return []

        result = self._parse_array_via_node(raw_array, "prompts")
        if result is None:
            return []

        logger.info(f"Parsed {len(result)} prompts")
        return result

    async def get_metaphors(self) -> list[dict]:
        """Parse CORE_METAPHORS array from the main bundle."""
        if not self._bundle_content:
            await self.fetch_bundle()

        raw_array = self._extract_const_array(
            self._bundle_content, BUNDLE_CONST_ARRAYS["metaphors"]
        )
        if not raw_array:
            logger.warning("CORE_METAPHORS array not found in bundle")
            return []

        result = self._parse_array_via_node(raw_array, "metaphors")
        if result is None:
            return []

        logger.info(f"Parsed {len(result)} metaphors")
        return result

    async def get_all_content(self) -> dict:
        """Fetch and parse all available LMS content.

        Ensures bundle and chunks are downloaded once, then returns
        a dict with all content categories.
        """
        # Pre-fetch bundle and all chunks in one pass
        if not self._bundle_content:
            await self.fetch_bundle()
        if not self._chunks:
            await self.fetch_chunks()

        return {
            "sessions": [s.raw for s in await self.get_sessions()],
            "sprints": await self.get_sprints(),
            "labs": await self.get_labs(),
            "masterclasses": await self.get_masterclasses(),
            "programs": await self.get_programs(),
            "vibe_coding_kb": await self.get_vibe_coding_kb(),
            "tools": await self.get_tools(),
            "prompts": await self.get_prompts(),
            "metaphors": await self.get_metaphors(),
        }

    async def get_bundle_hash(self) -> str:
        """Get hash of the current bundle for change detection."""
        if not self._bundle_content:
            await self.fetch_bundle()
        return hashlib.sha256(
            self._bundle_content.encode()
        ).hexdigest()[:16]

    def content_hash_for(self, data: list | dict) -> str:
        """Compute a short content hash for arbitrary parsed data."""
        serialized = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(serialized.encode()).hexdigest()[:16]
