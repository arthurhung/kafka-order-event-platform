"""Path, size, and secret controls for Phase 9 metadata."""

import json
import re
from pathlib import Path
from typing import Any

MAX_ARTIFACT_BYTES = 8 * 1024 * 1024
MAX_RESPONSE_BYTES = 256 * 1024
SECRET_KEY = re.compile(r"(?i)(token|password|secret|private[_-]?key|authorization|credential)")
CONNECTION = re.compile(r"(?i)(postgres(?:ql)?|https?)://[^\s\"']+")
BEARER = re.compile(r"(?i)bearer\s+[a-z0-9._~+/-]+")


class ArtifactSecurityError(ValueError):
    """Raised for non-allowlisted or unsafe artifact access."""


def resolve_allowlisted(root: Path, relative: str, allowed: set[str]) -> Path:
    """Resolve a named artifact while rejecting traversal and arbitrary paths."""
    if relative not in allowed or Path(relative).is_absolute() or ".." in Path(relative).parts:
        raise ArtifactSecurityError("artifact path is not allowlisted")
    resolved_root = root.resolve()
    resolved = (resolved_root / relative).resolve()
    if not resolved.is_relative_to(resolved_root):
        raise ArtifactSecurityError("artifact path escapes repository root")
    return resolved


def load_json(path: Path) -> dict[str, Any]:
    """Read a bounded JSON object and reject malformed or oversized inputs."""
    if path.stat().st_size > MAX_ARTIFACT_BYTES:
        raise ArtifactSecurityError(f"artifact exceeds {MAX_ARTIFACT_BYTES} bytes")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ArtifactSecurityError("artifact is not valid UTF-8 JSON") from error
    if not isinstance(value, dict):
        raise ArtifactSecurityError("artifact root must be a JSON object")
    return value


def redact(value: object) -> object:
    """Recursively remove likely credentials and connection strings."""
    if isinstance(value, dict):
        return {
            str(key): "[REDACTED]" if SECRET_KEY.search(str(key)) else redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, str):
        return BEARER.sub("[REDACTED]", CONNECTION.sub("[REDACTED]", value))
    return value


def bounded_json(value: object, *, maximum: int = MAX_RESPONSE_BYTES) -> str:
    """Serialize a redacted response and enforce the MCP result-size ceiling."""
    rendered = json.dumps(redact(value), sort_keys=True, separators=(",", ":"), default=str)
    if len(rendered.encode("utf-8")) > maximum:
        raise ArtifactSecurityError("tool response exceeds configured size limit")
    return rendered
