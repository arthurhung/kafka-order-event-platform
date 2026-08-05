"""Sanitized host and Docker metadata collection for benchmark reports."""

import json
import os
import platform
import subprocess
from typing import Any


def _command(arguments: list[str]) -> str | None:
    try:
        completed = subprocess.run(  # noqa: S603 - fixed executable allowlist
            arguments,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return completed.stdout.strip() or None


def _cpu_model() -> str | None:
    if platform.system() == "Darwin":
        return _command(["sysctl", "-n", "machdep.cpu.brand_string"])
    value = platform.processor().strip()
    return value or None


def _memory_bytes() -> int | None:
    if platform.system() == "Darwin":
        value = _command(["sysctl", "-n", "hw.memsize"])
        return int(value) if value and value.isdigit() else None
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        pages = os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, OSError, ValueError):
        return None
    return int(page_size * pages)


def _docker_metadata() -> dict[str, Any]:
    raw = _command(
        [
            "docker",
            "info",
            "--format",
            '{{json .}}',
        ]
    )
    if raw is None:
        return {
            "status": "not_available",
            "version": None,
            "logical_cpu_count": None,
            "memory_bytes": None,
        }
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {"status": "not_available", "version": None}
    return {
        "status": "measured",
        "version": parsed.get("ServerVersion"),
        "operating_system": parsed.get("OperatingSystem"),
        "logical_cpu_count": parsed.get("NCPU"),
        "memory_bytes": parsed.get("MemTotal"),
        "desktop_configured_resource_limits": "not_available",
    }


def collect_environment() -> dict[str, Any]:
    """Collect a secret-free environment metadata allowlist."""
    return {
        "os": platform.system(),
        "os_release": platform.release(),
        "architecture": platform.machine(),
        "cpu": _cpu_model(),
        "logical_cpu_count": os.cpu_count(),
        "memory_bytes": _memory_bytes(),
        "python_version": platform.python_version(),
        "docker": _docker_metadata(),
        "kafka_image": "apache/kafka:4.1.0",
        "postgres_image": "postgres:16-alpine",
    }
