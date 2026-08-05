"""Bounded subprocess lifecycle management for consumers and generators."""

import signal
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO


@dataclass(slots=True)
class ManagedProcess:
    """A child process and its temporary combined output stream."""

    name: str
    process: subprocess.Popen[bytes]
    output: BinaryIO
    log_path: Path


class ProcessManager:
    """Launch children and guarantee bounded graceful cleanup."""

    def __init__(self, log_directory: Path) -> None:
        """Create a manager that stores transient logs in one directory."""
        self._log_directory = log_directory
        self._processes: list[ManagedProcess] = []

    def start(
        self,
        name: str,
        arguments: list[str],
        *,
        environment: dict[str, str] | None = None,
    ) -> ManagedProcess:
        """Start a child with output redirected away from bounded pipes."""
        log_path = self._log_directory / f"{name}.log"
        output = log_path.open("wb")
        try:
            process = subprocess.Popen(  # noqa: S603 - explicit argument vectors
                arguments,
                stdout=output,
                stderr=subprocess.STDOUT,
                env=environment,
            )
        except Exception:
            output.close()
            raise
        managed = ManagedProcess(name, process, output, log_path)
        self._processes.append(managed)
        return managed

    def stop(self, managed: ManagedProcess, timeout_seconds: float = 20.0) -> bool:
        """SIGTERM a running child, using SIGKILL only after the deadline."""
        forced = False
        if managed.process.poll() is None:
            managed.process.send_signal(signal.SIGTERM)
            try:
                managed.process.wait(timeout=timeout_seconds)
            except subprocess.TimeoutExpired:
                managed.process.kill()
                managed.process.wait(timeout=5)
                forced = True
        managed.output.close()
        if managed in self._processes:
            self._processes.remove(managed)
        return forced

    def stop_all(self, timeout_seconds: float = 20.0) -> list[str]:
        """Stop every tracked process and return names that required SIGKILL."""
        forced = []
        for managed in list(reversed(self._processes)):
            if self.stop(managed, timeout_seconds):
                forced.append(managed.name)
        return forced
