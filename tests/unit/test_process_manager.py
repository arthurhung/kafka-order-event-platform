import sys
from time import monotonic, sleep

from streaming_platform.benchmark.processes import ProcessManager


def test_cleanup_forces_process_only_after_graceful_timeout(tmp_path) -> None:
    manager = ProcessManager(tmp_path)
    process = manager.start(
        "ignores-term",
        [
            sys.executable,
            "-c",
            (
                "import signal,time; "
                "signal.signal(signal.SIGTERM, lambda *_: None); "
                "print('ready', flush=True); time.sleep(30)"
            ),
        ],
    )
    deadline = monotonic() + 1
    while monotonic() < deadline and "ready" not in process.log_path.read_text():
        sleep(0.01)

    forced = manager.stop(process, timeout_seconds=0.1)

    assert forced is True
    assert process.process.returncode is not None
    assert manager.stop_all() == []
