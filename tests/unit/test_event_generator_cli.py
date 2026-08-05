"""Tests for event-generator command-line shutdown behavior."""

import logging

import pytest

from apps.event_generator.__main__ import _run_with_graceful_interrupt


class InterruptingRunner:
    def run(self):
        raise KeyboardInterrupt


def test_keyboard_interrupt_exits_without_propagating_traceback(tmp_path) -> None:
    runner = InterruptingRunner()
    logger = logging.LoggerAdapter(logging.getLogger("generator-cli-test"), {})

    with pytest.raises(SystemExit) as raised:
        _run_with_graceful_interrupt(runner, logger, tmp_path / "report.json")

    assert raised.value.code == 130
