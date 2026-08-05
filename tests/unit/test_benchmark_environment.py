import json

from streaming_platform.benchmark import environment


def test_environment_metadata_is_allowlisted_and_sanitized(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "must-not-appear")
    monkeypatch.setenv("DATABASE_URL", "postgresql://secret")
    monkeypatch.setattr(environment, "_cpu_model", lambda: "Test CPU")
    monkeypatch.setattr(environment, "_memory_bytes", lambda: 1024)
    monkeypatch.setattr(
        environment,
        "_docker_metadata",
        lambda: {"status": "measured", "version": "test"},
    )

    metadata = environment.collect_environment()
    serialized = json.dumps(metadata)

    assert metadata["cpu"] == "Test CPU"
    assert "must-not-appear" not in serialized
    assert "postgresql://secret" not in serialized
    assert "POSTGRES_PASSWORD" not in serialized
