.PHONY: help up down restart logs topics list-topics describe-topics smoke-kafka migrate test \
	test-unit test-integration lint format typecheck clean \
	generate-smoke generate-standard generate-stress inject-bad-events consumer-lag benchmark demo

PYTHON ?= python
KAFKA_CLI := /opt/kafka/bin

help:
	@echo "Phase 1 targets:"
	@echo "  up / down / restart / logs"
	@echo "  topics / list-topics / describe-topics / smoke-kafka"
	@echo "  migrate / lint / format / typecheck / test / test-unit / clean"

up:
	docker compose up -d kafka postgres kafka-ui
	$(PYTHON) scripts/wait_for_services.py

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

topics:
	$(PYTHON) scripts/create_topics.py

list-topics:
	docker compose exec -T kafka $(KAFKA_CLI)/kafka-topics.sh \
		--bootstrap-server localhost:9092 --list

describe-topics:
	docker compose exec -T kafka $(KAFKA_CLI)/kafka-topics.sh \
		--bootstrap-server localhost:9092 --describe

smoke-kafka:
	$(PYTHON) scripts/smoke_kafka.py

migrate:
	$(PYTHON) -m alembic upgrade head

test:
	$(PYTHON) -m pytest

test-unit:
	$(PYTHON) -m pytest tests/unit

test-integration:
	@echo "Integration tests are scheduled for Phase 3; no placeholder tests are run."
	@exit 2

lint:
	$(PYTHON) -m ruff check .

format:
	$(PYTHON) -m ruff format .

typecheck:
	$(PYTHON) -m mypy

clean:
	find . -type d \( -name __pycache__ -o -name .pytest_cache -o -name .mypy_cache -o -name .ruff_cache \) -prune -exec rm -rf {} +
	rm -f .coverage

generate-smoke generate-standard generate-stress inject-bad-events consumer-lag benchmark demo:
	@echo "$@ is intentionally unavailable until its phase is implemented."
	@exit 2
