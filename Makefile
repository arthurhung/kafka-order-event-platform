.PHONY: help up down restart logs topics list-topics describe-topics smoke-kafka migrate test \
	test-unit test-integration test-e2e lint format typecheck clean order-consumer \
	log-consumer \
	generate-smoke generate-standard generate-stress inject-bad-events consumer-lag benchmark \
	benchmark-smoke benchmark-standard benchmark-stress demo \
	dbt-deps dbt-debug dbt-parse dbt-compile dbt-build dbt-test dbt-source-freshness \
	dbt-docs data-platform-fixtures test-data-platform

-include .env
export

PYTHON ?= python
KAFKA_CLI := /opt/kafka/bin
GENERATOR := $(PYTHON) -m apps.event_generator
DBT ?= dbt
DBT_ARGS := --project-dir dbt --profiles-dir dbt --target $(or $(DBT_TARGET),local)

help:
	@echo "Kafka streaming platform targets:"
	@echo "  up / down / restart / logs"
	@echo "  topics / list-topics / describe-topics / smoke-kafka"
	@echo "  generate-smoke / generate-standard / generate-stress / inject-bad-events"
	@echo "  order-consumer / migrate / lint / format / typecheck"
	@echo "  log-consumer"
	@echo "  consumer-lag (human table; add FORMAT=json for machine output)"
	@echo "  benchmark / benchmark-smoke / benchmark-standard / benchmark-stress"
	@echo "  demo (mixed events plus stop/restart and uncommitted replay)"
	@echo "  test / test-unit / test-integration / test-e2e / clean"
	@echo "  data-platform-fixtures / dbt-deps / dbt-debug / dbt-parse / dbt-compile"
	@echo "  dbt-build / dbt-test / dbt-source-freshness / dbt-docs / test-data-platform"

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
	$(PYTHON) -m pytest tests/integration

test-e2e:
	$(PYTHON) -m pytest tests/e2e

lint:
	$(PYTHON) -m ruff check .

format:
	$(PYTHON) -m ruff format .

typecheck:
	$(PYTHON) -m mypy

clean:
	find . -type d \( -name __pycache__ -o -name .pytest_cache -o -name .mypy_cache -o -name .ruff_cache \) -prune -exec rm -rf {} +
	rm -f .coverage

generate-smoke:
	$(GENERATOR) --events-per-second 100 --duration-seconds 60 \
		--order-ratio 0.2 --log-ratio 0.8 --seed 42 --report-path reports/latest.json

generate-standard:
	$(GENERATOR) --events-per-second 1000 --duration-seconds 300 \
		--order-ratio 0.2 --log-ratio 0.8 --seed 42 --report-path reports/standard.json

generate-stress:
	$(GENERATOR) --events-per-second 5000 --duration-seconds 300 \
		--order-ratio 0.2 --log-ratio 0.8 --seed 42 --report-path reports/stress.json

inject-bad-events:
	$(GENERATOR) --events-per-second 100 --duration-seconds 10 \
		--order-ratio 0.5 --log-ratio 0.5 --invalid-rate 1.0 --seed 42 \
		--report-path reports/invalid-events.json

order-consumer:
	$(PYTHON) -m apps.order_consumer

log-consumer:
	$(PYTHON) -m apps.log_consumer

consumer-lag:
	$(PYTHON) scripts/consumer_lag.py --format $(or $(FORMAT),table)

benchmark:
	$(PYTHON) -m apps.benchmark

benchmark-smoke:
	$(PYTHON) -m apps.benchmark --profile smoke

benchmark-standard:
	$(PYTHON) -m apps.benchmark --profile standard

benchmark-stress:
	$(PYTHON) -m apps.benchmark --profile stress

demo:
	$(PYTHON) -m apps.demo

dbt-deps:
	$(DBT) deps $(DBT_ARGS)

dbt-debug:
	$(DBT) debug $(DBT_ARGS)

dbt-parse:
	$(DBT) parse $(DBT_ARGS)

dbt-compile:
	$(DBT) compile $(DBT_ARGS)

dbt-build:
	$(DBT) build $(DBT_ARGS)

dbt-test:
	$(DBT) test $(DBT_ARGS)

dbt-source-freshness:
	$(DBT) source freshness $(DBT_ARGS)

dbt-docs:
	$(DBT) docs generate $(DBT_ARGS)

data-platform-fixtures:
	$(PYTHON) scripts/data_platform/load_phase6_fixtures.py

test-data-platform:
	$(PYTHON) -m pytest tests/data_platform
