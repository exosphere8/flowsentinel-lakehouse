# Common tasks. Everything runs through uv, so no virtualenv needs activating.
LAKE ?= lake
LANDING ?= landing
SITE ?= site
export FLOWLAKE_LAKE := $(abspath $(LAKE))
export FLOWLAKE_LANDING := $(abspath $(LANDING))
export FLOWLAKE_SITE := $(abspath $(SITE))

.PHONY: install upgrade demo ingest transform report dagster up down stream-demo suite suite-test lint test test-all check clean

install:            ## Install the package with every extra and the dev tools
	uv sync --all-extras

upgrade:            ## Move every Python dependency to its newest allowed version, then run the checks
	uv lock --upgrade
	uv sync --all-extras
	$(MAKE) check

demo:               ## Generate data, ingest, build, score the detections, write the dashboard
	uv run flowlake --lake $(LAKE) demo --landing $(LANDING) --out $(SITE)

ingest:             ## Ingest everything in the landing directory
	uv run flowlake --lake $(LAKE) ingest $(LANDING) --workers 4

transform:          ## dbt build (models and tests)
	uv run flowlake --lake $(LAKE) transform

report:             ## Write the dashboard to site/
	uv run flowlake --lake $(LAKE) report --out $(SITE)

dagster:            ## Start the Dagster UI on http://localhost:3000
	uv run dagster dev

up:                 ## Start Redpanda (Kafka API on localhost:19092)
	docker compose up -d --wait

down:               ## Stop Redpanda
	docker compose down

stream-demo: up     ## Publish one synthetic day to Redpanda and consume it into the lake
	uv run flowlake stream produce --days 1
	uv run flowlake --lake $(LAKE) stream consume --idle-timeout 10
	uv run flowlake --lake $(LAKE) transform

suite:              ## Set up and start the self-hosted suite (docs/deployment.md)
	cd deploy && ./setup.sh && docker compose up -d --build --wait

suite-test:         ## Build and start a throwaway suite, ingest captures, check the dashboard
	deploy/smoke-test.sh

lint:               ## Format check, lint, types, contract schemas
	uv run ruff format --check src tests scripts
	uv run ruff check src tests scripts
	uv run mypy
	uv run flowlake contract --check

test:               ## Tests that need no broker and no FlowSentinel binary
	uv run pytest

test-all: up        ## Everything, including the Redpanda tests
	FLOWLAKE_KAFKA_BOOTSTRAP=localhost:19092 uv run pytest

check: lint test    ## What CI runs on every push

clean:
	rm -rf $(LAKE) $(LANDING) $(SITE) bench transform/target transform/logs .pytest_cache .mypy_cache .ruff_cache
