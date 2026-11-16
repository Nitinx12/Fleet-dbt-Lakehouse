ifeq ($(OS),Windows_NT)
SHELL := C:/Program Files/Git/bin/bash.exe
else
SHELL := /bin/bash
endif
TARGET     ?= dev
UV         ?= uv
PY         := $(UV) run python

.PHONY: help setup up down deps parse build unit freshness ci-build \
        sync serve-build serve-test lint-fast lint-sql docs docs-static \
        daily weekly deploy-prod \
        notebooks notebooks-build notebooks-lab \
        contracts-build contracts-check \
        ingest py-test

help:            ## List targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | sed 's/:.*## /\t/'

setup:           ## Install dependencies
	$(UV) sync

up:              ## Start local sources and Postgres
	docker compose up -d

down:            ## Stop local services
	docker compose down

deps:            ## Install dbt packages
	$(UV) run dbt deps --project-dir dbt/fleet --profiles-dir dbt
	$(UV) run dbt deps --project-dir dbt/serving --profiles-dir dbt

parse:           ## Validate the dbt project without building
	$(UV) run dbt --project-dir dbt/fleet --profiles-dir dbt parse --target $(TARGET)

build:           ## dbt build (run + test + snapshot + seed)
	$(UV) run dbt --project-dir dbt/fleet --profiles-dir dbt build --target $(TARGET)

unit:            ## dbt unit tests only
	$(UV) run dbt --project-dir dbt/fleet --profiles-dir dbt test --select "test_type:unit" --target $(TARGET)

freshness:       ## Source freshness
	$(UV) run dbt --project-dir dbt/fleet --profiles-dir dbt source freshness --target $(TARGET)

sync:            ## Databricks -> Postgres (swap load, row-count check)
	$(PY) -m sync.run --env $(TARGET)

serve-build:     ## Build the Postgres serving project
	$(UV) run dbt --project-dir dbt/serving --profiles-dir dbt build --target $(TARGET)

serve-test:      ## Serving parity tests
	$(UV) run dbt --project-dir dbt/serving --profiles-dir dbt test --target $(TARGET)

lint-fast:       ## ruff + yamllint
	$(UV) run ruff check . && $(UV) run yamllint .

lint-sql:        ## sqlfluff with the dbt templater (needs a profile)
	$(UV) run sqlfluff lint dbt/fleet/models dbt/serving/models

docs:            ## Local docs server
	$(UV) run dbt --project-dir dbt/fleet --profiles-dir dbt docs generate --target $(TARGET) && $(UV) run dbt --project-dir dbt/fleet --profiles-dir dbt docs serve

docs-static:     ## Single-file docs site for GitHub Pages
	$(UV) run dbt --project-dir dbt/fleet --profiles-dir dbt docs generate --target prod --static
	mkdir -p out/site
	cp dbt/fleet/target/static_index.html out/site/index.html

daily:           ## Full orchestrated run: ingest, build, sync, serve
	$(PY) -m orchestration.run --job daily_job

weekly:          ## Full refresh of incremental models
	$(PY) -m orchestration.run --job weekly_full_refresh_job

deploy-prod:     ## Used by deploy.yml inside the image
	mkdir -p out
	$(MAKE) deps
	$(UV) run dbt --project-dir dbt/fleet --profiles-dir dbt build --target prod
	$(MAKE) sync TARGET=prod
	$(MAKE) serve-build TARGET=prod
	$(MAKE) serve-test TARGET=prod
	$(MAKE) docs-static
	cp dbt/fleet/target/manifest.json out/manifest.json

notebooks:       ## Execute EDA notebooks in-place (writes outputs + charts)
	$(PY) scripts/_run_eda_notebooks.py

notebooks-build: ## Regenerate EDA notebook skeletons (clears outputs)
	$(PY) scripts/_build_eda_notebooks.py

notebooks-lab:   ## Open JupyterLab on notebooks/
	$(UV) run jupyter lab notebooks/

contracts-build: ## Regenerate contracts/sources/*.yml + contracts/samples/*.json from raw CSVs
	$(PY) scripts/build_source_contracts.py

contracts-check: ## Validate contracts, samples and raw CSVs for drift
	$(PY) scripts/check_source_contracts.py

ingest:          ## Run dlt ingestion (SOURCE_MODE=live|csv|fixture)
	$(PY) -m ingestion $(INGEST_ARGS)

py-test:         ## Python unit tests with coverage on ingestion
	$(UV) run pytest tests/unit -m unit --cov=ingestion --cov-report=term-missing --cov-fail-under=80
