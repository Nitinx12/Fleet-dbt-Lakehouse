# Fleet dbt Warehouse - run `make help` for the target list.

.DEFAULT_GOAL := help

UV      := uv
PYTHON  ?= python
PSQL    ?= psql
DB_USER ?= postgres
DB_NAME ?= fleet_warehouse

.PHONY: help install setup lint-fast fmt py-test check env-check db-init ingest clean

help:
	@echo.
	@echo Fleet dbt Warehouse - make targets
	@echo.
	@echo   make install     uv sync --frozen, creates .venv
	@echo   make setup       alias for install
	@echo   make lint-fast   ruff check and format check
	@echo   make fmt         ruff format plus safe autofix
	@echo   make py-test     pytest
	@echo   make check       lint-fast and py-test
	@echo   make env-check   fail if a required env var is missing
	@echo   make db-init     run sql/scripts/01 to 05 in order
	@echo   make ingest      extract MySQL and MongoDB sources into bronze
	@echo   make clean       remove __pycache__ and .pytest_cache
	@echo   make dbt-debug   dbt debug (loads .env)
	@echo   make dbt-silver dbt run silver models
	@echo   make dbt-silver-tests dbt test silver models
	@echo   make dbt-run     dbt run all models
	@echo.

install:
	$(UV) sync --frozen

setup: install

lint-fast:
	$(UV) run ruff check .
	$(UV) run ruff format --check .

fmt:
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

py-test:
	$(UV) run pytest

check: lint-fast py-test

env-check:
	$(UV) run python -c "import utils.engine; print('env OK: all required variables set')"

db-init:
	$(PSQL) -U $(DB_USER) -d postgres -v ON_ERROR_STOP=1 -f sql/scripts/01_init_schema.sql
	$(PSQL) -U $(DB_USER) -d $(DB_NAME) -v ON_ERROR_STOP=1 -f sql/scripts/02_audit_tables.sql
	$(PSQL) -U $(DB_USER) -d $(DB_NAME) -v ON_ERROR_STOP=1 -f sql/scripts/03_control_pipeline_registry.sql
	$(PSQL) -U $(DB_USER) -d $(DB_NAME) -v ON_ERROR_STOP=1 -f sql/scripts/04_control_batch_tracking.sql
	$(PSQL) -U $(DB_USER) -d $(DB_NAME) -v ON_ERROR_STOP=1 -f sql/scripts/05_control_watermarks.sql

ingest:
	$(UV) run python scripts/ingestion.py

clean:
	@$(PYTHON) -c "import pathlib, shutil; [shutil.rmtree(p, ignore_errors=True) for d in ('utils', 'tests') for p in pathlib.Path(d).rglob('__pycache__')]; shutil.rmtree('.pytest_cache', ignore_errors=True)"

dbt-debug:
	$(UV) run python scripts/run_dbt.py debug

dbt-silver:
	$(UV) run python scripts/run_dbt.py run --select silver

dbt-run:
	$(UV) run python scripts/run_dbt.py run


dbt-silver-tests:
	$(UV) run python scripts/run_dbt.py test --select silver

