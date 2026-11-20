# Fleet dbt Warehouse build entry point. Run `make help`; add targets as `name: ## summary`.
# Configuration: ENV=dev|prod SELECT=<dbt selector> CONFIRM=yes V=1.

.DEFAULT_GOAL := help
.SUFFIXES:
.DELETE_ON_ERROR:

SHELL := bash
ifneq ($(wildcard C:/PROGRA~1/Git/bin/bash.exe),)
SHELL := C:/PROGRA~1/Git/bin/bash.exe
endif
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --warn-undefined-variables --no-builtin-rules

# Configuration
UV ?= uv
PSQL ?= psql
ENV ?= dev
SELECT ?=
CONFIRM ?=
V ?= 0
DB_USER ?= postgres
DB_NAME ?= fleet_warehouse
PYTHON_VERSION_FILE ?= .python-version
VENV_MARKER ?= .venv/pyvenv.cfg
VENV_STAMP ?= .venv/.installed

-include .env

DBT_SELECT = $(if $(strip $(SELECT)),--select "$(SELECT)",)
DBT = "$(UV)" run --no-sync python scripts/run_dbt.py

ifeq ($(V),1)
Q :=
else
Q := @
endif

define require_cmd
command -v "$(1)" >/dev/null 2>&1 || { printf '%s\n' "make: *** '$(1)' not found. $(2)" >&2; exit 1; }
endef

define require_venv
test -f "$(VENV_MARKER)" || { printf '%s\n' "make: *** virtual environment not found. Run 'make install'." >&2; exit 1; }
endef

define require_environment
case "$(ENV)" in dev|prod) ;; *) printf '%s\n' "make: *** ENV must be dev or prod, got '$(ENV)'." >&2; exit 1 ;; esac
endef

define confirm_production
if [ "$(ENV)" = "prod" ] && [ "$(CONFIRM)" != "yes" ]; then printf '%s\n' "make: *** refusing production operation; rerun with ENV=prod CONFIRM=yes." >&2; exit 1; fi
endef

define confirm_destructive
if [ "$(CONFIRM)" != "yes" ]; then printf '%s\n' "make: *** refusing destructive operation; rerun with CONFIRM=yes." >&2; exit 1; fi
endef

## Help
.PHONY: help
help: ## Show this help and available configuration.
	$(Q)awk '\
		/^## / { group = substr($$0, 4); seen[group] = 1; next } \
		/^[A-Za-z0-9][A-Za-z0-9_-]*:.*## / { \
			target = $$1; sub(/:.*/, "", target); \
			description = $$0; sub(/^.*## /, "", description); \
			key = group SUBSEP target; groups[key] = group; targets[key] = target; descriptions[key] = description; \
		} \
		function next_group(  candidate, value) { \
			candidate = ""; for (value in seen) if (!(value in printed_group) && (candidate == "" || value < candidate)) candidate = value; return candidate; \
		} \
		function print_group(name,  i, j, key, count, temp) { \
			count = 0; for (key in targets) if (groups[key] == name) ordered[++count] = key; \
			for (i = 1; i <= count; i++) for (j = i + 1; j <= count; j++) if (targets[ordered[j]] < targets[ordered[i]]) { temp = ordered[i]; ordered[i] = ordered[j]; ordered[j] = temp; } \
			if (count) { printf "\\n%s:\\n", name; for (i = 1; i <= count; i++) { key = ordered[i]; printf "  %-20s %s\\n", targets[key], descriptions[key]; } } \
		} \
		END { while ((group = next_group()) != "") { printed_group[group] = 1; print_group(group); } printf "\\nConfiguration: ENV=dev|prod (default dev), SELECT=<dbt selector>, CONFIRM=yes, V=1\\n"; }' "$(firstword $(MAKEFILE_LIST))"

## Setup
.PHONY: check-tools install setup
check-tools: ## Verify the base command-line tools are available.
	$(Q)$(call require_cmd,bash,Install Git Bash or another Bash implementation.)
	$(Q)$(call require_cmd,$(UV),Install uv from https://docs.astral.sh/uv/.)
	$(Q)$(call require_cmd,awk,Install a POSIX-compatible awk implementation.)
	$(Q)printf '%s\n' "Tool check passed."

$(VENV_STAMP): uv.lock pyproject.toml
	$(Q)$(call require_cmd,$(UV),Install uv from https://docs.astral.sh/uv/.)
	$(Q)printf '%s\n' "==> Syncing locked Python dependencies"
	$(Q)"$(UV)" sync --frozen
	$(Q)touch "$(VENV_STAMP)"

install: $(VENV_STAMP) ## Create or update .venv from uv.lock.

setup: install ## Alias for install.

.PHONY: check-venv check-version
check-venv:
	$(Q)$(call require_venv)

check-version: check-tools check-venv
	$(Q)test -f "$(PYTHON_VERSION_FILE)" || { printf '%s\n' "make: *** $(PYTHON_VERSION_FILE) is missing." >&2; exit 1; }
	$(Q)actual="$$("$(UV)" run --no-sync python -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')"; expected="$$(tr -d '\r\n' < "$(PYTHON_VERSION_FILE)")"; if [ "$$actual" != "$$expected" ]; then printf '%s\n' "warning: Python $$actual is installed; $(PYTHON_VERSION_FILE) pins $$expected." >&2; fi

## Quality
.PHONY: lint-fast lint fmt format py-test test check ci verify
lint-fast: check-version ## Run Ruff lint and formatting checks.
	$(Q)printf '%s\n' "==> Running Ruff checks"
	$(Q)"$(UV)" run --no-sync ruff check .
	$(Q)"$(UV)" run --no-sync ruff format --check .

lint: lint-fast ## Alias for lint-fast.

fmt: check-version ## Apply Ruff formatting and safe fixes.
	$(Q)printf '%s\n' "==> Formatting Python sources"
	$(Q)"$(UV)" run --no-sync ruff format .
	$(Q)"$(UV)" run --no-sync ruff check --fix .

format: fmt ## Alias for fmt.

py-test: check-version ## Run the Python test suite.
	$(Q)printf '%s\n' "==> Running Python tests"
	$(Q)"$(UV)" run --no-sync pytest

test: py-test ## Alias for py-test.

verify: lint-fast py-test ## Run all non-destructive local quality checks.

check: verify ## Alias for verify.

ci: verify ## Run the checks supported by the tracked repository.

## Contracts
.PHONY: contracts-check
contracts-check: check-version ## Validate the source-contract index and dbt bronze source list.
	$(Q)"$(UV)" run --no-sync python -c "import json; from pathlib import Path; from scripts.ingestion import read_contract; root=Path('.'); index=json.loads((root/'contracts/sources_index.json').read_text(encoding='utf-8')); indexed={item['file']: item['source'] for item in index['contracts']}; paths={path.name for path in (root/'contracts/sources').glob('*.yml')}; contracts={path.name: read_contract(path) for path in (root/'contracts/sources').glob('*.yml')}; dbt_tables={line.strip().removeprefix('- name: ').strip() for line in (root/'dbt/models/bronze/sources.yml').read_text(encoding='utf-8').splitlines() if line.strip().startswith('- name:')}; dbt_tables.discard('bronze'); contract_tables={str(contract['landing_table']).split('.', 1)[1] for contract in contracts.values()}; assert paths == set(indexed), 'contract index files differ from contracts/sources'; assert {name: contract['source'] for name, contract in contracts.items()} == indexed, 'contract index sources differ from contract YAML'; assert contract_tables == dbt_tables, 'dbt bronze source tables differ from contract landing tables'; print(f'contracts OK: {len(contracts)} sources')"

## Airflow
.PHONY: airflow-test airflow-up airflow-down
airflow-test: check-version ## Run Airflow orchestration tests.
	$(Q)PYTHONPATH=airflow "$(UV)" run --no-sync pytest \
		airflow/tests/test_dag_integrity.py airflow/tests/test_daily_dag_structure.py \
		airflow/tests/test_ingest_tiers.py airflow/tests/test_airflow_helpers.py \
		airflow/tests/test_commads.py airflow/tests/test_notify.py airflow/tests/test_redact.py \
		airflow/tests/test_templates.py airflow/tests/test_prefilght.py \
		airflow/tests/test_finalize_audit.py airflow/tests/test_audit_reader.py \
		airflow/tests/test_callbacks.py airflow/tests/test_summary.py airflow/tests/test_watchdog.py

airflow-up: ## Start the local Airflow stack.
	$(Q)docker compose --env-file .env -f airflow/compose.yml -f airflow/compose.dev.yml up -d --build

airflow-down: ## Stop the local Airflow stack.
	$(Q)docker compose --env-file .env -f airflow/compose.yml -f airflow/compose.dev.yml down

## Notebooks
.PHONY: notebooks
notebooks: check-version ## Execute EDA notebooks into out/notebooks.
	$(Q)"$(UV)" run --no-sync python scripts/run_notebooks.py

## Environment
.PHONY: env-check
env-check: check-version ## Verify application-required environment variables.
	$(Q)"$(UV)" run --no-sync python -c "import utils.engine; print('env OK: all required variables set')"

## Database and ingestion
.PHONY: db-init ingest publish-marts
db-init: check-tools ## Create the warehouse database and apply SQL bootstrap scripts.
	$(Q)$(call require_cmd,$(PSQL),Install the PostgreSQL client and put psql on PATH.)
	$(Q)$(call confirm_destructive)
	$(Q)$(call confirm_production)
	$(Q)printf '%s\n' "==> Initialising database $(DB_NAME) for ENV=$(ENV)"
	$(Q)"$(PSQL)" -U "$(DB_USER)" -d postgres -v ON_ERROR_STOP=1 -f "sql/scripts/01_init_schema.sql"
	$(Q)"$(PSQL)" -U "$(DB_USER)" -d "$(DB_NAME)" -v ON_ERROR_STOP=1 -f "sql/scripts/02_audit_tables.sql"
	$(Q)"$(PSQL)" -U "$(DB_USER)" -d "$(DB_NAME)" -v ON_ERROR_STOP=1 -f "sql/scripts/03_control_pipeline_registry.sql"
	$(Q)"$(PSQL)" -U "$(DB_USER)" -d "$(DB_NAME)" -v ON_ERROR_STOP=1 -f "sql/scripts/04_control_batch_tracking.sql"
	$(Q)"$(PSQL)" -U "$(DB_USER)" -d "$(DB_NAME)" -v ON_ERROR_STOP=1 -f "sql/scripts/05_control_watermarks.sql"
	$(Q)"$(PSQL)" -U "$(DB_USER)" -d "$(DB_NAME)" -v ON_ERROR_STOP=1 -f "sql/scripts/06_audit_relationships.sql"

ingest: check-version ## Load MySQL and MongoDB sources into the bronze layer.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)printf '%s\n' "==> Ingesting sources into ENV=$(ENV)"
	$(Q)"$(UV)" run --no-sync python scripts/ingestion.py

publish-marts: check-version ## Incrementally publish approved Postgres marts to Databricks.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)printf '%s\n' "==> Publishing Postgres marts to Databricks for ENV=$(ENV)"
	$(Q)"$(UV)" run --no-sync python scripts/publish_marts.py

## dbt
.PHONY: dbt-debug dbt-deps dbt-parse dbt-compile dbt-seed dbt-silver dbt-silver-tests dbt-gold dbt-gold-tests dbt-marts dbt-marts-tests dbt-snapshots dbt-build dbt-test dbt-run dbt-docs dbt-full-refresh build
dbt-debug: check-version ## Validate dbt connectivity for ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) debug --target "$(ENV)"

dbt-deps: check-version ## Install dbt packages from dbt/package-lock.yml.
	$(Q)$(DBT) deps

dbt-parse: check-version ## Parse the dbt project without querying the warehouse.
	$(Q)$(call require_environment)
	$(Q)$(DBT) parse --target "$(ENV)"

dbt-compile: check-version ## Compile dbt SQL; optionally set SELECT.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) compile --target "$(ENV)" $(DBT_SELECT)

dbt-seed: check-version ## Load dbt seed files into ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) seed --target "$(ENV)" $(DBT_SELECT)

dbt-silver: check-version ## Run silver models in ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) run --target "$(ENV)" --select silver $(DBT_SELECT)

dbt-silver-tests: check-version ## Test silver models in ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) test --target "$(ENV)" --select silver $(DBT_SELECT)

dbt-gold: check-version ## Run gold models in ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) run --target "$(ENV)" --select gold $(DBT_SELECT)

dbt-gold-tests: check-version ## Test gold models in ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) test --target "$(ENV)" --select gold $(DBT_SELECT)

dbt-marts: check-version ## Run mart models in ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) run --target "$(ENV)" --select marts $(DBT_SELECT)

dbt-marts-tests: check-version ## Test mart models in ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) test --target "$(ENV)" --select marts $(DBT_SELECT)

dbt-snapshots: check-version ## Run dbt snapshots in ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) snapshot --target "$(ENV)" $(DBT_SELECT)

dbt-build: check-version ## Build dbt resources in ENV; optionally set SELECT.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) build --target "$(ENV)" $(DBT_SELECT)

build: dbt-build ## Alias for dbt-build.

dbt-test: check-version ## Test all dbt resources in ENV; optionally set SELECT.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) test --target "$(ENV)" $(DBT_SELECT)

dbt-run: check-version ## Run all dbt models in ENV; optionally set SELECT.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) run --target "$(ENV)" $(DBT_SELECT)

dbt-docs: check-version ## Generate local dbt documentation for ENV.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_production)
	$(Q)$(DBT) docs generate --target "$(ENV)"

dbt-full-refresh: ## Rebuild selected dbt resources; requires CONFIRM=yes.
	$(Q)$(call require_environment)
	$(Q)$(call confirm_destructive)
	$(Q)$(call confirm_production)
	$(Q)$(call require_cmd,$(UV),Install uv from https://docs.astral.sh/uv/.)
	$(Q)$(call require_venv)
	$(Q)printf '%s\n' "==> Full-refreshing dbt resources in ENV=$(ENV)"
	$(Q)$(DBT) build --target "$(ENV)" --full-refresh $(DBT_SELECT)

## Clean
.PHONY: clean distclean
clean: ## Remove Python, dbt and generated output artifacts.
	$(Q)printf '%s\n' "==> Removing local generated artifacts"
	$(Q)rm -rf -- ".pytest_cache" ".ruff_cache" "dbt/target" "dbt/logs" "out"
	$(Q)find "utils" "tests" "scripts" -type d -name "__pycache__" -prune -exec rm -rf -- {} +

distclean: ## Remove caches and .venv; requires CONFIRM=yes.
	$(Q)$(call confirm_destructive)
	$(Q)$(MAKE) clean
	$(Q)rm -rf -- ".venv"
