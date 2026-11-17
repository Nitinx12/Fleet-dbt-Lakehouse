# Source contracts

Versioned agreements at the **source → bronze** boundary. One YAML per table/collection, plus a JSON sample of real rows.

| Path | Purpose |
|---|---|
| `sources/*.yml` | The contract: columns, types, PK, FKs, extraction, quality, freshness, change policy |
| `samples/*.sample.json` | 3 real rows (Mongo: raw strings + staged `try_cast` preview) |
| `sources_index.json` | Machine index of all contracts |

## Format

See [ARCHITECTURE.md §11.2](../../ARCHITECTURE.md#112-source-contract-format).

- **MySQL** — `type` is the landed type in Postgres `bronze.mysql__*`.
- **Mongo** — every raw value is a string; `staged_type` + `cast` describe what staging produces (`try_cast`, `flag`).

## Commands

```bash
make contracts-build    # regenerate from data/{mysql,mongo}/*.csv
make contracts-check    # fail on drift (columns, baseline, sample alignment)
```

`contracts-check` runs in pre-push (JSON samples) and should run in CI before any ingestion change.

## Changing a contract

1. Edit the YAML **and** the sample in the same PR.
2. Additive nullable column → `minor_bump` in `change_policy` / minor version.
3. Remove, rename or retype → **breaking**: bump `contract_version` major, note in `CHANGELOG.md`, ask first.
4. Never edit a deployed contract version in place to hide a break.
