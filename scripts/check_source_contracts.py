"""Validate contracts/sources/*.yml against contracts/samples/*.json and the raw CSVs.

Exit 0 = no drift. Non-zero = a boundary changed without the contract.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
SOURCES = ROOT / "contracts" / "sources"
SAMPLES = ROOT / "contracts" / "samples"
DATA = {"mysql": ROOT / "data" / "mysql", "mongo": ROOT / "data" / "mongo"}

REQUIRED_KEYS = {
    "contract_version",
    "source",
    "landing_table",
    "owner",
    "primary_key",
    "extraction",
    "columns",
    "quality",
    "freshness",
    "change_policy",
}


def fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    errors.append(msg)


errors: list[str] = []


def check_contract(path: Path) -> None:
    name = path.stem  # mysql__loads / mongo__delivery_events
    rel = path.relative_to(ROOT)
    print(f"{rel}")

    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        fail(f"{name}: invalid YAML: {exc}")
        return

    missing = REQUIRED_KEYS - set(doc)
    if missing:
        fail(f"{name}: missing keys {sorted(missing)}")
        return

    # system split
    if name.startswith("mysql__"):
        system, table = "mysql", name.removeprefix("mysql__")
        csv_path = DATA["mysql"] / f"{table}.csv"
        if doc["source"] != f"mysql.fleet_registry.{table}":
            fail(f"{name}: source={doc['source']!r} expected mysql.fleet_registry.{table}")
        if doc["landing_table"] != f"raw.mysql__{table}":
            fail(f"{name}: landing_table={doc['landing_table']!r}")
    elif name.startswith("mongo__"):
        system, table = "mongo", name.removeprefix("mongo__")
        csv_path = DATA["mongo"] / f"{table}.csv"
        if doc["source"] != f"mongodb.fleet_operations.{table}":
            fail(f"{name}: source={doc['source']!r} expected mongodb.fleet_operations.{table}")
        if doc["landing_table"] != f"raw.mongo__{table}":
            fail(f"{name}: landing_table={doc['landing_table']!r}")
    else:
        fail(f"{name}: filename must be mysql__* or mongo__*")
        return

    if not csv_path.exists():
        fail(f"{name}: raw CSV missing at {csv_path}")
        return

    df = pd.read_csv(csv_path, dtype=str, nrows=5000)
    contract_cols = [c["name"] for c in doc["columns"]]
    csv_cols = list(df.columns)

    # exact column set match (order-insensitive)
    if set(contract_cols) != set(csv_cols):
        only_contract = set(contract_cols) - set(csv_cols)
        only_csv = set(csv_cols) - set(contract_cols)
        fail(f"{name}: column drift contract-only={only_contract or '{}'} csv-only={only_csv or '{}'}")

    # primary key present + not nullable
    pk = doc["primary_key"]
    if len(pk) != 1:
        fail(f"{name}: primary_key must have exactly one column, got {pk}")
    else:
        pk_col = pk[0]
        pk_def = next((c for c in doc["columns"] if c["name"] == pk_col), None)
        if pk_def is None:
            fail(f"{name}: primary_key {pk_col} not in columns")
        elif pk_def.get("nullable") is not False:
            fail(f"{name}: primary_key {pk_col} must be nullable: false")

    # row baseline within sanity of actual
    actual = len(pd.read_csv(csv_path, usecols=[0]))
    baseline = doc["quality"]["row_count"]["baseline"]
    if actual != baseline:
        fail(f"{name}: row baseline {baseline} != actual {actual}")

    # sample JSON present and aligned
    sample_path = SAMPLES / f"{name}.sample.json"
    if not sample_path.exists():
        fail(f"{name}: missing sample {sample_path.name}")
        return
    try:
        sample = json.loads(sample_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        fail(f"{name}: sample JSON invalid: {exc}")
        return

    if sample.get("contract") != f"{name}.yml":
        fail(f"{name}: sample.contract={sample.get('contract')!r}")
    if sample.get("source") != doc["source"]:
        fail(f"{name}: sample.source drift vs contract")
    if sample.get("primary_key") != doc["primary_key"]:
        fail(f"{name}: sample.primary_key drift vs contract")

    rows_key = "sample_rows" if "sample_rows" in sample else "sample_rows_raw"
    rows = sample.get(rows_key, [])
    if not rows:
        fail(f"{name}: sample has no rows")
        return
    sample_cols = set(rows[0])
    if sample_cols != set(contract_cols):
        fail(f"{name}: sample columns != contract columns")

    # every sample row must satisfy non-null PK
    pk_col = doc["primary_key"][0]
    for i, row in enumerate(rows):
        if row.get(pk_col) in (None, ""):
            fail(f"{name}: sample row {i} missing pk {pk_col}")

    if not errors or not any(name in e for e in errors):
        print(f"  ok    {len(contract_cols)} cols, baseline {baseline}, {len(rows)} sample row(s)")


def main() -> int:
    contracts = sorted(SOURCES.glob("*.yml"))
    if not contracts:
        print("no contracts found under contracts/sources/")
        return 1
    print(f"checking {len(contracts)} contract(s)\n")
    for path in contracts:
        check_contract(path)

    # every sample must have a contract
    for sp in sorted(SAMPLES.glob("*.sample.json")):
        yml = SOURCES / sp.name.replace(".sample.json", ".yml")
        if not yml.exists():
            fail(f"{sp.name}: no matching contract {yml.name}")

    print()
    if errors:
        print(f"{len(errors)} problem(s)")
        return 1
    print("contracts-check: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
