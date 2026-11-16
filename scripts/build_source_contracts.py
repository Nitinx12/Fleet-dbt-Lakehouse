"""Generate contracts/sources/*.yml and contracts/samples/*.json from the raw CSVs."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
MYSQL = ROOT / "data" / "mysql"
MONGO = ROOT / "data" / "mongo"
OUT_YML = ROOT / "contracts" / "sources"
OUT_JSON = ROOT / "contracts" / "samples"

# table -> (extraction mode, cursor, lookback) from ARCHITECTURE §7.2
MYSQL_EXTRACTION = {
    "drivers": ("full_replace", None, None),
    "trucks": ("full_replace", None, None),
    "trailers": ("full_replace", None, None),
    "customers": ("full_replace", None, None),
    "facilities": ("full_replace", None, None),
    "routes": ("full_replace", None, None),
    "loads": ("incremental", "load_date", 14),
    "trips": ("incremental", "dispatch_date", 14),
    "fuel_purchases": ("incremental", "purchase_date", 14),
}
MONGO_EXTRACTION = {
    "delivery_events": ("incremental", "scheduled_datetime", 14),
    "maintenance_records": ("full_replace", None, None),
    "safety_incidents": ("full_replace", None, None),
}

# accepted value domains observed / documented
ACCEPTED = {
    ("drivers", "employment_status"): ["Active", "Terminated"],
    ("drivers", "cdl_class"): ["A", "B", "C"],
    ("trucks", "status"): ["Active", "Maintenance", "Inactive"],
    ("trailers", "status"): ["Active", "Inactive", "Maintenance"],
    ("customers", "customer_type"): ["Contract", "Spot", "Dedicated"],
    ("customers", "account_status"): ["Active", "Inactive"],
    ("loads", "load_status"): ["Completed", "In Transit", "Delivered", "Cancelled", "Booked"],
    ("loads", "booking_type"): ["Dedicated", "Contract", "Spot"],
    ("loads", "load_type"): ["Dry Van", "Refrigerated", "Flatbed", "Tanker"],
    ("trips", "trip_status"): ["Completed", "In Transit", "Planned", "Cancelled"],
}

# foreign keys: (table, col) -> referenced
FKS = {
    ("loads", "customer_id"): "customers.customer_id",
    ("loads", "route_id"): "routes.route_id",
    ("trips", "load_id"): "loads.load_id",
    ("trips", "driver_id"): "drivers.driver_id",
    ("trips", "truck_id"): "trucks.truck_id",
    ("trips", "trailer_id"): "trailers.trailer_id",
    ("fuel_purchases", "trip_id"): "trips.trip_id",
    ("fuel_purchases", "truck_id"): "trucks.truck_id",
    ("fuel_purchases", "driver_id"): "drivers.driver_id",
}

MONGO_FKS = {
    ("delivery_events", "load_id"): "mysql.fleet_registry.loads.load_id",
    ("delivery_events", "trip_id"): "mysql.fleet_registry.trips.trip_id",
    ("delivery_events", "facility_id"): "mysql.fleet_registry.facilities.facility_id",
    ("maintenance_records", "truck_id"): "mysql.fleet_registry.trucks.truck_id",
    ("safety_incidents", "trip_id"): "mysql.fleet_registry.trips.trip_id",
    ("safety_incidents", "truck_id"): "mysql.fleet_registry.trucks.truck_id",
    ("safety_incidents", "driver_id"): "mysql.fleet_registry.drivers.driver_id",
}

PK = {
    "drivers": "driver_id",
    "trucks": "truck_id",
    "trailers": "trailer_id",
    "customers": "customer_id",
    "facilities": "facility_id",
    "routes": "route_id",
    "loads": "load_id",
    "trips": "trip_id",
    "fuel_purchases": "fuel_purchase_id",
    "delivery_events": "event_id",
    "maintenance_records": "maintenance_id",
    "safety_incidents": "incident_id",
}

# money / ratio columns get precision hints
MONEY = {
    "revenue", "fuel_surcharge", "accessorial_charges", "total_cost",
    "labor_cost", "parts_cost", "price_per_gallon", "vehicle_damage_cost",
    "cargo_damage_cost", "claim_amount", "base_rate_per_mile",
    "annual_revenue_potential", "fuel_surcharge_rate",
}
MIN0 = {
    "revenue", "fuel_surcharge", "accessorial_charges", "weight_lbs", "pieces",
    "actual_distance_miles", "fuel_gallons_used", "idle_time_hours",
    "detention_minutes", "gallons", "total_cost", "labor_cost", "parts_cost",
    "downtime_hours", "odometer_reading", "claim_amount", "vehicle_damage_cost",
    "cargo_damage_cost", "years_experience", "dock_doors",
}


def infer_series(s: pd.Series) -> str:
    nonempty = s.dropna().astype(str)
    nonempty = nonempty[nonempty.str.strip() != ""]
    if nonempty.empty:
        return "string"
    num = pd.to_numeric(nonempty, errors="coerce")
    if num.notna().mean() > 0.95:
        vals = num.dropna()
        if (vals % 1 == 0).all():
            if vals.abs().max() > 2**31 - 1:
                return "bigint"
            return "int"
        return "decimal(12,2)" if vals.abs().max() > 1000 else "decimal(10,4)"
    # date / timestamp
    head = nonempty.iloc[0]
    try:
        dt = pd.to_datetime(nonempty, errors="coerce", utc=True)
        if dt.notna().mean() > 0.95:
            if any(ch in str(head) for ch in (":", "T")):
                return "timestamp"
            return "date"
    except Exception:
        pass
    return "string"


def nullable(s: pd.Series) -> bool:
    return bool(s.isna().any() or (s.fillna("").astype(str).str.strip() == "").any())


def build_mysql_contract(name: str, df: pd.DataFrame) -> dict:
    mode, cursor, lookback = MYSQL_EXTRACTION[name]
    pk = PK[name]
    columns = []
    for col in df.columns:
        typ = infer_series(df[col])
        col_def: dict = {
            "name": col,
            "type": typ,
            "nullable": nullable(df[col]) if col != pk else False,
        }
        ref = FKS.get((name, col))
        if ref:
            col_def["references"] = ref
        if col in ACCEPTED.get((name, col), []) or (name, col) in ACCEPTED:
            col_def["accepted_values"] = ACCEPTED[(name, col)]
        if col in MONEY and typ.startswith("decimal"):
            col_def["min"] = 0
        if col in MIN0:
            col_def.setdefault("min", 0)
        if col == "loaded_at":
            col_def["nullable"] = False
        columns.append(col_def)

    extraction: dict = {"mode": mode}
    if cursor:
        extraction["cursor"] = cursor
        extraction["lookback_days"] = lookback

    total = len(df)
    return {
        "contract_version": "1.0.0",
        "source": f"mysql.fleet_registry.{name}",
        "landing_table": f"raw.mysql__{name}",
        "owner": "fleet-data-eng",
        "primary_key": [pk],
        "extraction": extraction,
        "columns": columns,
        "quality": {
            "row_count": {"baseline": total, "tolerance_pct": 5},
        },
        "freshness": {
            "loaded_at_field": "loaded_at",
            "warn_after_hours": 26,
            "error_after_hours": 48,
        },
        "change_policy": {
            "additive_column": "minor_bump",
            "removed_or_retyped": "major_bump",
        },
    }


def build_mongo_contract(name: str, df: pd.DataFrame) -> dict:
    mode, cursor, lookback = MONGO_EXTRACTION[name]
    pk = PK[name]
    columns = []
    for col in df.columns:
        raw = "string"  # Mongo documents store everything as strings
        # staged type = what try_cast produces
        probe = df[col].dropna().astype(str)
        probe = probe[probe.str.strip() != ""]
        staged = "string"
        if len(probe):
            num = pd.to_numeric(probe, errors="coerce")
            if num.notna().mean() > 0.95:
                vals = num.dropna()
                if (vals % 1 == 0).all():
                    staged = "bigint" if vals.abs().max() > 2**31 - 1 else "int"
                else:
                    staged = "decimal(12,2)"
            else:
                try:
                    dt = pd.to_datetime(probe, errors="coerce", utc=True)
                    if dt.notna().mean() > 0.95:
                        head = probe.iloc[0]
                        staged = "timestamp" if any(ch in str(head) for ch in (":", "T")) else "date"
                except Exception:
                    pass

        # flag normalisation columns
        if col.endswith("_flag") or col in ("on_time_flag", "at_fault_flag", "injury_flag", "preventable_flag"):
            staged = "boolean"
            cast = "flag"
        elif staged in ("int", "bigint", "decimal(12,2)", "date", "timestamp"):
            cast = "try_cast"
        else:
            cast = "none"

        col_def: dict = {
            "name": col,
            "raw_type": raw,
            "staged_type": staged,
            "nullable": nullable(df[col]) if col != pk else False,
        }
        if cast != "none":
            col_def["cast"] = cast
        ref = MONGO_FKS.get((name, col))
        if ref:
            col_def["references"] = ref
        if col in MIN0 and staged in ("int", "bigint", "decimal(12,2)"):
            col_def["min"] = 0
        if col == "loaded_at":
            col_def["nullable"] = False
        columns.append(col_def)

    extraction: dict = {"mode": mode}
    if cursor:
        extraction["cursor"] = cursor
        extraction["lookback_days"] = lookback

    return {
        "contract_version": "1.0.0",
        "source": f"mongodb.fleet_operations.{name}",
        "landing_table": f"raw.mongo__{name}",
        "owner": "fleet-data-eng",
        "primary_key": [pk],
        "extraction": extraction,
        "columns": columns,
        "quality": {
            "row_count": {"baseline": len(df), "tolerance_pct": 5},
            "max_cast_failure_pct": 0.1,
        },
        "freshness": {
            "loaded_at_field": "loaded_at",
            "warn_after_hours": 26,
            "error_after_hours": 48,
        },
        "change_policy": {
            "additive_column": "minor_bump",
            "removed_or_retyped": "major_bump",
        },
    }


def sample_rows(df: pd.DataFrame, n: int = 3) -> list[dict]:
    """First n rows as JSON-safe values (ISO dates, native numbers)."""
    out = []
    for _, row in df.head(n).iterrows():
        rec = {}
        for k, v in row.items():
            if pd.isna(v):
                rec[k] = None
            elif isinstance(v, pd.Timestamp):
                rec[k] = v.isoformat()
            elif hasattr(v, "item"):
                rec[k] = v.item()
            else:
                rec[k] = v
        out.append(rec)
    return out


def main() -> None:
    OUT_YML.mkdir(parents=True, exist_ok=True)
    OUT_JSON.mkdir(parents=True, exist_ok=True)

    # clear previous generated files only
    for p in list(OUT_YML.glob("*.yml")) + list(OUT_JSON.glob("*.json")):
        p.unlink()

    index: list[dict] = []

    for name in sorted(MYSQL_EXTRACTION):
        path = MYSQL / f"{name}.csv"
        df = pd.read_csv(path, dtype=str, keep_default_na=True)
        # parse typed view for baseline count only; contracts use string-inferred types
        typed = pd.read_csv(path)
        contract = build_mysql_contract(name, typed)
        yml_path = OUT_YML / f"mysql__{name}.yml"
        yml_path.write_text(
            f"# Source contract: mysql.fleet_registry.{name}\n"
            f"# Generated from data/mysql/{name}.csv — edit deliberately; breaking changes = major bump.\n"
            + yaml.safe_dump(contract, sort_keys=False, default_flow_style=False, allow_unicode=True),
            encoding="utf-8",
        )
        samples = sample_rows(typed)
        (OUT_JSON / f"mysql__{name}.sample.json").write_text(
            json.dumps(
                {
                    "contract": f"mysql__{name}.yml",
                    "source": contract["source"],
                    "landing_table": contract["landing_table"],
                    "primary_key": contract["primary_key"],
                    "row_count_baseline": contract["quality"]["row_count"]["baseline"],
                    "sample_rows": samples,
                },
                indent=2,
                default=str,
            )
            + "\n",
            encoding="utf-8",
        )
        index.append({"file": yml_path.name, "source": contract["source"], "rows": len(typed)})
        print(f"  {yml_path.name} ({len(typed)} rows, {len(contract['columns'])} cols)")

    for name in sorted(MONGO_EXTRACTION):
        path = MONGO / f"{name}.csv"
        df = pd.read_csv(path, dtype=str)
        contract = build_mongo_contract(name, df)
        yml_path = OUT_YML / f"mongo__{name}.yml"
        yml_path.write_text(
            f"# Source contract: mongodb.fleet_operations.{name}\n"
            f"# Raw values are strings; staged_type is produced by try_cast / flag normalisation.\n"
            + yaml.safe_dump(contract, sort_keys=False, default_flow_style=False, allow_unicode=True),
            encoding="utf-8",
        )
        # sample rows: show raw strings + a cast preview
        raw_samples = sample_rows(df)
        cast_preview = []
        for rec in raw_samples[:2]:
            preview = {}
            for col_def in contract["columns"]:
                raw_v = rec.get(col_def["name"])
                st = col_def["staged_type"]
                if raw_v is None:
                    preview[col_def["name"]] = None
                elif st == "boolean":
                    preview[col_def["name"]] = str(raw_v).lower() in ("true", "1", "yes")
                elif st in ("int", "bigint"):
                    try:
                        preview[col_def["name"]] = int(float(raw_v))
                    except ValueError:
                        preview[col_def["name"]] = None
                elif st.startswith("decimal"):
                    try:
                        preview[col_def["name"]] = float(raw_v)
                    except ValueError:
                        preview[col_def["name"]] = None
                elif st in ("date", "timestamp"):
                    preview[col_def["name"]] = raw_v
                else:
                    preview[col_def["name"]] = raw_v
            cast_preview.append(preview)
        (OUT_JSON / f"mongo__{name}.sample.json").write_text(
            json.dumps(
                {
                    "contract": f"mongo__{name}.yml",
                    "source": contract["source"],
                    "landing_table": contract["landing_table"],
                    "primary_key": contract["primary_key"],
                    "row_count_baseline": contract["quality"]["row_count"]["baseline"],
                    "note": "raw_values are strings as stored in MongoDB; staged_preview shows try_cast result",
                    "sample_rows_raw": raw_samples,
                    "sample_rows_staged_preview": cast_preview,
                },
                indent=2,
                default=str,
            )
            + "\n",
            encoding="utf-8",
        )
        index.append({"file": yml_path.name, "source": contract["source"], "rows": len(df)})
        print(f"  {yml_path.name} ({len(df)} rows, {len(contract['columns'])} cols)")

    (ROOT / "contracts" / "sources_index.json").write_text(
        json.dumps({"generated_from": "data/{mysql,mongo}/*.csv", "contracts": index}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"\n{len(index)} contracts -> {OUT_YML}")
    print(f"{len(index)} samples  -> {OUT_JSON}")


if __name__ == "__main__":
    main()
