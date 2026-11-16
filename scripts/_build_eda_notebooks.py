"""Generate mysql_eda.ipynb and mongo_eda.ipynb."""

from pathlib import Path

import nbformat as nbf

OUT = Path("notebooks")


def md(source: str):
    return nbf.v4.new_markdown_cell(source.strip() + "\n")


def code(source: str):
    return nbf.v4.new_code_cell(source.strip() + "\n")


def write_nb(path: Path, cells):
    nb = nbf.v4.new_notebook(
        cells=cells,
        metadata={
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "pygments_lexer": "ipython3"},
        },
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        nbf.write(nb, f)
    print(f"wrote {path} ({len(cells)} cells)")


# --------------------------------------------------------------------------
# MySQL EDA
# --------------------------------------------------------------------------
mysql_cells = [
    md("""
# MySQL EDA — `fleet_registry`

Exploratory data analysis on the nine MySQL tables that back the Fleet dbt Lakehouse.

**Use cases covered:** driver performance · route profitability · fleet utilization · fuel efficiency · customer analysis · seasonal patterns

> Companion notebook: `mongo_eda.ipynb` (MongoDB `fleet_operations` collections).
    """),
    md("## 1. Setup"),
    code("""
import warnings
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

warnings.filterwarnings("ignore")
sns.set_theme(style="whitegrid", palette="deep", font_scale=1.05)

DATA = Path("..") / "data" / "mysql"
if not DATA.exists():
    DATA = Path("data/mysql")
FIGSIZE = (12, 5)
RANDOM = 42
pd.set_option("display.max_columns", 50)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")
print("pandas", pd.__version__, "| seaborn", sns.__version__)
print("data dir:", DATA.resolve())
    """),
    md("""
## 2. Load tables

Parse date columns up front so temporal EDA works without re-casting.
    """),
    code("""
DATE_COLS = {
    "loads": ["load_date"],
    "trips": ["dispatch_date"],
    "drivers": ["hire_date", "termination_date", "date_of_birth"],
    "trucks": ["acquisition_date"],
    "trailers": ["acquisition_date"],
    "customers": ["contract_start_date"],
    "fuel_purchases": ["purchase_date"],
}

tables = {}
for name, dates in DATE_COLS.items():
    tables[name] = pd.read_csv(DATA / f"{name}.csv", parse_dates=dates)

tables["routes"] = pd.read_csv(DATA / "routes.csv")
tables["facilities"] = pd.read_csv(DATA / "facilities.csv")

loads = tables["loads"]
trips = tables["trips"]
drivers = tables["drivers"]
trucks = tables["trucks"]
trailers = tables["trailers"]
customers = tables["customers"]
routes = tables["routes"]
facilities = tables["facilities"]
fuel = tables["fuel_purchases"]

overview = pd.DataFrame(
    {"rows": {k: len(v) for k, v in tables.items()}, "cols": {k: v.shape[1] for k, v in tables.items()}}
).sort_values("rows", ascending=False)
display(overview)
    """),
    md("""
## 3. Data quality audit

Nulls, duplicates on primary keys, and key column dtypes. FKs are **logical only** (no constraints in MySQL).
    """),
    code("""
pk = {
    "loads": "load_id", "trips": "trip_id", "drivers": "driver_id", "trucks": "truck_id",
    "trailers": "trailer_id", "customers": "customer_id", "routes": "route_id",
    "facilities": "facility_id", "fuel_purchases": "fuel_purchase_id",
}

rows = []
for name, df in tables.items():
    key = pk[name]
    rows.append({
        "table": name,
        "rows": len(df),
        "null_cells": int(df.isna().sum().sum()),
        "dup_pk": int(df[key].duplicated().sum()),
        "mem_mb": round(df.memory_usage(deep=True) / 1e6, 1),
    })
display(pd.DataFrame(rows))

# Null heatmap (tables with any nulls only)
nulls = {k: v.isna().sum() for k, v in tables.items()}
null_df = pd.DataFrame(nulls)
null_df = null_df.loc[:, null_df.sum() > 0]
if null_df.empty:
    print("No nulls in any table.")
else:
    display(null_df.T)
    ax = null_df.T.plot(kind="bar", figsize=FIGSIZE, legend=False, color="#c44e52")
    ax.set_title("Null counts by column (tables with nulls)")
    ax.set_ylabel("nulls")
    plt.tight_layout()
plt.show()
    """),
    md("""
## 4. Drivers

Employment status, experience, age and CDL class — foundation for the driver-performance mart.
    """),
    code("""
fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))

sns.countplot(data=drivers, x="employment_status", ax=axes[0], order=["Active", "Terminated"])
axes[0].set_title("Driver employment status")

sns.histplot(data=drivers, x="years_experience", bins=15, kde=True, ax=axes[1], color="#4c72b0")
axes[1].set_title("Years of experience")

sns.countplot(data=drivers, x="cdl_class", ax=axes[2])
axes[2].set_title("CDL class")

plt.tight_layout()
plt.show()

drivers["age"] = (pd.Timestamp("2024-12-31") - drivers["date_of_birth"]).dt.days / 365.25
display(
    drivers.groupby("employment_status")
    .agg(n=("driver_id", "count"), exp_mean=("years_experience", "mean"), age_mean=("age", "mean"))
    .round(1)
)
print("Hire years:", sorted(drivers["hire_date"].dt.year.unique().tolist()))
print("Home terminals:", drivers["home_terminal"].nunique(), "unique")
    """),
    md("""
## 5. Fleet assets — trucks & trailers

Status mix and age profile drive the fleet-utilization and maintenance stories.
    """),
    code("""
fig, axes = plt.subplots(2, 2, figsize=(14, 8))

sns.countplot(data=trucks, y="status", ax=axes[0, 0], order=trucks["status"].value_counts().index)
axes[0, 0].set_title("Truck status")

sns.countplot(data=trucks, y="make", ax=axes[0, 1], order=trucks["make"].value_counts().index)
axes[0, 1].set_title("Truck make")

trucks["age_years"] = 2024 - trucks["model_year"]
sns.histplot(data=trucks, x="age_years", bins=12, ax=axes[1, 0], color="#55a868")
axes[1, 0].set_title("Truck age (years)")

sns.countplot(data=trailers, y="trailer_type", ax=axes[1, 1], order=trailers["trailer_type"].value_counts().index)
axes[1, 1].set_title("Trailer type")

plt.tight_layout()
plt.show()

print("Truck status:", trucks["status"].value_counts().to_dict())
print("Trailer status:", trailers["status"].value_counts().to_dict())
print("Trailer lengths (ft):", sorted(trailers["length_feet"].unique().tolist()))
    """),
    md("""
## 6. Customers & facilities
    """),
    code("""
fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))

sns.countplot(data=customers, x="customer_type", ax=axes[0])
axes[0].set_title("Customer type")

sns.countplot(data=customers, x="account_status", ax=axes[1])
axes[1].set_title("Account status")

sns.histplot(data=customers, x="annual_revenue_potential", bins=20, kde=True, ax=axes[2], color="#8172b2")
axes[2].set_title("Annual revenue potential")

plt.tight_layout()
plt.show()

print("Top freight types:", customers["primary_freight_type"].value_counts().head(5).to_dict())
print("Credit terms (days):", sorted(customers["credit_terms_days"].unique().tolist()))

fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))
sns.countplot(data=facilities, y="facility_type", ax=axes[0], order=facilities["facility_type"].value_counts().index)
axes[0].set_title("Facility type")
sns.countplot(
    data=facilities, y="state", ax=axes[1],
    order=facilities["state"].value_counts().head(12).index,
)
axes[1].set_title("Top 12 states by facility count")
plt.tight_layout()
plt.show()
    """),
    md("""
## 7. Routes (lanes)

Distance, base rate per mile, and transit time — inputs to lane profitability.
    """),
    code("""
fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))

sns.histplot(data=routes, x="typical_distance_miles", bins=15, kde=True, ax=axes[0], color="#4c72b0")
axes[0].set_title("Typical lane distance (mi)")

sns.histplot(data=routes, x="base_rate_per_mile", bins=15, kde=True, ax=axes[1], color="#c44e52")
axes[1].set_title("Base rate per mile ($)")

sns.scatterplot(
    data=routes, x="typical_distance_miles", y="base_rate_per_mile",
    hue="typical_transit_days", size="typical_transit_days", ax=axes[2], sizes=(40, 200),
)
axes[2].set_title("Distance vs rate (coloured by transit days)")

plt.tight_layout()
plt.show()

routes["lane"] = routes["origin_state"] + " → " + routes["destination_state"]
print("Longest lanes:")
display(routes.nlargest(5, "typical_distance_miles")[["lane", "typical_distance_miles", "base_rate_per_mile", "typical_transit_days"]])
    """),
    md("""
## 8. Loads — volume, revenue, mix

Three years of completed loads (2022-01-01 → 2024-12-31). Seasonality and booking mix are first-class KPIs.
    """),
    code("""
monthly = (
    loads.set_index("load_date")
    .resample("ME")
    .agg(loads=("load_id", "count"), revenue=("revenue", "sum"), weight=("weight_lbs", "mean"))
)

fig, axes = plt.subplots(2, 1, figsize=(13, 8), sharex=True)
monthly["loads"].plot(ax=axes[0], color="#4c72b0", marker="o", markersize=3)
axes[0].set_title("Monthly load volume")
axes[0].set_ylabel("loads")

monthly["revenue"].plot(ax=axes[1], color="#55a868", marker="o", markersize=3)
axes[1].set_title("Monthly revenue ($)")
axes[1].set_ylabel("revenue")

plt.tight_layout()
plt.show()

loads["month"] = loads["load_date"].dt.month
loads["year"] = loads["load_date"].dt.year
season = loads.groupby("month").agg(loads=("load_id", "count"), avg_revenue=("revenue", "mean"))

fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))
sns.barplot(x=season.index, y=season["loads"], ax=axes[0], color="#4c72b0")
axes[0].set_title("Loads by month (seasonality)")
axes[0].set_xlabel("month")
sns.barplot(x=season.index, y=season["avg_revenue"], ax=axes[1], color="#c44e52")
axes[1].set_title("Average revenue by month")
axes[1].set_xlabel("month")
plt.tight_layout()
plt.show()
    """),
    code("""
fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))

sns.boxplot(data=loads, x="booking_type", y="revenue", ax=axes[0], showfliers=False)
axes[0].set_title("Revenue by booking type")

sns.countplot(data=loads, x="load_type", ax=axes[1])
axes[1].set_title("Load type mix")

sns.histplot(data=loads, x="weight_lbs", bins=40, ax=axes[2], color="#8172b2")
axes[2].set_title("Load weight (lbs)")

plt.tight_layout()
plt.show()

display(
    loads.groupby(["booking_type", "load_type"])
    .agg(loads=("load_id", "count"), avg_revenue=("revenue", "mean"), avg_weight=("weight_lbs", "mean"))
    .round(1)
)
print("Revenue range:", loads["revenue"].min(), "→", loads["revenue"].max())
print("Accessorial charges: mean", round(loads["accessorial_charges"].mean(), 2), "| nonzero share",
      round((loads["accessorial_charges"] > 0).mean(), 3))
    """),
    md("""
## 9. Trips — distance, MPG, idle time

1:1 with loads. MPG and idle are the fuel-efficiency levers.
    """),
    code("""
fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))

sns.histplot(data=trips, x="actual_distance_miles", bins=40, kde=True, ax=axes[0], color="#4c72b0")
axes[0].set_title("Trip distance (mi)")

sns.histplot(data=trips, x="average_mpg", bins=20, kde=True, ax=axes[1], color="#55a868")
axes[1].set_title("Average MPG")

sns.histplot(data=trips, x="idle_time_hours", bins=30, ax=axes[2], color="#c44e52")
axes[2].set_title("Idle time (hours)")

plt.tight_layout()
plt.show()

trips["idle_share"] = trips["idle_time_hours"] / (trips["actual_duration_hours"] + 1e-9)
display(
    trips[["actual_distance_miles", "actual_duration_hours", "fuel_gallons_used", "average_mpg", "idle_time_hours"]]
    .describe()
    .round(2)
)
print("MPG stddev:", round(trips["average_mpg"].std(), 3), "(tight — synthetic data)")
print("Idle share of duration: mean", round(trips["idle_share"].mean(), 3))
    """),
    md("""
## 10. Fuel purchases

Price volatility, station geography, and card usage — cost side of route profitability.
    """),
    code("""
fuel_monthly = (
    fuel.set_index("purchase_date")
    .resample("ME")
    .agg(gallons=("gallons", "sum"), cost=("total_cost", "sum"), ppg=("price_per_gallon", "mean"))
)

fig, axes = plt.subplots(2, 1, figsize=(13, 8), sharex=True)
fuel_monthly["ppg"].plot(ax=axes[0], color="#c44e52", marker="o", markersize=3)
axes[0].set_title("Average price per gallon ($)")
fuel_monthly["gallons"].plot(ax=axes[1], color="#4c72b0", marker="o", markersize=3)
axes[1].set_title("Gallons purchased")
plt.tight_layout()
plt.show()

fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))
sns.histplot(data=fuel, x="price_per_gallon", bins=40, kde=True, ax=axes[0], color="#c44e52")
axes[0].set_title("Price per gallon distribution")
top_states = fuel["location_state"].value_counts().head(12)
sns.barplot(x=top_states.values, y=top_states.index, ax=axes[1], color="#4c72b0")
axes[1].set_title("Top 12 states by fuel purchases")
plt.tight_layout()
plt.show()

print("Total gallons:", f"{fuel['gallons'].sum():,.0f}")
print("Total fuel cost:", f"${fuel['total_cost'].sum():,.0f}")
print("Avg PPG:", round(fuel["price_per_gallon"].mean(), 3))
    """),
    md("""
## 11. Cross-table insights

Joins that feed the marts: revenue by customer, MPG by make, distance by lane, driver workload.
    """),
    code("""
trip_load = trips.merge(loads, on="load_id", how="inner", validate="one_to_one")
trip_load = trip_load.merge(customers[["customer_id", "customer_type", "primary_freight_type"]], on="customer_id", how="left")
trip_load = trip_load.merge(routes[["route_id", "origin_state", "destination_state", "typical_distance_miles"]], on="route_id", how="left")
trip_load["lane"] = trip_load["origin_state"] + " → " + trip_load["destination_state"]
trip_load["revenue_per_mile"] = trip_load["revenue"] / trip_load["actual_distance_miles"].replace(0, np.nan)

print("trip↔load join rows:", len(trip_load), "| 1:1 OK:", len(trip_load) == len(trips))

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
cust_rev = (
    trip_load.groupby("customer_type")["revenue"].sum().sort_values(ascending=False)
)
sns.barplot(x=cust_rev.index, y=cust_rev.values, ax=axes[0], color="#4c72b0")
axes[0].set_title("Total revenue by customer type ($)")
axes[0].ticklabel_format(axis="y", style="plain")

lane_rev = trip_load.groupby("lane")["revenue"].sum().nlargest(10)
sns.barplot(y=lane_rev.index, x=lane_rev.values, ax=axes[1], color="#55a868")
axes[1].set_title("Top 10 lanes by revenue ($)")
axes[1].ticklabel_format(axis="x", style="plain")
plt.tight_layout()
plt.show()
    """),
    code("""
# Driver workload (active drivers only)
active_ids = drivers.loc[drivers["employment_status"] == "Active", "driver_id"]
driver_stats = (
    trips[trips["driver_id"].isin(active_ids)]
    .groupby("driver_id")
    .agg(trips=("trip_id", "count"), miles=("actual_distance_miles", "sum"),
         avg_mpg=("average_mpg", "mean"), idle=("idle_time_hours", "sum"))
    .join(drivers.set_index("driver_id")[["years_experience", "home_terminal"]])
)

fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
sns.histplot(driver_stats["trips"], bins=20, ax=axes[0], color="#4c72b0")
axes[0].set_title("Trips per active driver")
sns.scatterplot(data=driver_stats, x="years_experience", y="avg_mpg", ax=axes[1], color="#55a868")
axes[1].set_title("Experience vs average MPG")
sns.histplot(driver_stats["miles"] / 1000, bins=20, ax=axes[2], color="#c44e52")
axes[2].set_title("Thousand miles per driver")
plt.tight_layout()
plt.show()

display(driver_stats.describe().round(2))
    """),
    code("""
# MPG by truck make / model year — is there real separation?
truck_trips = trips.merge(trucks[["truck_id", "make", "model_year", "status"]], on="truck_id")
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
sns.boxplot(data=truck_trips, x="make", y="average_mpg", ax=axes[0], order=trucks["make"].value_counts().index)
axes[0].set_title("MPG by truck make")
axes[0].tick_params(axis="x", rotation=30)
sns.boxplot(data=truck_trips, x="model_year", y="average_mpg", ax=axes[1])
axes[1].set_title("MPG by model year")
plt.tight_layout()
plt.show()

# Maintenance-ish proxy: idle hours vs MPG
sns.scatterplot(data=trips.sample(5000, random_state=RANDOM), x="idle_time_hours", y="average_mpg", alpha=0.35, s=20)
plt.title("Idle hours vs MPG (5k sample)")
plt.tight_layout()
plt.show()
    """),
    md("""
## 12. Correlation matrix

Numeric drivers of revenue and efficiency.
    """),
    code("""
num_cols = [
    "revenue", "fuel_surcharge", "weight_lbs", "pieces",
    "actual_distance_miles", "actual_duration_hours", "fuel_gallons_used",
    "average_mpg", "idle_time_hours",
]
corr = trip_load[num_cols].corr()

fig, ax = plt.subplots(figsize=(10, 8))
sns.heatmap(corr, annot=True, fmt=".2f", cmap="RdBu_r", center=0, ax=ax, square=True, linewidths=0.5)
ax.set_title("Correlation matrix — loads × trips")
plt.tight_layout()
plt.show()

display(corr["revenue"].sort_values(ascending=False).round(3))
    """),
    md("""
## 13. MySQL EDA — summary

See `docs/FINDING.md` for the consolidated write-up across both notebooks.
    """),
    code("""
print("Tables:", len(tables))
print("Total rows:", f"{sum(len(v) for v in tables.values()):,}")
print("Load window:", loads["load_date"].min().date(), "→", loads["load_date"].max().date())
print("Revenue:", f"${loads['revenue'].sum():,.0f}", "| Fuel cost:", f"${fuel['total_cost'].sum():,.0f}")
print("Avg MPG:", round(trips['average_mpg'].mean(), 2), "| On-time deliveries: see mongo notebook")
    """),
]


# --------------------------------------------------------------------------
# Mongo EDA
# --------------------------------------------------------------------------
mongo_cells = [
    md("""
# MongoDB EDA — `fleet_operations`

Exploratory analysis of the three MongoDB collections: `delivery_events`, `maintenance_records`, `safety_incidents`.

**Important:** documents are raw CSV rows with **every value stored as a string**, plus Mongo's `_id`. Staging models must `try_cast` everything — this notebook quantifies cast risk and the business signals (on-time, detention, maintenance cost, safety).

> Companion notebook: `mysql_eda.ipynb`.
    """),
    md("## 1. Setup"),
    code("""
import warnings
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

warnings.filterwarnings("ignore")
sns.set_theme(style="whitegrid", palette="deep", font_scale=1.05)

DATA = Path("..") / "data" / "mongo"
if not DATA.exists():
    DATA = Path("data/mongo")
FIGSIZE = (12, 5)
RANDOM = 42
pd.set_option("display.max_columns", 50)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

# All values arrive as strings — keep that honest on read
print("pandas", pd.__version__, "| seaborn", sns.__version__)
print("data dir:", DATA.resolve())
    """),
    md("""
## 2. Load collections
    """),
    code("""
events = pd.read_csv(DATA / "delivery_events.csv", dtype=str)
maint = pd.read_csv(DATA / "maintenance_records.csv", dtype=str)
safety = pd.read_csv(DATA / "safety_incidents.csv", dtype=str)

for name, df in [("delivery_events", events), ("maintenance_records", maint), ("safety_incidents", safety)]:
    print(f"{name}: {df.shape[0]:,} docs × {df.shape[1]} fields")

display(events.head(3))
    """),
    md("""
## 3. Data-quality audit (string world)

- Business keys unique?
- How many values fail numeric / datetime casts?
- Which columns are null-ish (empty string vs real null)?
    """),
    code("""
def cast_report(df: pd.DataFrame, name: str, numeric_cols, datetime_cols) -> pd.DataFrame:
    rows = []
    for col in df.columns:
        s = df[col]
        empty = (s.isna() | (s.str.strip() == "") | (s.str.lower() == "nan")).sum()
        row = {"collection": name, "column": col, "empty": int(empty), "dtype_in": str(s.dtype)}
        if col in numeric_cols:
            bad = pd.to_numeric(s, errors="coerce").isna().sum() - empty
            row["cast_fail_numeric"] = int(max(bad, 0))
        if col in datetime_cols:
            bad = pd.to_datetime(s, errors="coerce", format="mixed").isna().sum() - empty
            row["cast_fail_datetime"] = int(max(bad, 0))
        rows.append(row)
    return pd.DataFrame(rows)

event_num = ["detention_minutes"]
event_dt = ["scheduled_datetime", "actual_datetime", "loaded_at"]
maint_num = ["odometer_reading", "labor_hours", "labor_cost", "parts_cost", "total_cost", "downtime_hours"]
maint_dt = ["maintenance_date", "loaded_at"]
safety_num = ["vehicle_damage_cost", "cargo_damage_cost", "claim_amount"]
safety_dt = ["incident_date", "loaded_at"]

report = pd.concat([
    cast_report(events, "delivery_events", event_num, event_dt),
    cast_report(maint, "maintenance_records", maint_num, maint_dt),
    cast_report(safety, "safety_incidents", safety_num, safety_dt),
], ignore_index=True)

display(report)

fail_cols = report.loc[
    (report.get("cast_fail_numeric", 0).fillna(0) > 0) | (report.get("cast_fail_datetime", 0).fillna(0) > 0)
]
print("Columns with cast failures:", len(fail_cols))
    """),
    code("""
# Keys and duplicates
for name, df, key in [
    ("delivery_events", events, "event_id"),
    ("maintenance_records", maint, "maintenance_id"),
    ("safety_incidents", safety, "incident_id"),
]:
    print(f"{name}: unique {key}={df[key].nunique():,} / {len(df):,} | dups={df[key].duplicated().sum()}")

# Loaded_at freshness window
for name, df in [("delivery_events", events), ("maintenance_records", maint), ("safety_incidents", safety)]:
    la = pd.to_datetime(df["loaded_at"], errors="coerce")
    print(f"{name} loaded_at: {la.min()} → {la.max()}")
    """),
    md("""
## 4. Delivery events — on-time and detention

Two events per load (Pickup + Delivery) → 170,820 docs for 85,410 loads.
    """),
    code("""
# Light typing for analysis (staging does this in dbt)
for col in ["detention_minutes"]:
    events[col] = pd.to_numeric(events[col], errors="coerce")
events["scheduled_dt"] = pd.to_datetime(events["scheduled_datetime"], errors="coerce")
events["actual_dt"] = pd.to_datetime(events["actual_datetime"], errors="coerce")
events["on_time"] = events["on_time_flag"].str.lower().map({"true": True, "false": False})

print("on_time_flag raw values:", events["on_time_flag"].value_counts(dropna=False).to_dict())
print("parsed on_time:", events["on_time"].value_counts(dropna=False).to_dict())

fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
sns.countplot(data=events, x="event_type", ax=axes[0], order=["Pickup", "Delivery"])
axes[0].set_title("Event type mix")

ot = events.groupby("event_type")["on_time"].mean()
sns.barplot(x=ot.index, y=ot.values, ax=axes[1], color="#55a868")
axes[1].set_title("On-time rate by event type")
axes[1].set_ylim(0, 1)
axes[1].axhline(ot.mean(), ls="--", color="gray", label=f"overall {ot.mean():.2%}")
axes[1].legend()

sns.countplot(data=events, y="location_state", ax=axes[2],
              order=events["location_state"].value_counts().head(10).index)
axes[2].set_title("Top 10 states by events")
plt.tight_layout()
plt.show()

overall_ot = events["on_time"].mean()
print(f"Overall on-time rate: {overall_ot:.2%}  (late: {(~events['on_time']).sum():,})")
    """),
    code("""
fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))

sns.histplot(data=events, x="detention_minutes", bins=40, ax=axes[0], color="#c44e52")
axes[0].set_title("Detention minutes (all events)")

det_delivery = events.loc[events["event_type"] == "Delivery", "detention_minutes"]
sns.histplot(data=det_delivery, bins=40, ax=axes[1], color="#8172b2")
axes[1].set_title("Detention — Delivery only")

sns.boxplot(data=events, x="event_type", y="detention_minutes", ax=axes[2], showfliers=False)
axes[2].set_title("Detention by event type")
plt.tight_layout()
plt.show()

print(events.groupby("event_type")["detention_minutes"].describe().round(1))
print("Detention > 120 min share:", f"{(events['detention_minutes'] > 120).mean():.2%}")
    """),
    code("""
# Temporal pattern — volume by day of week and hour
events["dow"] = events["scheduled_dt"].dt.day_name()
events["hour"] = events["scheduled_dt"].dt.hour
events["month"] = events["scheduled_dt"].dt.to_period("M").astype(str)

fig, axes = plt.subplots(1, 2, figsize=(14, 4.5))
order_dow = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
sns.countplot(data=events, x="dow", order=order_dow, ax=axes[0], color="#4c72b0")
axes[0].set_title("Events by day of week")
axes[0].tick_params(axis="x", rotation=30)
sns.countplot(data=events, x="hour", ax=axes[1], color="#55a868")
axes[1].set_title("Events by hour of day")
plt.tight_layout()
plt.show()

monthly_vol = events.groupby("month").size()
ax = monthly_vol.plot(figsize=FIGSIZE, marker="o", markersize=3, color="#4c72b0")
ax.set_title("Delivery events per month")
ax.set_ylabel("events")
plt.tight_layout()
plt.show()
    """),
    md("""
## 5. Maintenance records

Cost and downtime by type — feeds `mart_maintenance_cost_per_mile`.
    """),
    code("""
for col in ["odometer_reading", "labor_hours", "labor_cost", "parts_cost", "total_cost", "downtime_hours"]:
    maint[col] = pd.to_numeric(maint[col], errors="coerce")
maint["maintenance_dt"] = pd.to_datetime(maint["maintenance_date"], errors="coerce")
maint["month"] = maint["maintenance_dt"].dt.to_period("M").astype(str)

fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
type_order = maint["maintenance_type"].value_counts().index
sns.countplot(data=maint, y="maintenance_type", order=type_order, ax=axes[0], color="#4c72b0")
axes[0].set_title("Maintenance events by type")

sns.boxplot(data=maint, y="maintenance_type", x="total_cost", order=type_order, ax=axes[1], showfliers=False, color="#c44e52")
axes[1].set_title("Total cost by type ($)")

sns.boxplot(data=maint, y="maintenance_type", x="downtime_hours", order=type_order, ax=axes[2], showfliers=False, color="#55a868")
axes[2].set_title("Downtime by type (hours)")
plt.tight_layout()
plt.show()
    """),
    code("""
fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
sns.histplot(data=maint, x="total_cost", bins=40, ax=axes[0], color="#c44e52")
axes[0].set_title("Total cost distribution")
sns.histplot(data=maint, x="downtime_hours", bins=30, ax=axes[1], color="#55a868")
axes[1].set_title("Downtime distribution")
sns.scatterplot(data=maint, x="labor_hours", y="total_cost", hue="maintenance_type",
                alpha=0.5, s=25, ax=axes[2], hue_order=type_order)
axes[2].set_title("Labor hours vs total cost")
plt.tight_layout()
plt.show()

cost_by_type = maint.groupby("maintenance_type").agg(
    n=("maintenance_id", "count"),
    avg_cost=("total_cost", "mean"),
    avg_downtime=("downtime_hours", "mean"),
    labor_share=("labor_cost", lambda s: (s / maint.loc[s.index, "total_cost"]).mean()),
).round(2).sort_values("avg_cost", ascending=False)
display(cost_by_type)

monthly_cost = maint.groupby("month").agg(cost=("total_cost", "sum"), downtime=("downtime_hours", "sum"))
ax = monthly_cost["cost"].plot(figsize=FIGSIZE, color="#c44e52", marker="o", markersize=3)
ax.set_title("Monthly maintenance cost ($)")
plt.tight_layout()
plt.show()
    """),
    md("""
## 6. Safety incidents

Small collection (170) — treat rates carefully; focus on mix and claim severity.
    """),
    code("""
for col in ["vehicle_damage_cost", "cargo_damage_cost", "claim_amount"]:
    safety[col] = pd.to_numeric(safety[col], errors="coerce")
safety["incident_dt"] = pd.to_datetime(safety["incident_date"], errors="coerce")
safety["at_fault"] = safety["at_fault_flag"].str.lower().map({"true": True, "false": False})
safety["preventable"] = safety["preventable_flag"].str.lower().map({"true": True, "false": False})
safety["injury"] = safety["injury_flag"].str.lower().map({"true": True, "false": False})

fig, axes = plt.subplots(2, 2, figsize=(14, 9))
sns.countplot(data=safety, y="incident_type", order=safety["incident_type"].value_counts().index,
              ax=axes[0, 0], color="#4c72b0")
axes[0, 0].set_title("Incident type")

sns.countplot(data=safety, x="at_fault", ax=axes[0, 1], color="#c44e52")
axes[0, 1].set_title("At-fault share")

sns.countplot(data=safety, x="preventable", ax=axes[1, 0], color="#55a868")
axes[1, 0].set_title("Preventable share")

sns.histplot(data=safety, x="claim_amount", bins=25, ax=axes[1, 1], color="#8172b2")
axes[1, 1].set_title("Claim amount ($)")
plt.tight_layout()
plt.show()

print("At-fault:", safety["at_fault"].value_counts(dropna=False).to_dict())
print("Preventable:", safety["preventable"].value_counts(dropna=False).to_dict())
print("Injury:", safety["injury"].value_counts(dropna=False).to_dict())
print("Claim total:", f"${safety['claim_amount'].sum():,.0f}", "| mean:", f"${safety['claim_amount'].mean():,.0f}")
    """),
    code("""
# Severity cross-tab: type × at-fault, and claim by type
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
ct = pd.crosstab(safety["incident_type"], safety["at_fault"])
ct.plot(kind="bar", stacked=True, ax=axes[0], color=["#55a868", "#c44e52"])
axes[0].set_title("Incident type × at-fault")
axes[0].tick_params(axis="x", rotation=30)
axes[0].legend(title="at_fault")

sns.boxplot(data=safety, y="incident_type", x="claim_amount", order=safety["incident_type"].value_counts().index,
            ax=axes[1], showfliers=False, color="#4c72b0")
axes[1].set_title("Claim amount by incident type")
plt.tight_layout()
plt.show()

# Cost components
safety["total_damage"] = safety["vehicle_damage_cost"].fillna(0) + safety["cargo_damage_cost"].fillna(0)
print("Mean claim by at-fault:")
display(safety.groupby("at_fault")["claim_amount"].agg(["count", "mean", "sum"]).round(0))
print("Preventable claim mean:", round(safety.loc[safety["preventable"], "claim_amount"].mean(), 0))
print("Not-preventable claim mean:", round(safety.loc[~safety["preventable"], "claim_amount"].mean(), 0))
    """),
    md("""
## 7. Cross-store sanity (Mongo → MySQL)

Delivery events reference `load_id` / `trip_id` from MySQL. Verify join coverage and compute **on-time rate by joining to drivers** — the key input to `mart_driver_performance`.
    """),
    code("""
mysql_dir = DATA.parent / "mysql"
if not mysql_dir.exists():
    mysql_dir = Path("data/mysql")
trips = pd.read_csv(mysql_dir / "trips.csv", dtype=str)
loads = pd.read_csv(mysql_dir / "loads.csv", dtype=str)
drivers = pd.read_csv(mysql_dir / "drivers.csv", dtype=str)
routes = pd.read_csv(mysql_dir / "routes.csv", dtype=str)

# Referential coverage
ev_loads = set(events["load_id"])
ev_trips = set(events["trip_id"])
my_loads = set(loads["load_id"])
my_trips = set(trips["trip_id"])
print(f"event load_ids in MySQL loads: {len(ev_loads & my_loads):,} / {len(ev_loads):,}")
print(f"event trip_ids in MySQL trips: {len(ev_trips & my_trips):,} / {len(ev_trips):,}")
print(f"MySQL loads with zero events: {len(my_loads - ev_loads):,}")
    """),
    code("""
# Delivery events already carry load_id — join only driver/truck from trips
deliv = events.loc[events["event_type"] == "Delivery"].copy()
deliv = deliv.merge(trips[["trip_id", "driver_id", "truck_id"]], on="trip_id", how="inner")
print("Delivery rows after join:", len(deliv))

driver_ot = (
    deliv.groupby("driver_id")
    .agg(events=("event_id", "count"), on_time_rate=("on_time", "mean"),
         avg_detention=("detention_minutes", "mean"))
    .join(drivers.set_index("driver_id")[["employment_status", "years_experience"]])
    .query("employment_status == 'Active'")
)

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
sns.histplot(driver_ot["on_time_rate"], bins=20, ax=axes[0], color="#55a868")
axes[0].set_title("On-time rate per active driver")
axes[0].axvline(driver_ot["on_time_rate"].mean(), ls="--", color="gray",
                label=f"mean {driver_ot['on_time_rate'].mean():.2%}")
axes[0].legend()

sns.scatterplot(data=driver_ot, x="years_experience", y="on_time_rate", ax=axes[1], s=50, color="#4c72b0")
axes[1].set_title("Experience vs on-time rate")
plt.tight_layout()
plt.show()

display(driver_ot[["events", "on_time_rate", "avg_detention"]].describe().round(3))
print(f"Fleet on-time (delivery): {deliv['on_time'].mean():.2%}")
    """),
    code("""
# Route-level on-time for profitability join
deliv2 = deliv.merge(loads[["load_id", "route_id", "revenue"]], on="load_id", how="left")
deliv2 = deliv2.merge(routes[["route_id", "origin_state", "destination_state"]], on="route_id", how="left")
deliv2["lane"] = deliv2["origin_state"] + " → " + deliv2["destination_state"]

lane_ot = deliv2.groupby("lane").agg(
    deliveries=("event_id", "count"),
    on_time_rate=("on_time", "mean"),
    avg_detention=("detention_minutes", "mean"),
).sort_values("deliveries", ascending=False)

fig, ax = plt.subplots(figsize=(12, 6))
top = lane_ot.head(15)
sns.barplot(y=top.index, x=top["on_time_rate"], ax=ax, color="#4c72b0")
ax.axvline(deliv2["on_time"].mean(), ls="--", color="gray", label="fleet mean")
ax.set_title("On-time rate — top 15 lanes by volume")
ax.set_xlabel("on-time rate")
ax.legend()
plt.tight_layout()
plt.show()

display(lane_ot.head(10).round(3))
    """),
    md("""
## 8. Mongo EDA — summary

See `docs/FINDING.md` for the consolidated write-up across both notebooks.
    """),
    code("""
print("Collections:", 3)
print("Docs:", f"{len(events) + len(maint) + len(safety):,}")
print(f"On-time (delivery): {events.loc[events.event_type == 'Delivery', 'on_time'].mean():.2%}")
print(f"Mean detention: {events['detention_minutes'].mean():.1f} min")
print(f"Maintenance cost: ${maint['total_cost'].sum():,.0f} | downtime {maint['downtime_hours'].sum():,.0f} h")
print(f"Safety claims: ${safety['claim_amount'].sum():,.0f}")
    """),
]


write_nb(OUT / "mysql_eda.ipynb", mysql_cells)
write_nb(OUT / "mongo_eda.ipynb", mongo_cells)
