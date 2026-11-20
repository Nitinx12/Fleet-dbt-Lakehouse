import pytest

from scripts import load_fixtures


def test_pg_type_maps_contract_types():
    assert load_fixtures._pg_type("string") == "text"
    assert load_fixtures._pg_type("DECIMAL(10,4)") == "decimal(10,4)"
    assert load_fixtures._pg_type("timestamp") == "timestamptz"
    assert load_fixtures._pg_type("unknown") == "text"


def test_require_fixture_flag_fails_closed(monkeypatch):
    monkeypatch.delenv("FIXTURE_LOAD", raising=False)
    with pytest.raises(RuntimeError, match="FIXTURE_LOAD=1"):
        load_fixtures._require_fixture_flag()


def test_require_fixture_flag_allows_explicit_load(monkeypatch):
    monkeypatch.setenv("FIXTURE_LOAD", "1")
    assert load_fixtures._require_fixture_flag() is None


def test_require_fixture_database_fails_closed(monkeypatch):
    monkeypatch.setenv("PG_DBNAME", "fleet_warehouse")
    with pytest.raises(RuntimeError, match="PG_DBNAME=fleet_dev"):
        load_fixtures._require_fixture_database()


def test_require_fixture_database_allows_dev(monkeypatch):
    monkeypatch.setenv("PG_DBNAME", "fleet_dev")
    assert load_fixtures._require_fixture_database() is None


def test_bronze_types_prefers_raw_type():
    contract = {
        "columns": [
            {"name": "event_id", "raw_type": "string", "staged_type": "string"},
            {"name": "detention_minutes", "raw_type": "string", "staged_type": "int"},
            {"name": "amount", "type": "decimal(12,2)"},
        ]
    }
    assert load_fixtures._bronze_types(contract) == {
        "event_id": "text",
        "detention_minutes": "text",
        "amount": "decimal(12,2)",
    }


def test_sample_rows_accepts_raw_mongo_payloads():
    assert load_fixtures._sample_rows({"sample_rows_raw": [{"id": 1}]}) == [{"id": 1}]
    assert load_fixtures._sample_rows({"sample_rows": [{"id": 2}]}) == [{"id": 2}]


def test_sample_rows_fails_without_rows():
    with pytest.raises(ValueError, match="sample_rows"):
        load_fixtures._sample_rows({"contract": "x.yml"})


def test_references_parse_full_and_short_forms():
    contract = {
        "columns": [
            {"name": "load_id", "references": "mysql.fleet_registry.loads.load_id"},
            {"name": "trip_id", "references": "trips.trip_id"},
        ]
    }
    assert load_fixtures._references(contract) == [
        ("load_id", "loads.load_id"),
        ("trip_id", "trips.trip_id"),
    ]


def test_expand_fixtures_adds_missing_referenced_rows():
    customer_contract = {
        "source": "mysql.fleet_registry.customers",
        "landing_table": "bronze.mysql__customers",
        "extraction": {"mode": "full_replace"},
        "primary_key": ["customer_id"],
        "columns": [{"name": "customer_id"}],
    }
    load_contract = {
        "source": "mysql.fleet_registry.loads",
        "landing_table": "bronze.mysql__loads",
        "extraction": {"mode": "incremental", "cursor": "load_id"},
        "primary_key": ["load_id"],
        "columns": [
            {"name": "load_id"},
            {"name": "customer_id", "references": "customers.customer_id"},
        ],
    }
    customers = load_fixtures.Fixture(
        load_fixtures._source_spec(customer_contract),
        {},
        customer_contract,
        [{"customer_id": "C1"}],
    )
    loads = load_fixtures.Fixture(
        load_fixtures._source_spec(load_contract),
        {},
        load_contract,
        [{"load_id": "L1", "customer_id": "C2"}],
    )
    load_fixtures._expand_fixtures([customers, loads])
    assert [row["customer_id"] for row in customers.rows] == ["C1", "C2"]


def test_source_spec_uses_tuple_keys():
    contract = {
        "source": "mysql.fleet_registry.trucks",
        "landing_table": "bronze.mysql__trucks",
        "extraction": {"mode": "full_replace"},
        "primary_key": ["truck_id"],
        "columns": [{"name": "truck_id"}, {"name": "status"}],
    }
    spec = load_fixtures._source_spec(contract)
    assert spec.pk == ("truck_id",)
    assert spec.columns == ("truck_id", "status")
    assert spec.load_type == "FULL"
