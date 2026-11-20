"""Load Airflow DAG modules when the pinned Airflow runtime is available."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DAGS_DIRECTORY = ROOT / "airflow" / "dags"


@pytest.mark.unit
def test_dag_modules_import_without_errors() -> None:
    """Keep the phase-one and phase-two DAG modules syntactically importable."""
    if importlib.util.find_spec("airflow.sdk") is None:
        pytest.skip("Airflow Task SDK is installed in the Airflow image, not this venv")
    for dag_file in sorted(DAGS_DIRECTORY.glob("*.py")):
        spec = importlib.util.spec_from_file_location(dag_file.stem, dag_file)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        assert module.dag.doc_md
        assert module.dag.owner == "fleet-data-eng"
        assert module.dag.tags
