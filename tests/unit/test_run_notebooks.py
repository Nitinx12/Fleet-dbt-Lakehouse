"""Notebook input validation fails before starting a kernel."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.run_notebooks import ROOT, NotebookRunError, validate_inputs


def test_validate_inputs_names_missing_data_directories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject notebook runs before starting a kernel when extracts are absent."""
    from scripts import run_notebooks

    notebook = Path("mongo_eda.ipynb")
    missing_mongo = ROOT / "missing" / "mongo"
    missing_mysql = ROOT / "missing" / "mysql"
    monkeypatch.setattr(
        run_notebooks,
        "_REQUIRED_DATA",
        {notebook.name: (missing_mongo, missing_mysql)},
    )

    with pytest.raises(NotebookRunError, match="missing required notebook data"):
        validate_inputs([notebook])
