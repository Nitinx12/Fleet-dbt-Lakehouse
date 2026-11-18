"""Notebook execution writes reproducible copies outside the source tree."""

from __future__ import annotations

from pathlib import Path

import nbformat
import pytest

from scripts.run_notebooks import (
    ROOT,
    NotebookRunError,
    execute_notebook,
    validate_inputs,
)


@pytest.mark.unit
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


@pytest.mark.integration
def test_execute_notebook_writes_an_executed_copy(tmp_path: Path) -> None:
    """Execute a small notebook without changing its tracked source copy."""
    source = tmp_path / "source.ipynb"
    output_dir = tmp_path / "out"
    notebook = nbformat.v4.new_notebook(
        cells=[nbformat.v4.new_code_cell("value = 40 + 2\nprint(value)")]
    )
    nbformat.write(notebook, source)

    output = execute_notebook(source, output_dir, timeout=60, working_dir=tmp_path)

    executed = nbformat.read(output, as_version=4)
    assert source.read_text(encoding="utf-8") != output.read_text(encoding="utf-8")
    assert executed.cells[0].outputs[0]["text"] == "42\n"
