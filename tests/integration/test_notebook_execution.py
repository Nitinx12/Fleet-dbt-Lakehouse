"""Notebook execution against a real Jupyter kernel."""

from __future__ import annotations

from pathlib import Path

import nbformat

from scripts.run_notebooks import execute_notebook


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
