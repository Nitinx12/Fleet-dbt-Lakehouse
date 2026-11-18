"""Execute the tracked EDA notebooks into ignored output artifacts."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import nbformat
from nbclient.exceptions import CellExecutionError, CellTimeoutError, DeadKernelError
from nbconvert.preprocessors import ExecutePreprocessor

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS_DIR = ROOT / "notebooks"
DEFAULT_OUTPUT_DIR = ROOT / "out" / "notebooks"
_REQUIRED_DATA = {
    "mongo_eda.ipynb": (ROOT / "data" / "mongo", ROOT / "data" / "mysql"),
    "mysql_eda.ipynb": (ROOT / "data" / "mysql",),
}

log = logging.getLogger("run_notebooks")


class NotebookRunError(RuntimeError):
    """Raised when a requested notebook cannot run in this workspace."""


def _arguments() -> argparse.Namespace:
    """Parse the requested notebook names and execution options."""
    parser = argparse.ArgumentParser(
        description="Execute tracked EDA notebooks into out/notebooks."
    )
    parser.add_argument(
        "notebooks",
        choices=tuple(sorted(_REQUIRED_DATA)),
        nargs="*",
        help="Notebook filenames to execute; omit to run all tracked notebooks.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        type=Path,
        help="Directory for executed notebook copies.",
    )
    parser.add_argument(
        "--timeout",
        default=600,
        type=int,
        help="Per-cell execution timeout in seconds.",
    )
    return parser.parse_args()


def selected_notebooks(names: list[str]) -> list[Path]:
    """Return tracked notebook paths for the requested names."""
    selected = names or sorted(_REQUIRED_DATA)
    return [NOTEBOOKS_DIR / name for name in selected]


def validate_inputs(notebooks: list[Path]) -> None:
    """Require the local source extracts needed by every selected notebook."""
    missing = {
        directory
        for notebook in notebooks
        for directory in _REQUIRED_DATA[notebook.name]
        if not directory.is_dir()
    }
    if missing:
        relative_paths = ", ".join(
            str(path.relative_to(ROOT)) for path in sorted(missing)
        )
        raise NotebookRunError(f"missing required notebook data: {relative_paths}")


def execute_notebook(
    notebook_path: Path,
    output_dir: Path,
    *,
    timeout: int,
    working_dir: Path = ROOT,
) -> Path:
    """Execute one notebook and write its executed copy outside the source tree."""
    with notebook_path.open(encoding="utf-8") as handle:
        notebook = nbformat.read(handle, as_version=4)

    processor = ExecutePreprocessor(timeout=timeout, kernel_name="python3")
    processor.preprocess(notebook, {"metadata": {"path": str(working_dir)}})

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{notebook_path.stem}.executed.ipynb"
    with output_path.open("w", encoding="utf-8") as handle:
        nbformat.write(notebook, handle)
    return output_path


def main() -> int:
    """Run requested notebooks and return a shell-compatible status code."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _arguments()
    notebooks = selected_notebooks(args.notebooks)
    try:
        validate_inputs(notebooks)
    except NotebookRunError as error:
        log.error("notebook run blocked: %s", error)
        return 1

    for notebook in notebooks:
        try:
            output = execute_notebook(notebook, args.output_dir, timeout=args.timeout)
        except (
            CellExecutionError,
            CellTimeoutError,
            DeadKernelError,
            OSError,
        ) as error:
            log.error("notebook %s failed (%s)", notebook.name, type(error).__name__)
            return 1
        log.info("executed %s -> %s", notebook.name, output.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    sys.exit(main())
