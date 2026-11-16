"""Execute EDA notebooks in-place with the project root as cwd."""

from pathlib import Path

import nbformat
from nbclient import NotebookClient

ROOT = Path(__file__).resolve().parents[1]


def run(path: Path) -> None:
    print(f"executing {path.name} ...")
    nb = nbformat.read(path, as_version=4)
    client = NotebookClient(
        nb,
        timeout=300,
        kernel_name="python3",
        resources={"metadata": {"path": str(ROOT)}},
    )
    client.execute()
    nbformat.write(nb, path)
    print(f"  ok — {len(nb.cells)} cells")


if __name__ == "__main__":
    for name in ("mysql_eda.ipynb", "mongo_eda.ipynb"):
        run(ROOT / "notebooks" / name)
