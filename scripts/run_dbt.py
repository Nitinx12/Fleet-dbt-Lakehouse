"""Load .env and invoke dbt with the given arguments."""


import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

DBT = ROOT / ".venv" / "Scripts" / "dbt.exe"
if not DBT.exists():
    DBT = "dbt"

sys.exit(subprocess.call([str(DBT), *sys.argv[1:]], cwd=ROOT / "dbt"))
