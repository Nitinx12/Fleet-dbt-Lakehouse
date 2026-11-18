"""Load .env, invoke dbt, then land the run results in the audit tables."""

from __future__ import annotations

import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
# run as a script, so the repo root is not on sys.path yet
sys.path.insert(0, str(ROOT))

DBT = ROOT / ".venv" / "Scripts" / "dbt.exe"
if not DBT.exists():
    DBT = "dbt"


def main() -> int:
    """Run dbt, audit the invocation, and return dbt's own exit code."""
    started = datetime.now(UTC)
    exit_code = subprocess.call([str(DBT), *sys.argv[1:]], cwd=ROOT / "dbt")
    audit_invocation(started, exit_code)
    return exit_code


def audit_invocation(started: datetime, exit_code: int) -> None:
    """Best effort: a broken audit trail must never hide dbt's exit code."""
    from utils import dbt_audit
    from utils.logger import get_logger

    log = get_logger("run_dbt")
    try:
        dbt_audit.audit_invocation(
            ROOT / "dbt", started_after=started, exit_code=exit_code
        )
    except Exception:
        log.exception("dbt audit load failed")


if __name__ == "__main__":
    sys.exit(main())
