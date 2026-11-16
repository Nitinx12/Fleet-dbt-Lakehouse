"""CLI: python -m ingestion [--dry-run] [--mode live|csv|fixture] [--system mysql|mongo]."""

from __future__ import annotations

import argparse
import json
import logging
import sys

from ingestion.config import IngestError, SourceMode, source_mode
from ingestion.pipeline import run_ingestion


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="ingestion", description="Land sources into Databricks raw"
    )
    parser.add_argument("--mode", choices=[m.value for m in SourceMode], default=None)
    parser.add_argument("--system", action="append", choices=["mysql", "mongo"], default=None)
    parser.add_argument("--destination", default=None, help="dlt destination (default: databricks)")
    parser.add_argument("--dry-run", action="store_true", help="build resources only, do not load")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        mode = SourceMode(args.mode) if args.mode else source_mode()
        summary = run_ingestion(
            mode=mode,
            systems=set(args.system) if args.system else None,
            destination=args.destination,
            dry_run=args.dry_run,
        )
    except IngestError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
