"""Command line interface for Atlas residual identity review."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from api.settings import AtlasSettings
from identity_engine.residual_matcher import rank_residual_candidates
from identity_engine.snowflake_io import export_rows_to_csv, load_residual_inputs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Atlas identity-engine utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    residual = subparsers.add_parser(
        "residual-candidates",
        help="Export review-only residual candidate matches from the stewardship queue.",
    )
    residual.add_argument("--limit", type=int, default=500)
    residual.add_argument("--top-n", type=int, default=3)
    residual.add_argument("--minimum-score", type=float, default=0.75)
    residual.add_argument("--output", type=Path)
    residual.set_defaults(func=run_residual_candidates)
    return parser


def run_residual_candidates(args: argparse.Namespace) -> int:
    settings = AtlasSettings.from_env()
    source_rows, canonical_rows = load_residual_inputs(settings, limit=args.limit)
    candidates = rank_residual_candidates(
        source_rows,
        canonical_rows,
        top_n=args.top_n,
        minimum_score=args.minimum_score,
    )
    export_rows = [candidate.as_export_row() for candidate in candidates]

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        export_rows_to_csv(export_rows, str(args.output))
        print(f"Wrote {len(export_rows)} residual review candidates to {args.output}")
    else:
        print(json.dumps(export_rows, indent=2, sort_keys=True))

    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    func: Any = args.func
    return int(func(args))


if __name__ == "__main__":
    raise SystemExit(main())
