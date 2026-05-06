"""Command line interface for Atlas residual identity review."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from api.settings import AtlasSettings
from identity_engine.evaluation import (
    evaluate_against_deterministic_hints,
    render_proxy_evaluation_report,
    render_residual_report,
    summarize_residual_candidates,
)
from identity_engine.residual_matcher import (
    ResidualCandidate,
    SourceIdentity,
    rank_residual_candidates,
)
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

    report = subparsers.add_parser(
        "residual-report",
        help="Render a markdown summary of residual candidate review coverage.",
    )
    report.add_argument("--limit", type=int, default=500)
    report.add_argument("--top-n", type=int, default=3)
    report.add_argument("--minimum-score", type=float, default=0.75)
    report.add_argument("--output", type=Path)
    report.add_argument("--top-candidates", type=int, default=10)
    report.set_defaults(func=run_residual_report)

    evaluate = subparsers.add_parser(
        "residual-evaluate",
        help="Render optional proxy evaluation against stewardship deterministic hints.",
    )
    evaluate.add_argument("--limit", type=int, default=500)
    evaluate.add_argument("--top-n", type=int, default=3)
    evaluate.add_argument("--minimum-score", type=float, default=0.75)
    evaluate.add_argument("--output", type=Path)
    evaluate.set_defaults(func=run_residual_evaluate)
    return parser


def run_residual_candidates(args: argparse.Namespace) -> int:
    _source_rows, candidates = _rank_candidates(args)
    export_rows = [candidate.as_export_row() for candidate in candidates]

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        export_rows_to_csv(export_rows, str(args.output))
        print(f"Wrote {len(export_rows)} residual review candidates to {args.output}")
    else:
        print(json.dumps(export_rows, indent=2, sort_keys=True))

    return 0


def run_residual_report(args: argparse.Namespace) -> int:
    source_rows, candidates = _rank_candidates(args)
    summary = summarize_residual_candidates(candidates, source_record_count=len(source_rows))
    report = render_residual_report(summary, candidates, top_n=args.top_candidates)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
        print(f"Wrote residual review report to {args.output}")
    else:
        print(report)

    return 0


def run_residual_evaluate(args: argparse.Namespace) -> int:
    source_rows, candidates = _rank_candidates(args)
    summary = evaluate_against_deterministic_hints(source_rows, candidates)
    report = render_proxy_evaluation_report(
        summary,
        top_n=args.top_n,
        minimum_score=args.minimum_score,
    )

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
        print(f"Wrote residual proxy evaluation to {args.output}")
    else:
        print(report)

    return 0


def _rank_candidates(
    args: argparse.Namespace,
) -> tuple[list[SourceIdentity], list[ResidualCandidate]]:
    settings = AtlasSettings.from_env()
    source_rows, canonical_rows = load_residual_inputs(settings, limit=args.limit)
    candidates = rank_residual_candidates(
        source_rows,
        canonical_rows,
        top_n=args.top_n,
        minimum_score=args.minimum_score,
    )
    return source_rows, candidates


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    func: Any = args.func
    return int(func(args))


if __name__ == "__main__":
    raise SystemExit(main())
