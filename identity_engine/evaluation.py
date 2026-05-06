"""Evaluation and reporting helpers for residual identity review."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field

from identity_engine.residual_matcher import ResidualCandidate

FEATURE_COLUMNS = (
    "first_name_root",
    "last_name",
    "email_local",
    "hire_date",
    "deterministic_hint",
)


@dataclass(frozen=True)
class ResidualEvaluationSummary:
    source_record_count: int
    candidate_count: int
    source_records_with_candidates: int
    recommendation_counts: dict[str, int] = field(default_factory=dict)
    source_system_counts: dict[str, int] = field(default_factory=dict)
    mean_score_by_recommendation: dict[str, float] = field(default_factory=dict)
    mean_evidence_by_recommendation: dict[str, float] = field(default_factory=dict)
    feature_coverage: dict[str, float] = field(default_factory=dict)

    @property
    def review_yield_rate(self) -> float:
        if self.source_record_count == 0:
            return 0.0
        return self.source_records_with_candidates / self.source_record_count


def summarize_residual_candidates(
    candidates: list[ResidualCandidate],
    *,
    source_record_count: int | None = None,
) -> ResidualEvaluationSummary:
    unique_sources = {candidate.source_record_key for candidate in candidates}
    denominator = source_record_count if source_record_count is not None else len(unique_sources)

    recommendation_counts = Counter(candidate.recommendation for candidate in candidates)
    source_system_counts = Counter(candidate.source_system for candidate in candidates)

    scores_by_recommendation: dict[str, list[float]] = defaultdict(list)
    evidence_by_recommendation: dict[str, list[float]] = defaultdict(list)
    feature_present_counts = Counter[str]()
    for candidate in candidates:
        scores_by_recommendation[candidate.recommendation].append(candidate.residual_score)
        evidence_by_recommendation[candidate.recommendation].append(candidate.evidence_weight)
        for feature in FEATURE_COLUMNS:
            if candidate.feature_scores.get(feature) is not None:
                feature_present_counts[feature] += 1

    return ResidualEvaluationSummary(
        source_record_count=denominator,
        candidate_count=len(candidates),
        source_records_with_candidates=len(unique_sources),
        recommendation_counts=dict(sorted(recommendation_counts.items())),
        source_system_counts=dict(sorted(source_system_counts.items())),
        mean_score_by_recommendation={
            recommendation: _mean(scores)
            for recommendation, scores in sorted(scores_by_recommendation.items())
        },
        mean_evidence_by_recommendation={
            recommendation: _mean(values)
            for recommendation, values in sorted(evidence_by_recommendation.items())
        },
        feature_coverage={
            feature: (feature_present_counts[feature] / len(candidates) if candidates else 0.0)
            for feature in FEATURE_COLUMNS
        },
    )


def render_residual_report(
    summary: ResidualEvaluationSummary,
    candidates: list[ResidualCandidate],
    *,
    top_n: int = 10,
) -> str:
    lines = [
        "# Residual Review Report",
        "",
        "This report summarizes review-only residual identity candidates produced",
        "after deterministic dbt matching. These rows are steward-review aids,",
        "not canonical truth and not automatic merges.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Stewardship rows sampled | {summary.source_record_count:,} |",
        f"| Rows with at least one suggested candidate | {summary.source_records_with_candidates:,} |",
        f"| Candidate rows emitted | {summary.candidate_count:,} |",
        f"| Review yield rate | {_pct(summary.review_yield_rate)} |",
        "",
        "## Recommendation Mix",
        "",
        "| Recommendation | Candidates | Mean score | Mean evidence weight |",
        "|---|---:|---:|---:|",
    ]
    for recommendation, count in summary.recommendation_counts.items():
        lines.append(
            "| "
            f"{recommendation} | "
            f"{count:,} | "
            f"{summary.mean_score_by_recommendation.get(recommendation, 0.0):.3f} | "
            f"{summary.mean_evidence_by_recommendation.get(recommendation, 0.0):.3f} |"
        )

    lines.extend(
        [
            "",
            "## Source-System Mix",
            "",
            "| Source system | Candidates |",
            "|---|---:|",
        ]
    )
    for source_system, count in summary.source_system_counts.items():
        lines.append(f"| {source_system} | {count:,} |")

    lines.extend(
        [
            "",
            "## Feature Coverage",
            "",
            "| Feature | Candidate coverage |",
            "|---|---:|",
        ]
    )
    for feature, coverage in summary.feature_coverage.items():
        lines.append(f"| {feature} | {_pct(coverage)} |")

    lines.extend(
        [
            "",
            "## Top Review Candidates",
            "",
            "| Source record | Candidate canonical person | Recommendation | Score | Anchors | Reasons |",
            "|---|---|---|---:|---:|---|",
        ]
    )
    for candidate in sorted(
        candidates,
        key=lambda item: (
            item.residual_score,
            item.positive_anchor_count,
            item.evidence_weight,
            item.source_record_key,
        ),
        reverse=True,
    )[:top_n]:
        lines.append(
            "| "
            f"{candidate.source_record_key} | "
            f"{candidate.canonical_person_id} | "
            f"{candidate.recommendation} | "
            f"{candidate.residual_score:.3f} | "
            f"{candidate.positive_anchor_count} | "
            f"{_markdown_cell('; '.join(candidate.reasons))} |"
        )

    lines.extend(
        [
            "",
            "## Control Boundary",
            "",
            "The residual engine is deliberately outside the canonical-person write path.",
            "Any suggested match must still be adjudicated by a human steward before",
            "it can influence canonical employee records or downstream marts.",
            "",
        ]
    )
    return "\n".join(lines)


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _pct(value: float) -> str:
    return f"{value:.1%}"


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|")
