"""Evaluation and reporting helpers for residual identity review."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field

from identity_engine.residual_matcher import ResidualCandidate, SourceIdentity

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


@dataclass(frozen=True)
class ResidualProxyEvaluationSummary:
    evaluated_source_count: int
    source_records_with_candidates: int
    top_1_alignment_count: int
    top_k_alignment_count: int
    missing_candidate_count: int
    mean_proxy_label_rank: float
    candidate_counts_by_recommendation: dict[str, int] = field(default_factory=dict)
    proxy_alignment_by_recommendation: dict[str, float] = field(default_factory=dict)

    @property
    def candidate_coverage_rate(self) -> float:
        if self.evaluated_source_count == 0:
            return 0.0
        return self.source_records_with_candidates / self.evaluated_source_count

    @property
    def top_1_alignment_rate(self) -> float:
        if self.evaluated_source_count == 0:
            return 0.0
        return self.top_1_alignment_count / self.evaluated_source_count

    @property
    def top_k_alignment_rate(self) -> float:
        if self.evaluated_source_count == 0:
            return 0.0
        return self.top_k_alignment_count / self.evaluated_source_count

    @property
    def missing_candidate_rate(self) -> float:
        if self.evaluated_source_count == 0:
            return 0.0
        return self.missing_candidate_count / self.evaluated_source_count


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


def evaluate_against_deterministic_hints(
    source_rows: list[SourceIdentity],
    candidates: list[ResidualCandidate],
) -> ResidualProxyEvaluationSummary:
    """Evaluate candidate ranking against stewardship hints, not true labels.

    `int_stewardship_queue.suggested_canonical_person_id` is the best
    deterministic candidate that failed auto-merge controls. It is useful as a
    weak proxy for walkthrough evaluation, but it is not ground truth and must
    not be used to auto-resolve employee identity.
    """

    proxy_labels: dict[str, str] = {}
    for source in source_rows:
        if source.suggested_canonical_person_id:
            proxy_labels[source.source_record_key] = source.suggested_canonical_person_id
    candidates_by_source: dict[str, list[ResidualCandidate]] = defaultdict(list)
    for candidate in candidates:
        candidates_by_source[candidate.source_record_key].append(candidate)

    sources_with_candidates = 0
    top_1_alignment_count = 0
    top_k_alignment_count = 0
    missing_candidate_count = 0
    proxy_label_ranks: list[int] = []

    for source_record_key, proxy_label in proxy_labels.items():
        source_candidates = candidates_by_source.get(source_record_key, [])
        if not source_candidates:
            missing_candidate_count += 1
            continue

        sources_with_candidates += 1
        if source_candidates[0].canonical_person_id == proxy_label:
            top_1_alignment_count += 1

        for rank, candidate in enumerate(source_candidates, start=1):
            if candidate.canonical_person_id == proxy_label:
                top_k_alignment_count += 1
                proxy_label_ranks.append(rank)
                break

    recommendation_counts = Counter[str]()
    recommendation_alignments = Counter[str]()
    for candidate in candidates:
        proxy_hint = proxy_labels.get(candidate.source_record_key)
        if not proxy_hint:
            continue
        recommendation_counts[candidate.recommendation] += 1
        if candidate.canonical_person_id == proxy_hint:
            recommendation_alignments[candidate.recommendation] += 1

    return ResidualProxyEvaluationSummary(
        evaluated_source_count=len(proxy_labels),
        source_records_with_candidates=sources_with_candidates,
        top_1_alignment_count=top_1_alignment_count,
        top_k_alignment_count=top_k_alignment_count,
        missing_candidate_count=missing_candidate_count,
        mean_proxy_label_rank=_mean([float(rank) for rank in proxy_label_ranks]),
        candidate_counts_by_recommendation=dict(sorted(recommendation_counts.items())),
        proxy_alignment_by_recommendation={
            recommendation: recommendation_alignments[recommendation] / count
            for recommendation, count in sorted(recommendation_counts.items())
            if count
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
        "## How To Read This Report",
        "",
        "A row in this report means the residual engine found a reviewable",
        "candidate for a source identity that deterministic dbt matching left",
        "in stewardship. It does not mean the candidate is correct, and it does",
        "not change `int_canonical_person`.",
        "",
        "The highest-value operating metric is not raw match rate. It is whether",
        "the queue gives stewards enough ranked evidence to make safer manual",
        "decisions without creating false-positive merges.",
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
            "## Recommended Stewardship Workflow",
            "",
            "1. Start with `high_confidence_review` rows and verify the evidence",
            "   against authorized source-system context.",
            "2. Use `possible_review` rows to reduce search effort, not to approve",
            "   automatically.",
            "3. Record reviewer, decision, reason, timestamp, and source evidence",
            "   in the future stewardship workflow before any canonical update.",
            "4. Re-run downstream marts only after an approved identity decision is",
            "   applied through a governed write path.",
            "",
            "## Risk Controls",
            "",
            "- No SIN_LAST_4, full email, or DOB is exported by the residual engine.",
            "- Recommendations are outside the canonical-person write path.",
            "- Review suggestions are intentionally biased toward false negatives",
            "  over false positives.",
            "- Low-evidence candidates remain invisible rather than appearing as",
            "  weak suggestions.",
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


def render_proxy_evaluation_report(
    summary: ResidualProxyEvaluationSummary,
    *,
    top_n: int,
    minimum_score: float,
) -> str:
    lines = [
        "# Residual Proxy Evaluation",
        "",
        "This optional evaluation checks residual-review candidate ranking against",
        "the stewardship queue's `suggested_canonical_person_id` when that hint",
        "exists.",
        "",
        "This is **not** ground-truth accuracy evaluation. The hint is the best",
        "deterministic candidate that failed auto-merge controls, so it is useful",
        "for walkthrough diagnostics but must not be treated as an approved match.",
        "",
        "## Run Configuration",
        "",
        "| Setting | Value |",
        "|---|---:|",
        f"| Top candidates per source | {top_n:,} |",
        f"| Minimum residual score | {minimum_score:.2f} |",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Source rows with proxy hint | {summary.evaluated_source_count:,} |",
        f"| Proxy-hint rows with at least one candidate | {summary.source_records_with_candidates:,} |",
        f"| Candidate coverage rate | {_pct(summary.candidate_coverage_rate)} |",
        f"| Top-1 proxy alignment count | {summary.top_1_alignment_count:,} |",
        f"| Top-1 proxy alignment rate | {_pct(summary.top_1_alignment_rate)} |",
        f"| Top-{top_n} proxy alignment count | {summary.top_k_alignment_count:,} |",
        f"| Top-{top_n} proxy alignment rate | {_pct(summary.top_k_alignment_rate)} |",
        f"| Missing candidate count | {summary.missing_candidate_count:,} |",
        f"| Missing candidate rate | {_pct(summary.missing_candidate_rate)} |",
        f"| Mean proxy-label rank when found | {summary.mean_proxy_label_rank:.2f} |",
        "",
        "## Alignment By Recommendation",
        "",
        "| Recommendation | Candidate rows | Proxy alignment rate |",
        "|---|---:|---:|",
    ]
    for recommendation, count in summary.candidate_counts_by_recommendation.items():
        lines.append(
            "| "
            f"{recommendation} | "
            f"{count:,} | "
            f"{_pct(summary.proxy_alignment_by_recommendation.get(recommendation, 0.0))} |"
        )

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- High top-1 alignment means the residual scorer tends to put the",
            "  deterministic hint first when it emits a candidate.",
            "- Low coverage means the scorer is preserving the conservative control",
            "  boundary and leaving weak-evidence rows for manual search.",
            "- Alignment below 100% is not automatically bad: the deterministic hint",
            "  itself is not a steward-approved label.",
            "- This report should guide threshold tuning and reviewer workload",
            "  planning, not canonical identity updates.",
            "",
            "## Control Boundary",
            "",
            "Proxy evaluation is a diagnostic artifact. It does not approve matches,",
            "write `int_canonical_person`, or change downstream People Analytics",
            "marts.",
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
