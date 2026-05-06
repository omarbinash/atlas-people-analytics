"""Residual identity-review tools for Atlas."""

from identity_engine.evaluation import (
    ResidualEvaluationSummary,
    render_residual_report,
    summarize_residual_candidates,
)
from identity_engine.residual_matcher import (
    CanonicalIdentity,
    ResidualCandidate,
    SourceIdentity,
    rank_residual_candidates,
    score_residual_candidate,
)

__all__ = [
    "CanonicalIdentity",
    "ResidualCandidate",
    "ResidualEvaluationSummary",
    "SourceIdentity",
    "rank_residual_candidates",
    "render_residual_report",
    "score_residual_candidate",
    "summarize_residual_candidates",
]
