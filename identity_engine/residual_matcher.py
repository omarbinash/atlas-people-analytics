"""Explainable residual matching for stewardship review.

Phase 5 deliberately keeps this engine out of the automatic canonical-person
path. It ranks possible candidates for HR/data-steward review after the
deterministic dbt matcher has already decided a source row is not safe to merge.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from rapidfuzz import fuzz

HIGH_CONFIDENCE_THRESHOLD = 0.92
POSSIBLE_MATCH_THRESHOLD = 0.75


@dataclass(frozen=True)
class SourceIdentity:
    source_system: str
    source_record_key: str
    source_primary_id: str | None = None
    source_first_name_root: str | None = None
    source_last_name_norm: str | None = None
    source_hire_date: date | None = None
    source_email_local_part: str | None = None
    source_email_domain: str | None = None
    suggested_canonical_person_id: str | None = None
    stewardship_reason: str | None = None

    @classmethod
    def from_mapping(cls, row: dict[str, Any]) -> SourceIdentity:
        return cls(
            source_system=str(row.get("source_system") or ""),
            source_record_key=str(row.get("source_record_key") or ""),
            source_primary_id=_optional_str(row.get("source_primary_id")),
            source_first_name_root=_optional_str(row.get("source_first_name_root")),
            source_last_name_norm=_optional_str(row.get("source_last_name_norm")),
            source_hire_date=_parse_date(row.get("source_hire_date")),
            source_email_local_part=_optional_str(row.get("source_email_local_part")),
            source_email_domain=_optional_str(row.get("source_email_domain")),
            suggested_canonical_person_id=_optional_str(row.get("suggested_canonical_person_id")),
            stewardship_reason=_optional_str(row.get("stewardship_reason")),
        )


@dataclass(frozen=True)
class CanonicalIdentity:
    canonical_person_id: str
    hris_person_key: str | None = None
    first_name_norm: str | None = None
    last_name_norm: str | None = None
    canonical_hire_date: date | None = None
    work_email_local_part: str | None = None
    personal_email_local_part: str | None = None
    current_department: str | None = None
    current_location: str | None = None
    current_employment_type: str | None = None

    @classmethod
    def from_mapping(cls, row: dict[str, Any]) -> CanonicalIdentity:
        return cls(
            canonical_person_id=str(row.get("canonical_person_id") or ""),
            hris_person_key=_optional_str(row.get("hris_person_key")),
            first_name_norm=_optional_str(
                row.get("first_name_norm") or row.get("canonical_legal_first_name")
            ),
            last_name_norm=_optional_str(
                row.get("last_name_norm") or row.get("canonical_legal_last_name")
            ),
            canonical_hire_date=_parse_date(row.get("canonical_hire_date")),
            work_email_local_part=_optional_str(row.get("work_email_local_part")),
            personal_email_local_part=_optional_str(row.get("personal_email_local_part")),
            current_department=_optional_str(row.get("current_department")),
            current_location=_optional_str(row.get("current_location")),
            current_employment_type=_optional_str(row.get("current_employment_type")),
        )


@dataclass(frozen=True)
class ResidualCandidate:
    source_record_key: str
    source_system: str
    canonical_person_id: str
    residual_score: float
    recommendation: str
    positive_anchor_count: int
    evidence_weight: float
    feature_scores: dict[str, float | None] = field(default_factory=dict)
    reasons: tuple[str, ...] = ()

    def as_export_row(self) -> dict[str, Any]:
        return {
            "source_record_key": self.source_record_key,
            "source_system": self.source_system,
            "candidate_canonical_person_id": self.canonical_person_id,
            "residual_score": round(self.residual_score, 4),
            "recommendation": self.recommendation,
            "positive_anchor_count": self.positive_anchor_count,
            "evidence_weight": round(self.evidence_weight, 4),
            "reasons": "; ".join(self.reasons),
            "first_name_root_score": self.feature_scores.get("first_name_root"),
            "last_name_score": self.feature_scores.get("last_name"),
            "email_local_score": self.feature_scores.get("email_local"),
            "hire_date_score": self.feature_scores.get("hire_date"),
            "deterministic_hint_score": self.feature_scores.get("deterministic_hint"),
        }


def score_residual_candidate(
    source: SourceIdentity,
    candidate: CanonicalIdentity,
) -> ResidualCandidate:
    feature_scores = {
        "first_name_root": _exact_score(source.source_first_name_root, candidate.first_name_norm),
        "last_name": _similarity_score(source.source_last_name_norm, candidate.last_name_norm),
        "email_local": _best_email_score(source, candidate),
        "hire_date": _hire_date_score(source.source_hire_date, candidate.canonical_hire_date),
        "deterministic_hint": _deterministic_hint_score(source, candidate),
    }
    weights = {
        "first_name_root": 0.20,
        "last_name": 0.30,
        "email_local": 0.20,
        "hire_date": 0.20,
        "deterministic_hint": 0.10,
    }

    observed_weight = sum(
        weight for key, weight in weights.items() if feature_scores[key] is not None
    )
    weighted_score = sum(
        (feature_scores[key] or 0.0) * weight
        for key, weight in weights.items()
        if feature_scores[key] is not None
    )
    residual_score = weighted_score / observed_weight if observed_weight else 0.0
    positive_anchor_count = sum(
        1 for score in feature_scores.values() if score is not None and score >= 0.85
    )
    evidence_weight = observed_weight / sum(weights.values())
    recommendation = _recommendation(residual_score, evidence_weight, positive_anchor_count)

    return ResidualCandidate(
        source_record_key=source.source_record_key,
        source_system=source.source_system,
        canonical_person_id=candidate.canonical_person_id,
        residual_score=residual_score,
        recommendation=recommendation,
        positive_anchor_count=positive_anchor_count,
        evidence_weight=evidence_weight,
        feature_scores=feature_scores,
        reasons=_reasons(feature_scores, recommendation),
    )


def rank_residual_candidates(
    source_rows: list[SourceIdentity],
    canonical_rows: list[CanonicalIdentity],
    *,
    top_n: int = 3,
    minimum_score: float = POSSIBLE_MATCH_THRESHOLD,
) -> list[ResidualCandidate]:
    canonical_by_id = {row.canonical_person_id: row for row in canonical_rows}
    canonical_by_last_name: dict[str, list[CanonicalIdentity]] = {}
    for row in canonical_rows:
        if row.last_name_norm:
            canonical_by_last_name.setdefault(row.last_name_norm, []).append(row)

    ranked: list[ResidualCandidate] = []
    for source in source_rows:
        pool = _candidate_pool(source, canonical_rows, canonical_by_id, canonical_by_last_name)
        source_candidates = [
            score_residual_candidate(source, candidate)
            for candidate in pool
            if candidate.canonical_person_id
        ]
        source_candidates = [
            candidate
            for candidate in source_candidates
            if candidate.residual_score >= minimum_score
            and candidate.recommendation != "do_not_suggest"
        ]
        ranked.extend(
            sorted(
                source_candidates,
                key=lambda candidate: (
                    candidate.residual_score,
                    candidate.positive_anchor_count,
                    candidate.evidence_weight,
                    candidate.canonical_person_id,
                ),
                reverse=True,
            )[:top_n]
        )
    return ranked


def _candidate_pool(
    source: SourceIdentity,
    canonical_rows: list[CanonicalIdentity],
    canonical_by_id: dict[str, CanonicalIdentity],
    canonical_by_last_name: dict[str, list[CanonicalIdentity]],
) -> list[CanonicalIdentity]:
    pool: dict[str, CanonicalIdentity] = {}
    if (
        source.suggested_canonical_person_id
        and source.suggested_canonical_person_id in canonical_by_id
    ):
        candidate = canonical_by_id[source.suggested_canonical_person_id]
        pool[candidate.canonical_person_id] = candidate

    if source.source_last_name_norm:
        for candidate in canonical_by_last_name.get(source.source_last_name_norm, []):
            pool[candidate.canonical_person_id] = candidate

    if not pool:
        for candidate in canonical_rows:
            pool[candidate.canonical_person_id] = candidate

    return list(pool.values())


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def _exact_score(left: str | None, right: str | None) -> float | None:
    if not left or not right:
        return None
    return 1.0 if left == right else 0.0


def _similarity_score(left: str | None, right: str | None) -> float | None:
    if not left or not right:
        return None
    return fuzz.ratio(left, right) / 100


def _best_email_score(source: SourceIdentity, candidate: CanonicalIdentity) -> float | None:
    if not source.source_email_local_part:
        return None
    candidate_locals = [
        value
        for value in [candidate.work_email_local_part, candidate.personal_email_local_part]
        if value
    ]
    if not candidate_locals:
        return None
    return max(
        fuzz.ratio(source.source_email_local_part, local_part) / 100
        for local_part in candidate_locals
    )


def _hire_date_score(source_date: date | None, candidate_date: date | None) -> float | None:
    if not source_date or not candidate_date:
        return None
    diff_days = abs((source_date - candidate_date).days)
    if diff_days <= 7:
        return 1.0
    if diff_days <= 30:
        return 0.90
    if diff_days <= 90:
        return 0.60
    if diff_days <= 365:
        return 0.25
    return 0.0


def _deterministic_hint_score(
    source: SourceIdentity,
    candidate: CanonicalIdentity,
) -> float | None:
    if not source.suggested_canonical_person_id:
        return None
    return 1.0 if source.suggested_canonical_person_id == candidate.canonical_person_id else 0.0


def _recommendation(
    residual_score: float,
    evidence_weight: float,
    positive_anchor_count: int,
) -> str:
    if (
        residual_score >= HIGH_CONFIDENCE_THRESHOLD
        and evidence_weight >= 0.70
        and positive_anchor_count >= 3
    ):
        return "high_confidence_review"
    if (
        residual_score >= POSSIBLE_MATCH_THRESHOLD
        and evidence_weight >= 0.50
        and positive_anchor_count >= 2
    ):
        return "possible_review"
    return "do_not_suggest"


def _reasons(
    feature_scores: dict[str, float | None],
    recommendation: str,
) -> tuple[str, ...]:
    reasons: list[str] = [f"recommendation={recommendation}"]
    if feature_scores["last_name"] is not None:
        reasons.append(f"last_name_similarity={feature_scores['last_name']:.2f}")
    if feature_scores["first_name_root"] == 1.0:
        reasons.append("first_name_root_exact")
    if feature_scores["email_local"] is not None and feature_scores["email_local"] >= 0.85:
        reasons.append(f"email_local_similarity={feature_scores['email_local']:.2f}")
    if feature_scores["hire_date"] is not None and feature_scores["hire_date"] >= 0.90:
        reasons.append("hire_date_within_30_days")
    if feature_scores["deterministic_hint"] == 1.0:
        reasons.append("deterministic_candidate_hint")
    return tuple(reasons)
