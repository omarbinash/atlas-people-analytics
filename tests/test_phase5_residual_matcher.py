from __future__ import annotations

from datetime import date

from api.settings import AtlasSettings
from identity_engine.evaluation import (
    evaluate_against_deterministic_hints,
    render_proxy_evaluation_report,
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
from identity_engine.snowflake_io import build_canonical_query, build_stewardship_query


def test_residual_candidate_can_be_ranked_for_review_without_auto_merge() -> None:
    source = SourceIdentity(
        source_system="CRM",
        source_record_key="CRM::123",
        source_first_name_root="robert",
        source_last_name_norm="smith",
        source_hire_date=date(2024, 1, 8),
        source_email_local_part="rsmith",
        suggested_canonical_person_id="cp_abc",
    )
    candidate = CanonicalIdentity(
        canonical_person_id="cp_abc",
        first_name_norm="robert",
        last_name_norm="smith",
        canonical_hire_date=date(2024, 1, 1),
        work_email_local_part="robert.smith",
        personal_email_local_part="rsmith",
    )

    scored = score_residual_candidate(source, candidate)

    assert scored.recommendation == "high_confidence_review"
    assert scored.residual_score >= 0.92
    assert scored.as_export_row()["candidate_canonical_person_id"] == "cp_abc"
    assert "recommendation=high_confidence_review" in scored.reasons


def test_sparse_or_conflicting_evidence_is_not_suggested() -> None:
    source = SourceIdentity(
        source_system="PAYROLL",
        source_record_key="PAYROLL::999",
        source_first_name_root="ana",
        source_last_name_norm="lopez",
    )
    candidate = CanonicalIdentity(
        canonical_person_id="cp_xyz",
        first_name_norm="charles",
        last_name_norm="nguyen",
    )

    scored = score_residual_candidate(source, candidate)

    assert scored.recommendation == "do_not_suggest"
    assert scored.residual_score < 0.75


def test_rank_residual_candidates_keeps_top_reviewable_candidates() -> None:
    source = SourceIdentity(
        source_system="DMS_ERP",
        source_record_key="DMS_ERP::1",
        source_first_name_root="patrick",
        source_last_name_norm="obrien",
        source_hire_date=date(2025, 6, 1),
        source_email_local_part="pobrien",
    )
    candidates = [
        CanonicalIdentity(
            canonical_person_id="cp_good",
            first_name_norm="patrick",
            last_name_norm="obrien",
            canonical_hire_date=date(2025, 6, 5),
            work_email_local_part="pobrien",
        ),
        CanonicalIdentity(
            canonical_person_id="cp_bad",
            first_name_norm="maria",
            last_name_norm="obrien",
            canonical_hire_date=date(2021, 1, 1),
            work_email_local_part="mobrien",
        ),
    ]

    ranked = rank_residual_candidates([source], candidates, top_n=1)

    assert len(ranked) == 1
    assert ranked[0].canonical_person_id == "cp_good"
    assert ranked[0].recommendation in {"high_confidence_review", "possible_review"}


def test_phase5_queries_do_not_select_sensitive_identifiers() -> None:
    settings = _settings()
    sql = (
        f"{build_stewardship_query(settings, limit=25)}\n{build_canonical_query(settings)}".lower()
    )

    assert "sin_last_4" not in sql
    assert "date_of_birth" not in sql
    assert "personal_email," not in sql
    assert "work_email," not in sql


def test_residual_report_summarizes_review_coverage() -> None:
    candidates = [
        ResidualCandidate(
            source_record_key="ATS::1",
            source_system="ATS",
            canonical_person_id="cp_1",
            residual_score=0.96,
            recommendation="high_confidence_review",
            positive_anchor_count=4,
            evidence_weight=1.0,
            feature_scores={
                "first_name_root": 1.0,
                "last_name": 1.0,
                "email_local": 0.9,
                "hire_date": 1.0,
                "deterministic_hint": 1.0,
            },
            reasons=("recommendation=high_confidence_review", "first_name_root_exact"),
        ),
        ResidualCandidate(
            source_record_key="CRM::2",
            source_system="CRM",
            canonical_person_id="cp_2",
            residual_score=0.78,
            recommendation="possible_review",
            positive_anchor_count=2,
            evidence_weight=0.7,
            feature_scores={
                "first_name_root": 1.0,
                "last_name": 0.8,
                "email_local": None,
                "hire_date": 0.9,
                "deterministic_hint": None,
            },
            reasons=("recommendation=possible_review", "hire_date_within_30_days"),
        ),
    ]

    summary = summarize_residual_candidates(candidates, source_record_count=5)
    report = render_residual_report(summary, candidates, top_n=1)

    assert summary.candidate_count == 2
    assert summary.source_records_with_candidates == 2
    assert summary.review_yield_rate == 0.4
    assert summary.recommendation_counts["high_confidence_review"] == 1
    assert "## Recommendation Mix" in report
    assert "ATS::1" in report
    assert "CRM::2" not in report


def test_proxy_evaluation_uses_stewardship_hints_without_claiming_ground_truth() -> None:
    source_rows = [
        SourceIdentity(
            source_system="ATS",
            source_record_key="ATS::1",
            suggested_canonical_person_id="cp_1",
        ),
        SourceIdentity(
            source_system="CRM",
            source_record_key="CRM::2",
            suggested_canonical_person_id="cp_2",
        ),
        SourceIdentity(
            source_system="PAYROLL",
            source_record_key="PAYROLL::3",
        ),
    ]
    candidates = [
        ResidualCandidate(
            source_record_key="ATS::1",
            source_system="ATS",
            canonical_person_id="cp_1",
            residual_score=0.96,
            recommendation="high_confidence_review",
            positive_anchor_count=4,
            evidence_weight=1.0,
        ),
        ResidualCandidate(
            source_record_key="CRM::2",
            source_system="CRM",
            canonical_person_id="cp_wrong",
            residual_score=0.90,
            recommendation="possible_review",
            positive_anchor_count=3,
            evidence_weight=0.8,
        ),
        ResidualCandidate(
            source_record_key="CRM::2",
            source_system="CRM",
            canonical_person_id="cp_2",
            residual_score=0.86,
            recommendation="possible_review",
            positive_anchor_count=3,
            evidence_weight=0.8,
        ),
    ]

    summary = evaluate_against_deterministic_hints(source_rows, candidates)
    report = render_proxy_evaluation_report(summary, top_n=2, minimum_score=0.75)

    assert summary.evaluated_source_count == 2
    assert summary.source_records_with_candidates == 2
    assert summary.top_1_alignment_count == 1
    assert summary.top_k_alignment_count == 2
    assert summary.top_1_alignment_rate == 0.5
    assert summary.top_k_alignment_rate == 1.0
    assert summary.mean_proxy_label_rank == 1.5
    assert summary.candidate_counts_by_recommendation["possible_review"] == 2
    assert summary.proxy_alignment_by_recommendation["possible_review"] == 0.5
    assert "ground-truth model evaluation" in report
    assert "does not approve matches" in report


def _settings() -> AtlasSettings:
    return AtlasSettings(
        snowflake_account="acct",
        snowflake_user="user",
        snowflake_password="password",
        snowflake_role="ATLAS_DEVELOPER",
        snowflake_warehouse="ATLAS_WH",
        snowflake_database="ATLAS",
        snowflake_schema="RAW",
        snowflake_region=None,
        dbt_schema="DBT_DEV",
        people_analytics_schema="DBT_DEV_PEOPLE_ANALYTICS",
        api_host="127.0.0.1",
        api_port=8000,
        k_anonymity_min=5,
    )
