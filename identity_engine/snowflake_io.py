"""Snowflake IO for Phase 5 residual-review candidate exports."""

from __future__ import annotations

from typing import Any

from api.settings import AtlasSettings, normalize_snowflake_identifier
from api.snowflake_client import AtlasSnowflakeClient
from identity_engine.residual_matcher import CanonicalIdentity, SourceIdentity


def intermediate_schema(settings: AtlasSettings) -> str:
    schema = f"{settings.dbt_schema}_INTERMEDIATE"
    return normalize_snowflake_identifier(schema, "intermediate_schema")


def intermediate_table(settings: AtlasSettings, table_name: str) -> str:
    return (
        f"{settings.database_identifier}."
        f"{intermediate_schema(settings)}."
        f"{normalize_snowflake_identifier(table_name, 'table_name')}"
    )


def build_stewardship_query(settings: AtlasSettings, *, limit: int) -> str:
    table = intermediate_table(settings, "int_stewardship_queue")
    return f"""
select
    source_system,
    source_record_key,
    source_primary_id,
    source_first_name_root,
    source_last_name_norm,
    source_hire_date,
    source_email_local_part,
    source_email_domain,
    suggested_canonical_person_id,
    stewardship_reason
from {table}
order by source_system, source_record_key
limit {max(1, limit)}
""".strip()


def build_canonical_query(settings: AtlasSettings) -> str:
    table = intermediate_table(settings, "int_canonical_person")
    return f"""
select
    canonical_person_id,
    hris_person_key,
    canonical_legal_first_name,
    canonical_legal_last_name,
    canonical_hire_date,
    work_email_local_part,
    personal_email_local_part,
    current_department,
    current_location,
    current_employment_type
from {table}
""".strip()


def load_residual_inputs(
    settings: AtlasSettings,
    *,
    limit: int,
) -> tuple[list[SourceIdentity], list[CanonicalIdentity]]:
    client = AtlasSnowflakeClient(settings)
    stewardship_rows = client.fetch_all(build_stewardship_query(settings, limit=limit))
    canonical_rows = client.fetch_all(build_canonical_query(settings))
    return (
        [SourceIdentity.from_mapping(row) for row in stewardship_rows],
        [CanonicalIdentity.from_mapping(row) for row in canonical_rows],
    )


def export_rows_to_csv(rows: list[dict[str, Any]], output_path: str) -> None:
    import csv

    fieldnames = [
        "source_record_key",
        "source_system",
        "candidate_canonical_person_id",
        "residual_score",
        "recommendation",
        "positive_anchor_count",
        "evidence_weight",
        "reasons",
        "first_name_root_score",
        "last_name_score",
        "email_local_score",
        "hire_date_score",
        "deterministic_hint_score",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
