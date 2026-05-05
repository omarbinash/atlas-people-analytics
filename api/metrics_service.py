"""Privacy-aware FastAPI metrics service for Atlas."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import date
from typing import Annotated, Any, Protocol

from fastapi import Depends, FastAPI, Header, HTTPException, Query

from api.settings import AtlasSettings
from api.snowflake_client import AtlasSnowflakeClient, QueryParams

LOGGER = logging.getLogger(__name__)

MAX_LIMIT = 5_000
PUBLIC_SURFACES = {
    "workforce_headcount_daily",
    "workforce_attrition_monthly",
    "privacy_suppression_summary",
}


class MetricsClient(Protocol):
    def fetch_all(self, sql: str, params: QueryParams = ()) -> list[dict[str, Any]]: ...

    def execute(self, sql: str, params: QueryParams = ()) -> None: ...


@dataclass(frozen=True)
class MetricFilters:
    start_date: date | None = None
    end_date: date | None = None
    department: str | None = None
    location: str | None = None
    employment_type: str | None = None
    privacy_surface: str | None = None
    limit: int = 1_000

    def audit_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        return {
            key: value.isoformat() if isinstance(value, date) else value
            for key, value in payload.items()
            if value is not None
        }


@dataclass(frozen=True)
class SqlStatement:
    sql: str
    params: QueryParams = ()


app = FastAPI(
    title="Atlas People Analytics Metrics API",
    version="0.4.0",
    description=("Privacy-aware metric service over synthetic Atlas People Analytics marts."),
)


def get_settings() -> AtlasSettings:
    return AtlasSettings.from_env()


SettingsDep = Annotated[AtlasSettings, Depends(get_settings)]


def get_client(settings: SettingsDep) -> MetricsClient:
    return AtlasSnowflakeClient(settings)


ClientDep = Annotated[MetricsClient, Depends(get_client)]


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "atlas-metrics-api",
        "docs": "/docs",
        "health": "/health",
        "metadata": "/metadata",
        "metrics": {
            "daily_headcount": "/headcount/daily",
            "monthly_attrition": "/attrition/monthly",
            "suppression_summary": "/privacy/suppression-summary",
        },
        "privacy_note": "Metric endpoints read only from k-anonymous People Analytics marts.",
    }


def _where_clause(predicates: list[str]) -> str:
    if not predicates:
        return ""
    return "where " + "\n  and ".join(predicates)


def _bounded_limit(limit: int) -> int:
    return max(1, min(limit, MAX_LIMIT))


def _add_dimension_filters(
    predicates: list[str],
    params: list[Any],
    filters: MetricFilters,
) -> None:
    if filters.department:
        predicates.append("department = %s")
        params.append(filters.department)
    if filters.location:
        predicates.append("location = %s")
        params.append(filters.location)
    if filters.employment_type:
        predicates.append("employment_type = %s")
        params.append(filters.employment_type)


def build_headcount_query(settings: AtlasSettings, filters: MetricFilters) -> SqlStatement:
    table = settings.public_table("workforce_headcount_daily")
    predicates: list[str] = []
    params: list[Any] = []

    if filters.start_date:
        predicates.append("snapshot_date >= %s")
        params.append(filters.start_date)
    if filters.end_date:
        predicates.append("snapshot_date <= %s")
        params.append(filters.end_date)
    _add_dimension_filters(predicates, params, filters)

    sql = f"""
select
    snapshot_date,
    department,
    location,
    employment_type,
    headcount,
    reportable_cohort_employee_count,
    cohort_size_bucket,
    is_reportable,
    suppression_reason,
    k_anonymity_threshold
from {table}
{_where_clause(predicates)}
order by snapshot_date, department, location, employment_type
limit {_bounded_limit(filters.limit)}
""".strip()
    return SqlStatement(sql=sql, params=tuple(params))


def build_attrition_query(settings: AtlasSettings, filters: MetricFilters) -> SqlStatement:
    table = settings.public_table("workforce_attrition_monthly")
    predicates: list[str] = []
    params: list[Any] = []

    if filters.start_date:
        predicates.append("month_start_date >= %s")
        params.append(filters.start_date)
    if filters.end_date:
        predicates.append("month_start_date <= %s")
        params.append(filters.end_date)
    _add_dimension_filters(predicates, params, filters)

    sql = f"""
select
    month_start_date,
    month_end_date,
    department,
    location,
    employment_type,
    start_headcount,
    terminations,
    attrition_rate,
    cohort_size_bucket,
    is_reportable,
    suppression_reason,
    k_anonymity_threshold
from {table}
{_where_clause(predicates)}
order by month_start_date, department, location, employment_type
limit {_bounded_limit(filters.limit)}
""".strip()
    return SqlStatement(sql=sql, params=tuple(params))


def build_suppression_summary_query(
    settings: AtlasSettings,
    filters: MetricFilters,
) -> SqlStatement:
    table = settings.public_table("privacy_suppression_summary")
    predicates: list[str] = []
    params: list[Any] = []
    if filters.privacy_surface:
        predicates.append("privacy_surface = %s")
        params.append(filters.privacy_surface)

    sql = f"""
select
    privacy_surface,
    date_grain,
    row_count,
    reportable_row_count,
    suppressed_row_count,
    suppressed_row_rate,
    k_anonymity_threshold,
    generated_at
from {table}
{_where_clause(predicates)}
order by privacy_surface
limit {_bounded_limit(filters.limit)}
""".strip()
    return SqlStatement(sql=sql, params=tuple(params))


def build_metadata_query(settings: AtlasSettings) -> SqlStatement:
    table = settings.public_table("workforce_headcount_daily")
    sql = f"""
select
    min(snapshot_date) as min_snapshot_date,
    max(snapshot_date) as max_snapshot_date,
    count(*) as headcount_row_count,
    count_if(not is_reportable) as suppressed_headcount_row_count,
    listagg(distinct department, '||') within group (order by department) as departments,
    listagg(distinct location, '||') within group (order by location) as locations,
    listagg(distinct employment_type, '||') within group (order by employment_type) as employment_types,
    min(k_anonymity_threshold) as k_anonymity_threshold
from {table}
""".strip()
    return SqlStatement(sql=sql)


def build_audit_insert(
    settings: AtlasSettings,
    *,
    actor: str,
    query_surface: str,
    purpose: str,
    filters: dict[str, Any],
    result_row_count: int,
    suppressed_row_count: int,
) -> SqlStatement:
    table = settings.public_table("privacy_audit_log")
    sql = f"""
insert into {table} (
    audit_event_id,
    audited_at,
    actor,
    query_surface,
    purpose,
    filters_json,
    k_anonymity_threshold,
    result_row_count,
    suppressed_row_count,
    privacy_policy_version,
    dbt_invocation_id
)
select
    uuid_string(),
    current_timestamp(),
    %s,
    %s,
    %s,
    try_parse_json(%s),
    %s,
    %s,
    %s,
    %s,
    null
""".strip()
    params = (
        actor,
        query_surface,
        purpose,
        json.dumps(filters, sort_keys=True),
        settings.k_anonymity_min,
        result_row_count,
        suppressed_row_count,
        settings.privacy_policy_version,
    )
    return SqlStatement(sql=sql, params=params)


def _suppressed_row_count(rows: list[dict[str, Any]]) -> int:
    return sum(1 for row in rows if row.get("is_reportable") is False)


def _split_dimension_list(value: Any) -> list[str]:
    if not value:
        return []
    return [item for item in str(value).split("||") if item]


def _query_rows(client: MetricsClient, statement: SqlStatement) -> list[dict[str, Any]]:
    try:
        return client.fetch_all(statement.sql, statement.params)
    except Exception as exc:  # pragma: no cover - exercised by live service failures
        LOGGER.exception("Metrics warehouse query failed")
        raise HTTPException(status_code=503, detail="Metrics warehouse query failed") from exc


def _audit_access(
    client: MetricsClient,
    settings: AtlasSettings,
    *,
    actor: str,
    query_surface: str,
    purpose: str,
    filters: MetricFilters,
    rows: list[dict[str, Any]],
) -> bool:
    statement = build_audit_insert(
        settings,
        actor=actor,
        query_surface=query_surface,
        purpose=purpose,
        filters=filters.audit_payload(),
        result_row_count=len(rows),
        suppressed_row_count=_suppressed_row_count(rows),
    )
    try:
        client.execute(statement.sql, statement.params)
    except Exception:  # pragma: no cover - audit failure should not break reads
        LOGGER.exception("Privacy audit insert failed")
        return False
    return True


def _metric_response(
    client: MetricsClient,
    settings: AtlasSettings,
    statement: SqlStatement,
    *,
    query_surface: str,
    purpose: str,
    filters: MetricFilters,
    actor: str | None,
) -> dict[str, Any]:
    rows = _query_rows(client, statement)
    audit_logged = _audit_access(
        client,
        settings,
        actor=actor or "anonymous",
        query_surface=query_surface,
        purpose=purpose,
        filters=filters,
        rows=rows,
    )
    return {
        "data": rows,
        "row_count": len(rows),
        "suppressed_row_count": _suppressed_row_count(rows),
        "audit_logged": audit_logged,
    }


@app.get("/health")
def health(settings: SettingsDep) -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "atlas-metrics-api",
        "warehouse_configured": not settings.missing_connection_values(),
        "database": settings.database_identifier,
        "people_analytics_schema": settings.people_analytics_schema_identifier,
        "public_surfaces": sorted(PUBLIC_SURFACES),
    }


@app.get("/metadata")
def metadata(
    client: ClientDep,
    settings: SettingsDep,
) -> dict[str, Any]:
    rows = _query_rows(client, build_metadata_query(settings))
    row = rows[0] if rows else {}
    return {
        "min_snapshot_date": row.get("min_snapshot_date"),
        "max_snapshot_date": row.get("max_snapshot_date"),
        "headcount_row_count": row.get("headcount_row_count", 0),
        "suppressed_headcount_row_count": row.get("suppressed_headcount_row_count", 0),
        "departments": _split_dimension_list(row.get("departments")),
        "locations": _split_dimension_list(row.get("locations")),
        "employment_types": _split_dimension_list(row.get("employment_types")),
        "k_anonymity_threshold": row.get("k_anonymity_threshold", settings.k_anonymity_min),
        "public_surfaces": sorted(PUBLIC_SURFACES),
    }


@app.get("/headcount/daily")
def headcount_daily(
    client: ClientDep,
    settings: SettingsDep,
    start_date: date | None = None,
    end_date: date | None = None,
    department: str | None = None,
    location: str | None = None,
    employment_type: str | None = None,
    limit: int = Query(default=1_000, ge=1, le=MAX_LIMIT),
    purpose: str = Query(default="dashboard_view", min_length=1, max_length=255),
    actor: str | None = Header(default=None, alias="X-Atlas-Actor"),
) -> dict[str, Any]:
    filters = MetricFilters(
        start_date=start_date,
        end_date=end_date,
        department=department,
        location=location,
        employment_type=employment_type,
        limit=limit,
    )
    return _metric_response(
        client,
        settings,
        build_headcount_query(settings, filters),
        query_surface="workforce_headcount_daily",
        purpose=purpose,
        filters=filters,
        actor=actor,
    )


@app.get("/attrition/monthly")
def attrition_monthly(
    client: ClientDep,
    settings: SettingsDep,
    start_date: date | None = None,
    end_date: date | None = None,
    department: str | None = None,
    location: str | None = None,
    employment_type: str | None = None,
    limit: int = Query(default=1_000, ge=1, le=MAX_LIMIT),
    purpose: str = Query(default="dashboard_view", min_length=1, max_length=255),
    actor: str | None = Header(default=None, alias="X-Atlas-Actor"),
) -> dict[str, Any]:
    filters = MetricFilters(
        start_date=start_date,
        end_date=end_date,
        department=department,
        location=location,
        employment_type=employment_type,
        limit=limit,
    )
    return _metric_response(
        client,
        settings,
        build_attrition_query(settings, filters),
        query_surface="workforce_attrition_monthly",
        purpose=purpose,
        filters=filters,
        actor=actor,
    )


@app.get("/privacy/suppression-summary")
def privacy_suppression_summary(
    client: ClientDep,
    settings: SettingsDep,
    privacy_surface: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=MAX_LIMIT),
    purpose: str = Query(default="dashboard_view", min_length=1, max_length=255),
    actor: str | None = Header(default=None, alias="X-Atlas-Actor"),
) -> dict[str, Any]:
    if privacy_surface and privacy_surface not in PUBLIC_SURFACES:
        raise HTTPException(status_code=422, detail="Unsupported privacy_surface")
    filters = MetricFilters(privacy_surface=privacy_surface, limit=limit)
    return _metric_response(
        client,
        settings,
        build_suppression_summary_query(settings, filters),
        query_surface="privacy_suppression_summary",
        purpose=purpose,
        filters=filters,
        actor=actor,
    )
