from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from api.metrics_service import (
    MetricFilters,
    build_audit_insert,
    build_headcount_query,
    headcount_daily,
)
from api.settings import AtlasSettings


def test_public_table_rejects_injected_schema() -> None:
    settings = AtlasSettings(
        snowflake_account="acct",
        snowflake_user="user",
        snowflake_password="password",
        snowflake_role="ATLAS_DEVELOPER",
        snowflake_warehouse="ATLAS_WH",
        snowflake_database="ATLAS",
        snowflake_schema="RAW",
        snowflake_region=None,
        dbt_schema="DBT_DEV",
        people_analytics_schema="DBT_DEV_PEOPLE_ANALYTICS;DROP_TABLE",
        api_host="127.0.0.1",
        api_port=8000,
        k_anonymity_min=5,
    )

    with pytest.raises(ValueError, match="simple Snowflake identifier"):
        settings.public_table("workforce_headcount_daily")


def test_headcount_query_uses_privacy_mart_and_bound_filters() -> None:
    settings = _settings()
    filters = MetricFilters(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
        department="Engineering",
        location="Toronto",
        employment_type="FTE",
        limit=50,
    )

    statement = build_headcount_query(settings, filters)

    assert "ATLAS.DBT_DEV_PEOPLE_ANALYTICS.WORKFORCE_HEADCOUNT_DAILY" in statement.sql
    assert "canonical_person_id" not in statement.sql.lower()
    assert "sin_last_4" not in statement.sql.lower()
    assert statement.params == (
        date(2026, 1, 1),
        date(2026, 1, 31),
        "Engineering",
        "Toronto",
        "FTE",
    )
    assert "limit 50" in statement.sql.lower()


def test_audit_insert_targets_audit_log_with_json_filters() -> None:
    settings = _settings()
    statement = build_audit_insert(
        settings,
        actor="demo_hrbp",
        query_surface="workforce_headcount_daily",
        purpose="dashboard_view",
        filters={"department": "Engineering"},
        result_row_count=10,
        suppressed_row_count=2,
    )

    assert "ATLAS.DBT_DEV_PEOPLE_ANALYTICS.PRIVACY_AUDIT_LOG" in statement.sql
    assert "try_parse_json(%s)" in statement.sql
    assert statement.params[0:3] == (
        "demo_hrbp",
        "workforce_headcount_daily",
        "dashboard_view",
    )
    assert statement.params[3] == '{"department": "Engineering"}'
    assert statement.params[5:7] == (10, 2)


def test_headcount_endpoint_returns_data_and_writes_audit() -> None:
    fake_client = FakeMetricsClient(
        rows=[
            {
                "snapshot_date": "2026-01-01",
                "department": "Engineering",
                "location": "Toronto",
                "employment_type": "FTE",
                "headcount": 12,
                "reportable_cohort_employee_count": 12,
                "cohort_size_bucket": "12",
                "is_reportable": True,
                "suppression_reason": None,
                "k_anonymity_threshold": 5,
            },
            {
                "snapshot_date": "2026-01-01",
                "department": "People",
                "location": "Toronto",
                "employment_type": "FTE",
                "headcount": None,
                "reportable_cohort_employee_count": None,
                "cohort_size_bucket": "<5",
                "is_reportable": False,
                "suppression_reason": "K_ANONYMITY_THRESHOLD",
                "k_anonymity_threshold": 5,
            },
        ],
    )
    body = headcount_daily(
        client=fake_client,
        settings=_settings(),
        department="Engineering",
        limit=1000,
        purpose="dashboard_view",
        actor="demo_hrbp",
    )

    assert body["row_count"] == 2
    assert body["suppressed_row_count"] == 1
    assert body["audit_logged"] is True
    assert len(fake_client.executed) == 1
    assert fake_client.executed[0][1][0:3] == (
        "demo_hrbp",
        "workforce_headcount_daily",
        "dashboard_view",
    )


class FakeMetricsClient:
    def __init__(self, rows: list[dict[str, Any]]):
        self.rows = rows
        self.queries: list[tuple[str, tuple[Any, ...]]] = []
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        self.queries.append((sql, params))
        return self.rows

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        self.executed.append((sql, params))


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
