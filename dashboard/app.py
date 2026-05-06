"""Streamlit HRBP dashboard for the Atlas privacy-safe metrics API."""

from __future__ import annotations

import json
import os
from datetime import date, timedelta
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd
import streamlit as st

DEFAULT_API_URL = "http://127.0.0.1:8000"


def api_base_url() -> str:
    return os.getenv("ATLAS_API_URL", DEFAULT_API_URL).rstrip("/")


def fetch_json(
    path: str, params: dict[str, Any] | None = None, actor: str = "demo_hrbp"
) -> dict[str, Any]:
    query = urlencode(
        {key: value for key, value in (params or {}).items() if value not in (None, "")}
    )
    url = f"{api_base_url()}{path}"
    if query:
        url = f"{url}?{query}"
    request = Request(url, headers={"X-Atlas-Actor": actor})
    with urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        return {}
    return cast(dict[str, Any], payload)


def records_frame(payload: dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(payload.get("data", []))


def option_filter(label: str, options: list[str]) -> str | None:
    choice = st.sidebar.selectbox(label, ["All", *options])
    return None if choice == "All" else choice


def apply_page_style() -> None:
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 1.6rem;
            padding-bottom: 2rem;
            max-width: 1320px;
        }
        [data-testid="stMetric"] {
            border: 1px solid #d9e2e7;
            border-radius: 8px;
            padding: 0.75rem 0.9rem;
            background: #fbfcfd;
        }
        [data-testid="stMetricLabel"] p {
            font-size: 0.82rem;
            color: #45545f;
        }
        h1, h2, h3 {
            letter-spacing: 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def coerce_date(value: Any, fallback: date) -> date:
    if not value:
        return fallback
    return date.fromisoformat(str(value))


def build_filters(metadata: dict[str, Any]) -> dict[str, Any]:
    today = date.today()
    max_snapshot = coerce_date(metadata.get("max_snapshot_date"), today)
    min_snapshot = coerce_date(
        metadata.get("min_snapshot_date"), max_snapshot - timedelta(days=365)
    )
    default_start = max(min_snapshot, max_snapshot - timedelta(days=180))

    st.sidebar.header("Filters")
    date_range = st.sidebar.date_input(
        "Date range",
        value=(default_start, max_snapshot),
        min_value=min_snapshot,
        max_value=max_snapshot,
    )
    if isinstance(date_range, tuple):
        start_date = date_range[0] if len(date_range) >= 1 else default_start
        end_date = date_range[1] if len(date_range) >= 2 else max_snapshot
    else:
        start_date = date_range
        end_date = max_snapshot
    actor = st.sidebar.text_input("Actor", value="demo_hrbp", max_chars=80)
    department = option_filter("Department", metadata.get("departments", []))
    location = option_filter("Location", metadata.get("locations", []))
    employment_type = option_filter("Employment type", metadata.get("employment_types", []))
    st.sidebar.divider()
    st.sidebar.metric("Data through", max_snapshot.isoformat())
    st.sidebar.metric("Public headcount rows", f"{metadata.get('headcount_row_count', 0):,}")

    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "department": department,
        "location": location,
        "employment_type": employment_type,
        "actor": actor or "demo_hrbp",
    }


def current_headcount(df: pd.DataFrame) -> int | None:
    if df.empty or "headcount" not in df:
        return None
    reportable = df.dropna(subset=["headcount"])
    if reportable.empty:
        return None
    latest_date = reportable["snapshot_date"].max()
    return int(reportable.loc[reportable["snapshot_date"] == latest_date, "headcount"].sum())


def latest_attrition_rate(df: pd.DataFrame) -> float | None:
    if df.empty or "attrition_rate" not in df:
        return None
    reportable = df.dropna(subset=["attrition_rate"])
    if reportable.empty:
        return None
    latest_month = reportable["month_start_date"].max()
    latest = reportable.loc[reportable["month_start_date"] == latest_month, "attrition_rate"]
    return float(latest.mean())


def suppression_rate(payload: dict[str, Any]) -> float:
    row_count = payload.get("row_count") or 0
    suppressed = payload.get("suppressed_row_count") or 0
    if row_count == 0:
        return 0.0
    return suppressed / row_count


def render_overview(
    metadata: dict[str, Any],
    headcount_payload: dict[str, Any],
    attrition_payload: dict[str, Any],
    suppression: pd.DataFrame,
) -> None:
    cols = st.columns(4)
    cols[0].metric(
        "Snapshot window",
        f"{metadata.get('min_snapshot_date')} to {metadata.get('max_snapshot_date')}",
    )
    cols[1].metric("Departments", len(metadata.get("departments", [])))
    cols[2].metric("Locations", len(metadata.get("locations", [])))
    cols[3].metric("Employment types", len(metadata.get("employment_types", [])))

    audit_cols = st.columns(3)
    audit_cols[0].metric("Headcount query rows", f"{headcount_payload.get('row_count', 0):,}")
    audit_cols[1].metric("Attrition query rows", f"{attrition_payload.get('row_count', 0):,}")
    audit_cols[2].metric("Headcount suppression", f"{suppression_rate(headcount_payload):.1%}")

    if not suppression.empty:
        st.dataframe(
            suppression[
                [
                    "privacy_surface",
                    "date_grain",
                    "row_count",
                    "reportable_row_count",
                    "suppressed_row_count",
                    "suppressed_row_rate",
                ]
            ],
            width="stretch",
            hide_index=True,
        )


def render_dashboard(metadata: dict[str, Any], filters: dict[str, Any]) -> None:
    query_params = {
        "start_date": filters["start_date"],
        "end_date": filters["end_date"],
        "department": filters["department"],
        "location": filters["location"],
        "employment_type": filters["employment_type"],
        "limit": 5000,
        "purpose": "dashboard_view",
    }
    actor = filters["actor"]

    headcount_payload = fetch_json("/headcount/daily", query_params, actor=actor)
    attrition_payload = fetch_json("/attrition/monthly", query_params, actor=actor)
    suppression_payload = fetch_json(
        "/privacy/suppression-summary", {"purpose": "dashboard_view"}, actor=actor
    )

    headcount = records_frame(headcount_payload)
    attrition = records_frame(attrition_payload)
    suppression = records_frame(suppression_payload)

    st.title("Atlas HRBP Dashboard")
    st.caption("Privacy-safe People Analytics from the canonical employee record")

    metric_cols = st.columns(4)
    metric_cols[0].metric(
        "Current reportable headcount", current_headcount(headcount) or "Suppressed"
    )
    rate = latest_attrition_rate(attrition)
    metric_cols[1].metric(
        "Latest monthly attrition", "Suppressed" if rate is None else f"{rate:.1%}"
    )
    metric_cols[2].metric("Suppressed rows", headcount_payload.get("suppressed_row_count", 0))
    metric_cols[3].metric("k threshold", metadata.get("k_anonymity_threshold", 5))

    tab_overview, tab_headcount, tab_attrition, tab_privacy = st.tabs(
        ["Overview", "Headcount", "Attrition", "Privacy"]
    )

    with tab_overview:
        render_overview(metadata, headcount_payload, attrition_payload, suppression)

    with tab_headcount:
        if headcount.empty:
            st.info("No headcount rows returned.")
        else:
            chart_data = (
                headcount.dropna(subset=["headcount"])
                .groupby("snapshot_date", as_index=False)["headcount"]
                .sum()
                .set_index("snapshot_date")
            )
            st.line_chart(chart_data)
            st.dataframe(headcount, width="stretch", hide_index=True)

    with tab_attrition:
        if attrition.empty:
            st.info("No attrition rows returned.")
        else:
            chart_data = (
                attrition.dropna(subset=["attrition_rate"])
                .groupby("month_start_date", as_index=False)["attrition_rate"]
                .mean()
                .set_index("month_start_date")
            )
            st.line_chart(chart_data)
            st.dataframe(attrition, width="stretch", hide_index=True)

    with tab_privacy:
        if suppression.empty:
            st.info("No privacy summary rows returned.")
        else:
            privacy_chart = suppression.set_index("privacy_surface")[
                ["reportable_row_count", "suppressed_row_count"]
            ]
            st.bar_chart(privacy_chart)
            st.dataframe(suppression, width="stretch", hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Atlas HRBP Dashboard", layout="wide")
    apply_page_style()

    try:
        metadata = fetch_json("/metadata")
        filters = build_filters(metadata)
        render_dashboard(metadata, filters)
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        st.title("Atlas HRBP Dashboard")
        st.error(f"Metrics API unavailable: {exc}")


if __name__ == "__main__":
    main()
