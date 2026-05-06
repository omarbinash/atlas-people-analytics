# Reviewer Release Note

## What This Version Shows

This version of Atlas is a complete synthetic People Analytics reference
project for canonical employee identity, point-in-time workforce facts,
privacy-safe marts, and stakeholder-facing metric access.

It is built to be reviewed quickly by a hiring manager or technical interviewer.
The repo now includes the data foundation, serving layer, interview walkthroughs,
visual artifacts, and validation commands needed to understand the work without
reverse-engineering the project.

## Highlights

- Deterministic canonical employee matching across synthetic HRIS, ATS, payroll,
  CRM, DMS, and ERP source shapes.
- Stewardship queue for unresolved identities instead of unsafe auto-merges.
- SCD2-style `dim_employee` and daily `fct_workforce_daily` for point-in-time
  workforce analysis.
- Privacy-safe headcount and attrition marts with k-anonymity suppression.
- FastAPI metrics service that reads only from public people analytics marts and
  logs access events.
- Streamlit HRBP dashboard with an executive overview for demos.
- Airflow DAG shape for ordered dbt refreshes.
- Residual review assistant that ranks candidates without writing canonical
  truth.
- dbt exposures for API/dashboard lineage.
- Interview docs for Wealthsimple role mapping, SQL preparation, 30/60/90 plan,
  live demo readiness, and productionization tradeoffs.

## How To Review

Recommended path:

1. Read [Atlas One-Page Brief](one-page-brief.md).
2. Skim [Wealthsimple Interview Map](wealthsimple-role-map.md).
3. Open the [dashboard screenshot](../assets/dashboard-executive-overview.png).
4. Read [Edward Demo Talk Track](edward-demo-talk-track.md).
5. Review [Productionization Plan And Metric Catalog](productionization-and-metric-catalog.md).
6. Inspect the dbt models in `dbt_project/models/intermediate/`,
   `dbt_project/models/marts/core/`, and
   `dbt_project/models/marts/people_analytics/`.
7. Run the validation commands below if local credentials are configured.

## Validation Commands

```bash
.venv/bin/ruff check api dashboard airflow identity_engine tests
.venv/bin/mypy --config-file pyproject.toml identity_engine api dashboard
.venv/bin/python -m pytest tests/

cd dbt_project
set -a && source ../.env && set +a
../.venv/bin/dbt parse
```

For a full Snowflake-backed build:

```bash
cd dbt_project
set -a && source ../.env && set +a
../.venv/bin/dbt build --target dev
```

## Boundaries

- Atlas uses synthetic data only.
- It does not connect to real HRIS, ATS, payroll, finance, or Wealthsimple
  systems.
- The dashboard is a demo surface over governed marts, not the product's core
  accomplishment.
- The residual matching engine is review-only and does not make employment,
  compensation, performance, or canonical-identity decisions.

## Reviewer Takeaway

The project is designed to demonstrate senior People Analytics engineering:
identity resolution, dated workforce modeling, metric governance,
privacy-by-design, stakeholder delivery, and clear tradeoff documentation.
