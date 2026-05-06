# Wealthsimple Interview Map

This document maps Atlas to the Senior Analytics Developer, People Analytics
role. It is written for hiring-manager review, not as production deployment
documentation.

Atlas uses synthetic data only. The project intentionally does not connect to
real HRIS, ATS, payroll, finance, or people systems.

## The Core Story

Wealthsimple is building a new People Analytics foundation. The hard part is
not the first dashboard. The hard part is creating a trusted employee identity
spine across messy operating systems, then building point-in-time workforce
metrics on top of it with governance and privacy designed in from the start.

Atlas demonstrates that pattern end to end:

1. Generate realistic synthetic employee identity drift across six source
   systems.
2. Standardize the sources in dbt staging models.
3. Resolve employee identity with deterministic matching and a stewardship
   queue.
4. Build an SCD2 employee dimension and daily workforce snapshot.
5. Publish privacy-safe people metrics through marts, an API, and an HRBP
   dashboard.
6. Add an explainable residual review assistant that helps stewards without
   changing canonical truth.

## Requirement Map

| Wealthsimple signal | Atlas implementation | What it proves |
| --- | --- | --- |
| Build a new People Analytics foundation from scratch | Raw -> staging -> intermediate -> core -> people_analytics schemas, plus Airflow, API, dashboard, CI | Can structure ambiguous zero-to-one analytics work into durable layers |
| Canonical employee record with stable identifier | `int_canonical_person` and the deterministic identity pass models | Understands employee identity as a first-class data product |
| Survive job changes, rehires, name changes, migrations | Synthetic lifecycle data includes rehires, contractor-to-FTE changes, preferred/legal name drift, email drift, and cross-system ID drift | Can model the real failure modes that break HR reporting |
| Dimensional modeling and SCD patterns | `dim_employee` and `fct_workforce_daily` | Can support point-in-time headcount, tenure, and attrition questions |
| Semantic layer for people metrics | `workforce_headcount_daily`, `workforce_attrition_monthly`, API metadata, and dashboard filters | Can turn warehouse models into governed business-facing metrics |
| dbt quality practices | dbt tests, custom tests, model docs, and CI validation | Treats analytics code like production software |
| Data governance and privacy | k-anonymity macros, privacy-safe marts, field whitelisting, and `privacy_audit_log` | Designs for sensitive people data rather than adding controls later |
| Airflow orchestration | `airflow/dags/atlas_people_analytics.py` | Can define dependency-aware refresh order across analytics layers |
| Python engineering | Synthetic data generator, residual review engine, FastAPI service, tests | Can build outside SQL when the workflow needs software engineering |
| Stakeholder-facing delivery | Streamlit HRBP dashboard and demo script | Can expose complex warehouse work through a usable product surface |
| Pragmatic AI and automation | Residual review assistant ranks candidates but never auto-merges | Uses AI-style assistance where it reduces effort without weakening controls |

## Interview Sound Bites

- "I did not start with a dashboard. I started with the employee identity spine,
  because every downstream people metric depends on it."
- "The matcher is deterministic first because false positives in people data
  are worse than false negatives. A bad merge can contaminate compensation,
  attrition, tenure, and performance attribution."
- "The stewardship queue is not a fallback. It is part of the control design."
- "The SCD2 dimension and daily workforce fact are what make point-in-time
  questions defensible."
- "The privacy layer is not just row filtering. It changes the shape of the
  public marts so small cohorts cannot leak exact counts."
- "The residual matcher is deliberately review-only. It helps a steward decide
  where to look, but it does not write canonical identity."

## What Not To Claim

- Do not claim Atlas connects to real Wealthsimple, Workday, Ashby, payroll, or
  finance data.
- Do not claim the residual review assistant is a trained production ML model.
  It is an explainable scoring layer for review assistance.
- Do not claim the Streamlit dashboard is the main deliverable. It is the
  stakeholder surface over the governed data foundation.
- Do not imply the synthetic data is production representative in volume,
  access control, or legal risk. The patterns are production-shaped; the data is
  fabricated.

## The Strongest Framing

Atlas is a practical answer to the role's central question:

> How would you build a trustworthy People Analytics foundation when employee
> identity, point-in-time reporting, and privacy are all hard at the same time?

The answer is not one clever model. It is a layered system: deterministic
identity resolution, dated workforce facts, governed metrics, privacy controls,
operational refresh, and clear stewardship paths for the cases software should
not decide alone.
