# Atlas One-Page Brief

## Purpose

Atlas is a synthetic People Analytics portfolio project built to demonstrate how
to create a trustworthy employee analytics foundation when identity, history,
metric definitions, and privacy all matter at the same time.

The project is aligned to a zero-to-one People Analytics role: build the data
spine first, then expose governed metrics to HR stakeholders.

## Problem

People systems rarely agree on the same employee. HRIS, ATS, payroll, CRM, DMS,
and ERP records can drift because of preferred names, legal names, rehires,
contractor-to-FTE conversions, lagging updates, and source-specific IDs.

When employee identity is wrong, downstream metrics become wrong:

- headcount
- attrition cohorts
- tenure
- compensation analysis
- manager and department reporting
- hiring funnel attribution

## Solution Shape

Atlas implements a layered analytics system:

1. **Synthetic source systems** generate realistic identity drift without using
   real employee data.
2. **dbt staging models** normalize raw operational fields.
3. **Deterministic identity resolution** creates a stable
   `canonical_person_id`.
4. **Stewardship queue** captures unresolved records instead of guessing.
5. **Core marts** produce `dim_employee` and `fct_workforce_daily`.
6. **Privacy-safe people marts** suppress exact metrics for small cohorts.
7. **FastAPI and Streamlit** expose controlled HRBP-facing metrics.
8. **Residual review assistant** ranks unresolved candidates without writing
   canonical truth.

## Key Engineering Choices

- Deterministic matching before ML because false-positive merges in people data
  are riskier than false negatives.
- Daily workforce snapshot because People Analytics needs point-in-time answers,
  not just current-state rows.
- k-anonymity in the public marts because privacy should shape the data product,
  not sit only in the dashboard.
- Audit logging in the API because access to people metrics should be
  observable.
- Synthetic-only data because the project is a public reference architecture,
  not a production HR system.

## What This Demonstrates

Atlas demonstrates the exact operating pattern a Senior Analytics Developer in
People Analytics needs:

- build from ambiguous requirements
- model employee identity carefully
- design SCD2 and date-spine facts
- define governed metrics
- test dbt models and Python services
- expose stakeholder-facing surfaces
- document tradeoffs clearly
- treat people data as sensitive by design

## Interview Close

The strongest summary is:

> Atlas is not primarily a dashboard. It is a governed employee identity and
> workforce metrics foundation, with a dashboard as one consumer of that
> foundation.
