# Productionization Plan And Metric Catalog

Atlas is a portfolio implementation using synthetic data. This document names
the production boundaries, the metric definitions the project is designed to
support, and the next hardening steps that would matter in a real People
Analytics environment.

## Boundaries

- Atlas does not connect to real HRIS, ATS, payroll, finance, or survey systems.
- The synthetic data is fabricated to exercise identity-drift cases.
- The Streamlit dashboard is a demonstration surface, not an enterprise BI
  deployment.
- The residual review assistant is not a write path and not an employment
  decision tool.
- The project should be evaluated as a reference architecture for governed
  people analytics, not as a drop-in production system.

## Metric Catalog

| Metric | Grain | Current Atlas source | Definition | Privacy rule |
| --- | --- | --- | --- | --- |
| Active headcount | Day x department x location x employment type | `workforce_headcount_daily` | Count of employee spells active on the date | Suppress exact count when cohort size is below k |
| Month-start workforce | Month x department x location x employment type | `workforce_attrition_monthly` | Active workforce at the start of the month | Suppress denominator and derived rate below k |
| Terminations | Month x department x location x employment type | `workforce_attrition_monthly` | Employee spells with termination date in the month | Suppress numerator below k-context |
| Attrition rate | Month x department x location x employment type | `workforce_attrition_monthly` | Terminations divided by month-start workforce | Suppress when denominator is below k |
| Employee spell | Employee employment spell | `dim_employee` | One row per canonical person and HRIS employment spell version | Not exposed in public people_analytics marts |
| Workforce daily row | Employee spell x date | `fct_workforce_daily` | One row per day an employment spell is observable | Not exposed directly to public users |
| Suppression coverage | Mart x suppression status | `privacy_suppression_summary` | Count of reportable versus suppressed public metric rows | Safe because it reports suppression metadata |
| Metric access event | API request | `privacy_audit_log` | Actor, purpose, endpoint, filters, row counts, suppressed counts | Internal governance table |

## Definition Notes

- Headcount should use `is_active_on_date = true`, not a current employee row.
- Attrition should be anchored to a month-start denominator so the rate is
  stable and explainable.
- Termination event rows stay visible in the core fact so attrition is not lost
  when someone is no longer active.
- Public metric tables should expose cohort-level aggregates only.
- Sensitive identifiers, including SIN fragments and full DOB, should remain out
  of business-facing marts.

## Production Hardening Roadmap

1. **Source contracts**

   Document expected fields, nullability, key behavior, update cadence, and
   authoritative ownership for each source. This should cover employee status,
   manager, department, location, employment type, hire date, termination date,
   and identity anchors.

2. **Freshness and reconciliation**

   Add freshness checks and control-total reconciliation against HRIS, Finance,
   and People Ops reports. Differences should be logged with known explanations,
   not treated as unexplained drift.

3. **Access model**

   Define role-based access for raw, staging, core, people_analytics, API, and
   dashboard layers. The default business surface should be aggregate and
   privacy-safe.

4. **Stewardship workflow**

   Turn `int_stewardship_queue` into an operating process: assignment, decision
   reason, reviewer, timestamp, evidence, and audit trail. Canonical identity
   changes should be reviewable and reversible.

5. **Metric governance**

   Establish owners for each metric definition and version changes. HR, Finance,
   and People Ops should agree on the definitions before the metrics become
   self-serve.

6. **Environment separation**

   Promote the dbt layers through dev, staging, and production with separate
   schemas, service accounts, and review gates.

7. **Operational observability**

   Monitor DAG success, dbt test failures, row-count changes, suppression-rate
   shifts, API errors, and dashboard availability.

8. **Cost and performance**

   Incrementalize large facts where appropriate, cluster or partition by date
   and common filters, and keep API queries pinned to aggregate marts.

9. **Incident response**

   Define what happens when a bad identity merge, source-system backfill, or
   privacy suppression failure is discovered. The answer should include
   rollback, communication, and metric restatement steps.

10. **Documentation and onboarding**

   Keep the README, demo script, dbt docs, metric catalog, and reviewer
   walkthrough current enough that a new analyst can understand the system
   without reverse-engineering the warehouse.

## Deliberate Non-Goals

- No real Workday, Ashby, payroll, or finance connector is included in Atlas.
- No individual-level employee analytics is exposed through the public demo
  surfaces.
- No ML output becomes canonical truth.
- No sensitive identifiers are propagated into the people_analytics marts.
- No dashboard is treated as a substitute for governed metric definitions.
