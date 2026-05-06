# Source Freshness And Reconciliation Plan

Atlas already defines source freshness expectations in
`dbt_project/models/staging/_sources.yml`. This document explains how those
checks would become an operating practice in a production People Analytics
environment.

Atlas uses synthetic source tables only. The system names below describe source
shapes, not live integrations.

## Freshness Expectations

| Source shape | Atlas table | Expected cadence | Why it matters | Example freshness rule |
| --- | --- | --- | --- | --- |
| HRIS | `RAW_HRIS_EMPLOYEES` | Daily | Employee status, org, manager, location, and hire/term events | Warn after 1 day, error after 3 days in production |
| ATS | `RAW_ATS_CANDIDATES` | Daily | Accepted offers, start dates, recruiting attribution | Warn after 1 day, error after 7 days |
| Payroll | `RAW_PAYROLL_RECORDS` | Per pay period | Payroll reconciliation and sensitive hard anchors | Warn after expected pay cycle, error after one missed cycle |
| CRM | `RAW_CRM_SALES_REPS` | Daily | Sales/support user presence and preferred-name drift | Warn after 2 days, error after 7 days |
| DMS | `RAW_DMS_USERS` | Daily | Operational user identity and shortened-name drift | Warn after 2 days, error after 7 days |
| ERP | `RAW_ERP_USERS` | Daily | Downstream operational account mapping | Warn after 2 days, error after 7 days |

The portfolio project uses a wider default freshness window because the
synthetic data is generated in batches. A production version should tighten the
rules around actual source SLAs.

## Reconciliation Checks

| Control total | Source of comparison | Atlas model | Expected tolerance |
| --- | --- | --- | --- |
| Active employees by day | HRIS operational report | `fct_workforce_daily` | Exact or explained difference |
| Active employees by department | HRIS plus Finance headcount view | `workforce_headcount_daily` | Exact after definition alignment |
| Monthly terminations | HRIS employment events | `workforce_attrition_monthly` | Exact or explained backfill |
| Payroll employee population | Payroll register | `int_payroll_spells` / stewardship queue | Explain unmatched payroll identities |
| New hires and rehires | ATS accepted offers plus HRIS hires | identity pass outputs | Explain late starts and cancelled offers |
| Suppressed metric rows | Privacy mart summary | `privacy_suppression_summary` | Monitor trend, not exact target |

## Operating Process

1. Run dbt source freshness before model builds.
2. Block public metric refreshes when a critical source is stale.
3. Reconcile HRIS active headcount before publishing workforce metrics.
4. Log known differences with owner, reason, affected dates, and expected fix.
5. Review stewardship queue volume as a data-quality signal, not just a matching
   backlog.
6. Reconcile HR and Finance definitions before exposing self-serve metrics.

## Interview Talking Point

"I would not treat reconciliation as a one-time QA step. In People Analytics,
source freshness, control totals, and unresolved identity volume are health
metrics for the data product itself."
