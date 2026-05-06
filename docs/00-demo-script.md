# Atlas Demo Script

This is the short reviewer path through Atlas. It is written for interview and
portfolio walkthroughs, not for a production deployment.

For the Wealthsimple hiring-manager version of the walkthrough, use
`docs/interview/edward-demo-talk-track.md` alongside this script.

## 1. The Problem

People Analytics reporting is only as good as its employee identity spine.
Atlas starts with six synthetic operational systems where the same fabricated
employee can appear with changed names, alternate IDs, lagged systems, and
different email conventions.

The point of the demo is that every source can look reasonable alone while the
cross-system employee record is still wrong.

## 2. The Identity Spine

The core dbt work is the deterministic matcher in `models/intermediate/`:

- hard anchors for email and exact safe identifiers
- normalized name, DOB, and hire-date proximity
- email-domain plus last-name recovery for harder cross-system cases
- stewardship queue for anything that should not be auto-merged

The engineering stance is conservative: false positives in HR data are worse
than false negatives, so unresolved identities are queued rather than guessed.

## 3. Point-In-Time Correctness

`dim_employee` and `fct_workforce_daily` turn the resolved identity spine into
dated workforce facts. The demo should emphasize that headcount and attrition
questions need to be answered as of a date, not as of the current employee row.

## 4. Privacy Layer

The public People Analytics marts suppress exact metrics for cohorts below the
k-anonymity threshold. Suppressed rows stay visible so analysts can see that a
cohort exists, but small exact counts are not exposed.

The FastAPI service reads only from these privacy-safe marts and writes audit
events for metric access.

## 5. Live App Path

Run the service and dashboard:

```bash
make api
make dashboard
```

Open:

- API docs: `http://127.0.0.1:8000/docs`
- Dashboard: `http://localhost:8501`

Suggested live path:

1. Show the dashboard overview and data-through date.
2. Filter to a department/location/employment type.
3. Show headcount and attrition changing with the filter.
4. Open the Privacy tab and point to reportable vs suppressed rows.
5. Hit `/privacy/suppression-summary` in the API docs.
6. Open the residual review report and show that Phase 5 suggestions remain
   outside the canonical write path.

## 6. What This Proves

Atlas demonstrates the pattern a People Analytics developer needs in a real
warehouse environment:

- canonical employee record construction
- deterministic identity resolution with stewardship
- SCD-style workforce modeling
- daily point-in-time facts
- privacy-preserving semantic marts
- API/dashboard consumption with audit logging
- orchestration shape through Airflow
