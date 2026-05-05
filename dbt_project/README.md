# Atlas dbt Project

This is the dbt transformation layer for Atlas. It reads from the six
synthetic raw tables (loaded by `seeds/synthesize.py`) and produces the
canonical employee record, dimensional models, and people-analytics marts.

## First-time setup

1. Make sure dependencies are installed (from repo root):
   ```bash
   pip install -e ".[dev]"
   ```

2. Copy the profile template to your home directory:
   ```bash
   mkdir -p ~/.dbt
   cp dbt_project/profiles.yml.template ~/.dbt/profiles.yml
   ```
   The template uses `env_var()` so no edits should be needed if your
   `.env` is correctly populated. The shell needs to have those vars
   exported when dbt runs — see "Running" below.

3. Install dbt packages:
   ```bash
   cd dbt_project
   dbt deps
   ```

4. Validate connection to Snowflake:
   ```bash
   dbt debug
   ```
   You should see "All checks passed!"

## Running

dbt reads connection details from environment variables. Source your `.env`
before running dbt commands:

```bash
set -a && source ../.env && set +a
cd dbt_project

# Build all staging models
dbt run --select staging

# Test all staging models
dbt test --select staging

# Build everything
dbt run

# Build everything and run tests
dbt build
```

## Phase 2C identity matcher

The intermediate layer now includes the deterministic canonical-person matcher:

- Pass 1: exact personal-email and work-email-local-part anchors
- Pass 2: normalized first-name-root + last-name + DOB + hire-date proximity
- Pass 3: company email domain + email last-name token + hire-date proximity
- Stewardship: unresolved, ambiguous, or below-threshold source identities

Validated command:

```bash
dbt build --select +int_canonical_person+ int_stewardship_queue
```

Latest result: 172/172 passed. The build produced 5,000 canonical people from
22,419 source identity nodes. Auto-matched nodes: ATS 4,961, CRM 1,175,
DMS/ERP 4,298. Queued nodes: ATS 196, CRM 237, DMS/ERP 1,395, payroll 5,157.
Payroll is queued because the current synthetic payroll feed does not expose a
safe DOB/email bridge; SIN_LAST_4 is not used as a matcher input.

## Phase 2D core marts

The core mart layer now includes:

- `dim_employee`: one SCD2-style row per canonical person employment spell
- `fct_workforce_daily`: one employee-spell row per day for point-in-time headcount and attrition

Validated command:

```bash
dbt build --select +dim_employee+ fct_workforce_daily
```

Latest result: 195/195 passed. The build produced 5,157 employee spell rows and
4,456,107 workforce daily rows from 2021-05-03 through 2026-05-05.

## Phase 3 privacy layer

The People Analytics mart layer now includes:

- `workforce_headcount_daily`: k-anonymous daily headcount by HRBP dimensions
- `workforce_attrition_monthly`: k-anonymous monthly attrition by HRBP dimensions
- `privacy_suppression_summary`: reportable vs suppressed row counts by public surface
- `privacy_audit_log`: incremental table for future access events
- `insert_privacy_audit_event`: macro for writing audit rows

Validated command:

```bash
dbt build --select +privacy_suppression_summary+ privacy_audit_log test_privacy_macros privacy__no_direct_employee_identifiers_in_people_analytics
```

Latest result: 235/235 passed. With `k_anonymity_threshold = 5`, the public
headcount mart produced 610,740 rows and the public attrition mart produced
20,717 rows. Suppressed rows keep dimensions visible but exact metrics null.

## Phase 4 service consumers

The Airflow DAG, FastAPI metric service, and Streamlit dashboard live outside
the dbt project, but they depend on these dbt surfaces:

- `workforce_headcount_daily`
- `workforce_attrition_monthly`
- `privacy_suppression_summary`
- `privacy_audit_log`

The API reads only from `DBT_DEV_PEOPLE_ANALYTICS` by default and inserts
access events into `privacy_audit_log`. Direct employee identifiers remain out
of the public People Analytics schema.

## Layer structure

| Layer | Materialization | Schema | Purpose |
|---|---|---|---|
| `staging/` | view | DBT_DEV_STAGING | 1:1 cleaned mirrors of raw tables |
| `intermediate/` | table | DBT_DEV_INTERMEDIATE | Identity resolution, stewardship queue |
| `marts/core/` | table | DBT_DEV_CORE | dim_employee (SCD2), fct_workforce_daily |
| `marts/people_analytics/` | table | DBT_DEV_PEOPLE_ANALYTICS | Headcount, attrition, comp marts |

## Convention

- Staging models: `stg_{source}__{entity}.sql`
- Intermediate models: `int_{description}.sql`
- Mart models: `dim_{entity}.sql` or `fct_{entity}.sql` for core; `{topic}_{description}.sql` for analytics
- Names in staging are lowercased and trimmed; original casing preserved in
  `*_original` columns for display
- Email local parts are pre-extracted in staging as identity anchors for
  cross-system matching
