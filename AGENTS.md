# Atlas — Project Context for Codex

## What this project is

Atlas is a portfolio People Analytics project demonstrating canonical-employee-record
construction across multiple operational source systems with realistic identity drift.

The work simulates the real-world problem at small-to-mid-size companies where the
"same" employee has different name representations across HRIS, ATS, payroll, CRM,
DMS, and ERP — creating silent reporting errors that affect compensation, performance
attribution, headcount, and attrition cohorts.

The author is interviewing for Senior Analytics Developer (People Analytics) at
Wealthsimple. This project demonstrates exactly the patterns described in that JD:
canonical employee record, SCD2, semantic layer, privacy-by-design.

## Critical context

**This is NOT a real production system.** Synthetic data only. The synthesizer
(`seeds/synthesize.py`) generates realistic but fabricated employees. Do not
suggest connecting to real HRIS APIs or anything implying the data is real.

**Quality matters more than speed.** No Edward-interview pressure on this build.
We're doing it right, not fast. Don't take shortcuts that compromise the engineering
quality of the deliverable.

## Tech stack

- **Python 3.11+** for the synthesizer (`seeds/synthesize.py`)
- **Snowflake** as the warehouse (account `QLSJUMK-DC22948.us-east-1`)
- **dbt-core 1.11+** with `dbt-snowflake` for transformations
- **dbt_utils** as the only third-party dbt package
- Future: Airflow for orchestration, FastAPI for the metric service, Streamlit for HRBP dashboard

Do NOT introduce additional tools without proposing them first. The tech choices are
deliberate to match Wealthsimple's stack (Redshift→Snowflake migration in progress, dbt,
Airflow, Workday HRIS, Ashby ATS).

## Current state — phase progression

- ✅ **Phase 1A** — repo scaffold, Snowflake DDL for 6 raw tables, name-strategy seed code
- ✅ **Phase 1B** — synthesizer that generates 5,000 employees × 5 years of lifecycle into Snowflake RAW
- ✅ **Phase 2A+2B** — dbt project scaffold + 6 staging models with 62 passing tests (tag: `phase-2b-complete`)
- ✅ **Phase 2C** — IDENTITY MATCHER — three-pass deterministic + stewardship queue, 172 passing dbt resources
- ⏳ **Phase 2D** — dim_employee SCD2 + fct_workforce_daily snapshot, NEXT
- 🔜 **Phase 3** — Privacy layer (k-anonymity macro, audit logging)
- 🔜 **Phase 4** — Airflow + FastAPI + Streamlit
- 🔜 **Phase 5** — ML residual matching + walkthrough docs

## Phase 2C — implemented design

The identity matcher must:

1. **Source from all 6 staging models** in `models/staging/`
2. **Generate a stable `canonical_person_id`** that survives:
   - Rehires (HRIS issues new HRIS_EMPLOYEE_ID)
   - Marriage name changes (HRIS updates, payroll lags)
   - Contractor → FTE conversion (sometimes new HRIS_ID with `_FTE` suffix)
   - Email-domain mismatches across systems
   - Multilingual names where Latin email is the only common anchor
3. **Run a three-pass deterministic match** in this order:
   - **Pass 1** — Government identifiers + email anchors
     - SIN_LAST_4 + DOB exact match
     - work_email_local_part exact match between HRIS and ERP/CRM/DMS
     - personal_email exact match between ATS and HRIS
   - **Pass 2** — Normalized name + DOB + hire-date proximity
     - lowercase, trim, accent-strip via Snowflake `unicode_normalize`
     - First-name-root match (with NICKNAME_MAP from seeds: Robert↔Bob↔Bobby)
     - DOB exact match
     - Hire-date within ±30 days
   - **Pass 3** — Email-domain + last-name match (catches cross-script cases)
4. **Drop unresolved records into `int_stewardship_queue`** — a table HR could
   adjudicate manually. Anything below the auto-match confidence threshold goes here.
5. **Have rigorous tests** — including:
   - No two HRIS_EMPLOYEE_IDs may resolve to two different canonical_person_ids if they share DOB + work_email_local_part
   - `canonical_person_id` is unique per person
   - Every staging row gets either a canonical_person_id or a stewardship_queue entry (no orphans)

## Important file locations

```
~/Desktop/PROJECTS/atlas-people-analytics/
├── seeds/
│   ├── synthesize.py        # data generator
│   ├── name_strategies.py   # NICKNAME_MAP, CanonicalIdentity dataclass
│   └── lifecycle.py         # event generation
├── infra/snowflake/
│   ├── 00_provision.sql     # creates ATLAS db, ATLAS_WH warehouse, ATLAS_DEVELOPER role
│   └── 01_raw_tables.sql    # 6 raw tables in ATLAS.RAW schema
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml.template  # → copy to ~/.dbt/profiles.yml (env_var-based)
│   ├── packages.yml           # dbt_utils
│   └── models/staging/        # 6 staging models, all building cleanly
├── .env                       # Snowflake credentials, GITIGNORED
└── .venv/                     # Python venv, GITIGNORED
```

Snowflake schemas:
- `ATLAS.RAW` — raw tables (5,157 HRIS, 5,157 ATS, 153K payroll, 1,412 CRM, 5,157 DMS, 5,157 ERP)
- `ATLAS.DBT_DEV_staging` — staging views (built by dbt)
- `ATLAS.DBT_DEV_intermediate` — Phase 2C will live here
- `ATLAS.DBT_DEV_core` — Phase 2D dim_employee, fct_workforce_daily
- `ATLAS.DBT_DEV_people_analytics` — Phase 2D analytics marts

## Conventions

- Staging models named `stg_{source}__{entity}.sql`
- Intermediate models named `int_{description}.sql`
- Mart models named `dim_{entity}.sql` or `fct_{entity}.sql`
- Names lowercased + trimmed in staging; original casing preserved in `*_original` columns
- Email local parts pre-extracted as identity anchors
- All credentials via `env_var()` in dbt; never hardcoded
- `seeds/output/` is gitignored — never commit synthesized CSVs

## How to run dbt

```bash
cd ~/Desktop/PROJECTS/atlas-people-analytics
source .venv/bin/activate
cd dbt_project
set -a && source ../.env && set +a
dbt run --select staging
dbt test --select staging
```

## Hard rules

1. **Never commit `.env` or anything containing real credentials**
2. **Never invent or modify the synthesizer's output without checking with me first** — the synthetic data has been carefully calibrated to expose specific identity-drift cases
3. **Never use `--dangerously-skip-permissions`** — Atlas is non-trivial; permission gates exist for a reason
4. **Run tests after every model change** — Phase 2C introduced real test coverage; never push code that hasn't passed the relevant `dbt build` / `dbt test`
5. **Never bypass the three-pass design with shortcuts** — the entire point is the discipline of deterministic matching with a stewardship queue
6. **Never reach for ML matching in Phase 2C** — that's Phase 5, separately scoped
7. **Document tradeoffs in code comments** — when you make an engineering choice (e.g., "deterministic over probabilistic because false positives in HR data are worse than false negatives"), write it down so it's defensible in a code review
8. **NEVER expose sensitive data in marts** — SIN_LAST_4 stays in payroll layer only, never propagates to public-facing marts

## What "done" looks like for Phase 2C

A successful Phase 2C delivers:

- `models/intermediate/int_identity_pass_1_hard_anchors.sql`
- `models/intermediate/int_identity_pass_2_name_dob_hire.sql`
- `models/intermediate/int_identity_pass_3_email_domain.sql`
- `models/intermediate/int_canonical_person.sql` — the unified output
- `models/intermediate/int_stewardship_queue.sql` — unresolved cases
- `macros/normalize_name.sql` — accent-stripping, lowercasing, nickname-mapping
- `seeds/nickname_map.csv` — Robert↔Bob, Patrick↔Paddy, Charles↔Charlie, etc.
- Custom tests in `tests/` for canonical_person_id stability and uniqueness invariants
- All tests passing on `dbt build --select +int_canonical_person+`
- README updated with a section on the matcher's design and test results
