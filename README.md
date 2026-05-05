# Atlas — Canonical Employee Record for People Analytics

> A from-scratch implementation of the canonical-employee-record pattern: a deterministic identity-resolution layer that produces a stable employee key surviving job changes, rehires, and source-system migrations — with the SCD2 dimensions, dated workforce snapshots, and privacy-preserving semantic layer that ride on top of it.

**Stack:** Python · dbt · Snowflake · Airflow · FastAPI · Streamlit · GitHub Actions

---

## The problem

Most People Analytics functions inherit a problem they cannot see.

Their HRIS knows one version of an employee. Payroll knows another. The applicant-tracking system that hired them four years ago has a third. The CRM where they appear as a salesperson has a fourth, often with a *preferred* name that differs from the legal name in payroll. The DMS where they actually log deals has a *shortened* name, because someone typed it in a hurry on day one. When the same person gets married, transferred between teams, terminated, or rehired, those records drift apart silently.

Headcount reports become wrong. Attrition cohorts become wrong. Compensation analyses become wrong. And no one notices, because every individual system *looks correct in isolation*.

Atlas solves this. It produces a single canonical employee record — a stable key that survives source-system migrations, rehires, and the messy reality of how humans get represented in operational software.

## Why this exists

I built the equivalent of this system once before, in production, at a 40-rooftop, $500M+ Canadian dealer group where I was the founding data hire. That implementation served HR, payroll, sales performance, and executive reporting for 800+ employees across multiple brands. It worked, but the code lived in a proprietary environment and could never be open-sourced.

Atlas is the open, reference implementation of that same pattern, rebuilt in modern infrastructure (dbt + Snowflake + Airflow) with the testing, documentation, and privacy engineering that production People Analytics deserves.

## What it does

1. **Ingests** six realistic source systems with the kind of name and ID drift that real HR data exhibits — legal names, preferred names, shortened names, marriage name changes, contractor-to-FTE conversions, terminations and rehires, and inter-system ID schema mismatches.
2. **Resolves identity** through a deterministic matching layer with explicit, auditable rules — not a black-box ML model. (An optional ML residual layer handles the long tail.)
3. **Produces a canonical `dim_employee`** as a Type 2 slowly-changing dimension with full effective-dating, so point-in-time queries return correct answers.
4. **Snapshots the workforce daily** into a date-spine fact table that supports any "what was true on date X" question — headcount, tenure, attrition cohort.
5. **Exposes metrics through a privacy-aware semantic layer** — k-anonymity guards on small cohorts, field whitelisting, and audit logging.
6. **Validates everything** through dbt tests, a chaos-corruption test suite, custom SCD2 contiguity tests, and a 7-job CI/CD pipeline.

## Current build: Phase 2C identity matcher

Phase 2C now builds the deterministic identity-resolution layer in `dbt_project/models/intermediate/`:

- `int_identity_source_nodes` standardizes HRIS, ATS, payroll-spell, CRM, and DMS/ERP records onto a common matching grain.
- `int_identity_pass_1_hard_anchors` resolves exact personal-email and work-email-local-part anchors.
- `int_identity_pass_2_name_dob_hire` evaluates normalized first-name-root + last-name + hire-date candidates, but only auto-merges when DOB is present.
- `int_identity_pass_3_email_domain` recovers company-domain + email last-name-token matches with hire-date proximity and a uniqueness gate.
- `int_canonical_person` emits one stable `canonical_person_id` per HRIS-distinct person.
- `int_stewardship_queue` captures every non-HRIS source identity that did not safely auto-merge.

Latest verification:

```bash
cd dbt_project
dbt build --select +int_canonical_person+ int_stewardship_queue
```

Result: 172/172 dbt resources passed, producing 5,000 canonical people and 6,985 stewardship queue records. Payroll is intentionally routed to stewardship in this phase because the current synthetic payroll feed has SIN_LAST_4 but no DOB/email bridge, and the generator makes SIN_LAST_4 unstable across pay periods. That is a deliberate false-negative-over-false-positive choice for HR data.

## Current build: Phase 2D core marts

Phase 2D now adds the first point-in-time marts in `dbt_project/models/marts/core/`:

- `dim_employee` is an SCD2-style employee dimension at HRIS employment-spell grain, keyed by `canonical_person_id`.
- `fct_workforce_daily` emits one employee-spell row per calendar date from hire through termination/as-of date.
- Termination dates are retained as non-active event rows, so headcount uses `is_active_on_date = true` while attrition can count `is_termination_date = true`.
- Full DOB and SIN_LAST_4 are not exposed in the core marts.

Latest verification:

```bash
cd dbt_project
dbt build --select +dim_employee+ fct_workforce_daily
```

Result: 195/195 dbt resources passed, producing 5,157 employee spell rows and 4,456,107 workforce daily rows from May 3, 2021 through May 5, 2026.

## Architecture at a glance

```
        ┌─────────────────── Source Systems ──────────────────┐
        │  HRIS (legal name)    ATS (preferred name)          │
        │  Payroll (legal+SIN)  CRM (preferred name)          │
        │  DMS (shortened name) ─→ ERP (mirrors DMS)          │
        └──────────────────────┬──────────────────────────────┘
                               │
                  Airflow DAG  ▼
                       ┌──────────────┐
                       │  raw schema  │
                       └──────┬───────┘
                              ▼
                       ┌──────────────┐
                       │  staging     │  1:1 source mirrors, normalized types
                       └──────┬───────┘
                              ▼
                       ┌──────────────┐
                       │ intermediate │  Identity resolution lives here
                       │              │  - normalized name candidates
                       │              │  - deterministic match clusters
                       │              │  - stable employee_key emission
                       └──────┬───────┘
                              ▼
                       ┌──────────────┐
                       │     core     │  dim_employee (SCD2)
                       │     marts    │  fct_workforce_daily (snapshot)
                       └──────┬───────┘
                              ▼
                       ┌──────────────┐
                       │  people_     │  attrition_cohort, tenure,
                       │  analytics   │  headcount_pit, comp_bands
                       │     marts    │
                       └──────┬───────┘
                              ▼
                ┌─────────────┴──────────────┐
                ▼                            ▼
        ┌──────────────┐            ┌──────────────┐
        │  FastAPI     │            │  Streamlit   │
        │  metrics svc │            │  HRBP demo   │
        │  (k-anon)    │            │  dashboard   │
        └──────────────┘            └──────────────┘
```

## Repository structure

```
atlas-people-analytics/
├── docs/                     # The story, the architecture, the walkthroughs
├── seeds/                    # Synthetic data generator (Faker + IBM HR Analytics seed)
├── dbt_project/              # Models, tests, macros, snapshots
├── airflow/                  # Production-shaped DAG
├── identity_engine/          # ML residual matching (stretch chapter)
├── api/                      # FastAPI privacy-aware metrics service
├── dashboard/                # Streamlit HRBP-facing demo
├── tests/                    # Python tests + chaos corruption suite
├── infra/                    # Snowflake provisioning SQL
├── .github/workflows/        # CI/CD
├── Makefile                  # `make seed && make build && make test`
└── pyproject.toml
```

## Quick start

> ⚠️ This project is configured against Snowflake. You will need a Snowflake account (the trial works).

```bash
# 1. Clone and install
git clone https://github.com/<your-username>/atlas-people-analytics.git
cd atlas-people-analytics
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# 2. Configure Snowflake credentials
cp .env.example .env
# Edit .env with your account / user / role / warehouse / database

# 3. Provision Snowflake objects (one-time)
make snowflake-init

# 4. Generate synthetic data and load to raw schema
make seed

# 5. Build the dbt models
make build

# 6. Run all tests
make test

# 7. Launch the dashboard
make dashboard
```

## Documentation

- [The Problem in Detail](docs/01-the-problem.md)
- [Architecture & Design Decisions](docs/02-architecture.md)
- [Identity Resolution: How the Matching Works](docs/03-identity-resolution.md)
- [SCD2 and Point-in-Time Correctness](docs/04-scd2-and-snapshots.md)
- [Privacy by Design](docs/05-privacy-design.md)
- [Walkthroughs](docs/walkthroughs/) — five real edge cases traced end-to-end
- [Stretch: ML Residual Matching](docs/07-stretch-ml-residuals.md)
- [AI Collaboration Log](docs/08-ai-collaboration.md) — honest log of where AI tools helped

## License

MIT — see [LICENSE](LICENSE).

---

*Atlas is a portfolio project, not a production system you should deploy as-is. The patterns are production-grade; the synthetic data is not. Use it as a reference implementation, a study aid, or a foundation for your own People Analytics work.*
