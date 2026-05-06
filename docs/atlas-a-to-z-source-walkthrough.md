# Atlas A-to-Z Source Walkthrough

Generated on 2026-05-06 from the local repository.

This walkthrough is intentionally source-indexed. It explains the execution path and then includes every authored code, SQL, YAML, TOML, CSV, and command-surface file needed to understand the project. It excludes local credentials, generated outputs, virtual environments, dbt build artifacts, and binary screenshots.

## A-to-Z Execution Story

Atlas starts with a deliberately simple but realistic premise: People Analytics cannot be trusted until employee identity is trusted. The project therefore builds from the lowest layer upward instead of starting with a dashboard.

1. **Command surface and environment**: `Makefile`, `pyproject.toml`, and dbt config define how developers install, lint, test, seed, build, serve, and validate the project.
2. **Snowflake foundation**: `infra/snowflake/00_provision.sql` creates the database, warehouse, schemas, role, and grants. `01_raw_tables.sql` creates six raw source tables.
3. **Synthetic source systems**: `seeds/` fabricates employees and lifecycle drift. The data is synthetic only, but shaped to mimic the identity problems caused by HRIS, ATS, payroll, CRM, DMS, and ERP.
4. **Staging**: dbt staging models normalize each source one-to-one. This preserves raw-system meaning while making field names, types, names, and email anchors consistent.
5. **Identity node graph**: `int_identity_source_nodes` unions all source representations onto one matching grain: one source identity node per observable source-system person/spell/account.
6. **HRIS person spine**: `int_hris_persons` creates the HRIS-seeded person anchor that survives rehires and HRIS employee ID churn.
7. **Deterministic matching**: three matching passes move from strongest to weaker evidence. Pass 1 uses hard anchors, Pass 2 uses normalized names plus DOB/hire-date proximity, and Pass 3 uses constrained email-domain/last-name recovery.
8. **Canonical person output**: `int_canonical_person` emits stable `canonical_person_id` records and safe source-system ID arrays. Sensitive fields do not propagate to marts.
9. **Stewardship queue**: `int_stewardship_queue` captures unresolved non-HRIS source records. This is a control surface, not a failure table.
10. **Core workforce modeling**: `dim_employee` and `fct_workforce_daily` turn identity into SCD-style history and daily point-in-time workforce facts.
11. **Privacy-safe marts**: headcount and attrition aggregate marts apply k-anonymity suppression before business-facing consumption.
12. **Serving layer**: FastAPI exposes privacy-safe endpoints, writes audit events, and refuses unsafe schema/table identifiers. Streamlit consumes the API for an HRBP-facing dashboard.
13. **Orchestration**: the Airflow DAG documents production-shaped dependency order.
14. **Residual review**: Phase 5 ranks unresolved candidates for steward review without writing canonical identity.
15. **Tests and docs**: SQL tests, Python tests, model docs, walkthroughs, and interview material make the system reviewable and defensible.

The central design choice is conservative identity resolution: false positives in people data are worse than false negatives. Atlas would rather queue a record for stewardship than silently merge two humans and contaminate headcount, attrition, compensation, or performance history.

## Core Concepts Explained

### Canonical Employee Record
A canonical employee record separates the human from the source-system row. HRIS IDs, payroll IDs, ATS IDs, CRM users, DMS users, and ERP accounts are representations. `canonical_person_id` is the stable analytical identity that survives rehires, source migrations, and name changes.

### Deterministic Identity Resolution
Deterministic matching uses explicit rules instead of a black-box model. Atlas orders the passes from safest to weakest. This makes each match auditable, explainable, and testable.

### Stewardship Queue
A stewardship queue is the manual-review surface for records the system should not decide alone. It is a safety mechanism: uncertainty becomes visible instead of being hidden inside aggressive matching.

### SCD2
Slowly Changing Dimension Type 2 preserves history by creating a new version when important attributes change. In People Analytics, this protects historical manager, department, location, employment type, and status analysis.

### Date Spine And Workforce Snapshot
Source HR systems often store events: hire date, termination date, transfer date. People leaders ask state questions: who was active on March 31? A date spine expands events into daily rows so point-in-time metrics are easy and correct.

### Semantic Layer
A semantic layer is where business definitions become governed logic. Headcount and attrition are not just SQL counts. They need grain, date rules, inclusions/exclusions, privacy thresholds, and owner signoff.

### k-Anonymity
Atlas suppresses exact metrics for cohorts smaller than a threshold. Suppressed rows remain visible so users know the cohort exists, but small exact values do not leak.

### Residual Review Evaluation
The Phase 5B evaluation uses deterministic hints as weak proxy labels. It is a diagnostic report for ranking behavior, not a truth set and not an approval mechanism.

## Source Inventory By Layer

### 0. Command Surface And Python Project Config
- `Makefile`
- `pyproject.toml`
### 1. Snowflake Provisioning And Raw Tables
- `infra/snowflake/00_provision.sql`
- `infra/snowflake/01_raw_tables.sql`
### 2. Synthetic Data Generator
- `seeds/__init__.py`
- `seeds/name_strategies.py`
- `seeds/lifecycle.py`
- `seeds/synthesize.py`
### 3. dbt Project Config, Seeds, And Fixtures
- `dbt_project/dbt_project.yml`
- `dbt_project/packages.yml`
- `dbt_project/profiles.yml.template`
- `dbt_project/seeds/_seeds.yml`
- `dbt_project/seeds/nickname_map.csv`
- `dbt_project/fixtures/normalize_name.yml`
- `dbt_project/fixtures/first_name_root.yml`
### 4. dbt Macros
- `dbt_project/macros/normalize_name.sql`
- `dbt_project/macros/first_name_root.sql`
- `dbt_project/macros/match_confidence.sql`
- `dbt_project/macros/privacy.sql`
### 5. dbt Staging Models
- `dbt_project/models/staging/_sources.yml`
- `dbt_project/models/staging/_staging.yml`
- `dbt_project/models/staging/hris/stg_hris__employees.sql`
- `dbt_project/models/staging/ats/stg_ats__candidates.sql`
- `dbt_project/models/staging/payroll/stg_payroll__records.sql`
- `dbt_project/models/staging/crm/stg_crm__sales_reps.sql`
- `dbt_project/models/staging/dms/stg_dms__users.sql`
- `dbt_project/models/staging/erp/stg_erp__users.sql`
### 6. dbt Intermediate Identity Models
- `dbt_project/models/intermediate/_intermediate.yml`
- `dbt_project/models/intermediate/int_identity_source_nodes.sql`
- `dbt_project/models/intermediate/int_hris_persons.sql`
- `dbt_project/models/intermediate/int_payroll_spells.sql`
- `dbt_project/models/intermediate/int_dms_erp_unified.sql`
- `dbt_project/models/intermediate/int_identity_pass_1_hard_anchors.sql`
- `dbt_project/models/intermediate/int_identity_pass_2_name_dob_hire.sql`
- `dbt_project/models/intermediate/int_identity_pass_3_email_domain.sql`
- `dbt_project/models/intermediate/int_canonical_person.sql`
- `dbt_project/models/intermediate/int_stewardship_queue.sql`
### 7. dbt Core Marts
- `dbt_project/models/marts/core/_core.yml`
- `dbt_project/models/marts/core/dim_employee.sql`
- `dbt_project/models/marts/core/fct_workforce_daily.sql`
### 8. dbt Privacy-Safe People Analytics Marts
- `dbt_project/models/marts/people_analytics/_people_analytics.yml`
- `dbt_project/models/marts/people_analytics/_exposures.yml`
- `dbt_project/models/marts/people_analytics/workforce_headcount_daily.sql`
- `dbt_project/models/marts/people_analytics/workforce_attrition_monthly.sql`
- `dbt_project/models/marts/people_analytics/privacy_suppression_summary.sql`
- `dbt_project/models/marts/people_analytics/privacy_audit_log.sql`
### 9. dbt Custom Tests
- `dbt_project/tests/macros/test_normalize_name.sql`
- `dbt_project/tests/macros/test_first_name_root.sql`
- `dbt_project/tests/macros/test_privacy_macros.sql`
- `dbt_project/tests/intermediate/int_identity_source_nodes__covers_expected_grain.sql`
- `dbt_project/tests/intermediate/int_hris_persons__no_orphan_spells.sql`
- `dbt_project/tests/intermediate/int_payroll_spells__period_range_valid.sql`
- `dbt_project/tests/intermediate/int_payroll_spells__no_orphan_periods.sql`
- `dbt_project/tests/intermediate/int_dms_erp_unified__broken_link_implies_erp.sql`
- `dbt_project/tests/intermediate/int_dms_erp_unified__dms_user_id_unique_where_not_null.sql`
- `dbt_project/tests/intermediate/int_dms_erp_unified__erp_user_id_unique_where_not_null.sql`
- `dbt_project/tests/intermediate/int_dms_erp_unified__every_row_has_source_node.sql`
- `dbt_project/tests/intermediate/int_canonical_person__hris_work_email_local_stability.sql`
- `dbt_project/tests/intermediate/int_canonical_person__no_duplicate_source_auto_matches.sql`
- `dbt_project/tests/intermediate/int_canonical_person__no_orphan_source_nodes.sql`
- `dbt_project/tests/intermediate/int_stewardship_queue__no_resolved_source_overlap.sql`
- `dbt_project/tests/marts/dim_employee__covers_hris_spells.sql`
- `dbt_project/tests/marts/dim_employee__no_overlapping_effective_dates.sql`
- `dbt_project/tests/marts/fct_workforce_daily__active_one_row_per_person_day.sql`
- `dbt_project/tests/marts/fct_workforce_daily__date_bounds_valid.sql`
- `dbt_project/tests/marts/workforce_headcount_daily__suppressed_metrics_null.sql`
- `dbt_project/tests/marts/workforce_headcount_daily__suppresses_small_cohorts.sql`
- `dbt_project/tests/marts/workforce_attrition_monthly__suppressed_metrics_null.sql`
- `dbt_project/tests/marts/workforce_attrition_monthly__suppresses_small_cohorts.sql`
- `dbt_project/tests/marts/privacy_suppression_summary__matches_public_surfaces.sql`
- `dbt_project/tests/marts/privacy__no_direct_employee_identifiers_in_people_analytics.sql`
### 10. Airflow Orchestration
- `airflow/dags/atlas_people_analytics.py`
### 11. FastAPI Metrics Service
- `api/__init__.py`
- `api/settings.py`
- `api/snowflake_client.py`
- `api/metrics_service.py`
### 12. Streamlit Dashboard
- `dashboard/__init__.py`
- `dashboard/app.py`
### 13. Phase 5 Residual Review Engine
- `identity_engine/__init__.py`
- `identity_engine/residual_matcher.py`
- `identity_engine/evaluation.py`
- `identity_engine/snowflake_io.py`
- `identity_engine/cli.py`
### 14. Python Tests
- `tests/test_phase4_api.py`
- `tests/test_phase4_dag_dashboard.py`
- `tests/test_phase5_residual_matcher.py`

## Full Source Walkthrough Appendix

Each file below has a short purpose note followed by the full source. This is the section to use when you want to trace every SQL query and every Python code path.

### 1. `Makefile`

**Purpose:** The command surface for the whole project: install, seed, build, test, lint, serve the API/dashboard, and syntax-check the Airflow DAG.

**Source:**

```makefile
# =============================================================================
# Atlas Makefile — the project's command surface
#
# Run `make help` to see all available targets.
# =============================================================================

.PHONY: help install snowflake-init seed build test lint format clean dashboard api dag-test all

PYTHON := python
PIP := pip
DBT_DIR := dbt_project

# Default target
help:
	@echo "Atlas — People Analytics Foundation"
	@echo ""
	@echo "Setup:"
	@echo "  install          Install Python dependencies (run inside a venv)"
	@echo "  snowflake-init   Provision Snowflake objects (one-time, requires .env)"
	@echo ""
	@echo "Pipeline:"
	@echo "  seed             Generate synthetic data and load to RAW schema"
	@echo "  build            Run dbt build (deps + run + test)"
	@echo "  test             Run dbt tests + Python tests"
	@echo "  all              seed + build + test"
	@echo ""
	@echo "Quality:"
	@echo "  lint             Run ruff and mypy"
	@echo "  format           Auto-format code with ruff"
	@echo ""
	@echo "Local serving:"
	@echo "  dashboard        Launch the Streamlit HRBP dashboard"
	@echo "  api              Launch the FastAPI metrics service"
	@echo "  dag-test         Syntax-check the Airflow DAG"
	@echo ""
	@echo "Maintenance:"
	@echo "  clean            Remove build artifacts and caches"

# -----------------------------------------------------------------------------
# Setup
# -----------------------------------------------------------------------------
install:
	$(PIP) install -e ".[dev]"
	cd $(DBT_DIR) && dbt deps

snowflake-init:
	@echo "Provisioning Snowflake objects (database, warehouse, role, schemas)..."
	@bash scripts/snowflake_init.sh

# -----------------------------------------------------------------------------
# Pipeline
# -----------------------------------------------------------------------------
seed:
	$(PYTHON) -m seeds.synthesize

build:
	cd $(DBT_DIR) && dbt build --target dev

test:
	cd $(DBT_DIR) && dbt test --target dev
	pytest tests/

all: seed build test

# -----------------------------------------------------------------------------
# Quality
# -----------------------------------------------------------------------------
lint:
	ruff check api dashboard airflow identity_engine tests
	mypy --config-file pyproject.toml identity_engine api dashboard

format:
	ruff check --fix api dashboard airflow identity_engine tests
	ruff format api dashboard airflow identity_engine tests

# -----------------------------------------------------------------------------
# Local serving
# -----------------------------------------------------------------------------
dashboard:
	streamlit run dashboard/app.py --server.port $${ATLAS_DASHBOARD_PORT:-8501}

api:
	uvicorn api.metrics_service:app --reload --host $${ATLAS_API_HOST:-127.0.0.1} --port $${ATLAS_API_PORT:-8000}

dag-test:
	$(PYTHON) -m py_compile airflow/dags/atlas_people_analytics.py

# -----------------------------------------------------------------------------
# Maintenance
# -----------------------------------------------------------------------------
clean:
	rm -rf build/ dist/ *.egg-info
	rm -rf .pytest_cache/ .mypy_cache/ .ruff_cache/ .coverage htmlcov/
	rm -rf $(DBT_DIR)/target/ $(DBT_DIR)/dbt_packages/ $(DBT_DIR)/logs/
	rm -rf seeds/output/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
```

### 2. `pyproject.toml`

**Purpose:** Python packaging and quality configuration: dependencies, dev tooling, ruff, and mypy settings.

**Source:**

```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "atlas-people-analytics"
version = "0.1.0"
description = "Canonical employee record and People Analytics foundation on dbt + Snowflake"
readme = "README.md"
requires-python = ">=3.11"
license = { text = "MIT" }
authors = [{ name = "Omar Abdalla" }]

dependencies = [
    # Core data
    "snowflake-connector-python>=3.7.0",
    "snowflake-sqlalchemy>=1.5.1",
    "sqlalchemy>=2.0.25",
    "pandas>=2.2.0",
    "pyarrow>=15.0.0",
    "python-dotenv>=1.0.0",

    # Synthesis
    "faker>=22.5.0",
    "pyyaml>=6.0.1",

    # Identity resolution (deterministic + ML residual)
    "rapidfuzz>=3.6.0",
    "unidecode>=1.3.8",

    # API + Dashboard
    "fastapi>=0.110.0",
    "uvicorn[standard]>=0.27.0",
    "streamlit>=1.31.0",

    # dbt
    "dbt-core>=1.7.0",
    "dbt-snowflake>=1.7.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-cov>=4.1.0",
    "ruff>=0.2.0",
    "mypy>=1.8.0",
    "pre-commit>=3.6.0",
    "ipython>=8.20.0",
]
airflow = [
    "apache-airflow>=2.8.0",
    "apache-airflow-providers-snowflake>=5.3.0",
]

[tool.setuptools.packages.find]
where = ["."]
include = ["atlas*", "seeds*", "identity_engine*", "api*", "dashboard*"]

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "N", "W", "B", "UP", "SIM", "RUF"]
ignore = ["E501"]  # handled by formatter

[tool.pytest.ini_options]
minversion = "8.0"
addopts = "-ra -q --strict-markers --strict-config"
testpaths = ["tests"]
markers = [
    "slow: marks tests as slow (deselect with '-m \"not slow\"')",
    "snowflake: marks tests requiring a live Snowflake connection",
]

[tool.mypy]
python_version = "3.11"
warn_unused_configs = true
warn_return_any = true
disallow_untyped_defs = false
ignore_missing_imports = true
```

### 3. `infra/snowflake/00_provision.sql`

**Purpose:** Provisioning DDL for Snowflake database, schemas, warehouse, role, and grants.

**Source:**

```sql
-- =============================================================================
-- Atlas — Snowflake Provisioning Script
-- =============================================================================
-- This script creates a clean, isolated environment for the Atlas project:
--   - A dedicated database (ATLAS) with schemas for each pipeline tier
--   - A dedicated warehouse (ATLAS_WH) with auto-suspend to control cost
--   - A dedicated role (ATLAS_DEVELOPER) with least-privilege grants
--
-- Run as ACCOUNTADMIN.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- Warehouse: small, auto-suspend to keep credit usage minimal
-- -----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS ATLAS_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60          -- suspend after 60 seconds of inactivity
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Compute warehouse for the Atlas People Analytics project';

-- -----------------------------------------------------------------------------
-- Database and schemas (medallion architecture)
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS ATLAS
    COMMENT = 'Atlas — canonical employee record + people analytics';

USE DATABASE ATLAS;

CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Untransformed source-system mirrors. Loaded by seed pipeline.';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'dbt staging models — type-cast and renamed source mirrors.';

CREATE SCHEMA IF NOT EXISTS INTERMEDIATE
    COMMENT = 'dbt intermediate models — identity resolution and matching.';

CREATE SCHEMA IF NOT EXISTS MARTS_CORE
    COMMENT = 'dbt marts — core dimensional models (dim_employee SCD2, facts).';

CREATE SCHEMA IF NOT EXISTS MARTS_PEOPLE
    COMMENT = 'dbt marts — People Analytics business-facing models.';

CREATE SCHEMA IF NOT EXISTS AUDIT
    COMMENT = 'Audit log for privacy-sensitive metric access.';

-- -----------------------------------------------------------------------------
-- Role: ATLAS_DEVELOPER (least-privilege for the pipeline)
-- -----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS ATLAS_DEVELOPER
    COMMENT = 'Developer role for the Atlas project — read/write on ATLAS DB only';

-- Warehouse usage
GRANT USAGE ON WAREHOUSE ATLAS_WH TO ROLE ATLAS_DEVELOPER;
GRANT OPERATE ON WAREHOUSE ATLAS_WH TO ROLE ATLAS_DEVELOPER;

-- Database-level
GRANT USAGE ON DATABASE ATLAS TO ROLE ATLAS_DEVELOPER;
GRANT CREATE SCHEMA ON DATABASE ATLAS TO ROLE ATLAS_DEVELOPER;

-- Schema-level — full access to all Atlas schemas
GRANT ALL PRIVILEGES ON SCHEMA ATLAS.RAW TO ROLE ATLAS_DEVELOPER;
GRANT ALL PRIVILEGES ON SCHEMA ATLAS.STAGING TO ROLE ATLAS_DEVELOPER;
GRANT ALL PRIVILEGES ON SCHEMA ATLAS.INTERMEDIATE TO ROLE ATLAS_DEVELOPER;
GRANT ALL PRIVILEGES ON SCHEMA ATLAS.MARTS_CORE TO ROLE ATLAS_DEVELOPER;
GRANT ALL PRIVILEGES ON SCHEMA ATLAS.MARTS_PEOPLE TO ROLE ATLAS_DEVELOPER;
GRANT ALL PRIVILEGES ON SCHEMA ATLAS.AUDIT TO ROLE ATLAS_DEVELOPER;

-- Future objects (so dbt can create new tables/views without re-granting)
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE ATLAS TO ROLE ATLAS_DEVELOPER;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN DATABASE ATLAS TO ROLE ATLAS_DEVELOPER;

-- -----------------------------------------------------------------------------
-- Assign the role to your user
-- -----------------------------------------------------------------------------
-- IMPORTANT: replace OMARBINASH with your Snowflake username if different
GRANT ROLE ATLAS_DEVELOPER TO USER OMARBINASH;

-- Make ATLAS_DEVELOPER the default role for that user (optional but convenient)
ALTER USER OMARBINASH SET DEFAULT_ROLE = ATLAS_DEVELOPER;
ALTER USER OMARBINASH SET DEFAULT_WAREHOUSE = ATLAS_WH;
ALTER USER OMARBINASH SET DEFAULT_NAMESPACE = ATLAS.RAW;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
-- After running this script, switch to the role and verify access:
--
--   USE ROLE ATLAS_DEVELOPER;
--   USE WAREHOUSE ATLAS_WH;
--   USE DATABASE ATLAS;
--   SHOW SCHEMAS;
--
-- You should see RAW, STAGING, INTERMEDIATE, MARTS_CORE, MARTS_PEOPLE, AUDIT,
-- plus the default INFORMATION_SCHEMA and PUBLIC.
```

### 4. `infra/snowflake/01_raw_tables.sql`

**Purpose:** Raw schema table DDL for the six synthetic operational systems.

**Source:**

```sql
-- =============================================================================
-- Atlas — Raw Source Tables
-- =============================================================================
-- Five source systems, each with a different name representation strategy
-- (the realistic mess we are about to model and resolve).
--
-- Run as ATLAS_DEVELOPER after 00_provision.sql.
-- =============================================================================

USE ROLE ATLAS_DEVELOPER;
USE WAREHOUSE ATLAS_WH;
USE DATABASE ATLAS;
USE SCHEMA RAW;

-- -----------------------------------------------------------------------------
-- HRIS (BambooHR-shape) — legal first + last name, source of truth for employment
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_HRIS_EMPLOYEES (
    HRIS_EMPLOYEE_ID         VARCHAR(32)   NOT NULL,
    LEGAL_FIRST_NAME         VARCHAR(100)  NOT NULL,
    LEGAL_LAST_NAME          VARCHAR(100)  NOT NULL,
    PREFERRED_NAME           VARCHAR(100),
    DATE_OF_BIRTH            DATE,
    PERSONAL_EMAIL           VARCHAR(255),
    WORK_EMAIL               VARCHAR(255),
    HIRE_DATE                DATE          NOT NULL,
    TERMINATION_DATE         DATE,
    EMPLOYMENT_STATUS        VARCHAR(32)   NOT NULL,  -- ACTIVE | TERMINATED | ON_LEAVE
    EMPLOYMENT_TYPE          VARCHAR(32),             -- FTE | CONTRACTOR | PART_TIME
    DEPARTMENT               VARCHAR(100),
    JOB_TITLE                VARCHAR(150),
    MANAGER_HRIS_ID          VARCHAR(32),
    LOCATION                 VARCHAR(100),
    LOADED_AT                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- ATS (Greenhouse-shape) — preferred name, only knows people from hire to start date
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_ATS_CANDIDATES (
    ATS_CANDIDATE_ID         VARCHAR(32)   NOT NULL,
    PREFERRED_FIRST_NAME     VARCHAR(100)  NOT NULL,
    LAST_NAME                VARCHAR(100)  NOT NULL,
    EMAIL                    VARCHAR(255),
    PHONE                    VARCHAR(32),
    APPLICATION_DATE         DATE,
    OFFER_ACCEPTED_DATE      DATE,
    SOURCED_FROM             VARCHAR(64),
    REQUISITION_DEPARTMENT   VARCHAR(100),
    REQUISITION_JOB_TITLE    VARCHAR(150),
    LOADED_AT                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- Payroll (ADP-shape) — legal name, includes SIN/last-4 (sensitive!)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_PAYROLL_RECORDS (
    PAYROLL_RECORD_ID        VARCHAR(32)   NOT NULL,
    EMPLOYEE_PAYROLL_ID      VARCHAR(32)   NOT NULL,    -- payroll's own ID, separate from HRIS
    LEGAL_FIRST_NAME         VARCHAR(100)  NOT NULL,
    LEGAL_LAST_NAME          VARCHAR(100)  NOT NULL,
    SIN_LAST_4               VARCHAR(4),                -- last 4 only (sensitive)
    PAY_PERIOD_START         DATE          NOT NULL,
    PAY_PERIOD_END           DATE          NOT NULL,
    GROSS_AMOUNT_CAD         NUMBER(12,2),
    HOURS_WORKED             NUMBER(8,2),
    JOB_CODE                 VARCHAR(64),
    COST_CENTER              VARCHAR(64),
    LOADED_AT                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- CRM (Dabadu-shape) — preferred name, captures sales-floor activity
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_CRM_SALES_REPS (
    CRM_USER_ID              VARCHAR(32)   NOT NULL,
    PREFERRED_FIRST_NAME     VARCHAR(100)  NOT NULL,
    LAST_NAME                VARCHAR(100)  NOT NULL,
    DISPLAY_NAME             VARCHAR(200),              -- often "First Last" but spelled idiosyncratically
    CRM_EMAIL                VARCHAR(255),
    LOCATION_ID              VARCHAR(32),
    ROLE                     VARCHAR(64),               -- SALES_REP | F_AND_I | SERVICE_ADVISOR | MANAGER
    ACTIVE                   BOOLEAN       NOT NULL,
    CREATED_AT               TIMESTAMP_NTZ,
    DEACTIVATED_AT           TIMESTAMP_NTZ,
    LOADED_AT                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- DMS (PBS-shape) — shortened first name, deal flow + commissions
-- This is the system that mirrors into the ERP
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_DMS_USERS (
    DMS_USER_ID              VARCHAR(32)   NOT NULL,
    SHORT_FIRST_NAME         VARCHAR(50)   NOT NULL,    -- truncated/abbreviated
    LAST_NAME                VARCHAR(100)  NOT NULL,
    DMS_USERNAME             VARCHAR(64),               -- system login
    LOCATION_CODE            VARCHAR(16),
    DEPARTMENT_CODE          VARCHAR(16),               -- NEW | USED | F&I | SVC | PARTS | BDC
    HIRE_DATE_DMS            DATE,                      -- often differs slightly from HRIS hire date
    TERMINATED_DATE_DMS      DATE,
    LOADED_AT                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- ERP (custom) — mirrors DMS but adds an internal user_id and audit trail
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_ERP_USERS (
    ERP_USER_ID              VARCHAR(32)   NOT NULL,
    LINKED_DMS_USER_ID       VARCHAR(32),               -- foreign-key into DMS, sometimes NULL
    SHORT_FIRST_NAME         VARCHAR(50)   NOT NULL,    -- pulled from DMS
    LAST_NAME                VARCHAR(100)  NOT NULL,
    ERP_EMAIL                VARCHAR(255),
    ROLE_CODE                VARCHAR(32),
    PERMISSIONS_GROUP        VARCHAR(64),
    CREATED_AT               TIMESTAMP_NTZ,
    LAST_LOGIN_AT            TIMESTAMP_NTZ,
    LOADED_AT                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
SHOW TABLES IN SCHEMA ATLAS.RAW;
```

### 5. `seeds/__init__.py`

**Purpose:** Python implementation module in the project runtime.

**Source:**

```python
"""Atlas synthetic data generation."""
```

### 6. `seeds/name_strategies.py`

**Purpose:** Identity-drift vocabulary: canonical identity dataclasses, nicknames, name variants, and system-specific representations.

**Source:**

```python
"""
Name representation strategies for the five Atlas source systems.

This module models the *real* asymmetry in how operational systems represent
the same human:

    HRIS          → legal first + last name, plus optional preferred name
    ATS           → preferred first + last name (recruiters call you what you ask)
    Payroll       → legal first + last name, with rigid formatting (matches T4)
    CRM           → preferred first + last name (sales floor uses what's on the desk plate)
    DMS           → SHORTENED first name + last name (the user typed it in once on day one)
    ERP           → mirrors DMS with occasional drift (manual edits over years)

The asymmetry above is what makes the canonical-employee-record problem hard
in real organizations. This is not a typo problem; it is a representation
problem. No amount of fuzzy matching on first-name strings will resolve
"Robert" (legal/HRIS) → "Bob" (DMS) → "Bobby" (CRM) without explicit awareness
that systems have different naming *contracts*, not different *spellings*.
"""

from __future__ import annotations

import random
import re
import unicodedata
from dataclasses import dataclass

from unidecode import unidecode


# -----------------------------------------------------------------------------
# Common nickname / shortened-name pairs
# -----------------------------------------------------------------------------
# These are the realistic "Robert → Bob" mappings that fuzzy matching alone
# cannot solve. The data generator uses these to deliberately diverge legal
# names from preferred names from shortened names.
NICKNAME_MAP: dict[str, list[str]] = {
    # Classic English nicknames
    "Robert": ["Bob", "Rob", "Bobby"],
    "William": ["Bill", "Will", "Billy"],
    "Richard": ["Rick", "Dick", "Rich"],
    "James": ["Jim", "Jimmy", "Jamie"],
    "John": ["Jack", "Johnny"],
    "Michael": ["Mike", "Mikey"],
    "Christopher": ["Chris", "Topher"],
    "Matthew": ["Matt", "Matty"],
    "Joshua": ["Josh"],
    "Daniel": ["Dan", "Danny"],
    "David": ["Dave", "Davey"],
    "Anthony": ["Tony", "Ant"],
    "Andrew": ["Andy", "Drew"],
    "Steven": ["Steve"],
    "Stephen": ["Steve", "Steph"],
    "Edward": ["Ed", "Eddie", "Ted"],
    "Thomas": ["Tom", "Tommy"],
    "Charles": ["Charlie", "Chuck", "Chaz"],
    "Joseph": ["Joe", "Joey"],
    "Benjamin": ["Ben", "Benny"],
    "Nicholas": ["Nick", "Nicky"],
    "Alexander": ["Alex", "Xander", "Sasha"],
    "Jonathan": ["Jon", "Jonny", "Nathan"],
    "Patrick": ["Pat", "Paddy"],
    "Timothy": ["Tim", "Timmy"],
    "Samuel": ["Sam", "Sammy"],
    "Gregory": ["Greg", "Gregg"],
    "Frederick": ["Fred", "Freddie", "Rick"],
    "Lawrence": ["Larry", "Lars"],
    # Female
    "Elizabeth": ["Liz", "Beth", "Eliza", "Lizzie", "Betty"],
    "Catherine": ["Cathy", "Kate", "Katie", "Cat"],
    "Katherine": ["Kate", "Katie", "Kathy", "Kat"],
    "Margaret": ["Maggie", "Meg", "Peggy", "Marge"],
    "Patricia": ["Pat", "Patty", "Trish", "Tricia"],
    "Jennifer": ["Jen", "Jenny", "Jenni"],
    "Jessica": ["Jess", "Jessie"],
    "Stephanie": ["Steph", "Stephie"],
    "Christina": ["Chris", "Tina", "Christy"],
    "Christine": ["Chris", "Christy", "Tina"],
    "Samantha": ["Sam", "Sammy"],
    "Alexandra": ["Alex", "Sasha", "Lexi", "Sandy"],
    "Rebecca": ["Becky", "Becca"],
    "Deborah": ["Deb", "Debbie"],
    "Barbara": ["Barb", "Babs"],
    "Susan": ["Sue", "Susie"],
    "Sandra": ["Sandy", "Sandi"],
    "Charlotte": ["Charlie", "Lottie", "Char"],
    "Victoria": ["Vicky", "Tori"],
    "Nicole": ["Nicki", "Nikki"],
    "Michelle": ["Shell", "Mish"],
    # South Asian (common in Canadian dealership demographics)
    "Rajesh": ["Raj"],
    "Harpreet": ["Harry", "Harp"],
    "Amandeep": ["Aman", "Andy"],
    "Manpreet": ["Mani"],
    "Gurpreet": ["Gary", "Gurp"],
    "Jaspreet": ["Jas"],
    "Surinder": ["Sunny"],
    "Inderjit": ["Indy"],
    "Mohammed": ["Mo", "Mohamed"],
    "Muhammad": ["Mo", "Mohammad"],
    "Abdullah": ["Abdul", "Abdi"],
    # East Asian
    "Xiaoming": ["Ming", "Mike"],
    "Wenjie": ["Wen", "Will"],
    # Hispanic
    "Francisco": ["Frank", "Paco", "Cisco"],
    "Guillermo": ["Willy", "Memo"],
    "Alejandro": ["Alex", "Ale"],
    "Eduardo": ["Eddie", "Ed", "Lalo"],
    "Roberto": ["Rob", "Bob", "Beto"],
}


def _normalize_for_dms(first_name: str) -> str:
    """
    Mimic how someone types a name into a DMS on day one in a hurry.

    The DMS at 401 had ~50-character first-name field but users
    routinely typed shorter/sloppier versions because:
      1. The form was tedious; fewer keystrokes = faster
      2. The "salesperson display" on deals showed only ~10 chars anyway
      3. Once typed, no one ever updated it

    Patterns observed in real dealership DMS data:
      - First-name truncation to 4-8 chars ("Christop" for "Christopher")
      - Drop accents and diacritics ("Jose" for "José")
      - Single-name variants ("Mo" for "Mohammed")
      - Last-name initial only ("Mike S" for "Mike Sanchez")
    """
    cleaned = unidecode(first_name).strip()

    # 30% of records get truncated to 6-8 chars if the original was longer
    if len(cleaned) > 8 and random.random() < 0.30:
        cleaned = cleaned[: random.randint(6, 8)]

    return cleaned


@dataclass(frozen=True)
class CanonicalIdentity:
    """
    The 'true' identity of a person, before any source-system distortion.

    Generated once per synthetic employee, then projected into the five source
    systems with deliberate representation drift. This is the ground truth that
    Atlas's identity-resolution layer is supposed to recover — without ever
    seeing this struct directly.
    """

    person_id: str  # internal-only, used for evaluation, not exposed to dbt
    legal_first_name: str
    legal_last_name: str
    preferred_first_name: str  # what they want to be called day-to-day
    short_first_name: str  # how it ends up in the DMS
    date_of_birth: str  # YYYY-MM-DD
    personal_email: str
    work_email_local_part: str  # e.g. "sarah.kim" — combined with company domain


def _pick_preferred_from_legal(legal_first: str) -> str:
    """
    Decide what someone goes by at work, given their legal name.

    Distribution roughly matches what we observed at 401:
      - 70% use their legal first name
      - 20% use a common nickname (Robert → Bob)
      - 10% use a totally different preferred name (Mohammed → Mike)
    """
    if legal_first in NICKNAME_MAP and random.random() < 0.30:
        return random.choice(NICKNAME_MAP[legal_first])
    return legal_first


def build_canonical_identity(
    *,
    person_id: str,
    legal_first_name: str,
    legal_last_name: str,
    date_of_birth: str,
    company_email_domain: str = "401auto.com",
) -> CanonicalIdentity:
    """Construct a CanonicalIdentity with realistic name drift baked in."""
    preferred = _pick_preferred_from_legal(legal_first_name)
    short = _normalize_for_dms(preferred)

    # Email local part: usually based on legal name (HR sets it up first)
    work_local = f"{legal_first_name.lower()}.{legal_last_name.lower()}"
    work_local = re.sub(r"[^a-z.]", "", unidecode(work_local))

    personal_email = f"{preferred.lower()}.{legal_last_name.lower()}@gmail.com"
    personal_email = re.sub(r"[^a-z.@]", "", unidecode(personal_email))

    return CanonicalIdentity(
        person_id=person_id,
        legal_first_name=legal_first_name,
        legal_last_name=legal_last_name,
        preferred_first_name=preferred,
        short_first_name=short,
        date_of_birth=date_of_birth,
        personal_email=personal_email,
        work_email_local_part=work_local,
    )


def normalize_name_for_matching(name: str) -> str:
    """
    Canonical normalization used by the dbt identity-resolution layer.

    This is the ground-truth transformation that the dbt macro
    `normalize_name` is intended to mirror. We keep it here so the
    Python tests can verify equivalence with the SQL implementation.
    """
    if not name:
        return ""
    s = unidecode(name).lower().strip()
    s = re.sub(r"[^a-z]", "", s)  # strip non-alpha (hyphens, apostrophes, spaces)
    return s
```

### 7. `seeds/lifecycle.py`

**Purpose:** Synthetic employee lifecycle event generation: hires, rehires, terminations, transfers, and time-bounded employment spells.

**Source:**

```python
"""
Lifecycle event generation for the Atlas synthetic dataset.

The hard part of canonical-employee-record matching is not the snapshot view —
it's what happens *over time*:

    - Sarah Kim gets married → Sarah Kim-Patel → Sarah Patel (HRIS updated, DMS not)
    - Carlos Mendez quits in Q2 → rehired Q1 next year with a new HRIS_ID
    - Alex Chen converts from contractor to FTE (different employee_type, same person)
    - 50 employees come in via an acquisition with a different ID schema
    - An employee gets transferred between rooftops, getting a new DMS_USER_ID

This module synthesizes those events on top of the static identity records,
so the resulting source-system tables exhibit the kind of temporal complexity
a real People Analytics function inherits.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import Enum

from .name_strategies import CanonicalIdentity


class LifecycleEventType(str, Enum):
    HIRE = "HIRE"
    TERMINATE = "TERMINATE"
    REHIRE = "REHIRE"
    INTERNAL_TRANSFER = "INTERNAL_TRANSFER"
    NAME_CHANGE_MARRIAGE = "NAME_CHANGE_MARRIAGE"
    CONTRACTOR_TO_FTE = "CONTRACTOR_TO_FTE"
    ACQUISITION_LIFT = "ACQUISITION_LIFT"


@dataclass(frozen=True)
class LifecycleEvent:
    """A single thing that happens to a person over time."""

    person_id: str
    event_type: LifecycleEventType
    event_date: date
    # Free-form payload — interpretation depends on event_type
    payload: dict = field(default_factory=dict)


def _random_date_between(start: date, end: date) -> date:
    """Inclusive random date in [start, end]."""
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, max(delta, 0)))


def generate_employee_lifecycle(
    *,
    identity: CanonicalIdentity,
    earliest_hire: date,
    latest_hire: date,
    today: date,
    initial_department: str,
    initial_location: str,
    initial_employment_type: str = "FTE",
) -> list[LifecycleEvent]:
    """
    Generate a realistic sequence of lifecycle events for one person.

    Distribution of patterns (calibrated to roughly match a real dealer-group
    population observed at 401 over 5 years):

      - ~75% have a single uninterrupted tenure
      - ~10% are terminated and never rehired
      - ~5% are terminated and rehired (the rehire problem we care most about)
      - ~7% have an internal transfer between locations or departments
      - ~5% have a name change (marriage, divorce, legal change)
      - ~3% are contractors who convert to FTE
      - ~2% come in via an acquisition lift
    """
    events: list[LifecycleEvent] = []

    hire_date = _random_date_between(earliest_hire, latest_hire)

    events.append(
        LifecycleEvent(
            person_id=identity.person_id,
            event_type=LifecycleEventType.HIRE,
            event_date=hire_date,
            payload={
                "department": initial_department,
                "location": initial_location,
                "employment_type": initial_employment_type,
                "via_acquisition": random.random() < 0.02,
            },
        )
    )

    # ----- Did they get terminated? -----
    terminated_pct = 0.15  # 15% have a termination at some point
    if random.random() < terminated_pct:
        # Terminate sometime between 6 months after hire and today
        earliest_term = hire_date + timedelta(days=180)
        if earliest_term < today:
            term_date = _random_date_between(earliest_term, today)
            events.append(
                LifecycleEvent(
                    person_id=identity.person_id,
                    event_type=LifecycleEventType.TERMINATE,
                    event_date=term_date,
                    payload={"reason": random.choice(["VOLUNTARY", "INVOLUNTARY", "RETIREMENT"])},
                )
            )

            # ----- Of terminated, ~30% are eventually rehired -----
            if random.random() < 0.30:
                earliest_rehire = term_date + timedelta(days=90)
                if earliest_rehire < today:
                    rehire_date = _random_date_between(earliest_rehire, today)
                    events.append(
                        LifecycleEvent(
                            person_id=identity.person_id,
                            event_type=LifecycleEventType.REHIRE,
                            event_date=rehire_date,
                            payload={
                                # Different department / location is common on rehire
                                "department": random.choice(["NEW", "USED", "F&I", "SVC", "BDC"]),
                                "location": initial_location,  # usually same rooftop
                                "new_hris_id": True,  # ← the bane of analytics teams
                                "new_dms_id": True,
                            },
                        )
                    )

    # ----- Did they have an internal transfer? -----
    if random.random() < 0.07:
        transfer_date = _random_date_between(
            hire_date + timedelta(days=180),
            today - timedelta(days=30),
        )
        if transfer_date > hire_date:
            events.append(
                LifecycleEvent(
                    person_id=identity.person_id,
                    event_type=LifecycleEventType.INTERNAL_TRANSFER,
                    event_date=transfer_date,
                    payload={
                        "new_department": random.choice(["NEW", "USED", "F&I", "SVC"]),
                        "new_location": f"ROOFTOP_{random.randint(1, 40):02d}",
                        "issues_new_dms_id": random.random() < 0.6,
                    },
                )
            )

    # ----- Did they get married / change name? -----
    if random.random() < 0.05:
        change_date = _random_date_between(
            hire_date + timedelta(days=365),
            today - timedelta(days=30),
        )
        if change_date > hire_date:
            events.append(
                LifecycleEvent(
                    person_id=identity.person_id,
                    event_type=LifecycleEventType.NAME_CHANGE_MARRIAGE,
                    event_date=change_date,
                    payload={
                        # The new last name is generated at projection time
                        "old_last_name": identity.legal_last_name,
                        "hyphenate_first": random.random() < 0.30,  # Smith → Smith-Patel briefly
                    },
                )
            )

    # ----- Contractor → FTE conversion? -----
    if initial_employment_type == "CONTRACTOR" and random.random() < 0.40:
        conversion_date = _random_date_between(
            hire_date + timedelta(days=90),
            min(today, hire_date + timedelta(days=730)),
        )
        if conversion_date > hire_date:
            events.append(
                LifecycleEvent(
                    person_id=identity.person_id,
                    event_type=LifecycleEventType.CONTRACTOR_TO_FTE,
                    event_date=conversion_date,
                    payload={
                        # Often new HRIS_ID, often re-onboarded as if new
                        "issues_new_hris_id": random.random() < 0.7,
                    },
                )
            )

    return sorted(events, key=lambda e: e.event_date)
```

### 8. `seeds/synthesize.py`

**Purpose:** End-to-end synthetic data generator and Snowflake loader for all six raw source systems.

**Source:**

```python
"""
Atlas synthetic data generator.

Generates a realistic, time-evolved population of synthetic employees and
projects them into six operational source systems with deliberate name and
identity drift — exactly the kind of mess a real People Analytics function
inherits from the operational world.

Pipeline:

    1. Build N "true" canonical identities (the ground truth Atlas tries to recover)
    2. Generate lifecycle events on top of each identity over a configurable
       date range (hires, terminations, rehires, transfers, marriages,
       contractor conversions, acquisitions)
    3. Project each (identity, lifecycle) pair into the six source systems
       (HRIS, ATS, Payroll, CRM, DMS, ERP) with system-specific drift
    4. Write the result either to:
         a. CSV files in seeds/output/ (for local inspection / dbt seeds)
         b. Snowflake RAW.* tables directly (for the production pipeline)

Run:

    python -m seeds.synthesize                  # default: 1500 employees, CSV only
    python -m seeds.synthesize --count 5000     # specify count
    python -m seeds.synthesize --years 5        # historical depth
    python -m seeds.synthesize --load-snowflake # also push to Snowflake
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import random
import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from faker import Faker
from unidecode import unidecode

from .lifecycle import LifecycleEvent, LifecycleEventType, generate_employee_lifecycle
from .name_strategies import (
    CanonicalIdentity,
    _normalize_for_dms,
    build_canonical_identity,
)

# -----------------------------------------------------------------------------
# Setup
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("synthesize")

# -----------------------------------------------------------------------------
# Constants — calibrated to roughly match the 401-style dealership population
# -----------------------------------------------------------------------------
COMPANY_EMAIL_DOMAIN = "atlas-co.com"

DEPARTMENTS = [
    ("ENG", "Engineering"),
    ("PRD", "Product"),
    ("DSG", "Design"),
    ("DAT", "Data"),
    ("OPS", "Operations"),
    ("FIN", "Finance"),
    ("HR", "People"),
    ("MKT", "Marketing"),
    ("SAL", "Sales"),
    ("SUP", "Customer Support"),
    ("LEG", "Legal & Compliance"),
]

JOB_TITLES_BY_DEPT: dict[str, list[str]] = {
    "ENG": ["Software Engineer", "Senior Software Engineer", "Staff Engineer", "Engineering Manager"],
    "PRD": ["Product Manager", "Senior Product Manager", "Group Product Manager"],
    "DSG": ["Product Designer", "Senior Designer", "Design Lead"],
    "DAT": ["Data Analyst", "Data Engineer", "Senior Data Engineer", "Analytics Manager"],
    "OPS": ["Operations Analyst", "Operations Manager", "Director of Operations"],
    "FIN": ["Financial Analyst", "Senior Financial Analyst", "Finance Manager", "Controller"],
    "HR": ["People Partner", "Talent Acquisition", "People Analytics Lead", "Head of People"],
    "MKT": ["Marketing Specialist", "Senior Marketer", "Marketing Manager"],
    "SAL": ["Account Executive", "Senior AE", "Sales Manager"],
    "SUP": ["Support Specialist", "Senior Support", "Support Team Lead"],
    "LEG": ["Counsel", "Senior Counsel", "Compliance Manager"],
}

LOCATIONS = [
    ("TOR", "Toronto, ON"),
    ("MTL", "Montreal, QC"),
    ("VAN", "Vancouver, BC"),
    ("CAL", "Calgary, AB"),
    ("OTT", "Ottawa, ON"),
    ("WAT", "Waterloo, ON"),
    ("REM", "Remote — Canada"),
]

EMPLOYMENT_TYPES = ["FTE", "FTE", "FTE", "FTE", "CONTRACTOR", "PART_TIME"]  # weighted

# Distribution roughly approximating 5+ years of mid-size company name patterns
NAME_LOCALES = [
    ("en_CA", 0.55),  # Anglophone Canadian
    ("fr_CA", 0.20),  # Francophone Canadian
    ("en_IN", 0.10),  # South Asian-Canadian
    ("zh_CN", 0.05),  # East Asian-Canadian
    ("es_MX", 0.05),  # Hispanic-Canadian
    ("ar_AA", 0.05),  # Arabic-Canadian
]


# -----------------------------------------------------------------------------
# Configuration object
# -----------------------------------------------------------------------------
@dataclass
class SynthesizeConfig:
    """All knobs for one synthesis run."""

    employee_count: int = 1500
    years_of_history: int = 5
    random_seed: int = 42
    output_dir: Path = Path("seeds/output")
    load_to_snowflake: bool = False

    @property
    def earliest_hire_date(self) -> date:
        return date.today() - timedelta(days=365 * self.years_of_history)

    @property
    def latest_hire_date(self) -> date:
        # Stop hiring 30 days before today so there's some "stable" recent state
        return date.today() - timedelta(days=30)


# -----------------------------------------------------------------------------
# Step 1: Build canonical identities
# -----------------------------------------------------------------------------
def _weighted_choice(choices: list[tuple[Any, float]]) -> Any:
    """Pick from [(item, weight), ...] respecting weights."""
    total = sum(w for _, w in choices)
    pick = random.uniform(0, total)
    cumulative = 0.0
    for item, weight in choices:
        cumulative += weight
        if pick <= cumulative:
            return item
    return choices[-1][0]


def _faker_for_locale(locale: str) -> Faker:
    """Cache Faker instances per locale."""
    if not hasattr(_faker_for_locale, "_cache"):
        _faker_for_locale._cache = {}  # type: ignore[attr-defined]
    cache = _faker_for_locale._cache  # type: ignore[attr-defined]
    if locale not in cache:
        cache[locale] = Faker(locale)
    return cache[locale]


def generate_canonical_population(config: SynthesizeConfig) -> list[CanonicalIdentity]:
    """Generate N canonical identities with realistic demographic distribution."""
    log.info("Generating %d canonical identities", config.employee_count)

    identities: list[CanonicalIdentity] = []
    for i in range(config.employee_count):
        person_id = f"P{i + 1:06d}"
        locale = _weighted_choice(NAME_LOCALES)
        fake = _faker_for_locale(locale)

        # Realistic gender mix - affects name generation
        if random.random() < 0.5:
            legal_first = fake.first_name_male()
        else:
            legal_first = fake.first_name_female()
        legal_last = fake.last_name()

        # DOB: working-age population, biased toward 25-45
        age_years = random.choices(
            population=[20, 25, 30, 35, 40, 45, 50, 55, 60],
            weights=[0.05, 0.18, 0.25, 0.22, 0.15, 0.08, 0.04, 0.02, 0.01],
        )[0]
        dob = date.today() - timedelta(days=int(age_years * 365.25 + random.randint(-180, 180)))

        identities.append(
            build_canonical_identity(
                person_id=person_id,
                legal_first_name=legal_first,
                legal_last_name=legal_last,
                date_of_birth=dob.isoformat(),
                company_email_domain=COMPANY_EMAIL_DOMAIN,
            )
        )
    return identities


# -----------------------------------------------------------------------------
# Step 2: Generate lifecycle events
# -----------------------------------------------------------------------------
def generate_all_lifecycles(
    identities: list[CanonicalIdentity], config: SynthesizeConfig
) -> dict[str, list[LifecycleEvent]]:
    """For each identity, generate a sequence of lifecycle events over time."""
    log.info("Generating lifecycle events for %d people", len(identities))

    today = date.today()
    lifecycles: dict[str, list[LifecycleEvent]] = {}

    for identity in identities:
        dept_code = random.choice([d[0] for d in DEPARTMENTS])
        location_code = random.choice([loc[0] for loc in LOCATIONS])
        emp_type = random.choice(EMPLOYMENT_TYPES)

        events = generate_employee_lifecycle(
            identity=identity,
            earliest_hire=config.earliest_hire_date,
            latest_hire=config.latest_hire_date,
            today=today,
            initial_department=dept_code,
            initial_location=location_code,
            initial_employment_type=emp_type,
        )
        lifecycles[identity.person_id] = events

    # Log distribution for sanity check
    total_events = sum(len(evs) for evs in lifecycles.values())
    event_counts: dict[str, int] = {}
    for evs in lifecycles.values():
        for ev in evs:
            event_counts[ev.event_type.value] = event_counts.get(ev.event_type.value, 0) + 1
    log.info("  Generated %d total events across population", total_events)
    for et, cnt in sorted(event_counts.items()):
        log.info("    %s: %d", et, cnt)

    return lifecycles


# -----------------------------------------------------------------------------
# Step 3: Project into source systems
# -----------------------------------------------------------------------------
@dataclass
class SourceRecords:
    """All rows that will be written to the six raw tables."""

    hris_employees: list[dict] = None  # type: ignore[assignment]
    ats_candidates: list[dict] = None  # type: ignore[assignment]
    payroll_records: list[dict] = None  # type: ignore[assignment]
    crm_sales_reps: list[dict] = None  # type: ignore[assignment]
    dms_users: list[dict] = None  # type: ignore[assignment]
    erp_users: list[dict] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        for fld in (
            "hris_employees",
            "ats_candidates",
            "payroll_records",
            "crm_sales_reps",
            "dms_users",
            "erp_users",
        ):
            if getattr(self, fld) is None:
                setattr(self, fld, [])


def _id_for_system(system: str, person_id: str, suffix: str = "") -> str:
    """Generate a system-specific ID. Different systems use different ID schemes."""
    base = person_id.replace("P", "")
    if system == "HRIS":
        return f"HR_{base}{suffix}"
    if system == "ATS":
        return f"ats_{int(base):d}{suffix}"
    if system == "PAYROLL":
        return f"PAY-{base}{suffix}"
    if system == "CRM":
        return f"crm_user_{int(base):d}{suffix}"
    if system == "DMS":
        return f"DMS{int(base):05d}{suffix}"
    if system == "ERP":
        return f"erp.{int(base):d}{suffix}"
    raise ValueError(f"Unknown system: {system}")


def _project_hris(
    identity: CanonicalIdentity,
    events: list[LifecycleEvent],
) -> list[dict]:
    """
    Project into HRIS (BambooHR-style).

    HRIS gets a new row whenever there's a lifecycle event that changes
    employment status or identity (rehire, name change, contractor conversion).
    Real HRIS systems usually keep a single record per "current" employment
    spell, so we emit one row per spell (hire-to-termination period).
    """
    rows = []
    current_first = identity.legal_first_name
    current_last = identity.legal_last_name
    current_dept = None
    current_location = None
    current_emp_type = None
    current_hris_id = _id_for_system("HRIS", identity.person_id)
    current_hire_date: date | None = None
    rehire_count = 0

    for ev in events:
        if ev.event_type == LifecycleEventType.HIRE:
            current_dept = ev.payload["department"]
            current_location = ev.payload["location"]
            current_emp_type = ev.payload["employment_type"]
            current_hire_date = ev.event_date

        elif ev.event_type == LifecycleEventType.TERMINATE:
            assert current_hire_date is not None
            rows.append({
                "HRIS_EMPLOYEE_ID": current_hris_id,
                "LEGAL_FIRST_NAME": current_first,
                "LEGAL_LAST_NAME": current_last,
                "PREFERRED_NAME": identity.preferred_first_name
                if identity.preferred_first_name != current_first
                else None,
                "DATE_OF_BIRTH": identity.date_of_birth,
                "PERSONAL_EMAIL": identity.personal_email,
                "WORK_EMAIL": f"{identity.work_email_local_part}@{COMPANY_EMAIL_DOMAIN}",
                "HIRE_DATE": current_hire_date.isoformat(),
                "TERMINATION_DATE": ev.event_date.isoformat(),
                "EMPLOYMENT_STATUS": "TERMINATED",
                "EMPLOYMENT_TYPE": current_emp_type,
                "DEPARTMENT": current_dept,
                "JOB_TITLE": random.choice(JOB_TITLES_BY_DEPT.get(current_dept, ["Specialist"])),
                "MANAGER_HRIS_ID": None,  # left as null for simplicity in v1
                "LOCATION": current_location,
            })
            current_hire_date = None

        elif ev.event_type == LifecycleEventType.REHIRE:
            # Rehire creates a NEW HRIS_EMPLOYEE_ID — this is realistic and
            # exactly the canonical-record-survives-rehires problem we want.
            rehire_count += 1
            current_hris_id = _id_for_system("HRIS", identity.person_id, f"_R{rehire_count}")
            current_dept = ev.payload.get("department", current_dept)
            current_location = ev.payload.get("location", current_location)
            current_hire_date = ev.event_date

        elif ev.event_type == LifecycleEventType.INTERNAL_TRANSFER:
            current_dept = ev.payload.get("new_department", current_dept)
            current_location = ev.payload.get("new_location", current_location)

        elif ev.event_type == LifecycleEventType.NAME_CHANGE_MARRIAGE:
            # Generate a new last name to simulate marriage
            new_last_name = _faker_for_locale("en_CA").last_name()
            if ev.payload.get("hyphenate_first"):
                current_last = f"{current_last}-{new_last_name}"
            else:
                current_last = new_last_name

        elif ev.event_type == LifecycleEventType.CONTRACTOR_TO_FTE:
            current_emp_type = "FTE"
            if ev.payload.get("issues_new_hris_id"):
                current_hris_id = _id_for_system(
                    "HRIS", identity.person_id, f"_FTE"
                )

    # If no termination event was emitted, the person is still active —
    # write the current open spell now.
    if current_hire_date is not None:
        rows.append({
            "HRIS_EMPLOYEE_ID": current_hris_id,
            "LEGAL_FIRST_NAME": current_first,
            "LEGAL_LAST_NAME": current_last,
            "PREFERRED_NAME": identity.preferred_first_name
            if identity.preferred_first_name != current_first
            else None,
            "DATE_OF_BIRTH": identity.date_of_birth,
            "PERSONAL_EMAIL": identity.personal_email,
            "WORK_EMAIL": f"{identity.work_email_local_part}@{COMPANY_EMAIL_DOMAIN}",
            "HIRE_DATE": current_hire_date.isoformat(),
            "TERMINATION_DATE": None,
            "EMPLOYMENT_STATUS": "ACTIVE",
            "EMPLOYMENT_TYPE": current_emp_type,
            "DEPARTMENT": current_dept,
            "JOB_TITLE": random.choice(JOB_TITLES_BY_DEPT.get(current_dept or "OPS", ["Specialist"])),
            "MANAGER_HRIS_ID": None,
            "LOCATION": current_location,
        })

    return rows


def _project_ats(
    identity: CanonicalIdentity,
    events: list[LifecycleEvent],
) -> list[dict]:
    """
    Project into ATS (Greenhouse/Ashby-shape).

    ATS holds one record per application, NOT per employee. So we emit a row
    for each HIRE and REHIRE event (those represent applications that became
    offers).
    """
    rows = []
    application_count = 0

    for ev in events:
        if ev.event_type in (LifecycleEventType.HIRE, LifecycleEventType.REHIRE):
            application_count += 1
            ats_id = _id_for_system(
                "ATS",
                identity.person_id,
                f"_{application_count}" if application_count > 1 else "",
            )
            # Application date is 30-90 days before the offer-accepted date
            offer_date = ev.event_date
            app_date = offer_date - timedelta(days=random.randint(30, 90))

            rows.append({
                "ATS_CANDIDATE_ID": ats_id,
                "PREFERRED_FIRST_NAME": identity.preferred_first_name,
                "LAST_NAME": identity.legal_last_name,
                "EMAIL": identity.personal_email,
                "PHONE": _faker_for_locale("en_CA").phone_number(),
                "APPLICATION_DATE": app_date.isoformat(),
                "OFFER_ACCEPTED_DATE": offer_date.isoformat(),
                "SOURCED_FROM": random.choice(
                    ["LinkedIn", "Referral", "Career Site", "Indeed", "Recruiter"]
                ),
                "REQUISITION_DEPARTMENT": ev.payload.get("department"),
                "REQUISITION_JOB_TITLE": random.choice(
                    JOB_TITLES_BY_DEPT.get(ev.payload.get("department", "OPS"), ["Specialist"])
                ),
            })
    return rows


def _project_payroll(
    identity: CanonicalIdentity,
    events: list[LifecycleEvent],
) -> list[dict]:
    """
    Project into Payroll (ADP-style).

    Payroll emits one row per pay period the employee was active. To keep
    volume manageable, we'll emit monthly aggregates (12 per year of tenure).
    """
    rows = []

    # Determine active periods from events
    active_spells: list[tuple[date, date | None, dict]] = []  # (start, end, context)
    current_start: date | None = None
    current_context: dict = {}
    pay_id_counter = 0

    for ev in events:
        if ev.event_type in (LifecycleEventType.HIRE, LifecycleEventType.REHIRE):
            current_start = ev.event_date
            current_context = {
                "dept": ev.payload.get("department"),
                "location": ev.payload.get("location"),
            }
        elif ev.event_type == LifecycleEventType.TERMINATE:
            if current_start:
                active_spells.append((current_start, ev.event_date, current_context.copy()))
                current_start = None

    if current_start:
        active_spells.append((current_start, None, current_context.copy()))

    # Generate monthly payroll records for each spell
    for spell_start, spell_end, context in active_spells:
        end = spell_end or date.today()
        cursor = date(spell_start.year, spell_start.month, 1)
        # Payroll usually has its own employee ID, often unrelated to HRIS ID
        payroll_emp_id = f"PAY{spell_start.strftime('%Y%m')}-{identity.person_id[1:]}"

        while cursor < end:
            # Find period end (last day of month)
            if cursor.month == 12:
                next_month = date(cursor.year + 1, 1, 1)
            else:
                next_month = date(cursor.year, cursor.month + 1, 1)
            period_end = next_month - timedelta(days=1)

            pay_id_counter += 1
            rows.append({
                "PAYROLL_RECORD_ID": _id_for_system(
                    "PAYROLL", identity.person_id, f"_{cursor.strftime('%Y%m')}"
                ),
                "EMPLOYEE_PAYROLL_ID": payroll_emp_id,
                "LEGAL_FIRST_NAME": identity.legal_first_name,
                "LEGAL_LAST_NAME": identity.legal_last_name,  # Payroll doesn't always pick up name changes
                "SIN_LAST_4": f"{random.randint(1000, 9999)}",
                "PAY_PERIOD_START": cursor.isoformat(),
                "PAY_PERIOD_END": period_end.isoformat(),
                "GROSS_AMOUNT_CAD": round(random.uniform(4500, 14000), 2),
                "HOURS_WORKED": round(random.uniform(140, 180), 2),
                "JOB_CODE": context.get("dept", "GEN"),
                "COST_CENTER": f"CC-{context.get('location', 'TOR')}-{context.get('dept', 'GEN')}",
            })
            cursor = next_month

    return rows


def _project_crm(
    identity: CanonicalIdentity,
    events: list[LifecycleEvent],
) -> list[dict]:
    """
    Project into CRM (Dabadu-style).

    Only sales/customer-facing roles get CRM records. We emit one row per
    employment spell at a sales/support department.
    """
    rows = []
    crm_dept_codes = {"SAL", "SUP", "MKT"}
    spell_counter = 0
    current_spell_dept: str | None = None
    current_spell_loc: str | None = None
    current_spell_start: date | None = None

    for ev in events:
        if ev.event_type in (LifecycleEventType.HIRE, LifecycleEventType.REHIRE):
            current_spell_dept = ev.payload.get("department")
            current_spell_loc = ev.payload.get("location")
            current_spell_start = ev.event_date

        elif ev.event_type == LifecycleEventType.TERMINATE:
            if current_spell_dept in crm_dept_codes and current_spell_start:
                spell_counter += 1
                rows.append(_build_crm_row(
                    identity, current_spell_dept, current_spell_loc,
                    current_spell_start, ev.event_date, spell_counter,
                ))
            current_spell_dept = None
            current_spell_start = None

    if current_spell_dept in crm_dept_codes and current_spell_start:
        spell_counter += 1
        rows.append(_build_crm_row(
            identity, current_spell_dept, current_spell_loc,
            current_spell_start, None, spell_counter,
        ))
    return rows


def _build_crm_row(
    identity: CanonicalIdentity,
    dept: str | None,
    location: str | None,
    start: date,
    end: date | None,
    spell_index: int,
) -> dict:
    crm_id = _id_for_system(
        "CRM", identity.person_id,
        f"_{spell_index}" if spell_index > 1 else "",
    )
    # Display name in CRM is often a slightly idiosyncratic spelling
    display = f"{identity.preferred_first_name} {identity.legal_last_name}"
    return {
        "CRM_USER_ID": crm_id,
        "PREFERRED_FIRST_NAME": identity.preferred_first_name,
        "LAST_NAME": identity.legal_last_name,
        "DISPLAY_NAME": display,
        "CRM_EMAIL": f"{identity.preferred_first_name.lower()}.{identity.legal_last_name.lower()}@{COMPANY_EMAIL_DOMAIN}",
        "LOCATION_ID": location,
        "ROLE": {
            "SAL": "SALES_REP",
            "SUP": "SUPPORT_AGENT",
            "MKT": "MARKETING",
        }.get(dept or "SAL", "SALES_REP"),
        "ACTIVE": end is None,
        "CREATED_AT": start.isoformat() + "T09:00:00",
        "DEACTIVATED_AT": (end.isoformat() + "T17:00:00") if end else None,
    }


def _project_dms(
    identity: CanonicalIdentity,
    events: list[LifecycleEvent],
) -> list[dict]:
    """
    Project into DMS (PBS-style).

    DMS uses the SHORTENED first name (key drift point). One row per active
    employment spell.
    """
    rows = []
    spell_counter = 0
    current_dept: str | None = None
    current_location: str | None = None
    current_start: date | None = None

    for ev in events:
        if ev.event_type in (LifecycleEventType.HIRE, LifecycleEventType.REHIRE):
            current_dept = ev.payload.get("department")
            current_location = ev.payload.get("location")
            current_start = ev.event_date
        elif ev.event_type == LifecycleEventType.TERMINATE:
            if current_start:
                spell_counter += 1
                rows.append(_build_dms_row(
                    identity, current_dept, current_location,
                    current_start, ev.event_date, spell_counter,
                ))
            current_start = None

    if current_start:
        spell_counter += 1
        rows.append(_build_dms_row(
            identity, current_dept, current_location,
            current_start, None, spell_counter,
        ))
    return rows


def _build_dms_row(
    identity: CanonicalIdentity,
    dept: str | None,
    location: str | None,
    start: date,
    end: date | None,
    spell_index: int,
) -> dict:
    dms_id = _id_for_system(
        "DMS", identity.person_id,
        f"_{spell_index}" if spell_index > 1 else "",
    )
    # Sometimes the DMS hire date drifts a bit from HRIS hire date (real world)
    dms_hire = start + timedelta(days=random.choice([-2, -1, 0, 0, 0, 1, 2, 3]))
    return {
        "DMS_USER_ID": dms_id,
        "SHORT_FIRST_NAME": identity.short_first_name,
        "LAST_NAME": identity.legal_last_name,
        "DMS_USERNAME": f"{identity.short_first_name.lower()}{identity.legal_last_name.lower()[:3]}",
        "LOCATION_CODE": location,
        "DEPARTMENT_CODE": dept,
        "HIRE_DATE_DMS": dms_hire.isoformat(),
        "TERMINATED_DATE_DMS": end.isoformat() if end else None,
    }


def _project_erp(
    identity: CanonicalIdentity,
    dms_rows: list[dict],
) -> list[dict]:
    """
    Project into ERP. Mirrors DMS but with its own internal ID scheme.
    Sometimes the LINKED_DMS_USER_ID is missing (real-world manual data drift).
    """
    rows = []
    for i, dms_row in enumerate(dms_rows):
        # 90% of the time ERP correctly links to DMS; 10% of the time the link is broken
        link_dms = dms_row["DMS_USER_ID"] if random.random() < 0.90 else None
        rows.append({
            "ERP_USER_ID": _id_for_system(
                "ERP", identity.person_id,
                f"_{i + 1}" if i > 0 else "",
            ),
            "LINKED_DMS_USER_ID": link_dms,
            "SHORT_FIRST_NAME": dms_row["SHORT_FIRST_NAME"],
            "LAST_NAME": dms_row["LAST_NAME"],
            "ERP_EMAIL": f"{identity.short_first_name.lower()}.{identity.legal_last_name.lower()}@{COMPANY_EMAIL_DOMAIN}",
            "ROLE_CODE": dms_row["DEPARTMENT_CODE"],
            "PERMISSIONS_GROUP": f"{dms_row['DEPARTMENT_CODE']}_STD",
            "CREATED_AT": dms_row["HIRE_DATE_DMS"] + "T08:30:00",
            "LAST_LOGIN_AT": (date.today() - timedelta(days=random.randint(0, 90))).isoformat() + "T14:22:00",
        })
    return rows


def project_into_source_systems(
    identities: list[CanonicalIdentity],
    lifecycles: dict[str, list[LifecycleEvent]],
) -> SourceRecords:
    """Walk every identity + its lifecycle and emit rows into all six tables."""
    log.info("Projecting %d identities into 6 source systems", len(identities))

    out = SourceRecords()
    for identity in identities:
        events = lifecycles[identity.person_id]
        out.hris_employees.extend(_project_hris(identity, events))
        out.ats_candidates.extend(_project_ats(identity, events))
        out.payroll_records.extend(_project_payroll(identity, events))
        out.crm_sales_reps.extend(_project_crm(identity, events))
        dms_rows = _project_dms(identity, events)
        out.dms_users.extend(dms_rows)
        out.erp_users.extend(_project_erp(identity, dms_rows))

    log.info("  HRIS rows:    %d", len(out.hris_employees))
    log.info("  ATS rows:     %d", len(out.ats_candidates))
    log.info("  Payroll rows: %d", len(out.payroll_records))
    log.info("  CRM rows:     %d", len(out.crm_sales_reps))
    log.info("  DMS rows:     %d", len(out.dms_users))
    log.info("  ERP rows:     %d", len(out.erp_users))
    return out


# -----------------------------------------------------------------------------
# Step 4: Write outputs
# -----------------------------------------------------------------------------
def write_csv_outputs(records: SourceRecords, output_dir: Path) -> None:
    """Write each table to a CSV in output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    log.info("Writing CSV outputs to %s", output_dir)

    table_to_rows = [
        ("RAW_HRIS_EMPLOYEES.csv", records.hris_employees),
        ("RAW_ATS_CANDIDATES.csv", records.ats_candidates),
        ("RAW_PAYROLL_RECORDS.csv", records.payroll_records),
        ("RAW_CRM_SALES_REPS.csv", records.crm_sales_reps),
        ("RAW_DMS_USERS.csv", records.dms_users),
        ("RAW_ERP_USERS.csv", records.erp_users),
    ]
    for filename, rows in table_to_rows:
        if not rows:
            log.warning("  %s: 0 rows, skipping", filename)
            continue
        path = output_dir / filename
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        log.info("  %s: %d rows", filename, len(rows))


def load_to_snowflake(records: SourceRecords) -> None:
    """Load records into Snowflake RAW.* tables. Truncates before load."""
    try:
        import snowflake.connector  # noqa: F401
        from snowflake.connector.pandas_tools import write_pandas
        import pandas as pd
    except ImportError as e:
        log.error("Snowflake connector not installed. Run: pip install -e '.[dev]'")
        raise

    load_dotenv()

    conn_params = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "role": os.environ.get("SNOWFLAKE_ROLE", "ATLAS_DEVELOPER"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "ATLAS_WH"),
        "database": os.environ.get("SNOWFLAKE_DATABASE", "ATLAS"),
        "schema": "RAW",
    }
    log.info("Connecting to Snowflake (account=%s, role=%s)", conn_params["account"], conn_params["role"])
    conn = snowflake.connector.connect(**conn_params)

    table_to_rows = [
        ("RAW_HRIS_EMPLOYEES", records.hris_employees),
        ("RAW_ATS_CANDIDATES", records.ats_candidates),
        ("RAW_PAYROLL_RECORDS", records.payroll_records),
        ("RAW_CRM_SALES_REPS", records.crm_sales_reps),
        ("RAW_DMS_USERS", records.dms_users),
        ("RAW_ERP_USERS", records.erp_users),
    ]

    cursor = conn.cursor()
    try:
        for table_name, rows in table_to_rows:
            if not rows:
                log.warning("  %s: 0 rows, skipping", table_name)
                continue
            log.info("  Loading %s (%d rows)...", table_name, len(rows))
            cursor.execute(f"TRUNCATE TABLE IF EXISTS {table_name}")
            df = pd.DataFrame(rows)
            success, nchunks, nrows, _ = write_pandas(
                conn, df, table_name, auto_create_table=False, overwrite=False
            )
            if not success:
                raise RuntimeError(f"Failed to load {table_name}")
            log.info("    Loaded %d rows in %d chunks", nrows, nchunks)
    finally:
        cursor.close()
        conn.close()
    log.info("Snowflake load complete.")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def parse_args() -> SynthesizeConfig:
    parser = argparse.ArgumentParser(description="Atlas synthetic data generator")
    parser.add_argument(
        "--count", type=int, default=int(os.environ.get("ATLAS_SYNTHETIC_EMPLOYEE_COUNT", "1500")),
        help="Number of synthetic employees (default: 1500)",
    )
    parser.add_argument(
        "--years", type=int, default=5,
        help="Years of historical lifecycle data to generate (default: 5)",
    )
    parser.add_argument(
        "--seed", type=int, default=int(os.environ.get("ATLAS_SEED_RANDOM_STATE", "42")),
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("seeds/output"),
        help="Output directory for CSV files (default: seeds/output)",
    )
    parser.add_argument(
        "--load-snowflake", action="store_true",
        help="Also load into Snowflake RAW schema (requires .env)",
    )
    args = parser.parse_args()

    return SynthesizeConfig(
        employee_count=args.count,
        years_of_history=args.years,
        random_seed=args.seed,
        output_dir=args.output_dir,
        load_to_snowflake=args.load_snowflake,
    )


def main() -> None:
    load_dotenv()
    config = parse_args()
    random.seed(config.random_seed)
    Faker.seed(config.random_seed)

    log.info("Atlas synthesis starting (seed=%d)", config.random_seed)
    log.info("  Employees: %d", config.employee_count)
    log.info("  History:   %d years (%s to %s)",
             config.years_of_history,
             config.earliest_hire_date.isoformat(),
             date.today().isoformat())

    identities = generate_canonical_population(config)
    lifecycles = generate_all_lifecycles(identities, config)
    records = project_into_source_systems(identities, lifecycles)

    write_csv_outputs(records, config.output_dir)

    if config.load_to_snowflake:
        load_to_snowflake(records)

    log.info("Done.")


if __name__ == "__main__":
    main()
```

### 9. `dbt_project/dbt_project.yml`

**Purpose:** dbt project routing: model paths, materialization defaults, schema naming, and variables.

**Source:**

```yaml
name: 'atlas'
version: '0.1.0'
config-version: 2

# This profile must exist in ~/.dbt/profiles.yml and reference your Snowflake creds
profile: 'atlas'

# Folder paths (relative to dbt_project.yml)
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"
  - "logs"

# -----------------------------------------------------------------------------
# Variables — overridable from CLI with `dbt run --vars '{...}'`
# -----------------------------------------------------------------------------
vars:
  # Privacy: minimum cohort size for any aggregated metric to be returned.
  # Cohorts smaller than this are suppressed in the privacy-preserving views.
  k_anonymity_threshold: 5

  # Source schema in Snowflake (where the synthesizer loaded the raw tables)
  raw_schema: 'RAW'

  # Date used as "today" for snapshot tables. Override for backfills.
  # Defaults to current date in macro logic.
  snapshot_as_of_date: null

# -----------------------------------------------------------------------------
# Model defaults — staging is views (cheap, always fresh), marts are tables
# (cached, fast for downstream queries). Intermediate is ephemeral by default
# but can be materialized as table for inspection during development.
# -----------------------------------------------------------------------------
models:
  atlas:
    # Default: every model is a view unless overridden
    +materialized: view

    staging:
      +materialized: view
      +schema: staging
      # +tags: ['staging']  # uncomment if/when we want to filter selectively
      hris:
        +tags: ['staging', 'hris']
      ats:
        +tags: ['staging', 'ats']
      payroll:
        +tags: ['staging', 'payroll']
      crm:
        +tags: ['staging', 'crm']
      dms:
        +tags: ['staging', 'dms']
      erp:
        +tags: ['staging', 'erp']

    intermediate:
      +materialized: table
      +schema: intermediate

    marts:
      +materialized: table
      core:
        +schema: core
      people_analytics:
        +schema: people_analytics

# -----------------------------------------------------------------------------
# Seed defaults — for any reference CSVs we add later (e.g., NICKNAME_MAP)
# -----------------------------------------------------------------------------
seeds:
  atlas:
    +schema: seeds

# Snapshot configuration will be added when real dbt snapshots enter scope.
# Phase 2D's dim_employee is built from synthetic HRIS spell rows because the
# raw feed already represents the available effective-dated history.
```

### 10. `dbt_project/packages.yml`

**Purpose:** YAML configuration/documentation. It defines dbt metadata, tests, dependencies, sources, exposures, or fixtures.

**Source:**

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.1.0", "<2.0.0"]
```

### 11. `dbt_project/profiles.yml.template`

**Purpose:** YAML configuration/documentation. It defines dbt metadata, tests, dependencies, sources, exposures, or fixtures.

**Source:**

```yaml
# =============================================================================
# Atlas dbt profiles.yml TEMPLATE
# =============================================================================
# This file is a template. To use:
#
#   1. Copy it to ~/.dbt/profiles.yml:    cp dbt_project/profiles.yml.template ~/.dbt/profiles.yml
#   2. Edit ~/.dbt/profiles.yml and replace the env_var defaults with your account
#   3. Confirm SNOWFLAKE_USER and SNOWFLAKE_PASSWORD are set in your shell
#      (e.g. from .env via `set -a && source .env && set +a`)
#   4. Run `dbt debug` to validate the connection
#
# IMPORTANT: never commit a profiles.yml with real credentials.
# This template uses env_var() which is the safe approach.
# =============================================================================

atlas:
  target: dev
  outputs:
    dev:
      type: snowflake

      # Account identifier (region included). For our project this is QLSJUMK-DC22948.us-east-1
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"

      # Authentication
      user:     "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"

      # Compute and storage
      role:      "{{ env_var('SNOWFLAKE_ROLE',      'ATLAS_DEVELOPER') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE', 'ATLAS_WH') }}"
      database:  "{{ env_var('SNOWFLAKE_DATABASE',  'ATLAS') }}"
      schema:    "{{ env_var('SNOWFLAKE_DBT_SCHEMA', 'DBT_DEV') }}"   # default dev schema

      # Connection-level settings
      threads: 4                # parallel model execution; tune up to 8 if account allows
      client_session_keep_alive: false
      query_tag: 'atlas-dbt'    # shows up in QUERY_HISTORY for debugging
```

### 12. `dbt_project/seeds/_seeds.yml`

**Purpose:** YAML configuration/documentation. It defines dbt metadata, tests, dependencies, sources, exposures, or fixtures.

**Source:**

```yaml
version: 2

seeds:

  - name: nickname_map
    description: |
      Many-to-one mapping from common English / Punjabi / Hispanic nicknames
      to their canonical (legal) first-name form. Used by the
      `first_name_root` macro in the Phase 2C identity matcher to collapse
      "Robert" / "Bob" / "Bobby" / "Rob" into a single name root before
      cross-source name matching.

      Inverted from `seeds/name_strategies.py:NICKNAME_MAP` (which is
      one-to-many: canonical -> [nickname1, nickname2, ...]).

      EXCLUSION POLICY — ambiguous nicknames are deliberately omitted:
        - 'steve'       could be Steven or Stephen
        - 'kate' / 'katie' could be Catherine or Katherine
        - 'alex' / 'sasha' could be Alexander or Alexandra
        - 'sam' / 'sammy'  could be Samuel or Samantha
        - 'chris' / 'tina' / 'christy' could be Christopher / Christine / Christina
        - 'charlie'     could be Charles or Charlotte
        - 'ed' / 'eddie' could be Edward or Eduardo
        - 'andy'        could be Andrew or Amandeep
        - 'mo'          could be Mohammed or Muhammad
        - 'pat'         could be Patrick or Patricia
        - 'rick'        could be Richard or Frederick
        - 'frank'       could be Francisco or its own legal name
        - 'lars'        could be Lawrence or its own Scandinavian legal name

      These cases will not match on first_name_root alone. They rely on
      independent anchors (email, DOB, hire_date) to resolve. If no
      independent anchor is available, the record routes to the
      stewardship queue — which is the correct behavior because picking
      the dominant interpretation could merge two distinct people.

      To extend: only add a row if the nickname resolves unambiguously to
      ONE canonical first name. If you find yourself wanting to map both
      directions (steve -> steven AND steve -> stephen), exclude it instead.
    config:
      column_types:
        nickname: VARCHAR(50)
        canonical_first_name: VARCHAR(50)
    columns:
      - name: nickname
        description: Lowercase nickname or short form (the form an analyst sees in DMS / CRM / ATS).
        tests:
          - not_null
          - unique
      - name: canonical_first_name
        description: The legal / canonical first name this nickname unambiguously resolves to.
        tests:
          - not_null
```

### 13. `dbt_project/seeds/nickname_map.csv`

**Purpose:** Project source/configuration artifact.

**Source:**

```csv
nickname,canonical_first_name
bob,robert
bobby,robert
rob,robert
bill,william
billy,william
will,william
dick,richard
rich,richard
jim,james
jimmy,james
jamie,james
jack,john
johnny,john
mike,michael
mikey,michael
matt,matthew
matty,matthew
josh,joshua
dan,daniel
danny,daniel
dave,david
davey,david
tony,anthony
tom,thomas
tommy,thomas
joe,joseph
joey,joseph
ben,benjamin
benny,benjamin
nick,nicholas
nicky,nicholas
xander,alexander
jon,jonathan
jonny,jonathan
paddy,patrick
tim,timothy
timmy,timothy
greg,gregory
gregg,gregory
fred,frederick
freddie,frederick
larry,lawrence
liz,elizabeth
beth,elizabeth
eliza,elizabeth
lizzie,elizabeth
betty,elizabeth
maggie,margaret
meg,margaret
peggy,margaret
marge,margaret
patty,patricia
trish,patricia
tricia,patricia
jen,jennifer
jenny,jennifer
jenni,jennifer
jess,jessica
jessie,jessica
stephie,stephanie
becky,rebecca
becca,rebecca
deb,deborah
debbie,deborah
barb,barbara
babs,barbara
sue,susan
susie,susan
vicky,victoria
tori,victoria
nicki,nicole
nikki,nicole
raj,rajesh
mani,manpreet
jas,jaspreet
sunny,surinder
indy,inderjit
abdul,abdullah
abdi,abdullah
paco,francisco
cisco,francisco
beto,roberto
lalo,eduardo
```

### 14. `dbt_project/fixtures/normalize_name.yml`

**Purpose:** YAML configuration/documentation. It defines dbt metadata, tests, dependencies, sources, exposures, or fixtures.

**Source:**

```yaml
# =============================================================================
# normalize_name macro spec — written BEFORE the macro implementation
# =============================================================================
# This file is the human-readable spec. The executable form lives at
# `tests/macros/test_normalize_name.sql` and must mirror these cases exactly.
#
# Ground-truth Python equivalent: `seeds/name_strategies.py::normalize_name_for_matching`
#
# Snowflake limitation: pure-SQL implementation uses TRANSLATE for accent-folding
# of Latin-1 Supplement + Latin Extended-A characters (covers French, Spanish,
# Italian, Portuguese, Polish, Czech). It CANNOT transliterate non-Latin scripts
# (Arabic, CJK). The Python version uses `unidecode` which fully transliterates.
# For non-Latin inputs the SQL macro returns an empty string after non-alpha
# stripping; the Python function returns a Latin transliteration.
#
# This divergence is DELIBERATE — the matcher does not match across scripts via
# name root. Cross-script identity resolution relies on email-domain anchors
# (Pass 3), which the synthesizer guarantees are unidecode-transliterated and
# therefore Latin-script regardless of the source name's script.
#
# Test scope: all cases below MUST hold on the Snowflake side. Python parity is
# only asserted for ASCII + Latin-Extended inputs (cases 1-9, 12).
# =============================================================================

cases:

  - id: 1
    description: Mixed case lowercased
    input: "Robert"
    expected: "robert"

  - id: 2
    description: Surrounding whitespace trimmed
    input: "  Robert  "
    expected: "robert"

  - id: 3
    description: Single accented Latin char decomposed and stripped
    input: "Édouard"
    expected: "edouard"

  - id: 4
    description: Multiple accents within one name
    input: "Anaïs"
    expected: "anais"

  - id: 5
    description: Hyphens stripped (compound first name)
    input: "Mary-Jane"
    expected: "maryjane"

  - id: 6
    description: Apostrophes stripped (Irish-style surname)
    input: "O'Brien"
    expected: "obrien"

  - id: 7
    description: Internal whitespace stripped (compound first name)
    input: "Jean Paul"
    expected: "jeanpaul"

  - id: 8
    description: Tilde-n decomposed (Spanish surname)
    input: "Núñez"
    expected: "nunez"

  - id: 9
    description: Cedilla decomposed (French given name)
    input: "François"
    expected: "francois"

  - id: 10
    description: Empty string passes through as empty
    input: ""
    expected: ""

  - id: 11
    description: Null passes through as null
    input: null
    expected: null

  - id: 12
    description: Numerals stripped (defensive — names should not contain digits)
    input: "Robert3"
    expected: "robert"

  - id: 13
    description: |
      Non-Latin script (Chinese) — documented divergence from Python.
      Python unidecode transliterates "张伟" -> "zhangwei". The pure-SQL
      macro strips all non-Latin characters, returning empty string. The
      matcher relies on email anchors for these cases, not name root.
    input: "张伟"
    expected: ""

  - id: 14
    description: |
      Non-Latin script (Arabic) — same divergence as case 13. Python
      unidecode transliterates "محمد" -> "mhmd". Pure-SQL returns empty.
    input: "محمد"
    expected: ""

  - id: 15
    description: Polish accent (Latin Extended-A) — l-stroke
    input: "Łukasz"
    expected: "lukasz"
```

### 15. `dbt_project/fixtures/first_name_root.yml`

**Purpose:** YAML configuration/documentation. It defines dbt metadata, tests, dependencies, sources, exposures, or fixtures.

**Source:**

```yaml
# =============================================================================
# first_name_root macro spec — written BEFORE the macro implementation
# =============================================================================
# This file is the human-readable spec. The executable form lives at
# `tests/macros/test_first_name_root.sql` and must mirror these cases exactly.
#
# `first_name_root(col)` does:
#   1. normalize_name(col)           -> lowercase, accent-strip, strip non-alpha
#   2. left-join nickname_map         -> nickname -> canonical_first_name
#   3. coalesce(canonical, normalized) -> fall back to the literal normalized
#                                          form when no nickname row exists
#
# The seed `nickname_map.csv` deliberately excludes ambiguous nicknames
# (steve, alex, sam, chris, charlie, ed, andy, mo, pat, rick, frank). For those
# inputs, the macro returns the normalized literal — which matches OTHER rows
# with the same literal but cannot collapse e.g. Steven and Stephen. Those
# cases route to stewardship via independent anchors. See `_seeds.yml` for the
# full exclusion policy and rationale.
# =============================================================================

cases:

  - id: 1
    description: Already-canonical first name passes through
    input: "Robert"
    expected: "robert"

  - id: 2
    description: Common English nickname maps to canonical
    input: "Bob"
    expected: "robert"

  - id: 3
    description: Affectionate diminutive maps to canonical
    input: "Bobby"
    expected: "robert"

  - id: 4
    description: Truncation maps to canonical
    input: "Rob"
    expected: "robert"

  - id: 5
    description: Female nickname maps to canonical
    input: "Liz"
    expected: "elizabeth"

  - id: 6
    description: Female nickname maps to canonical (different form)
    input: "Beth"
    expected: "elizabeth"

  - id: 7
    description: Punjabi nickname maps to canonical
    input: "Raj"
    expected: "rajesh"

  - id: 8
    description: Hispanic nickname maps to canonical
    input: "Paco"
    expected: "francisco"

  - id: 9
    description: Unknown name passes through (not in nickname_map)
    input: "Aiden"
    expected: "aiden"

  - id: 10
    description: |
      Ambiguous nickname passes through — 'Steve' is deliberately not mapped
      because it could be Steven OR Stephen. Both legal forms will normalize
      to themselves; matching across them requires an independent anchor.
    input: "Steve"
    expected: "steve"

  - id: 11
    description: Casing and whitespace handled before lookup
    input: "  BoB  "
    expected: "robert"

  - id: 12
    description: Already-canonical name with accent (Latin-Extended)
    input: "Mária"
    expected: "maria"

  - id: 13
    description: Empty string passes through as empty
    input: ""
    expected: ""

  - id: 14
    description: Null passes through as null
    input: null
    expected: null
```

### 16. `dbt_project/macros/normalize_name.sql`

**Purpose:** Reusable dbt macro. It centralizes SQL logic so models/tests can share one definition.

**Source:**

```sql
{#-
============================================================================
  normalize_name
============================================================================
  Lowercase + accent-strip + non-alpha-strip transformation for cross-source
  name matching. Pure SQL — no Python UDFs, no extensions.

  Implementation: TRIM -> TRANSLATE (Latin-1 + Latin Extended-A folding) ->
  LOWER -> REGEXP_REPLACE non-[a-z].

  Spec: fixtures/normalize_name.yml
  Test: tests/macros/test_normalize_name.sql
  Python equivalent: seeds/name_strategies.py::normalize_name_for_matching

  Coverage: French, Spanish, Italian, Portuguese, Polish, Czech, Croatian
  diacritics. Synthesizer locales (en_CA, fr_CA, en_IN, es_MX) all covered.

  Non-Latin scripts (zh_CN, ar_AA): returns empty string after non-alpha
  strip. The matcher relies on email-domain anchors (Pass 3) for cross-script
  identity resolution — see CLAUDE.md and the anchor table memory.

  Tradeoff: pure SQL keeps the warehouse self-contained and the function
  inlinable for query optimization. Python UDF using `unidecode` would
  give full Unicode transliteration but adds Snowpark infrastructure and
  costs more per row. Revisit if Phase 5 ML matching needs CJK name roots.
-#}

{% macro normalize_name(col) -%}
regexp_replace(
    lower(
        translate(
            trim({{ col }}),
            'ÀÁÂÃÄÅàáâãäåÈÉÊËèéêëÌÍÎÏìíîïÒÓÔÕÖØòóôõöøÙÚÛÜùúûüÝýÿÇçÑñŁłŚśŠšŹźŻżŽžĐđŘřŤť',
            'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOOooooooUUUUuuuuYyyCcNnLlSsSsZzZzZzDdRrTt'
        )
    ),
    '[^a-z]',
    ''
)
{%- endmacro %}
```

### 17. `dbt_project/macros/first_name_root.sql`

**Purpose:** Reusable dbt macro. It centralizes SQL logic so models/tests can share one definition.

**Source:**

```sql
{#-
============================================================================
  first_name_root
============================================================================
  Returns the canonical (legal) first name for a given input first name,
  collapsing common nicknames to their root form. e.g. 'Bob' -> 'robert',
  'Liz' -> 'elizabeth'. If the input is not a known nickname, returns the
  normalize_name'd literal (passthrough).

  Spec: fixtures/first_name_root.yml
  Test: tests/macros/test_first_name_root.sql

  CALLER REQUIREMENT: this macro emits a column expression that depends on
  a LEFT JOIN to seeds.nickname_map being present in the same SELECT. The
  caller must include:

      LEFT JOIN {{ ref('nickname_map') }} {{ alias }}
          ON {{ alias }}.nickname = {{ normalize_name(input_col) }}

  ...where `input_col` matches the column passed to first_name_root and
  `alias` matches the second arg (default 'nm').

  This couples the macro to the join, which is intentional — it forces the
  caller to make the dependency explicit in the model SQL rather than
  hiding it in a correlated subquery (which would be slow at scale).

  Ambiguous nicknames (steve, alex, sam, chris, charlie, ed, andy, mo, pat,
  rick, frank) are deliberately omitted from nickname_map. For those, the
  COALESCE falls back to the normalized literal — so 'Steve' returns
  'steve', not 'steven' or 'stephen'. Cross-resolving those cases requires
  an independent anchor (email, DOB) in the matcher's pass logic, or the
  record routes to stewardship. See seeds/_seeds.yml for the full exclusion
  policy.
-#}

{% macro first_name_root(input_col, alias='nm') -%}
coalesce({{ alias }}.canonical_first_name, {{ normalize_name(input_col) }})
{%- endmacro %}
```

### 18. `dbt_project/macros/match_confidence.sql`

**Purpose:** Reusable dbt macro. It centralizes SQL logic so models/tests can share one definition.

**Source:**

```sql
{#-
============================================================================
  match_confidence — three macros that codify the locked Phase 2C scoring
============================================================================

  These macros emit SQL expressions that compute, per candidate match:

    1. match_score          — additive confidence in [0.0, 1.0]
    2. match_anchor_count   — count of medium-or-stronger independent anchors
    3. auto_merge_qualified — boolean: passes the >=0.95 score AND >=2 anchor
                              floor (or is a Pass-0 structural FK)

  Anchors and weights below mirror the locked table in
  `~/.claude/.../memory/phase_2c_anchor_table.md`. Edit weights ONLY by
  updating the memory and the int_match_audit_log doc together — the
  numbers are load-bearing and every change should be defensible.

  Anchor weights:
    +0.40  work_email_local_part exact         strong
    +0.35  personal_email_local_part exact     strong
    +0.30  DOB exact                           strong
    +0.20  EMPLOYEE_PAYROLL_ID continuity      medium (intra-payroll only)
    +0.20  last_name + first_name_root match   weak-medium (counts as 1 anchor)
    +0.20  hire_date exact                     medium
    +0.10  hire_date within +/- 7 days         weak
    +0.05  hire_date within +/- 30 days        very weak (does NOT count toward
                                                          independent anchor floor)

  Special case: structural FK match (Pass 0 ERP -> DMS via linked_dms_user_id)
  short-circuits to score = 1.0 and bypasses the anchor count check, because
  it is a deterministic FK rather than a probabilistic match.

  Caller usage — pass the BOOLEAN EXPRESSION as a string for each anchor that
  applies. Anchors that don't apply for a given pass can be omitted (they
  default to 'false'):

    select
      ...,
      {{ match_score(
          work_email_match='src.work_email_local_part = tgt.work_email_local_part',
          name_match='src.first_name_root = tgt.first_name_root and src.last_name = tgt.last_name',
          hire_date_exact='src.hire_date = tgt.hire_date'
      ) }} as match_score,
      {{ match_anchor_count(
          work_email_match='src.work_email_local_part = tgt.work_email_local_part',
          name_match='src.first_name_root = tgt.first_name_root and src.last_name = tgt.last_name',
          hire_date_exact='src.hire_date = tgt.hire_date'
      ) }} as match_anchor_count

  Yes, the same expressions get repeated. The alternative is a single macro
  that emits multiple columns at once — possible via {%- set ... -%} in a
  parent CTE, but it sacrifices SQL readability. The repetition is the
  honest trade.
-#}


{#- =====================================================================
    match_score
========================================================================= -#}

{% macro match_score(
    work_email_match='false',
    personal_email_match='false',
    dob_match='false',
    payroll_id_continuity='false',
    name_match='false',
    hire_date_exact='false',
    hire_date_within_7d='false',
    hire_date_within_30d='false',
    structural_fk_match='false'
) -%}
case
    when ({{ structural_fk_match }}) then 1.0
    else least(1.0,
        (case when ({{ work_email_match }})       then 0.40 else 0 end)
      + (case when ({{ personal_email_match }})   then 0.35 else 0 end)
      + (case when ({{ dob_match }})              then 0.30 else 0 end)
      + (case when ({{ payroll_id_continuity }})  then 0.20 else 0 end)
      + (case when ({{ name_match }})             then 0.20 else 0 end)
      + (case when ({{ hire_date_exact }})        then 0.20
              when ({{ hire_date_within_7d }})    then 0.10
              when ({{ hire_date_within_30d }})   then 0.05
              else 0 end)
    )
end
{%- endmacro %}


{#- =====================================================================
    match_anchor_count
    Counts only medium-or-stronger independent anchors. hire_date +/- 30d
    alone does NOT count toward this — it's too weak (matches ~10% of
    population for any given target hire_date).
========================================================================= -#}

{% macro match_anchor_count(
    work_email_match='false',
    personal_email_match='false',
    dob_match='false',
    payroll_id_continuity='false',
    name_match='false',
    hire_date_exact='false',
    hire_date_within_7d='false',
    hire_date_within_30d='false'
) -%}
(case when ({{ work_email_match }})      then 1 else 0 end)
+ (case when ({{ personal_email_match }})  then 1 else 0 end)
+ (case when ({{ dob_match }})             then 1 else 0 end)
+ (case when ({{ payroll_id_continuity }}) then 1 else 0 end)
+ (case when ({{ name_match }})            then 1 else 0 end)
+ (case when ({{ hire_date_exact }}) or ({{ hire_date_within_7d }}) then 1 else 0 end)
{%- endmacro %}


{#- =====================================================================
    auto_merge_qualified
    The locked auto-merge rule, in one place. Returns a boolean
    expression suitable for use in a CASE or WHERE clause.
========================================================================= -#}

{% macro auto_merge_qualified(score_col, anchor_count_col, structural_fk_col='false') -%}
(({{ structural_fk_col }}) or ({{ score_col }} >= 0.95 and {{ anchor_count_col }} >= 2))
{%- endmacro %}
```

### 19. `dbt_project/macros/privacy.sql`

**Purpose:** Reusable dbt macro. It centralizes SQL logic so models/tests can share one definition.

**Source:**

```sql
{#-
============================================================================
  Privacy macros — Phase 3
============================================================================
  Reusable SQL snippets for k-anonymity enforcement and privacy audit logging.

  The important design rule: public People Analytics marts may show dimensions
  for small cohorts, but they must not show exact metric values for cohorts
  below `var('k_anonymity_threshold')`. That keeps analysts oriented without
  leaking a one-person headcount/attrition fact.
-#}

{% macro k_anonymity_threshold() -%}
{{ var('k_anonymity_threshold', 5) }}
{%- endmacro %}


{% macro is_k_anonymous(cohort_count_expr) -%}
({{ cohort_count_expr }} >= {{ k_anonymity_threshold() }})
{%- endmacro %}


{% macro k_anonymize(metric_expr, cohort_count_expr, data_type='number(38, 6)') -%}
case
    when {{ is_k_anonymous(cohort_count_expr) }} then cast({{ metric_expr }} as {{ data_type }})
    else cast(null as {{ data_type }})
end
{%- endmacro %}


{% macro k_suppression_reason(cohort_count_expr) -%}
case
    when {{ is_k_anonymous(cohort_count_expr) }} then cast(null as varchar)
    else 'K_ANONYMITY_THRESHOLD'
end
{%- endmacro %}


{% macro k_cohort_size_bucket(cohort_count_expr) -%}
case
    when {{ is_k_anonymous(cohort_count_expr) }} then to_varchar({{ cohort_count_expr }})
    else '<' || to_varchar({{ k_anonymity_threshold() }})
end
{%- endmacro %}


{% macro sql_string_literal(value) -%}
'{{ (value | string).replace("'", "''") }}'
{%- endmacro %}


{% macro insert_privacy_audit_event(
    actor,
    query_surface,
    purpose,
    filters_json='{}',
    result_row_count='null',
    suppressed_row_count='null'
) -%}
{#-
  Inserts one access event into the Phase 3 audit table. This is intended for
  later FastAPI/Streamlit code to call via dbt run-operation or to mirror in
  application SQL.

  Example:
    dbt run-operation insert_privacy_audit_event --args '{
      "actor": "demo_hrbp",
      "query_surface": "workforce_headcount_daily",
      "purpose": "dashboard_view",
      "filters_json": "{\"department\":\"SAL\"}",
      "result_row_count": 10,
      "suppressed_row_count": 2
    }'

  The table is modeled as incremental so normal dbt builds do not wipe events.
  Avoid dbt full-refresh against privacy_audit_log in any environment where
  audit history matters.
-#}

{% set audit_relation = target.database ~ "." ~ target.schema ~ "_people_analytics.privacy_audit_log" %}
{% set insert_sql %}
insert into {{ audit_relation }} (
    audit_event_id,
    audited_at,
    actor,
    query_surface,
    purpose,
    filters_json,
    k_anonymity_threshold,
    result_row_count,
    suppressed_row_count,
    privacy_policy_version,
    dbt_invocation_id
)
select
    uuid_string(),
    current_timestamp(),
    {{ sql_string_literal(actor) }},
    {{ sql_string_literal(query_surface) }},
    {{ sql_string_literal(purpose) }},
    try_parse_json({{ sql_string_literal(filters_json) }}),
    {{ k_anonymity_threshold() }},
    {{ result_row_count }},
    {{ suppressed_row_count }},
    'phase_3_k_anonymity_v1',
    '{{ invocation_id }}'
{% endset %}

{% if execute %}
    {% do run_query(insert_sql) %}
{% endif %}

{{ return(insert_sql) }}
{%- endmacro %}
```

### 20. `dbt_project/models/staging/_sources.yml`

**Purpose:** Raw source definitions, source tests, freshness expectations, and raw-column docs.

**Source:**

```yaml
version: 2

sources:
  - name: atlas_raw
    description: |
      The six raw operational source tables, populated by `seeds/synthesize.py`.
      Each table mirrors a different real-world operational system, with deliberately
      drifted name and identity representations.
    database: ATLAS
    schema: RAW
    loader: 'atlas_synthesize'

    # Source-level freshness check.
    config:
      freshness:
        warn_after: { count: 7, period: day }
        error_after: { count: 30, period: day }
      loaded_at_field: LOADED_AT

    tables:

      # -----------------------------------------------------------------------
      # HRIS (BambooHR-shape) — system of record for employment status
      # -----------------------------------------------------------------------
      - name: RAW_HRIS_EMPLOYEES
        description: |
          BambooHR-style HRIS export. One row per employment spell. Rehires
          generate a NEW HRIS_EMPLOYEE_ID — a critical part of the canonical-record
          problem this project solves.
        columns:
          - name: HRIS_EMPLOYEE_ID
            description: Primary key. Unique to one employment spell.
            tests:
              - not_null
              - unique
          - name: LEGAL_FIRST_NAME
            description: Legal first name (matches government ID / T4).
            tests: [not_null]
          - name: LEGAL_LAST_NAME
            description: Legal last name. Updates on marriage if HR is notified.
            tests: [not_null]
          - name: PREFERRED_NAME
            description: Optional preferred first name. Often differs from legal.
          - name: DATE_OF_BIRTH
            description: Used by identity matcher for cross-system disambiguation.
          - name: PERSONAL_EMAIL
            description: "Personal email — useful as identity anchor."
          - name: WORK_EMAIL
            description: |
              Company email. Locked at hire and rarely updated, even after
              marriage. The local part is a stable identity anchor.
          - name: HIRE_DATE
            description: Date of this employment spell's start.
            tests: [not_null]
          - name: TERMINATION_DATE
            description: Termination date. NULL means currently active.
          - name: EMPLOYMENT_STATUS
            tests:
              - accepted_values:
                  arguments:
                    values: ['ACTIVE', 'TERMINATED', 'ON_LEAVE']
          - name: EMPLOYMENT_TYPE
            tests:
              - accepted_values:
                  arguments:
                    values: ['FTE', 'CONTRACTOR', 'PART_TIME']
          - name: DEPARTMENT
          - name: JOB_TITLE
          - name: MANAGER_HRIS_ID
            description: HRIS ID of manager. Self-referential; nullable for top-level.
          - name: LOCATION

      # -----------------------------------------------------------------------
      # ATS (Greenhouse/Ashby-shape) — application records, captured pre-hire
      # -----------------------------------------------------------------------
      - name: RAW_ATS_CANDIDATES
        description: |
          ATS application records. One row per offer accepted (i.e., per hire
          and per rehire). Uses preferred name, not legal.
        columns:
          - name: ATS_CANDIDATE_ID
            tests: [not_null, unique]
          - name: PREFERRED_FIRST_NAME
            tests: [not_null]
          - name: LAST_NAME
            tests: [not_null]
          - name: EMAIL
            description: Personal email used during application.
          - name: PHONE
          - name: APPLICATION_DATE
          - name: OFFER_ACCEPTED_DATE
          - name: SOURCED_FROM
            tests:
              - accepted_values:
                  arguments:
                    values: ['LinkedIn', 'Referral', 'Career Site', 'Indeed', 'Recruiter']
          - name: REQUISITION_DEPARTMENT
          - name: REQUISITION_JOB_TITLE

      # -----------------------------------------------------------------------
      # Payroll (ADP-shape) — monthly records, lags HRIS on name updates
      # -----------------------------------------------------------------------
      - name: RAW_PAYROLL_RECORDS
        description: |
          ADP-shape monthly payroll records. Uses legal name, but does NOT
          always pick up name changes (marriage, etc.) — a deliberate drift case.
          Includes SIN_LAST_4 as sensitive identity anchor.
        columns:
          - name: PAYROLL_RECORD_ID
            tests: [not_null, unique]
          - name: EMPLOYEE_PAYROLL_ID
            description: |
              Payroll's internal employee ID, separate from HRIS_EMPLOYEE_ID.
              Stable across pay periods within one spell.
            tests: [not_null]
          - name: LEGAL_FIRST_NAME
            tests: [not_null]
          - name: LEGAL_LAST_NAME
            tests: [not_null]
          - name: SIN_LAST_4
            description: Sensitive — last 4 digits of SIN. Restrict access in marts.
          - name: PAY_PERIOD_START
            tests: [not_null]
          - name: PAY_PERIOD_END
            tests: [not_null]
          - name: GROSS_AMOUNT_CAD
          - name: HOURS_WORKED
          - name: JOB_CODE
          - name: COST_CENTER

      # -----------------------------------------------------------------------
      # CRM (Dabadu-shape) — sales-floor activity, preferred name
      # -----------------------------------------------------------------------
      - name: RAW_CRM_SALES_REPS
        description: |
          CRM user records. Only sales/support roles get CRM accounts. Uses
          preferred name. One row per active employment spell.
        columns:
          - name: CRM_USER_ID
            tests: [not_null, unique]
          - name: PREFERRED_FIRST_NAME
            tests: [not_null]
          - name: LAST_NAME
            tests: [not_null]
          - name: DISPLAY_NAME
          - name: CRM_EMAIL
          - name: LOCATION_ID
          - name: ROLE
            tests:
              - accepted_values:
                  arguments:
                    values: ['SALES_REP', 'SUPPORT_AGENT', 'MARKETING']
          - name: ACTIVE
          - name: CREATED_AT
          - name: DEACTIVATED_AT

      # -----------------------------------------------------------------------
      # DMS (PBS-shape) — shortened first name typed-in-once
      # -----------------------------------------------------------------------
      - name: RAW_DMS_USERS
        description: |
          DMS (Dealer Management System) user records. Uses SHORTENED first name
          ('Bob' for 'Robert'). Often has hire-date drift of ±1-3 days from HRIS.
        columns:
          - name: DMS_USER_ID
            tests: [not_null, unique]
          - name: SHORT_FIRST_NAME
            description: Often a shortened/nickname version of legal first name.
            tests: [not_null]
          - name: LAST_NAME
            tests: [not_null]
          - name: DMS_USERNAME
            description: Login username, derived from short_first + first 3 of last name.
          - name: LOCATION_CODE
          - name: DEPARTMENT_CODE
          - name: HIRE_DATE_DMS
            description: Local hire date in DMS. May drift ±1-3 days from HRIS.HIRE_DATE.
          - name: TERMINATED_DATE_DMS

      # -----------------------------------------------------------------------
      # ERP — mirrors DMS but with broken FK link 10% of the time
      # -----------------------------------------------------------------------
      - name: RAW_ERP_USERS
        description: |
          ERP user records. Mostly mirrors DMS, but ~10% of LINKED_DMS_USER_IDs
          are NULL — modeling real-world manual data drift where the DMS-ERP
          link is broken.
        columns:
          - name: ERP_USER_ID
            tests: [not_null, unique]
          - name: LINKED_DMS_USER_ID
            description: |
              Foreign key to RAW_DMS_USERS.DMS_USER_ID. Nullable: ~10% of rows
              have a broken link, simulating real-world drift.
            tests:
              - relationships:
                  arguments:
                    to: source('atlas_raw', 'RAW_DMS_USERS')
                    field: DMS_USER_ID
                  config:
                    where: "LINKED_DMS_USER_ID IS NOT NULL"
          - name: SHORT_FIRST_NAME
          - name: LAST_NAME
          - name: ERP_EMAIL
          - name: ROLE_CODE
          - name: PERMISSIONS_GROUP
          - name: CREATED_AT
          - name: LAST_LOGIN_AT
```

### 21. `dbt_project/models/staging/_staging.yml`

**Purpose:** Staging model docs and tests for normalized source views.

**Source:**

```yaml
version: 2

models:

  # ===========================================================================
  - name: stg_hris__employees
    description: |
      Cleaned 1:1 view of RAW_HRIS_EMPLOYEES. One row per employment spell.
      Names are lowercased and trimmed; original casing preserved in *_original
      columns for display purposes.
    columns:
      - name: hris_employee_id
        description: "Primary key. Note: a single person may have multiple HRIS_EMPLOYEE_IDs across rehires."
        tests: [not_null, unique]
      - name: legal_first_name
        description: Lowercase, trimmed legal first name. Use for cross-system matching.
        tests: [not_null]
      - name: legal_last_name
        description: Lowercase, trimmed legal last name.
        tests: [not_null]
      - name: preferred_name
        description: Lowercase preferred first name, NULL if not provided.
      - name: work_email
        description: Company email address. Locked at hire, rarely updated.
      - name: work_email_local_part
        description: Email username part (before '@'). Stable identity anchor.
      - name: hire_date
        tests: [not_null]
      - name: employment_status
        tests:
          - accepted_values:
              arguments:
                values: ['ACTIVE', 'TERMINATED', 'ON_LEAVE']
      - name: employment_type
        tests:
          - accepted_values:
              arguments:
                values: ['FTE', 'CONTRACTOR', 'PART_TIME']

  # ===========================================================================
  - name: stg_ats__candidates
    description: |
      Cleaned 1:1 view of RAW_ATS_CANDIDATES. One row per offer accepted.
      Uses preferred name and personal email.
    columns:
      - name: ats_candidate_id
        tests: [not_null, unique]
      - name: preferred_first_name
        tests: [not_null]
      - name: last_name
        tests: [not_null]
      - name: email_local_part
        description: Email username (before '@'). Used to match candidates back to hired employees.
      - name: sourced_from
        tests:
          - accepted_values:
              arguments:
                values: ['LinkedIn', 'Referral', 'Career Site', 'Indeed', 'Recruiter']

  # ===========================================================================
  - name: stg_payroll__records
    description: |
      Cleaned 1:1 view of RAW_PAYROLL_RECORDS. One row per pay period (~monthly).
      Uses LEGAL name. Note: payroll often LAGS HRIS on name updates — same
      person may appear here under pre-marriage name long after HRIS updated.
      SIN_LAST_4 is sensitive — restrict access at marts layer.
    columns:
      - name: payroll_record_id
        tests: [not_null, unique]
      - name: employee_payroll_id
        description: |
          Payroll's internal ID, separate from HRIS_EMPLOYEE_ID. Stable across
          pay periods within a single employment spell — useful identity anchor.
        tests: [not_null]
      - name: legal_first_name
        tests: [not_null]
      - name: legal_last_name
        tests: [not_null]
      - name: pay_period_start
        tests: [not_null]
      - name: pay_period_end
        tests: [not_null]

  # ===========================================================================
  - name: stg_crm__sales_reps
    description: |
      Cleaned 1:1 view of RAW_CRM_SALES_REPS. One row per sales/support
      employment spell. Uses preferred name.
    columns:
      - name: crm_user_id
        tests: [not_null, unique]
      - name: preferred_first_name
        tests: [not_null]
      - name: last_name
        tests: [not_null]
      - name: role
        tests:
          - accepted_values:
              arguments:
                values: ['SALES_REP', 'SUPPORT_AGENT', 'MARKETING']

  # ===========================================================================
  - name: stg_dms__users
    description: |
      Cleaned 1:1 view of RAW_DMS_USERS. Uses SHORTENED first name (typed in
      once on day one, never updated). Hire date can drift ±1-3 days from HRIS.
    columns:
      - name: dms_user_id
        tests: [not_null, unique]
      - name: short_first_name
        tests: [not_null]
      - name: last_name
        tests: [not_null]
      - name: hire_date_dms
        description: Local hire date in DMS. May drift ±1-3 days from HRIS.HIRE_DATE.

  # ===========================================================================
  - name: stg_erp__users
    description: |
      Cleaned 1:1 view of RAW_ERP_USERS. ~10% of rows have linked_dms_user_id
      NULL — modeling real-world manual data drift.
    columns:
      - name: erp_user_id
        tests: [not_null, unique]
      - name: linked_dms_user_id
        description: |
          FK to stg_dms__users.dms_user_id. Nullable: ~10% of rows have a
          broken link, exposed via has_broken_dms_link flag.
      - name: has_broken_dms_link
        description: True when linked_dms_user_id is null. Useful for orphan-detection queries.
        tests:
          - not_null
```

### 22. `dbt_project/models/staging/hris/stg_hris__employees.sql`

**Purpose:** dbt SQL model. Read the CTEs from top to bottom: each CTE adds one layer of business logic before the final select defines the model grain.

**Source:**

```sql
{{
    config(
        materialized='view',
        tags=['staging', 'hris']
    )
}}

-- =============================================================================
-- stg_hris__employees
-- =============================================================================
-- 1:1 mirror of RAW_HRIS_EMPLOYEES with:
--   - lowercase column names
--   - trimmed strings
--   - explicit nulls (empty strings -> NULL)
--   - email_local_part extracted (useful identity anchor)
--
-- No joins, no business logic, no de-duplication. This layer's job is to be
-- predictable and boring.
-- =============================================================================

with source as (
    select * from {{ source('atlas_raw', 'RAW_HRIS_EMPLOYEES') }}
),

renamed as (
    select
        -- Identifiers
        trim(hris_employee_id)                                       as hris_employee_id,

        -- Names: trim and lower-case for cross-system matching consistency.
        -- We KEEP the original casing in `*_original` columns so analysts can
        -- still display names properly downstream.
        trim(legal_first_name)                                        as legal_first_name_original,
        trim(legal_last_name)                                         as legal_last_name_original,
        nullif(trim(preferred_name), '')                              as preferred_name_original,

        lower(trim(legal_first_name))                                 as legal_first_name,
        lower(trim(legal_last_name))                                  as legal_last_name,
        lower(nullif(trim(preferred_name), ''))                       as preferred_name,

        -- Dates
        date_of_birth                                                 as date_of_birth,
        hire_date                                                     as hire_date,
        termination_date                                              as termination_date,

        -- Emails: lower and trim. Local part is everything before '@'.
        lower(trim(personal_email))                                   as personal_email,
        lower(trim(work_email))                                       as work_email,
        case
            when work_email is not null and position('@' in work_email) > 0
                then lower(trim(split_part(work_email, '@', 1)))
        end                                                           as work_email_local_part,
        case
            when personal_email is not null and position('@' in personal_email) > 0
                then lower(trim(split_part(personal_email, '@', 1)))
        end                                                           as personal_email_local_part,

        -- Employment context
        upper(trim(employment_status))                                as employment_status,
        upper(trim(employment_type))                                  as employment_type,
        trim(department)                                              as department,
        trim(job_title)                                               as job_title,
        trim(manager_hris_id)                                         as manager_hris_id,
        trim(location)                                                as location,

        -- Provenance
        loaded_at                                                     as loaded_at,
        '{{ invocation_id }}'                                         as _dbt_invocation_id

    from source
)

select * from renamed
```

### 23. `dbt_project/models/staging/ats/stg_ats__candidates.sql`

**Purpose:** dbt SQL model. Read the CTEs from top to bottom: each CTE adds one layer of business logic before the final select defines the model grain.

**Source:**

```sql
{{
    config(
        materialized='view',
        tags=['staging', 'ats']
    )
}}

-- =============================================================================
-- stg_ats__candidates
-- =============================================================================
-- 1:1 mirror of RAW_ATS_CANDIDATES.
-- ATS uses preferred name and personal email (no work email at this stage).
-- =============================================================================

with source as (
    select * from {{ source('atlas_raw', 'RAW_ATS_CANDIDATES') }}
),

renamed as (
    select
        trim(ats_candidate_id)                                        as ats_candidate_id,

        -- Names
        trim(preferred_first_name)                                    as preferred_first_name_original,
        trim(last_name)                                               as last_name_original,
        lower(trim(preferred_first_name))                             as preferred_first_name,
        lower(trim(last_name))                                        as last_name,

        -- Email
        lower(trim(email))                                            as email,
        case
            when email is not null and position('@' in email) > 0
                then lower(trim(split_part(email, '@', 1)))
        end                                                           as email_local_part,

        -- Phone (kept as-is; standardization happens at intermediate layer if needed)
        trim(phone)                                                   as phone,

        -- Application lifecycle
        application_date                                              as application_date,
        offer_accepted_date                                           as offer_accepted_date,
        trim(sourced_from)                                            as sourced_from,

        -- Requisition context
        trim(requisition_department)                                  as requisition_department,
        trim(requisition_job_title)                                   as requisition_job_title,

        -- Provenance
        loaded_at                                                     as loaded_at,
        '{{ invocation_id }}'                                         as _dbt_invocation_id

    from source
)

select * from renamed
```

### 24. `dbt_project/models/staging/payroll/stg_payroll__records.sql`

**Purpose:** dbt SQL model. Read the CTEs from top to bottom: each CTE adds one layer of business logic before the final select defines the model grain.

**Source:**

```sql
{{
    config(
        materialized='view',
        tags=['staging', 'payroll']
    )
}}

-- =============================================================================
-- stg_payroll__records
-- =============================================================================
-- 1:1 mirror of RAW_PAYROLL_RECORDS.
-- Note: SIN_LAST_4 is sensitive — restrict access at the marts layer.
-- Payroll often LAGS HRIS on name updates (e.g. marriage), so the same
-- person may appear here under their pre-marriage name long after HRIS updated.
-- =============================================================================

with source as (
    select * from {{ source('atlas_raw', 'RAW_PAYROLL_RECORDS') }}
),

renamed as (
    select
        trim(payroll_record_id)                                       as payroll_record_id,
        trim(employee_payroll_id)                                     as employee_payroll_id,

        -- Names (keep original casing in *_original for display)
        trim(legal_first_name)                                        as legal_first_name_original,
        trim(legal_last_name)                                         as legal_last_name_original,
        lower(trim(legal_first_name))                                 as legal_first_name,
        lower(trim(legal_last_name))                                  as legal_last_name,

        -- Sensitive: surface but flag in column docs
        sin_last_4                                                    as sin_last_4,

        -- Period and amounts
        pay_period_start                                              as pay_period_start,
        pay_period_end                                                as pay_period_end,
        gross_amount_cad                                              as gross_amount_cad,
        hours_worked                                                  as hours_worked,

        -- Org context
        trim(job_code)                                                as job_code,
        trim(cost_center)                                             as cost_center,

        -- Provenance
        loaded_at                                                     as loaded_at,
        '{{ invocation_id }}'                                         as _dbt_invocation_id

    from source
)

select * from renamed
```

### 25. `dbt_project/models/staging/crm/stg_crm__sales_reps.sql`

**Purpose:** dbt SQL model. Read the CTEs from top to bottom: each CTE adds one layer of business logic before the final select defines the model grain.

**Source:**

```sql
{{
    config(
        materialized='view',
        tags=['staging', 'crm']
    )
}}

-- =============================================================================
-- stg_crm__sales_reps
-- =============================================================================
-- 1:1 mirror of RAW_CRM_SALES_REPS.
-- Only sales/support roles have CRM accounts. Uses preferred name.
-- =============================================================================

with source as (
    select * from {{ source('atlas_raw', 'RAW_CRM_SALES_REPS') }}
),

renamed as (
    select
        trim(crm_user_id)                                             as crm_user_id,

        -- Names
        trim(preferred_first_name)                                    as preferred_first_name_original,
        trim(last_name)                                               as last_name_original,
        trim(display_name)                                            as display_name_original,
        lower(trim(preferred_first_name))                             as preferred_first_name,
        lower(trim(last_name))                                        as last_name,

        -- Email + local part
        lower(trim(crm_email))                                        as crm_email,
        case
            when crm_email is not null and position('@' in crm_email) > 0
                then lower(trim(split_part(crm_email, '@', 1)))
        end                                                           as crm_email_local_part,

        -- Org context
        trim(location_id)                                             as location_id,
        upper(trim(role))                                             as role,

        -- Lifecycle
        active                                                        as active,
        created_at                                                    as created_at,
        deactivated_at                                                as deactivated_at,

        -- Provenance
        loaded_at                                                     as loaded_at,
        '{{ invocation_id }}'                                         as _dbt_invocation_id

    from source
)

select * from renamed
```

### 26. `dbt_project/models/staging/dms/stg_dms__users.sql`

**Purpose:** dbt SQL model. Read the CTEs from top to bottom: each CTE adds one layer of business logic before the final select defines the model grain.

**Source:**

```sql
{{
    config(
        materialized='view',
        tags=['staging', 'dms']
    )
}}

-- =============================================================================
-- stg_dms__users
-- =============================================================================
-- 1:1 mirror of RAW_DMS_USERS.
-- DMS uses the SHORTENED first name (key drift point: 'Bob' for 'Robert').
-- =============================================================================

with source as (
    select * from {{ source('atlas_raw', 'RAW_DMS_USERS') }}
),

renamed as (
    select
        trim(dms_user_id)                                             as dms_user_id,

        -- Names
        trim(short_first_name)                                        as short_first_name_original,
        trim(last_name)                                               as last_name_original,
        lower(trim(short_first_name))                                 as short_first_name,
        lower(trim(last_name))                                        as last_name,

        lower(trim(dms_username))                                     as dms_username,

        -- Org context
        trim(location_code)                                           as location_code,
        trim(department_code)                                         as department_code,

        -- Lifecycle (note: hire_date here can drift ±1-3 days from HRIS.HIRE_DATE)
        hire_date_dms                                                 as hire_date_dms,
        terminated_date_dms                                           as terminated_date_dms,

        -- Provenance
        loaded_at                                                     as loaded_at,
        '{{ invocation_id }}'                                         as _dbt_invocation_id

    from source
)

select * from renamed
```

### 27. `dbt_project/models/staging/erp/stg_erp__users.sql`

**Purpose:** dbt SQL model. Read the CTEs from top to bottom: each CTE adds one layer of business logic before the final select defines the model grain.

**Source:**

```sql
{{
    config(
        materialized='view',
        tags=['staging', 'erp']
    )
}}

-- =============================================================================
-- stg_erp__users
-- =============================================================================
-- 1:1 mirror of RAW_ERP_USERS.
-- ERP mostly mirrors DMS but ~10% of LINKED_DMS_USER_ID values are NULL,
-- modeling real-world drift where the DMS-ERP link is broken.
-- =============================================================================

with source as (
    select * from {{ source('atlas_raw', 'RAW_ERP_USERS') }}
),

renamed as (
    select
        trim(erp_user_id)                                             as erp_user_id,
        trim(linked_dms_user_id)                                      as linked_dms_user_id,

        -- Names
        trim(short_first_name)                                        as short_first_name_original,
        trim(last_name)                                               as last_name_original,
        lower(trim(short_first_name))                                 as short_first_name,
        lower(trim(last_name))                                        as last_name,

        -- Email + local part
        lower(trim(erp_email))                                        as erp_email,
        case
            when erp_email is not null and position('@' in erp_email) > 0
                then lower(trim(split_part(erp_email, '@', 1)))
        end                                                           as erp_email_local_part,

        -- Permissions context
        trim(role_code)                                               as role_code,
        trim(permissions_group)                                       as permissions_group,

        -- Lifecycle / activity
        created_at                                                    as created_at,
        last_login_at                                                 as last_login_at,

        -- Useful flag for downstream: is this row missing its DMS link?
        case when linked_dms_user_id is null then true else false end as has_broken_dms_link,

        -- Provenance
        loaded_at                                                     as loaded_at,
        '{{ invocation_id }}'                                         as _dbt_invocation_id

    from source
)

select * from renamed
```

### 28. `dbt_project/models/intermediate/_intermediate.yml`

**Purpose:** YAML configuration/documentation. It defines dbt metadata, tests, dependencies, sources, exposures, or fixtures.

**Source:**

```yaml
version: 2

models:

  # =========================================================================
  - name: int_dms_erp_unified
    description: |
      Phase 2C, Pass 0. Bipartite graph-merge of DMS users and ERP users via
      the hard FK ERP.LINKED_DMS_USER_ID -> DMS.DMS_USER_ID. One row per
      unified DMS+ERP person, with a topology classification and array of
      source-system attributes.
    columns:
      - name: dms_erp_person_key
        description: Surrogate hash of (dms_user_id, erp_user_id). Stable across reruns.
        tests:
          - not_null
          - unique
      - name: dms_user_id
        description: Nullable — NULL for ERP_ONLY_BROKEN_LINK topology. Uniqueness enforced where not null by tests/intermediate/.
      - name: erp_user_id
        description: Nullable — NULL for DMS_ONLY topology. Uniqueness enforced where not null by tests/intermediate/.
      - name: merge_topology
        description: Classification of which source systems contributed to this person.
        tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['DMS_AND_ERP', 'DMS_ONLY', 'ERP_ONLY_BROKEN_LINK']
      - name: has_dms
        tests: [not_null]
      - name: has_erp
        tests: [not_null]
      - name: has_broken_link
        description: TRUE iff has_erp AND ERP.linked_dms_user_id is NULL.
        tests: [not_null]

  # =========================================================================
  - name: int_hris_persons
    description: |
      Phase 2C, Step 1 prep. Collapses HRIS rehires + contractor-to-FTE
      transitions into one row per HRIS-distinct person, grouped by
      (date_of_birth, personal_email_local_part). Exposes both canonical
      (earliest-spell) and current (latest-spell) snapshots so the matcher
      can choose the right anchor for each pass.
    tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns:
              - date_of_birth
              - personal_email_local_part
    columns:
      - name: hris_person_key
        description: Surrogate hash of (date_of_birth, personal_email_local_part). Source-grain key.
        tests:
          - not_null
          - unique
      - name: date_of_birth
        tests: [not_null]
      - name: personal_email_local_part
        tests: [not_null]
      - name: canonical_legal_first_name
        description: legal_first_name from the earliest-hire spell. Anchors the cross-source canonical_person_id.
        tests: [not_null]
      - name: canonical_legal_last_name
        description: legal_last_name from the earliest-hire spell. Pre any marriage drift.
        tests: [not_null]
      - name: canonical_hire_date
        tests: [not_null]
      - name: spell_count
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              arguments:
                expression: ">= 1"
      - name: hris_employee_ids
        description: Array of all HRIS_EMPLOYEE_IDs that belong to this person across rehires.
        tests: [not_null]
      - name: has_rehires
        tests: [not_null]
      - name: has_name_change_marriage
        description: TRUE iff canonical_legal_last_name differs from current_legal_last_name.
        tests: [not_null]
      - name: work_email_local_part
        description: Constant across all spells of a given person. Pass 1 anchor.

  # =========================================================================
  - name: int_payroll_spells
    description: |
      Phase 2C, Step 1 prep. Collapses ~153K monthly pay-period rows into
      ~5K spell-grain rows, grouped by EMPLOYEE_PAYROLL_ID. Exposes first-
      observed and most-recent legal name snapshots, plus pay-period
      aggregates. Reduces downstream join cardinality from 153K x N to
      5K x N.
    columns:
      - name: payroll_spell_key
        description: Surrogate hash of EMPLOYEE_PAYROLL_ID.
        tests:
          - not_null
          - unique
      - name: employee_payroll_id
        description: |
          Stable spell-identity column. Embeds canonical person_id in the
          synthesizer ("oracle leak") — used here ONLY for grouping equality,
          NEVER parsed for cross-source matching.
        tests:
          - not_null
          - unique
      - name: first_observed_legal_first_name
        tests: [not_null]
      - name: first_observed_legal_last_name
        tests: [not_null]
      - name: first_pay_period_start
        tests: [not_null]
      - name: most_recent_pay_period_end
        tests: [not_null]
      - name: pay_period_count
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              arguments:
                expression: ">= 1"
      - name: gross_amount_cad_total
        tests: [not_null]
      - name: hours_worked_total
        tests: [not_null]
      - name: has_intra_spell_last_name_change
        tests: [not_null]
      - name: sin_last_4_distinct_count
        description: |
          Diagnostic only — count of distinct SIN_LAST_4 values seen across
          this spell's pay periods. The synthesizer regenerates SIN per
          period, so this typically equals pay_period_count. NEVER use as a
          matching anchor.

  # =========================================================================
  - name: int_identity_source_nodes
    description: |
      Phase 2C common identity-node grain used by the deterministic matcher.
      One row per HRIS person, ATS candidate, payroll spell, CRM user, and
      unified DMS/ERP topology component.
    columns:
      - name: source_record_key
        description: Stable source-scoped identity key, prefixed with source system.
        tests:
          - not_null
          - unique
      - name: source_system
        tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['HRIS', 'ATS', 'PAYROLL', 'CRM', 'DMS_ERP']
      - name: source_primary_id
        tests: [not_null]
      - name: source_first_name_root
        description: Normalized first-name root after nickname mapping.
      - name: source_last_name_norm
        description: Normalized last name for deterministic comparisons.

  # =========================================================================
  - name: int_identity_pass_1_hard_anchors
    description: |
      Phase 2C deterministic Pass 1. Matches non-HRIS source identities to HRIS
      using exact personal-email or work-email-local-part anchors. Payroll
      SIN_LAST_4 is deliberately not used because this synthetic feed lacks a
      safe DOB+SIN bridge.
    columns:
      - name: source_record_key
        tests: [not_null]
      - name: hris_person_key
        tests: [not_null]
      - name: match_pass
        tests:
          - not_null
          - accepted_values:
              arguments:
                values: [1]
      - name: match_score
        tests: [not_null]
      - name: candidate_hris_person_count
        tests: [not_null]
      - name: auto_merge_qualified
        tests: [not_null]

  # =========================================================================
  - name: int_identity_pass_2_name_dob_hire
    description: |
      Phase 2C deterministic Pass 2. Evaluates normalized first-name-root +
      last-name + hire-date proximity candidates, and only auto-merges when the
      source also exposes exact DOB. Current non-HRIS synthetic feeds generally
      lack DOB, so these rows mainly feed stewardship evidence.
    columns:
      - name: source_record_key
        tests: [not_null]
      - name: hris_person_key
        tests: [not_null]
      - name: match_pass
        tests:
          - not_null
          - accepted_values:
              arguments:
                values: [2]
      - name: match_score
        tests: [not_null]
      - name: candidate_hris_person_count
        tests: [not_null]
      - name: auto_merge_qualified
        tests: [not_null]

  # =========================================================================
  - name: int_identity_pass_3_email_domain
    description: |
      Phase 2C deterministic Pass 3. Recovers company-email-domain + last-name
      token matches when exact email local parts fail, gated by hire-date
      proximity and a unique HRIS candidate.
    columns:
      - name: source_record_key
        tests: [not_null]
      - name: hris_person_key
        tests: [not_null]
      - name: match_pass
        tests:
          - not_null
          - accepted_values:
              arguments:
                values: [3]
      - name: match_score
        tests: [not_null]
      - name: candidate_hris_person_count
        tests: [not_null]
      - name: auto_merge_qualified
        tests: [not_null]

  # =========================================================================
  - name: int_canonical_person
    description: |
      Phase 2C canonical identity output. One row per HRIS-distinct person with
      stable canonical_person_id and arrays of deterministically resolved
      source-system identifiers.
    columns:
      - name: canonical_person_id
        tests:
          - not_null
          - unique
      - name: hris_person_key
        tests:
          - not_null
          - unique
      - name: hris_employee_ids
        tests: [not_null]
      - name: current_hris_employee_id
        tests: [not_null]
      - name: date_of_birth
        tests: [not_null]
      - name: canonical_hire_date
        tests: [not_null]
      - name: matched_external_source_node_count
        tests: [not_null]
      - name: matched_external_source_system_count
        tests: [not_null]
      - name: payroll_spell_count
        description: |
          Count of payroll spells auto-merged to this canonical person. Expected
          to be zero until the payroll feed gains a safe DOB/email bridge.
        tests: [not_null]

  # =========================================================================
  - name: int_stewardship_queue
    description: |
      Phase 2C manual adjudication surface. One row per non-HRIS source identity
      that did not qualify for deterministic auto-merge, with best candidate
      evidence and a stewardship reason.
    columns:
      - name: stewardship_queue_id
        tests:
          - not_null
          - unique
      - name: source_record_key
        tests:
          - not_null
          - unique
      - name: source_system
        tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['ATS', 'PAYROLL', 'CRM', 'DMS_ERP']
      - name: stewardship_reason
        tests:
          - not_null
          - accepted_values:
              arguments:
                values:
                  - 'NO_DETERMINISTIC_CANDIDATE'
                  - 'AMBIGUOUS_CANDIDATES'
                  - 'BELOW_AUTO_MATCH_THRESHOLD'
                  - 'MANUAL_REVIEW_REQUIRED'
```

### 29. `dbt_project/models/intermediate/int_identity_source_nodes.sql`

**Purpose:** The common identity-node grain. This is the bridge from six source systems into one matching problem.

**Source:**

```sql
{{
    config(
        materialized='table',
        tags=['intermediate', 'phase_2c', 'identity_source_nodes']
    )
}}

-- =============================================================================
-- int_identity_source_nodes — Phase 2C common matching grain
-- =============================================================================
-- Puts each matchable source-system identity onto one normalized shape before
-- the deterministic passes run. This model deliberately keeps the grain close
-- to the operational systems:
--
--   HRIS      one row per HRIS-distinct person from int_hris_persons
--   ATS       one row per candidate/application
--   PAYROLL   one row per payroll spell from int_payroll_spells
--   CRM       one row per CRM user
--   DMS_ERP   one row per unified DMS/ERP topology component
--
-- DMS and ERP are represented together because int_dms_erp_unified has already
-- consumed the hard structural FK. The individual dms_user_id / erp_user_id
-- columns remain here so coverage tests can still prove every staging row is
-- either resolved or queued for stewardship.
--
-- Design note: this layer normalizes names and email anchors only. It does not
-- decide identity. False positives in HR data are worse than false negatives,
-- so deterministic decisions remain isolated in the three pass models where
-- their evidence and thresholds are auditable.
-- =============================================================================

with hris as (
    select * from {{ ref('int_hris_persons') }}
),

ats as (
    select * from {{ ref('stg_ats__candidates') }}
),

payroll as (
    select * from {{ ref('int_payroll_spells') }}
),

crm as (
    select * from {{ ref('stg_crm__sales_reps') }}
),

dms_erp as (
    select * from {{ ref('int_dms_erp_unified') }}
),

hris_nodes as (
    select
        'HRIS'                                                               as source_system,
        'HRIS_PERSON::' || h.hris_person_key                                 as source_record_key,
        h.hris_person_key                                                    as source_primary_id,
        h.hris_person_key                                                    as hris_person_key,

        h.canonical_legal_first_name                                         as source_first_name,
        h.canonical_legal_last_name                                          as source_last_name,
        h.canonical_legal_first_name_original                                as source_first_name_original,
        h.canonical_legal_last_name_original                                 as source_last_name_original,
        {{ first_name_root('h.canonical_legal_first_name', 'hris_nm') }}      as source_first_name_root,
        {{ normalize_name('h.canonical_legal_last_name') }}                  as source_last_name_norm,

        h.date_of_birth                                                      as date_of_birth,
        h.canonical_hire_date                                                as source_hire_date,
        h.latest_termination_date                                            as source_end_date,

        h.personal_email_local_part                                          as personal_email_local_part,
        case
            when h.personal_email is not null and position('@' in h.personal_email) > 0
                then split_part(h.personal_email, '@', 2)
        end                                                                  as personal_email_domain,
        h.work_email_local_part                                              as work_email_local_part,
        case
            when h.work_email is not null and position('@' in h.work_email) > 0
                then split_part(h.work_email, '@', 2)
        end                                                                  as work_email_domain,

        h.work_email_local_part                                              as source_email_local_part,
        case
            when h.work_email is not null and position('@' in h.work_email) > 0
                then split_part(h.work_email, '@', 2)
        end                                                                  as source_email_domain,

        cast(null as varchar)                                                as ats_candidate_id,
        cast(null as varchar)                                                as payroll_spell_key,
        cast(null as varchar)                                                as employee_payroll_id,
        cast(null as varchar)                                                as crm_user_id,
        cast(null as varchar)                                                as dms_erp_person_key,
        cast(null as varchar)                                                as dms_user_id,
        cast(null as varchar)                                                as erp_user_id,
        cast(null as varchar)                                                as merge_topology,
        cast(null as boolean)                                                as has_dms,
        cast(null as boolean)                                                as has_erp,
        cast(null as boolean)                                                as has_broken_link,

        h.loaded_at                                                          as loaded_at,
        '{{ invocation_id }}'                                                as _dbt_invocation_id
    from hris h
    left join {{ ref('nickname_map') }} hris_nm
        on hris_nm.nickname = {{ normalize_name('h.canonical_legal_first_name') }}
),

ats_nodes as (
    select
        'ATS'                                                                as source_system,
        'ATS::' || a.ats_candidate_id                                        as source_record_key,
        a.ats_candidate_id                                                   as source_primary_id,
        cast(null as varchar)                                                as hris_person_key,

        a.preferred_first_name                                               as source_first_name,
        a.last_name                                                          as source_last_name,
        a.preferred_first_name_original                                      as source_first_name_original,
        a.last_name_original                                                 as source_last_name_original,
        {{ first_name_root('a.preferred_first_name', 'ats_nm') }}             as source_first_name_root,
        {{ normalize_name('a.last_name') }}                                  as source_last_name_norm,

        cast(null as date)                                                   as date_of_birth,
        a.offer_accepted_date                                                as source_hire_date,
        cast(null as date)                                                   as source_end_date,

        a.email_local_part                                                   as personal_email_local_part,
        case
            when a.email is not null and position('@' in a.email) > 0
                then split_part(a.email, '@', 2)
        end                                                                  as personal_email_domain,
        cast(null as varchar)                                                as work_email_local_part,
        cast(null as varchar)                                                as work_email_domain,

        a.email_local_part                                                   as source_email_local_part,
        case
            when a.email is not null and position('@' in a.email) > 0
                then split_part(a.email, '@', 2)
        end                                                                  as source_email_domain,

        a.ats_candidate_id                                                   as ats_candidate_id,
        cast(null as varchar)                                                as payroll_spell_key,
        cast(null as varchar)                                                as employee_payroll_id,
        cast(null as varchar)                                                as crm_user_id,
        cast(null as varchar)                                                as dms_erp_person_key,
        cast(null as varchar)                                                as dms_user_id,
        cast(null as varchar)                                                as erp_user_id,
        cast(null as varchar)                                                as merge_topology,
        cast(null as boolean)                                                as has_dms,
        cast(null as boolean)                                                as has_erp,
        cast(null as boolean)                                                as has_broken_link,

        a.loaded_at                                                          as loaded_at,
        '{{ invocation_id }}'                                                as _dbt_invocation_id
    from ats a
    left join {{ ref('nickname_map') }} ats_nm
        on ats_nm.nickname = {{ normalize_name('a.preferred_first_name') }}
),

payroll_nodes as (
    select
        'PAYROLL'                                                            as source_system,
        'PAYROLL::' || p.payroll_spell_key                                   as source_record_key,
        p.employee_payroll_id                                                as source_primary_id,
        cast(null as varchar)                                                as hris_person_key,

        p.first_observed_legal_first_name                                    as source_first_name,
        p.first_observed_legal_last_name                                     as source_last_name,
        p.first_observed_legal_first_name_original                           as source_first_name_original,
        p.first_observed_legal_last_name_original                            as source_last_name_original,
        {{ first_name_root('p.first_observed_legal_first_name', 'pay_nm') }}  as source_first_name_root,
        {{ normalize_name('p.first_observed_legal_last_name') }}             as source_last_name_norm,

        cast(null as date)                                                   as date_of_birth,
        p.first_pay_period_start                                             as source_hire_date,
        p.most_recent_pay_period_end                                         as source_end_date,

        cast(null as varchar)                                                as personal_email_local_part,
        cast(null as varchar)                                                as personal_email_domain,
        cast(null as varchar)                                                as work_email_local_part,
        cast(null as varchar)                                                as work_email_domain,
        cast(null as varchar)                                                as source_email_local_part,
        cast(null as varchar)                                                as source_email_domain,

        cast(null as varchar)                                                as ats_candidate_id,
        p.payroll_spell_key                                                  as payroll_spell_key,
        p.employee_payroll_id                                                as employee_payroll_id,
        cast(null as varchar)                                                as crm_user_id,
        cast(null as varchar)                                                as dms_erp_person_key,
        cast(null as varchar)                                                as dms_user_id,
        cast(null as varchar)                                                as erp_user_id,
        cast(null as varchar)                                                as merge_topology,
        cast(null as boolean)                                                as has_dms,
        cast(null as boolean)                                                as has_erp,
        cast(null as boolean)                                                as has_broken_link,

        p.loaded_at                                                          as loaded_at,
        '{{ invocation_id }}'                                                as _dbt_invocation_id
    from payroll p
    left join {{ ref('nickname_map') }} pay_nm
        on pay_nm.nickname = {{ normalize_name('p.first_observed_legal_first_name') }}
),

crm_nodes as (
    select
        'CRM'                                                                as source_system,
        'CRM::' || c.crm_user_id                                             as source_record_key,
        c.crm_user_id                                                        as source_primary_id,
        cast(null as varchar)                                                as hris_person_key,

        c.preferred_first_name                                               as source_first_name,
        c.last_name                                                          as source_last_name,
        c.preferred_first_name_original                                      as source_first_name_original,
        c.last_name_original                                                 as source_last_name_original,
        {{ first_name_root('c.preferred_first_name', 'crm_nm') }}             as source_first_name_root,
        {{ normalize_name('c.last_name') }}                                  as source_last_name_norm,

        cast(null as date)                                                   as date_of_birth,
        cast(c.created_at as date)                                           as source_hire_date,
        cast(c.deactivated_at as date)                                       as source_end_date,

        cast(null as varchar)                                                as personal_email_local_part,
        cast(null as varchar)                                                as personal_email_domain,
        c.crm_email_local_part                                               as work_email_local_part,
        case
            when c.crm_email is not null and position('@' in c.crm_email) > 0
                then split_part(c.crm_email, '@', 2)
        end                                                                  as work_email_domain,
        c.crm_email_local_part                                               as source_email_local_part,
        case
            when c.crm_email is not null and position('@' in c.crm_email) > 0
                then split_part(c.crm_email, '@', 2)
        end                                                                  as source_email_domain,

        cast(null as varchar)                                                as ats_candidate_id,
        cast(null as varchar)                                                as payroll_spell_key,
        cast(null as varchar)                                                as employee_payroll_id,
        c.crm_user_id                                                        as crm_user_id,
        cast(null as varchar)                                                as dms_erp_person_key,
        cast(null as varchar)                                                as dms_user_id,
        cast(null as varchar)                                                as erp_user_id,
        cast(null as varchar)                                                as merge_topology,
        cast(null as boolean)                                                as has_dms,
        cast(null as boolean)                                                as has_erp,
        cast(null as boolean)                                                as has_broken_link,

        c.loaded_at                                                          as loaded_at,
        '{{ invocation_id }}'                                                as _dbt_invocation_id
    from crm c
    left join {{ ref('nickname_map') }} crm_nm
        on crm_nm.nickname = {{ normalize_name('c.preferred_first_name') }}
),

dms_erp_nodes as (
    select
        'DMS_ERP'                                                            as source_system,
        'DMS_ERP::' || d.dms_erp_person_key                                  as source_record_key,
        d.dms_erp_person_key                                                 as source_primary_id,
        cast(null as varchar)                                                as hris_person_key,

        d.short_first_name                                                   as source_first_name,
        d.last_name                                                          as source_last_name,
        d.short_first_name_original                                          as source_first_name_original,
        d.last_name_original                                                 as source_last_name_original,
        {{ first_name_root('d.short_first_name', 'dms_nm') }}                 as source_first_name_root,
        {{ normalize_name('d.last_name') }}                                  as source_last_name_norm,

        cast(null as date)                                                   as date_of_birth,
        coalesce(d.hire_date_dms, cast(d.erp_created_at as date))             as source_hire_date,
        d.terminated_date_dms                                                as source_end_date,

        cast(null as varchar)                                                as personal_email_local_part,
        cast(null as varchar)                                                as personal_email_domain,
        d.erp_email_local_part                                               as work_email_local_part,
        case
            when d.erp_email is not null and position('@' in d.erp_email) > 0
                then split_part(d.erp_email, '@', 2)
        end                                                                  as work_email_domain,
        d.erp_email_local_part                                               as source_email_local_part,
        case
            when d.erp_email is not null and position('@' in d.erp_email) > 0
                then split_part(d.erp_email, '@', 2)
        end                                                                  as source_email_domain,

        cast(null as varchar)                                                as ats_candidate_id,
        cast(null as varchar)                                                as payroll_spell_key,
        cast(null as varchar)                                                as employee_payroll_id,
        cast(null as varchar)                                                as crm_user_id,
        d.dms_erp_person_key                                                 as dms_erp_person_key,
        d.dms_user_id                                                        as dms_user_id,
        d.erp_user_id                                                        as erp_user_id,
        d.merge_topology                                                     as merge_topology,
        d.has_dms                                                            as has_dms,
        d.has_erp                                                            as has_erp,
        d.has_broken_link                                                    as has_broken_link,

        d.loaded_at                                                          as loaded_at,
        '{{ invocation_id }}'                                                as _dbt_invocation_id
    from dms_erp d
    left join {{ ref('nickname_map') }} dms_nm
        on dms_nm.nickname = {{ normalize_name('d.short_first_name') }}
)

select * from hris_nodes
union all
select * from ats_nodes
union all
select * from payroll_nodes
union all
select * from crm_nodes
union all
select * from dms_erp_nodes
```

### 30. `dbt_project/models/intermediate/int_hris_persons.sql`

**Purpose:** Collapses HRIS employment spells into HRIS person anchors, including rehire/name-change survival logic.

**Source:**

```sql
{{
    config(
        materialized='table',
        tags=['intermediate', 'phase_2c', 'pass_1_prep']
    )
}}

-- =============================================================================
-- int_hris_persons — Phase 2C, Step 1 prep
-- =============================================================================
-- Collapses HRIS rehires + contractor-to-FTE transitions into one row per
-- HRIS-distinct person. Each input row in stg_hris__employees represents one
-- employment spell (hire-to-termination); a single canonical person can have
-- multiple spells across rehires, each with a different HRIS_EMPLOYEE_ID
-- (suffixes _R1, _R2, _FTE per the synthesizer's _id_for_system).
--
-- ---------------------------------------------------------------------------
-- Grouping key: (date_of_birth, personal_email_local_part)
-- ---------------------------------------------------------------------------
-- Why these two columns:
--   * date_of_birth is immutable identity attribute, set at hire and never
--     updated. Same person across rehires has the same DOB.
--   * personal_email_local_part is set at canonical-identity creation
--     (synthesize.py: identity.personal_email) and persists across every
--     employment spell — marriage doesn't change it, rehire doesn't change it.
--
-- Why not (date_of_birth, normalize_name(legal_last_name))? Because
-- legal_last_name CAN change between spells: a marriage event during spell N
-- mutates the in-memory current_last, and the row emitted at TERMINATION of
-- spell N captures the post-marriage name. The next rehire (spell N+1)
-- starts with that post-marriage name. So two spells for one person CAN
-- have different last_names if any prior spell saw a marriage event. Last-
-- name grouping would silently fail to collapse those cases — we'd see them
-- as two distinct persons.
--
-- Why not date_of_birth alone? In a 5K-employee population with ~14,600
-- distinct DOBs in working-age range, expected DOB collisions are ~900+
-- (~18% of population shares DOB with at least one other person). DOB
-- alone is insufficient to distinguish people.
--
-- ---------------------------------------------------------------------------
-- Marriage name handling
-- ---------------------------------------------------------------------------
-- Per the locked design: the "canonical" identity tuple uses MIN(hire_date)
-- to anchor on the earliest observed name (pre any marriage drift). The
-- "current" snapshot uses MAX(hire_date) to reflect the latest known state.
-- Both are exposed for downstream use:
--   canonical_*  -> stable, anchors the cross-source canonical_person_id
--   current_*    -> latest, used for display and operational reporting
--
-- The has_name_change_marriage flag fires when canonical_legal_last_name
-- differs from current_legal_last_name. Useful for downstream auditing.
--
-- ---------------------------------------------------------------------------
-- Output grain: one row per HRIS-distinct person. Output `hris_person_key`
-- is the surrogate hash of the grouping key — NOT the final cross-source
-- canonical_person_id. That gets computed in int_canonical_person after all
-- passes have run.
-- =============================================================================

with hris as (
    select * from {{ ref('stg_hris__employees') }}
),

ranked as (
    -- Rank spells within each (DOB, personal_email_local_part) group:
    --   spell_rank_asc  = 1 -> earliest hire_date (canonical)
    --   spell_rank_desc = 1 -> latest hire_date   (current state)
    select
        h.*,
        row_number() over (
            partition by date_of_birth, personal_email_local_part
            order by hire_date asc, hris_employee_id asc
        ) as spell_rank_asc,
        row_number() over (
            partition by date_of_birth, personal_email_local_part
            order by hire_date desc, hris_employee_id desc
        ) as spell_rank_desc
    from hris h
    where date_of_birth is not null
      and personal_email_local_part is not null
),

person_aggs as (
    select
        -- ---- Grouping key ----
        date_of_birth,
        personal_email_local_part,

        -- ---- Canonical (earliest-spell) anchors ----
        max(case when spell_rank_asc = 1 then hire_date end)                     as canonical_hire_date,
        max(case when spell_rank_asc = 1 then legal_first_name end)              as canonical_legal_first_name,
        max(case when spell_rank_asc = 1 then legal_last_name end)               as canonical_legal_last_name,
        max(case when spell_rank_asc = 1 then legal_first_name_original end)     as canonical_legal_first_name_original,
        max(case when spell_rank_asc = 1 then legal_last_name_original end)      as canonical_legal_last_name_original,
        max(case when spell_rank_asc = 1 then preferred_name end)                as canonical_preferred_name,

        -- ---- Current (latest-spell) snapshot ----
        max(case when spell_rank_desc = 1 then hris_employee_id end)             as current_hris_employee_id,
        max(case when spell_rank_desc = 1 then hire_date end)                    as latest_hire_date,
        max(case when spell_rank_desc = 1 then termination_date end)             as latest_termination_date,
        max(case when spell_rank_desc = 1 then legal_first_name end)             as current_legal_first_name,
        max(case when spell_rank_desc = 1 then legal_last_name end)              as current_legal_last_name,
        max(case when spell_rank_desc = 1 then preferred_name end)               as current_preferred_name,
        max(case when spell_rank_desc = 1 then employment_status end)            as current_employment_status,
        max(case when spell_rank_desc = 1 then employment_type end)              as current_employment_type,
        max(case when spell_rank_desc = 1 then department end)                   as current_department,
        max(case when spell_rank_desc = 1 then job_title end)                    as current_job_title,
        max(case when spell_rank_desc = 1 then location end)                     as current_location,
        max(case when spell_rank_desc = 1 then manager_hris_id end)              as current_manager_hris_id,

        -- ---- Constants across spells ----
        -- These values are guaranteed by the synthesizer to be stable across
        -- all spells for a given canonical person (set once at canonical
        -- identity creation). Using max() here is just to satisfy GROUP BY
        -- semantics; assert_constant_across_spells_* tests in the YAML enforce
        -- the invariant.
        max(personal_email)                                                       as personal_email,
        max(work_email)                                                           as work_email,
        max(work_email_local_part)                                                as work_email_local_part,

        -- ---- Spell aggregates ----
        count(*)                                                                  as spell_count,
        array_agg(distinct hris_employee_id) within group (order by hris_employee_id) as hris_employee_ids,
        max(loaded_at)                                                            as loaded_at
    from ranked
    group by date_of_birth, personal_email_local_part
),

with_flags_and_key as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'date_of_birth',
            'personal_email_local_part'
        ]) }}                                                                     as hris_person_key,
        person_aggs.*,
        case when spell_count > 1 then true else false end                        as has_rehires,
        case
            when canonical_legal_last_name != current_legal_last_name then true
            else false
        end                                                                       as has_name_change_marriage,
        '{{ invocation_id }}'                                                     as _dbt_invocation_id
    from person_aggs
)

select * from with_flags_and_key
```

### 31. `dbt_project/models/intermediate/int_payroll_spells.sql`

**Purpose:** dbt SQL model. Read the CTEs from top to bottom: each CTE adds one layer of business logic before the final select defines the model grain.

**Source:**

```sql
{{
    config(
        materialized='table',
        tags=['intermediate', 'phase_2c', 'pass_1_prep']
    )
}}

-- =============================================================================
-- int_payroll_spells — Phase 2C, Step 1 prep
-- =============================================================================
-- Collapses ~153K monthly pay-period rows in stg_payroll__records into one
-- row per payroll spell (~5K rows). The grouping key is EMPLOYEE_PAYROLL_ID,
-- which the synthesizer guarantees is stable across all pay periods within
-- a single employment spell (synthesize.py:479 — same payroll_emp_id used
-- for every monthly record in a spell).
--
-- This collapse is essential before downstream matching. Joining 153K
-- payroll rows against 5K HRIS persons would produce noise (multiple
-- candidate matches per pay period). Spell-level grain reduces that to a
-- 5K-vs-5K join.
--
-- ---------------------------------------------------------------------------
-- Discipline note
-- ---------------------------------------------------------------------------
-- EMPLOYEE_PAYROLL_ID embeds the canonical person_id in the synthesizer
-- (PAY{YYYYMM}-{person_id[1:]} per synthesize.py:479). DO NOT parse the
-- numeric suffix to recover person_id — that is the synthesizer's "oracle
-- leak" and using it would short-circuit the entire matcher. We use the
-- column ONLY for grouping equality.
--
-- The legitimate signal extracted here is "spell continuity": every pay
-- period within a single spell shares the same EMPLOYEE_PAYROLL_ID. That
-- equality is the medium-strength anchor (+0.20) used in match_confidence,
-- valid only WITHIN payroll for spell collapse — NOT as a cross-source
-- bridge to HRIS.
--
-- See ~/.claude/.../memory/synthesizer_quirks.md for the full discussion of
-- the leak and which uses are legitimate.
--
-- ---------------------------------------------------------------------------
-- Why not collapse via SIN_LAST_4 + name?
-- ---------------------------------------------------------------------------
-- The synthesizer regenerates SIN_LAST_4 for every monthly pay period
-- (synthesize.py:497, `random.randint(1000, 9999)`). It is NOT stable
-- within a spell, NOT stable within a person. Was originally proposed in
-- CLAUDE.md as an anchor; dropped from the anchor table after verification.
-- See synthesizer_quirks memory.
--
-- ---------------------------------------------------------------------------
-- Earliest vs latest naming
-- ---------------------------------------------------------------------------
-- Payroll's legal_first_name and legal_last_name are captured per pay-period
-- row but typically don't change within a spell (the synthesizer doesn't
-- model intra-spell payroll-side name updates). However, payroll lags HRIS
-- on marriage updates in real life; the data exposes both:
--
--   first_observed_*   -> name as of earliest pay period (most stable anchor)
--   most_recent_*      -> name as of latest pay period (latest known state)
--
-- For Pass 2 matching, prefer first_observed_legal_last_name on the payroll
-- side joined against canonical_legal_last_name on the HRIS side — both
-- anchored to "as of earliest known observation."
--
-- ---------------------------------------------------------------------------
-- SIN_LAST_4 handling
-- ---------------------------------------------------------------------------
-- Surfaced as an array_agg of distinct values per spell, purely for
-- diagnostic visibility. Should never be used as a matching anchor. The
-- count of distinct SIN_LAST_4 values within a spell will typically be
-- close to spell_pay_period_count (one new random value per period).
-- =============================================================================

with payroll as (
    select * from {{ ref('stg_payroll__records') }}
),

ranked as (
    select
        p.*,
        row_number() over (
            partition by employee_payroll_id
            order by pay_period_start asc, payroll_record_id asc
        ) as period_rank_asc,
        row_number() over (
            partition by employee_payroll_id
            order by pay_period_start desc, payroll_record_id desc
        ) as period_rank_desc
    from payroll p
    where employee_payroll_id is not null
),

spell_aggs as (
    select
        -- ---- Grouping key ----
        employee_payroll_id,

        -- ---- First-observed (earliest pay period) snapshot ----
        max(case when period_rank_asc = 1 then payroll_record_id end)             as first_payroll_record_id,
        min(pay_period_start)                                                     as first_pay_period_start,
        max(case when period_rank_asc = 1 then legal_first_name end)              as first_observed_legal_first_name,
        max(case when period_rank_asc = 1 then legal_last_name end)               as first_observed_legal_last_name,
        max(case when period_rank_asc = 1 then legal_first_name_original end)     as first_observed_legal_first_name_original,
        max(case when period_rank_asc = 1 then legal_last_name_original end)      as first_observed_legal_last_name_original,

        -- ---- Most-recent (latest pay period) snapshot ----
        max(case when period_rank_desc = 1 then payroll_record_id end)            as most_recent_payroll_record_id,
        max(pay_period_end)                                                       as most_recent_pay_period_end,
        max(case when period_rank_desc = 1 then legal_first_name end)             as most_recent_legal_first_name,
        max(case when period_rank_desc = 1 then legal_last_name end)              as most_recent_legal_last_name,
        max(case when period_rank_desc = 1 then job_code end)                     as most_recent_job_code,
        max(case when period_rank_desc = 1 then cost_center end)                  as most_recent_cost_center,

        -- ---- Spell aggregates ----
        count(*)                                                                  as pay_period_count,
        sum(gross_amount_cad)                                                     as gross_amount_cad_total,
        sum(hours_worked)                                                         as hours_worked_total,
        avg(gross_amount_cad)                                                     as gross_amount_cad_avg_per_period,

        -- ---- Diagnostic surfaces ----
        -- SIN_LAST_4 is unstable within a spell (regenerated per pay period
        -- by the synthesizer). Surfaced as count of distinct values for
        -- visibility — should equal pay_period_count for any spell longer
        -- than a few periods. NEVER use as a matching anchor.
        count(distinct sin_last_4)                                                as sin_last_4_distinct_count,

        max(loaded_at)                                                            as loaded_at
    from ranked
    group by employee_payroll_id
),

with_flags_and_key as (
    select
        {{ dbt_utils.generate_surrogate_key(['employee_payroll_id']) }}           as payroll_spell_key,
        spell_aggs.*,
        case
            when most_recent_legal_last_name != first_observed_legal_last_name then true
            else false
        end                                                                       as has_intra_spell_last_name_change,
        '{{ invocation_id }}'                                                     as _dbt_invocation_id
    from spell_aggs
)

select * from with_flags_and_key
```

### 32. `dbt_project/models/intermediate/int_dms_erp_unified.sql`

**Purpose:** dbt SQL model. Read the CTEs from top to bottom: each CTE adds one layer of business logic before the final select defines the model grain.

**Source:**

```sql
{{
    config(
        materialized='table',
        tags=['intermediate', 'phase_2c', 'pass_0']
    )
}}

-- =============================================================================
-- int_dms_erp_unified — Phase 2C, Pass 0 (structural FK merge)
-- =============================================================================
-- Graph-merge of DMS users and ERP users via the hard FK
-- ERP.LINKED_DMS_USER_ID -> DMS.DMS_USER_ID. One row per DMS+ERP person,
-- where "person" = a connected component in the bipartite (DMS, ERP) graph.
--
-- Three topologies in this dataset:
--
--   DMS_AND_ERP (~90%):       DMS user with linked ERP user
--   DMS_ONLY:                 DMS user with no ERP account
--   ERP_ONLY_BROKEN_LINK:     ERP user whose linked_dms_user_id is NULL
--                             (~10% of ERP rows — modeled drift in the synth)
--
-- Pass 0 is structural, not probabilistic. Matched (dms_user_id, erp_user_id)
-- pairs get confidence = 1.0 in match_confidence and bypass the >=2-anchor
-- floor — the FK is deterministic. Downstream passes trust this linkage when
-- joining the unified DMS+ERP person to HRIS / ATS / payroll / CRM.
--
-- After this model, DMS and ERP do NOT appear separately in any Phase 2C
-- intermediate. The unified identity is the unit of work.
--
-- ---------------------------------------------------------------------------
-- Topology assumption: each DMS user has at most one ERP user pointing back
-- via linked_dms_user_id. Holds in this synthesizer (1:0..1 per person). If
-- real-world data introduces 1:N (rare — multiple ERP accounts per DMS user),
-- the unique tests on dms_user_id will fail loudly. That's the right
-- behavior — we want to surface the assumption violation, not silently
-- aggregate. A v2 could group by dms_user_id and array_agg the erp_user_ids;
-- defer that until a real case appears.
-- ---------------------------------------------------------------------------
--
-- Output schema:
--   dms_erp_person_key       Surrogate key over (dms_user_id, erp_user_id)
--   dms_user_id              Nullable (NULL for ERP_ONLY_BROKEN_LINK)
--   erp_user_id              Nullable (NULL for DMS_ONLY)
--   merge_topology           'DMS_AND_ERP' | 'DMS_ONLY' | 'ERP_ONLY_BROKEN_LINK'
--   has_dms / has_erp        Booleans for filter convenience
--   has_broken_link          TRUE iff has_erp AND erp.linked_dms_user_id IS NULL
--   short_first_name         DMS preferred, ERP fallback (lowercase, trimmed)
--   last_name                DMS preferred, ERP fallback (lowercase, trimmed)
--   short_first_name_original   Original casing for display
--   last_name_original          Original casing for display
--   dms_username             DMS-only (NULL when has_dms = false)
--   erp_email                ERP-only
--   erp_email_local_part     ERP-only — Pass 1 anchor against HRIS work email
--   ... + DMS-only and ERP-only org/lifecycle context preserved
-- =============================================================================

with dms as (
    select * from {{ ref('stg_dms__users') }}
),

erp as (
    select * from {{ ref('stg_erp__users') }}
),

merged as (
    select
        -- ---- Identifiers ----
        dms.dms_user_id                                                          as dms_user_id,
        erp.erp_user_id                                                          as erp_user_id,
        erp.linked_dms_user_id                                                   as erp_linked_dms_user_id,

        -- ---- Topology classification ----
        case
            when dms.dms_user_id is not null and erp.erp_user_id is not null then 'DMS_AND_ERP'
            when dms.dms_user_id is not null and erp.erp_user_id is null     then 'DMS_ONLY'
            when dms.dms_user_id is null     and erp.erp_user_id is not null then 'ERP_ONLY_BROKEN_LINK'
        end                                                                      as merge_topology,

        case when dms.dms_user_id is not null then true else false end           as has_dms,
        case when erp.erp_user_id is not null then true else false end           as has_erp,
        case
            when erp.erp_user_id is not null and erp.linked_dms_user_id is null
                then true else false
        end                                                                      as has_broken_link,

        -- ---- Identity attributes (DMS preferred, ERP fallback) ----
        -- DMS is the system of record for short-name + last-name when both
        -- exist; ERP is a downstream copy that drifts. Prefer DMS.
        coalesce(dms.short_first_name,           erp.short_first_name)           as short_first_name,
        coalesce(dms.last_name,                  erp.last_name)                  as last_name,
        coalesce(dms.short_first_name_original,  erp.short_first_name_original)  as short_first_name_original,
        coalesce(dms.last_name_original,         erp.last_name_original)         as last_name_original,

        -- ---- DMS-only fields ----
        dms.dms_username                                                         as dms_username,
        dms.location_code                                                        as dms_location_code,
        dms.department_code                                                      as dms_department_code,
        dms.hire_date_dms                                                        as hire_date_dms,
        dms.terminated_date_dms                                                  as terminated_date_dms,

        -- ---- ERP-only fields ----
        erp.erp_email                                                            as erp_email,
        erp.erp_email_local_part                                                 as erp_email_local_part,
        erp.role_code                                                            as erp_role_code,
        erp.permissions_group                                                    as erp_permissions_group,
        erp.created_at                                                           as erp_created_at,
        erp.last_login_at                                                        as erp_last_login_at,

        -- ---- Provenance ----
        greatest(
            coalesce(dms.loaded_at, erp.loaded_at),
            coalesce(erp.loaded_at, dms.loaded_at)
        )                                                                        as loaded_at,
        '{{ invocation_id }}'                                                    as _dbt_invocation_id

    from dms
    full outer join erp
        on erp.linked_dms_user_id = dms.dms_user_id
),

keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(['dms_user_id', 'erp_user_id']) }} as dms_erp_person_key,
        merged.*
    from merged
)

select * from keyed
```

### 33. `dbt_project/models/intermediate/int_identity_pass_1_hard_anchors.sql`

**Purpose:** Pass 1 deterministic matching: safest exact anchors such as personal email and work email local part.

**Source:**

```sql
{{
    config(
        materialized='table',
        tags=['intermediate', 'phase_2c', 'identity_pass_1']
    )
}}

-- =============================================================================
-- int_identity_pass_1_hard_anchors — government/email anchors
-- =============================================================================
-- Pass 1 is reserved for high-certainty anchors:
--
--   * ATS personal email exact match to HRIS personal email
--   * CRM / ERP work-email local-part exact match to HRIS work-email local-part
--
-- The design also allows SIN_LAST_4 + DOB as a hard government-identifier
-- anchor. In this synthetic source shape, HRIS does not expose SIN and payroll
-- does not expose DOB; the synthesizer also regenerates SIN_LAST_4 per pay
-- period. Using SIN here would create false confidence, so payroll government
-- ID matching is intentionally not implemented for Phase 2C's current data.
--
-- Deterministic over probabilistic: even exact email anchors must be unique
-- against HRIS before auto-merge. Collisions route to stewardship rather than
-- guessing.
-- =============================================================================

with nodes as (
    select * from {{ ref('int_identity_source_nodes') }}
),

sources as (
    select *
    from nodes
    where source_system != 'HRIS'
),

hris as (
    select *
    from nodes
    where source_system = 'HRIS'
),

candidates as (
    select
        src.source_system,
        src.source_record_key,
        src.source_primary_id,
        src.ats_candidate_id,
        src.payroll_spell_key,
        src.employee_payroll_id,
        src.crm_user_id,
        src.dms_erp_person_key,
        src.dms_user_id,
        src.erp_user_id,
        src.merge_topology,

        hris.hris_person_key,
        1                                                                    as match_pass,
        case
            when src.source_system = 'ATS'
                then 'personal_email_exact'
            when src.source_system in ('CRM', 'DMS_ERP')
                then 'work_email_local_part_exact'
        end                                                                  as match_rule,
        1.00::float                                                          as match_score,
        2                                                                    as match_anchor_count,
        cast(null as integer)                                                as hire_date_diff_days,

        src.source_first_name,
        src.source_last_name,
        src.source_first_name_root,
        src.source_last_name_norm,
        src.source_hire_date,
        src.source_email_local_part,
        src.source_email_domain,

        hris.source_first_name                                               as hris_first_name,
        hris.source_last_name                                                as hris_last_name,
        hris.source_first_name_root                                          as hris_first_name_root,
        hris.source_last_name_norm                                           as hris_last_name_norm,
        hris.source_hire_date                                                as hris_hire_date,

        src.loaded_at,
        '{{ invocation_id }}'                                                as _dbt_invocation_id
    from sources src
    inner join hris
        on (
            src.source_system = 'ATS'
            and src.personal_email_local_part is not null
            and hris.personal_email_local_part is not null
            and src.personal_email_local_part = hris.personal_email_local_part
            and coalesce(src.personal_email_domain, '') = coalesce(hris.personal_email_domain, '')
        )
        or (
            src.source_system in ('CRM', 'DMS_ERP')
            and src.source_email_local_part is not null
            and hris.work_email_local_part is not null
            and src.source_email_local_part = hris.work_email_local_part
            and coalesce(src.source_email_domain, '') = coalesce(hris.work_email_domain, '')
        )
),

candidate_counts as (
    select
        source_record_key,
        count(distinct hris_person_key) as candidate_hris_person_count
    from candidates
    group by source_record_key
)

select
    candidates.*,
    candidate_counts.candidate_hris_person_count,
    case
        when candidate_counts.candidate_hris_person_count = 1 then true
        else false
    end as auto_merge_qualified
from candidates
inner join candidate_counts
    on candidates.source_record_key = candidate_counts.source_record_key
```

### 34. `dbt_project/models/intermediate/int_identity_pass_2_name_dob_hire.sql`

**Purpose:** Pass 2 deterministic matching: normalized name roots, DOB where available, and hire-date proximity.

**Source:**

```sql
{{
    config(
        materialized='table',
        tags=['intermediate', 'phase_2c', 'identity_pass_2']
    )
}}

-- =============================================================================
-- int_identity_pass_2_name_dob_hire — normalized name + DOB + hire proximity
-- =============================================================================
-- Pass 2 implements the locked deterministic rule:
--
--   normalized first-name-root + normalized last-name
--   AND date_of_birth exact
--   AND hire date within +/- 30 days of an HRIS employment spell
--
-- Most non-HRIS sources in the current synthetic schema do not expose DOB, so
-- this model often produces candidate evidence without auto-merging it. That is
-- intentional. A name + hire-date match can be useful to HR stewardship, but it
-- is not enough to silently merge people in People Analytics data.
-- =============================================================================

with nodes as (
    select * from {{ ref('int_identity_source_nodes') }}
),

pass_1_auto as (
    select distinct source_record_key
    from {{ ref('int_identity_pass_1_hard_anchors') }}
    where auto_merge_qualified
),

sources as (
    select nodes.*
    from nodes
    left join pass_1_auto
        on nodes.source_record_key = pass_1_auto.source_record_key
    where nodes.source_system != 'HRIS'
      and pass_1_auto.source_record_key is null
      and nodes.source_first_name_root is not null
      and nodes.source_last_name_norm is not null
      and nodes.source_hire_date is not null
),

hris_spells as (
    select
        persons.hris_person_key,
        hris.hris_employee_id,
        hris.date_of_birth,
        hris.hire_date,
        {{ first_name_root('hris.legal_first_name', 'hris_legal_nm') }}       as hris_legal_first_name_root,
        {{ first_name_root('coalesce(hris.preferred_name, hris.legal_first_name)', 'hris_pref_nm') }}
                                                                               as hris_preferred_first_name_root,
        {{ normalize_name('hris.legal_last_name') }}                         as hris_last_name_norm,
        hris.legal_first_name                                                as hris_first_name,
        hris.legal_last_name                                                 as hris_last_name
    from {{ ref('stg_hris__employees') }} hris
    inner join {{ ref('int_hris_persons') }} persons
        on hris.date_of_birth = persons.date_of_birth
       and hris.personal_email_local_part = persons.personal_email_local_part
    left join {{ ref('nickname_map') }} hris_legal_nm
        on hris_legal_nm.nickname = {{ normalize_name('hris.legal_first_name') }}
    left join {{ ref('nickname_map') }} hris_pref_nm
        on hris_pref_nm.nickname = {{ normalize_name('coalesce(hris.preferred_name, hris.legal_first_name)') }}
),

candidates as (
    select
        src.source_system,
        src.source_record_key,
        src.source_primary_id,
        src.ats_candidate_id,
        src.payroll_spell_key,
        src.employee_payroll_id,
        src.crm_user_id,
        src.dms_erp_person_key,
        src.dms_user_id,
        src.erp_user_id,
        src.merge_topology,

        hris.hris_person_key,
        2                                                                    as match_pass,
        'normalized_name_dob_hire_proximity'                                 as match_rule,
        abs(datediff(day, src.source_hire_date, hris.hire_date))              as hire_date_diff_days,

        src.date_of_birth is not null                                         as source_has_dob,
        src.date_of_birth = hris.date_of_birth                                as dob_match,
        true                                                                 as name_match,

        src.source_first_name,
        src.source_last_name,
        src.source_first_name_root,
        src.source_last_name_norm,
        src.source_hire_date,
        src.source_email_local_part,
        src.source_email_domain,

        hris.hris_first_name,
        hris.hris_last_name,
        hris.hris_legal_first_name_root                                      as hris_first_name_root,
        hris.hris_last_name_norm,
        hris.hire_date                                                       as hris_hire_date,

        src.loaded_at,
        '{{ invocation_id }}'                                                as _dbt_invocation_id
    from sources src
    inner join hris_spells hris
        on src.source_last_name_norm = hris.hris_last_name_norm
       and src.source_first_name_root in (
            hris.hris_legal_first_name_root,
            hris.hris_preferred_first_name_root
       )
       and abs(datediff(day, src.source_hire_date, hris.hire_date)) <= 30
),

candidate_counts as (
    select
        source_record_key,
        count(distinct hris_person_key) as candidate_hris_person_count
    from candidates
    group by source_record_key
),

scored as (
    select
        candidates.*,
        candidate_counts.candidate_hris_person_count,
        case
            when source_has_dob and dob_match and hire_date_diff_days = 0 then 0.98
            when source_has_dob and dob_match and hire_date_diff_days <= 7 then 0.97
            when source_has_dob and dob_match and hire_date_diff_days <= 30 then 0.96
            when hire_date_diff_days = 0 then 0.70
            when hire_date_diff_days <= 7 then 0.65
            else 0.60
        end::float as match_score,
        case
            when source_has_dob and dob_match then 3
            else 2
        end as match_anchor_count
    from candidates
    inner join candidate_counts
        on candidates.source_record_key = candidate_counts.source_record_key
)

select
    scored.*,
    case
        when candidate_hris_person_count = 1
         and source_has_dob
         and dob_match
         and match_score >= 0.95
            then true
        else false
    end as auto_merge_qualified
from scored
```

### 35. `dbt_project/models/intermediate/int_identity_pass_3_email_domain.sql`

**Purpose:** Pass 3 deterministic matching: weaker email-domain and last-name evidence, still constrained by uniqueness gates.

**Source:**

```sql
{{
    config(
        materialized='table',
        tags=['intermediate', 'phase_2c', 'identity_pass_3']
    )
}}

-- =============================================================================
-- int_identity_pass_3_email_domain — company domain + email last-name token
-- =============================================================================
-- Pass 3 catches cases where exact email-local-part matching fails because the
-- first-name component differs across systems (Robert/Bob, preferred names,
-- transliteration), but the company email domain and last-name token still line
-- up with HRIS.
--
-- Auto-merge is intentionally narrow:
--   * source and HRIS emails are on the same company domain
--   * the last token of the email local part matches
--   * the source lifecycle date is within +/- 30 days of an HRIS spell
--   * exactly one HRIS person is a candidate
--
-- The uniqueness gate is the guardrail. Common last names hired near the same
-- date route to stewardship instead of being guessed.
-- =============================================================================

with nodes as (
    select * from {{ ref('int_identity_source_nodes') }}
),

pass_1_auto as (
    select distinct source_record_key
    from {{ ref('int_identity_pass_1_hard_anchors') }}
    where auto_merge_qualified
),

pass_2_auto as (
    select distinct source_record_key
    from {{ ref('int_identity_pass_2_name_dob_hire') }}
    where auto_merge_qualified
),

sources as (
    select
        nodes.*,
        regexp_substr(nodes.source_email_local_part, '[^.]+$')               as source_email_last_token
    from nodes
    left join pass_1_auto
        on nodes.source_record_key = pass_1_auto.source_record_key
    left join pass_2_auto
        on nodes.source_record_key = pass_2_auto.source_record_key
    where nodes.source_system in ('CRM', 'DMS_ERP')
      and pass_1_auto.source_record_key is null
      and pass_2_auto.source_record_key is null
      and nodes.source_email_local_part is not null
      and nodes.source_email_domain is not null
      and nodes.source_hire_date is not null
),

hris_spells as (
    select
        persons.hris_person_key,
        hris.hris_employee_id,
        hris.hire_date,
        hris.work_email_local_part,
        case
            when hris.work_email is not null and position('@' in hris.work_email) > 0
                then split_part(hris.work_email, '@', 2)
        end                                                                  as work_email_domain,
        regexp_substr(hris.work_email_local_part, '[^.]+$')                  as hris_email_last_token,
        {{ first_name_root('hris.legal_first_name', 'hris_legal_nm') }}       as hris_legal_first_name_root,
        {{ first_name_root('coalesce(hris.preferred_name, hris.legal_first_name)', 'hris_pref_nm') }}
                                                                               as hris_preferred_first_name_root,
        {{ normalize_name('hris.legal_last_name') }}                         as hris_last_name_norm,
        hris.legal_first_name                                                as hris_first_name,
        hris.legal_last_name                                                 as hris_last_name
    from {{ ref('stg_hris__employees') }} hris
    inner join {{ ref('int_hris_persons') }} persons
        on hris.date_of_birth = persons.date_of_birth
       and hris.personal_email_local_part = persons.personal_email_local_part
    left join {{ ref('nickname_map') }} hris_legal_nm
        on hris_legal_nm.nickname = {{ normalize_name('hris.legal_first_name') }}
    left join {{ ref('nickname_map') }} hris_pref_nm
        on hris_pref_nm.nickname = {{ normalize_name('coalesce(hris.preferred_name, hris.legal_first_name)') }}
),

candidates as (
    select
        src.source_system,
        src.source_record_key,
        src.source_primary_id,
        src.ats_candidate_id,
        src.payroll_spell_key,
        src.employee_payroll_id,
        src.crm_user_id,
        src.dms_erp_person_key,
        src.dms_user_id,
        src.erp_user_id,
        src.merge_topology,

        hris.hris_person_key,
        3                                                                    as match_pass,
        'company_email_domain_last_token_hire_unique'                        as match_rule,
        abs(datediff(day, src.source_hire_date, hris.hire_date))              as hire_date_diff_days,

        src.source_first_name,
        src.source_last_name,
        src.source_first_name_root,
        src.source_last_name_norm,
        src.source_hire_date,
        src.source_email_local_part,
        src.source_email_domain,

        hris.hris_first_name,
        hris.hris_last_name,
        hris.hris_legal_first_name_root                                      as hris_first_name_root,
        hris.hris_last_name_norm,
        hris.hire_date                                                       as hris_hire_date,

        src.loaded_at,
        '{{ invocation_id }}'                                                as _dbt_invocation_id
    from sources src
    inner join hris_spells hris
        on src.source_email_domain = hris.work_email_domain
       and src.source_email_last_token = hris.hris_email_last_token
       and abs(datediff(day, src.source_hire_date, hris.hire_date)) <= 30
),

candidate_counts as (
    select
        source_record_key,
        count(distinct hris_person_key) as candidate_hris_person_count
    from candidates
    group by source_record_key
),

scored as (
    select
        candidates.*,
        candidate_counts.candidate_hris_person_count,
        case
            when hire_date_diff_days = 0 then 0.97
            when hire_date_diff_days <= 7 then 0.96
            else 0.95
        end::float as match_score,
        2 as match_anchor_count
    from candidates
    inner join candidate_counts
        on candidates.source_record_key = candidate_counts.source_record_key
)

select
    scored.*,
    case
        when candidate_hris_person_count = 1
         and match_score >= 0.95
            then true
        else false
    end as auto_merge_qualified
from scored
```

### 36. `dbt_project/models/intermediate/int_canonical_person.sql`

**Purpose:** The canonical employee identity output: one stable canonical_person_id per HRIS-distinct person, enriched by safe source matches.

**Source:**

```sql
{{
    config(
        materialized='table',
        tags=['intermediate', 'phase_2c', 'canonical_person']
    )
}}

-- =============================================================================
-- int_canonical_person — Phase 2C unified identity output
-- =============================================================================
-- One row per canonical person, seeded from int_hris_persons and enriched with
-- source-system identifiers that passed deterministic auto-merge.
--
-- The canonical_person_id is a stable, non-PII surrogate derived from the HRIS
-- person key. That key already collapses rehires and contractor-to-FTE HRIS ID
-- churn by grouping on DOB + personal-email local part, so the emitted ID
-- survives the lifecycle drift this project is designed to demonstrate.
--
-- Sensitive fields such as SIN_LAST_4 are intentionally absent. Payroll is
-- represented only by its spell identifiers and aggregate spell context; marts
-- must not expose payroll government identifiers.
-- =============================================================================

with hris_persons as (
    select * from {{ ref('int_hris_persons') }}
),

auto_matches as (
    select
        source_system,
        source_record_key,
        source_primary_id,
        ats_candidate_id,
        payroll_spell_key,
        employee_payroll_id,
        crm_user_id,
        dms_erp_person_key,
        dms_user_id,
        erp_user_id,
        merge_topology,
        hris_person_key,
        match_pass,
        match_rule,
        match_score,
        match_anchor_count,
        loaded_at
    from {{ ref('int_identity_pass_1_hard_anchors') }}
    where auto_merge_qualified

    union all

    select
        source_system,
        source_record_key,
        source_primary_id,
        ats_candidate_id,
        payroll_spell_key,
        employee_payroll_id,
        crm_user_id,
        dms_erp_person_key,
        dms_user_id,
        erp_user_id,
        merge_topology,
        hris_person_key,
        match_pass,
        match_rule,
        match_score,
        match_anchor_count,
        loaded_at
    from {{ ref('int_identity_pass_2_name_dob_hire') }}
    where auto_merge_qualified

    union all

    select
        source_system,
        source_record_key,
        source_primary_id,
        ats_candidate_id,
        payroll_spell_key,
        employee_payroll_id,
        crm_user_id,
        dms_erp_person_key,
        dms_user_id,
        erp_user_id,
        merge_topology,
        hris_person_key,
        match_pass,
        match_rule,
        match_score,
        match_anchor_count,
        loaded_at
    from {{ ref('int_identity_pass_3_email_domain') }}
    where auto_merge_qualified
),

best_matches as (
    select *
    from auto_matches
    qualify row_number() over (
        partition by source_record_key
        order by match_pass asc, match_score desc, hris_person_key asc
    ) = 1
),

match_summary as (
    select
        hris_person_key,
        count(*)                                                             as matched_external_source_node_count,
        count(distinct source_system)                                        as matched_external_source_system_count,
        listagg(distinct to_varchar(match_pass), ',')
            within group (order by to_varchar(match_pass))                   as match_passes_used,
        max(loaded_at)                                                       as external_loaded_at
    from best_matches
    group by hris_person_key
),

ats_matches as (
    select
        hris_person_key,
        array_agg(distinct ats_candidate_id) within group (order by ats_candidate_id)
                                                                               as ats_candidate_ids,
        count(distinct ats_candidate_id)                                      as ats_candidate_count
    from best_matches
    where source_system = 'ATS'
      and ats_candidate_id is not null
    group by hris_person_key
),

payroll_matches as (
    select
        hris_person_key,
        array_agg(distinct employee_payroll_id) within group (order by employee_payroll_id)
                                                                               as employee_payroll_ids,
        array_agg(distinct payroll_spell_key) within group (order by payroll_spell_key)
                                                                               as payroll_spell_keys,
        count(distinct employee_payroll_id)                                   as payroll_spell_count
    from best_matches
    where source_system = 'PAYROLL'
      and employee_payroll_id is not null
    group by hris_person_key
),

crm_matches as (
    select
        hris_person_key,
        array_agg(distinct crm_user_id) within group (order by crm_user_id)   as crm_user_ids,
        count(distinct crm_user_id)                                           as crm_user_count
    from best_matches
    where source_system = 'CRM'
      and crm_user_id is not null
    group by hris_person_key
),

dms_matches as (
    select
        hris_person_key,
        array_agg(distinct dms_user_id) within group (order by dms_user_id)   as dms_user_ids,
        count(distinct dms_user_id)                                           as dms_user_count
    from best_matches
    where source_system = 'DMS_ERP'
      and dms_user_id is not null
    group by hris_person_key
),

erp_matches as (
    select
        hris_person_key,
        array_agg(distinct erp_user_id) within group (order by erp_user_id)   as erp_user_ids,
        count(distinct erp_user_id)                                           as erp_user_count
    from best_matches
    where source_system = 'DMS_ERP'
      and erp_user_id is not null
    group by hris_person_key
)

select
    'cp_' || h.hris_person_key                                               as canonical_person_id,
    h.hris_person_key,

    -- HRIS anchor attributes
    h.hris_employee_ids,
    h.current_hris_employee_id,
    h.date_of_birth,
    h.personal_email_local_part,
    h.work_email_local_part,
    h.canonical_hire_date,
    h.latest_hire_date,
    h.latest_termination_date,
    h.canonical_legal_first_name,
    h.canonical_legal_last_name,
    h.canonical_legal_first_name_original,
    h.canonical_legal_last_name_original,
    h.current_legal_first_name,
    h.current_legal_last_name,
    h.current_preferred_name,
    h.current_employment_status,
    h.current_employment_type,
    h.current_department,
    h.current_job_title,
    h.current_location,
    h.current_manager_hris_id,
    h.spell_count                                                            as hris_spell_count,
    h.has_rehires,
    h.has_name_change_marriage,

    -- Resolved source identifiers. Empty arrays are easier for downstream
    -- consumers than NULL when a person does not appear in a system.
    coalesce(ats.ats_candidate_ids, array_construct())                       as ats_candidate_ids,
    coalesce(ats.ats_candidate_count, 0)                                     as ats_candidate_count,
    coalesce(payroll.employee_payroll_ids, array_construct())                as employee_payroll_ids,
    coalesce(payroll.payroll_spell_keys, array_construct())                  as payroll_spell_keys,
    coalesce(payroll.payroll_spell_count, 0)                                 as payroll_spell_count,
    coalesce(crm.crm_user_ids, array_construct())                            as crm_user_ids,
    coalesce(crm.crm_user_count, 0)                                          as crm_user_count,
    coalesce(dms.dms_user_ids, array_construct())                            as dms_user_ids,
    coalesce(dms.dms_user_count, 0)                                          as dms_user_count,
    coalesce(erp.erp_user_ids, array_construct())                            as erp_user_ids,
    coalesce(erp.erp_user_count, 0)                                          as erp_user_count,

    coalesce(summary.matched_external_source_node_count, 0)                  as matched_external_source_node_count,
    coalesce(summary.matched_external_source_system_count, 0)                 as matched_external_source_system_count,
    coalesce(summary.match_passes_used, '')                                  as match_passes_used,

    greatest(
        coalesce(h.loaded_at, to_timestamp_ntz('1900-01-01')),
        coalesce(summary.external_loaded_at, to_timestamp_ntz('1900-01-01'))
    )                                                                        as loaded_at,
    '{{ invocation_id }}'                                                    as _dbt_invocation_id
from hris_persons h
left join match_summary summary
    on h.hris_person_key = summary.hris_person_key
left join ats_matches ats
    on h.hris_person_key = ats.hris_person_key
left join payroll_matches payroll
    on h.hris_person_key = payroll.hris_person_key
left join crm_matches crm
    on h.hris_person_key = crm.hris_person_key
left join dms_matches dms
    on h.hris_person_key = dms.hris_person_key
left join erp_matches erp
    on h.hris_person_key = erp.hris_person_key
```

### 37. `dbt_project/models/intermediate/int_stewardship_queue.sql`

**Purpose:** The manual review queue for source records that should not auto-merge.

**Source:**

```sql
{{
    config(
        materialized='table',
        tags=['intermediate', 'phase_2c', 'stewardship_queue']
    )
}}

-- =============================================================================
-- int_stewardship_queue — Phase 2C manual review surface
-- =============================================================================
-- One row per non-HRIS source identity that did not qualify for deterministic
-- auto-merge. This is not a failure table; it is the control surface that keeps
-- the matcher conservative. HR stewards can adjudicate these cases with the
-- best available candidate evidence without the model silently creating a bad
-- canonical employee record.
--
-- The queue intentionally avoids SIN_LAST_4 and other government identifiers.
-- Email is decomposed into local part + domain so reviewers have enough context
-- for synthetic demos without propagating full personal email addresses.
-- =============================================================================

with nodes as (
    select *
    from {{ ref('int_identity_source_nodes') }}
    where source_system != 'HRIS'
),

auto_matches as (
    select source_record_key, hris_person_key
    from {{ ref('int_identity_pass_1_hard_anchors') }}
    where auto_merge_qualified

    union all

    select source_record_key, hris_person_key
    from {{ ref('int_identity_pass_2_name_dob_hire') }}
    where auto_merge_qualified

    union all

    select source_record_key, hris_person_key
    from {{ ref('int_identity_pass_3_email_domain') }}
    where auto_merge_qualified
),

all_candidates as (
    select
        source_record_key,
        hris_person_key,
        match_pass,
        match_rule,
        match_score,
        match_anchor_count,
        candidate_hris_person_count,
        hire_date_diff_days,
        hris_first_name,
        hris_last_name,
        hris_hire_date
    from {{ ref('int_identity_pass_1_hard_anchors') }}

    union all

    select
        source_record_key,
        hris_person_key,
        match_pass,
        match_rule,
        match_score,
        match_anchor_count,
        candidate_hris_person_count,
        hire_date_diff_days,
        hris_first_name,
        hris_last_name,
        hris_hire_date
    from {{ ref('int_identity_pass_2_name_dob_hire') }}

    union all

    select
        source_record_key,
        hris_person_key,
        match_pass,
        match_rule,
        match_score,
        match_anchor_count,
        candidate_hris_person_count,
        hire_date_diff_days,
        hris_first_name,
        hris_last_name,
        hris_hire_date
    from {{ ref('int_identity_pass_3_email_domain') }}
),

best_candidate as (
    select *
    from all_candidates
    qualify row_number() over (
        partition by source_record_key
        order by match_score desc, match_pass asc, candidate_hris_person_count asc, hris_person_key asc
    ) = 1
),

unresolved as (
    select nodes.*
    from nodes
    left join auto_matches
        on nodes.source_record_key = auto_matches.source_record_key
    where auto_matches.source_record_key is null
)

select
    {{ dbt_utils.generate_surrogate_key(['unresolved.source_record_key']) }}  as stewardship_queue_id,
    unresolved.source_system,
    unresolved.source_record_key,
    unresolved.source_primary_id,
    unresolved.ats_candidate_id,
    unresolved.payroll_spell_key,
    unresolved.employee_payroll_id,
    unresolved.crm_user_id,
    unresolved.dms_erp_person_key,
    unresolved.dms_user_id,
    unresolved.erp_user_id,
    unresolved.merge_topology,

    unresolved.source_first_name,
    unresolved.source_last_name,
    unresolved.source_first_name_root,
    unresolved.source_last_name_norm,
    unresolved.source_hire_date,
    unresolved.source_end_date,
    unresolved.source_email_local_part,
    unresolved.source_email_domain,

    best_candidate.hris_person_key                                           as suggested_hris_person_key,
    case
        when best_candidate.hris_person_key is not null
            then 'cp_' || best_candidate.hris_person_key
    end                                                                      as suggested_canonical_person_id,
    best_candidate.hris_first_name                                           as suggested_hris_first_name,
    best_candidate.hris_last_name                                            as suggested_hris_last_name,
    best_candidate.hris_hire_date                                            as suggested_hris_hire_date,
    best_candidate.match_pass                                                as suggested_match_pass,
    best_candidate.match_rule                                                as suggested_match_rule,
    best_candidate.match_score                                               as suggested_match_score,
    best_candidate.match_anchor_count                                        as suggested_match_anchor_count,
    best_candidate.candidate_hris_person_count                               as candidate_hris_person_count,
    best_candidate.hire_date_diff_days                                       as suggested_hire_date_diff_days,

    case
        when best_candidate.source_record_key is null
            then 'NO_DETERMINISTIC_CANDIDATE'
        when best_candidate.candidate_hris_person_count > 1
            then 'AMBIGUOUS_CANDIDATES'
        when best_candidate.match_score < 0.95
            then 'BELOW_AUTO_MATCH_THRESHOLD'
        else 'MANUAL_REVIEW_REQUIRED'
    end                                                                      as stewardship_reason,

    unresolved.loaded_at,
    '{{ invocation_id }}'                                                    as _dbt_invocation_id
from unresolved
left join best_candidate
    on unresolved.source_record_key = best_candidate.source_record_key
```

### 38. `dbt_project/models/marts/core/_core.yml`

**Purpose:** YAML configuration/documentation. It defines dbt metadata, tests, dependencies, sources, exposures, or fixtures.

**Source:**

```yaml
version: 2

models:

  # =========================================================================
  - name: dim_employee
    description: |
      Phase 2D core SCD2-style employee dimension. One row per canonical person
      employment spell, effective-dated for point-in-time joins.
    columns:
      - name: employee_sk
        description: Surrogate key for this canonical person employment spell.
        tests:
          - not_null
          - unique
      - name: canonical_person_id
        tests: [not_null]
      - name: hris_person_key
        tests: [not_null]
      - name: hris_employee_id
        tests:
          - not_null
          - unique
      - name: effective_start_date
        tests: [not_null]
      - name: effective_end_date
        tests: [not_null]
      - name: is_current_record
        tests: [not_null]
      - name: employment_spell_number
        tests: [not_null]
      - name: source_employment_status
        tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['ACTIVE', 'TERMINATED', 'ON_LEAVE']
      - name: employment_type
        tests:
          - accepted_values:
              arguments:
                values: ['FTE', 'CONTRACTOR', 'PART_TIME']
      - name: is_open_ended_spell
        tests: [not_null]

  # =========================================================================
  - name: fct_workforce_daily
    description: |
      Phase 2D daily point-in-time workforce snapshot. One row per employee
      spell per date from hire through termination/as-of date. Headcount
      metrics should filter is_active_on_date = true.
    columns:
      - name: daily_workforce_key
        tests:
          - not_null
          - unique
      - name: snapshot_date
        tests: [not_null]
      - name: employee_sk
        tests: [not_null]
      - name: canonical_person_id
        tests: [not_null]
      - name: hris_employee_id
        tests: [not_null]
      - name: is_active_on_date
        tests: [not_null]
      - name: is_hire_date
        tests: [not_null]
      - name: is_termination_date
        tests: [not_null]
      - name: tenure_days
        tests: [not_null]
      - name: employee_day_count
        tests:
          - not_null
          - accepted_values:
              arguments:
                values: [1]
```

### 39. `dbt_project/models/marts/core/dim_employee.sql`

**Purpose:** SCD2-style employee dimension at employment-spell grain.

**Source:**

```sql
{{
    config(
        materialized='table',
        tags=['marts', 'core', 'phase_2d', 'dim_employee']
    )
}}

-- =============================================================================
-- dim_employee — Phase 2D core SCD2-style employee dimension
-- =============================================================================
-- One row per canonical person employment spell. This is the effective-dated
-- employee dimension that downstream point-in-time facts join to.
--
-- Tradeoff: the current synthetic HRIS feed emits one row per employment spell,
-- with department/location/job title captured as the latest value inside that
-- spell. It does not emit a full intra-spell event log for every transfer. So
-- this dimension is SCD2 at the employment-spell grain, not at every internal
-- transfer grain. That is still the right Phase 2D step: it proves rehire and
-- termination point-in-time correctness without inventing source history that
-- the raw feed does not contain.
--
-- Privacy: full date_of_birth and SIN_LAST_4 are intentionally absent from this
-- mart. DOB remains in intermediate matching models where it is needed for
-- identity resolution; SIN stays in payroll staging/intermediate only.
-- =============================================================================

with canonical as (
    select * from {{ ref('int_canonical_person') }}
),

hris_persons as (
    select * from {{ ref('int_hris_persons') }}
),

hris_spells as (
    select * from {{ ref('stg_hris__employees') }}
),

joined as (
    select
        canonical.canonical_person_id,
        canonical.hris_person_key,
        hris.hris_employee_id,

        hris.hire_date                                                       as effective_start_date,
        case
            when hris.termination_date is not null
                then dateadd(day, -1, hris.termination_date)
            else to_date('9999-12-31')
        end                                                                  as effective_end_date,
        hris.termination_date                                                as termination_date,

        row_number() over (
            partition by canonical.canonical_person_id
            order by hris.hire_date desc, hris.hris_employee_id desc
        ) = 1                                                                as is_current_record,

        row_number() over (
            partition by canonical.canonical_person_id
            order by hris.hire_date asc, hris.hris_employee_id asc
        )                                                                    as employment_spell_number,

        hris.legal_first_name_original,
        hris.legal_last_name_original,
        hris.preferred_name_original,
        hris.legal_first_name,
        hris.legal_last_name,
        hris.preferred_name,

        hris.employment_status                                               as source_employment_status,
        hris.employment_type,
        hris.department,
        hris.job_title,
        hris.manager_hris_id,
        hris.location,

        canonical.has_rehires,
        canonical.has_name_change_marriage,
        canonical.ats_candidate_count,
        canonical.payroll_spell_count,
        canonical.crm_user_count,
        canonical.dms_user_count,
        canonical.erp_user_count,
        canonical.matched_external_source_system_count,
        canonical.match_passes_used,

        greatest(
            coalesce(hris.loaded_at, to_timestamp_ntz('1900-01-01')),
            coalesce(canonical.loaded_at, to_timestamp_ntz('1900-01-01'))
        )                                                                    as loaded_at
    from hris_spells hris
    inner join hris_persons persons
        on hris.date_of_birth = persons.date_of_birth
       and hris.personal_email_local_part = persons.personal_email_local_part
    inner join canonical
        on persons.hris_person_key = canonical.hris_person_key
)

select
    {{ dbt_utils.generate_surrogate_key([
        'canonical_person_id',
        'hris_employee_id',
        'effective_start_date'
    ]) }}                                                                    as employee_sk,
    joined.*,
    case
        when termination_date is null then true
        else false
    end                                                                      as is_open_ended_spell,
    '{{ invocation_id }}'                                                    as _dbt_invocation_id
from joined
```

### 40. `dbt_project/models/marts/core/fct_workforce_daily.sql`

**Purpose:** Date-spine workforce fact: one employee spell per observable date for point-in-time analysis.

**Source:**

```sql
{{
    config(
        materialized='table',
        tags=['marts', 'core', 'phase_2d', 'fct_workforce_daily']
    )
}}

-- =============================================================================
-- fct_workforce_daily — Phase 2D daily point-in-time workforce snapshot
-- =============================================================================
-- One row per employee spell per calendar date from hire date through either:
--
--   * termination_date, for closed spells
--   * snapshot_as_of_date/current_date, for open spells
--
-- The termination date row is retained with is_active_on_date = false so
-- attrition counts can be computed from the same fact without a separate event
-- table. Headcount metrics should filter is_active_on_date = true.
--
-- Date convention: termination_date is treated as the first non-active day.
-- This matches common HRIS exports where the term date is the effective date of
-- termination rather than the last active workday.
-- =============================================================================

{% if var('snapshot_as_of_date') %}
    {% set snapshot_as_of_expr = "to_date('" ~ var('snapshot_as_of_date') ~ "')" %}
{% else %}
    {% set snapshot_as_of_expr = "current_date()" %}
{% endif %}

with dim_employee as (
    select * from {{ ref('dim_employee') }}
),

date_bounds as (
    select
        min(effective_start_date)                                            as min_snapshot_date,
        {{ snapshot_as_of_expr }}                                            as max_snapshot_date
    from dim_employee
),

date_offsets as (
    select seq4() as day_offset
    from table(generator(rowcount => 10000))
),

date_spine as (
    select
        dateadd(day, date_offsets.day_offset, date_bounds.min_snapshot_date) as snapshot_date
    from date_bounds
    cross join date_offsets
    where dateadd(day, date_offsets.day_offset, date_bounds.min_snapshot_date)
        <= date_bounds.max_snapshot_date
),

employee_days as (
    select
        date_spine.snapshot_date,
        dim_employee.employee_sk,
        dim_employee.canonical_person_id,
        dim_employee.hris_person_key,
        dim_employee.hris_employee_id,
        dim_employee.employment_spell_number,

        dim_employee.effective_start_date,
        dim_employee.effective_end_date,
        dim_employee.termination_date,

        dim_employee.legal_first_name_original,
        dim_employee.legal_last_name_original,
        dim_employee.preferred_name_original,
        dim_employee.employment_type,
        dim_employee.department,
        dim_employee.job_title,
        dim_employee.manager_hris_id,
        dim_employee.location,

        date_spine.snapshot_date >= dim_employee.effective_start_date
            and (
                dim_employee.termination_date is null
                or date_spine.snapshot_date < dim_employee.termination_date
            )                                                                as is_active_on_date,
        date_spine.snapshot_date = dim_employee.effective_start_date          as is_hire_date,
        dim_employee.termination_date is not null
            and date_spine.snapshot_date = dim_employee.termination_date      as is_termination_date,
        datediff(day, dim_employee.effective_start_date, date_spine.snapshot_date)
                                                                               as tenure_days,

        dim_employee.has_rehires,
        dim_employee.has_name_change_marriage,
        dim_employee.matched_external_source_system_count,
        dim_employee.loaded_at
    from date_spine
    inner join dim_employee
        on date_spine.snapshot_date >= dim_employee.effective_start_date
       and date_spine.snapshot_date <= least(
            coalesce(dim_employee.termination_date, date_spine.snapshot_date),
            (select max_snapshot_date from date_bounds)
       )
)

select
    {{ dbt_utils.generate_surrogate_key([
        'snapshot_date',
        'employee_sk'
    ]) }}                                                                    as daily_workforce_key,
    employee_days.*,
    1                                                                        as employee_day_count,
    '{{ invocation_id }}'                                                    as _dbt_invocation_id
from employee_days
```

### 41. `dbt_project/models/marts/people_analytics/_people_analytics.yml`

**Purpose:** YAML configuration/documentation. It defines dbt metadata, tests, dependencies, sources, exposures, or fixtures.

**Source:**

```yaml
version: 2

models:

  # =========================================================================
  - name: workforce_headcount_daily
    description: |
      Phase 3 privacy-preserving daily headcount mart. Exact metrics are null
      for cohorts below `k_anonymity_threshold`.
    columns:
      - name: headcount_daily_key
        tests:
          - not_null
          - unique
      - name: snapshot_date
        tests: [not_null]
      - name: department
        tests: [not_null]
      - name: location
        tests: [not_null]
      - name: employment_type
        tests: [not_null]
      - name: cohort_size_bucket
        tests: [not_null]
      - name: is_reportable
        tests: [not_null]
      - name: k_anonymity_threshold
        tests: [not_null]

  # =========================================================================
  - name: workforce_attrition_monthly
    description: |
      Phase 3 privacy-preserving monthly attrition mart. Metrics are suppressed
      when the month-start cohort is below the configured k threshold.
    columns:
      - name: attrition_monthly_key
        tests:
          - not_null
          - unique
      - name: month_start_date
        tests: [not_null]
      - name: month_end_date
        tests: [not_null]
      - name: department
        tests: [not_null]
      - name: location
        tests: [not_null]
      - name: employment_type
        tests: [not_null]
      - name: cohort_size_bucket
        tests: [not_null]
      - name: is_reportable
        tests: [not_null]
      - name: k_anonymity_threshold
        tests: [not_null]

  # =========================================================================
  - name: privacy_suppression_summary
    description: |
      Phase 3 privacy observability mart showing reportable vs suppressed row
      counts by public People Analytics surface.
    columns:
      - name: privacy_suppression_summary_key
        tests:
          - not_null
          - unique
      - name: privacy_surface
        tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['workforce_headcount_daily', 'workforce_attrition_monthly']
      - name: date_grain
        tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['daily', 'monthly']
      - name: row_count
        tests: [not_null]
      - name: reportable_row_count
        tests: [not_null]
      - name: suppressed_row_count
        tests: [not_null]
      - name: k_anonymity_threshold
        tests: [not_null]

  # =========================================================================
  - name: privacy_audit_log
    description: |
      Empty-on-build incremental audit table for privacy-protected mart access.
      Future API/dashboard code should insert one row per access event.
    columns:
      - name: audit_event_id
        description: UUID generated by insert_privacy_audit_event.
      - name: audited_at
      - name: actor
      - name: query_surface
      - name: purpose
      - name: filters_json
      - name: k_anonymity_threshold
      - name: result_row_count
      - name: suppressed_row_count
      - name: privacy_policy_version
```

### 42. `dbt_project/models/marts/people_analytics/_exposures.yml`

**Purpose:** YAML configuration/documentation. It defines dbt metadata, tests, dependencies, sources, exposures, or fixtures.

**Source:**

```yaml
version: 2

exposures:
  - name: atlas_metrics_api
    label: Atlas Metrics API
    type: application
    maturity: medium
    url: http://127.0.0.1:8000/docs
    description: |
      FastAPI service that exposes privacy-safe People Analytics metrics from
      governed marts and writes best-effort access events to the audit log.
    depends_on:
      - ref('workforce_headcount_daily')
      - ref('workforce_attrition_monthly')
      - ref('privacy_suppression_summary')
      - ref('privacy_audit_log')
    owner:
      name: Atlas People Analytics Owner
      email: atlas-people-analytics@example.com
    config:
      meta:
        consumer: api
        privacy_surface: true
        purpose: controlled_metric_access

  - name: atlas_hrbp_dashboard
    label: Atlas HRBP Dashboard
    type: dashboard
    maturity: medium
    url: http://localhost:8501
    description: |
      Streamlit demonstration dashboard for HRBP-style consumption of
      privacy-safe headcount, attrition, and suppression observability metrics.
    depends_on:
      - ref('workforce_headcount_daily')
      - ref('workforce_attrition_monthly')
      - ref('privacy_suppression_summary')
    owner:
      name: Atlas People Analytics Owner
      email: atlas-people-analytics@example.com
    config:
      meta:
        consumer: dashboard
        privacy_surface: true
        purpose: hrbp_metric_demo
```

### 43. `dbt_project/models/marts/people_analytics/workforce_headcount_daily.sql`

**Purpose:** Privacy-preserving daily headcount mart with k-anonymity suppression.

**Source:**

```sql
{{
    config(
        materialized='table',
        tags=['marts', 'people_analytics', 'phase_3', 'privacy']
    )
}}

-- =============================================================================
-- workforce_headcount_daily — privacy-preserving daily headcount
-- =============================================================================
-- Public People Analytics mart for daily active headcount by common HRBP
-- dimensions. Exact metrics are suppressed when the cohort size is below
-- `var('k_anonymity_threshold')`.
--
-- Tradeoff: dimension rows remain visible even when metrics are suppressed.
-- That helps analysts understand that a cohort exists, but the exact count is
-- hidden. This is more useful than dropping the row entirely and safer than
-- returning small exact counts.
-- =============================================================================

with active_employee_days as (
    select
        snapshot_date,
        coalesce(department, 'UNKNOWN')                                      as department,
        coalesce(location, 'UNKNOWN')                                        as location,
        coalesce(employment_type, 'UNKNOWN')                                 as employment_type,
        canonical_person_id
    from {{ ref('fct_workforce_daily') }}
    where is_active_on_date
),

cohorts as (
    select
        snapshot_date,
        department,
        location,
        employment_type,
        count(distinct canonical_person_id)                                  as cohort_employee_count
    from active_employee_days
    group by
        snapshot_date,
        department,
        location,
        employment_type
)

select
    {{ dbt_utils.generate_surrogate_key([
        'snapshot_date',
        'department',
        'location',
        'employment_type'
    ]) }}                                                                    as headcount_daily_key,
    snapshot_date,
    department,
    location,
    employment_type,
    {{ k_anonymize('cohort_employee_count', 'cohort_employee_count', 'number(38, 0)') }}
                                                                               as headcount,
    {{ k_anonymize('cohort_employee_count', 'cohort_employee_count', 'number(38, 0)') }}
                                                                               as reportable_cohort_employee_count,
    {{ k_cohort_size_bucket('cohort_employee_count') }}                       as cohort_size_bucket,
    {{ is_k_anonymous('cohort_employee_count') }}                             as is_reportable,
    {{ k_suppression_reason('cohort_employee_count') }}                       as suppression_reason,
    {{ k_anonymity_threshold() }}                                             as k_anonymity_threshold,
    '{{ invocation_id }}'                                                     as _dbt_invocation_id
from cohorts
```

### 44. `dbt_project/models/marts/people_analytics/workforce_attrition_monthly.sql`

**Purpose:** Privacy-preserving monthly attrition mart with explicit numerator/denominator logic.

**Source:**

```sql
{{
    config(
        materialized='table',
        tags=['marts', 'people_analytics', 'phase_3', 'privacy']
    )
}}

-- =============================================================================
-- workforce_attrition_monthly — privacy-preserving monthly attrition
-- =============================================================================
-- Public monthly attrition mart by HRBP dimensions. The privacy cohort is the
-- active population at the start of the month for that dimension combination.
-- If that population is smaller than k, start headcount, terminations, and rate
-- are suppressed together.
--
-- Suppressing by denominator rather than termination count avoids hiding every
-- month with one departure in a large cohort while still protecting genuinely
-- small teams/locations.
-- =============================================================================

with workforce_daily as (
    select * from {{ ref('fct_workforce_daily') }}
),

month_dimension_spine as (
    select distinct
        date_trunc('month', snapshot_date)::date                              as month_start_date,
        coalesce(department, 'UNKNOWN')                                      as department,
        coalesce(location, 'UNKNOWN')                                        as location,
        coalesce(employment_type, 'UNKNOWN')                                 as employment_type
    from workforce_daily
),

month_first_snapshot as (
    select
        date_trunc('month', snapshot_date)::date                              as month_start_date,
        min(snapshot_date)                                                    as first_snapshot_date
    from workforce_daily
    group by date_trunc('month', snapshot_date)::date
),

month_start_population as (
    select
        date_trunc('month', snapshot_date)::date                              as month_start_date,
        coalesce(department, 'UNKNOWN')                                      as department,
        coalesce(location, 'UNKNOWN')                                        as location,
        coalesce(employment_type, 'UNKNOWN')                                 as employment_type,
        count(distinct canonical_person_id)                                  as start_cohort_employee_count
    from workforce_daily
    inner join month_first_snapshot
        on date_trunc('month', workforce_daily.snapshot_date)::date = month_first_snapshot.month_start_date
       and workforce_daily.snapshot_date = month_first_snapshot.first_snapshot_date
    where is_active_on_date
    group by
        date_trunc('month', snapshot_date)::date,
        coalesce(department, 'UNKNOWN'),
        coalesce(location, 'UNKNOWN'),
        coalesce(employment_type, 'UNKNOWN')
),

terminations as (
    select
        date_trunc('month', snapshot_date)::date                              as month_start_date,
        coalesce(department, 'UNKNOWN')                                      as department,
        coalesce(location, 'UNKNOWN')                                        as location,
        coalesce(employment_type, 'UNKNOWN')                                 as employment_type,
        count(distinct canonical_person_id)                                  as termination_count
    from workforce_daily
    where is_termination_date
    group by
        date_trunc('month', snapshot_date)::date,
        coalesce(department, 'UNKNOWN'),
        coalesce(location, 'UNKNOWN'),
        coalesce(employment_type, 'UNKNOWN')
),

cohorts as (
    select
        spine.month_start_date,
        last_day(spine.month_start_date)                                     as month_end_date,
        spine.department,
        spine.location,
        spine.employment_type,
        coalesce(pop.start_cohort_employee_count, 0)                         as start_cohort_employee_count,
        coalesce(term.termination_count, 0)                                  as termination_count,
        coalesce(term.termination_count, 0)::float
            / nullif(coalesce(pop.start_cohort_employee_count, 0), 0)         as attrition_rate_raw
    from month_dimension_spine spine
    left join month_start_population pop
        on spine.month_start_date = pop.month_start_date
       and spine.department = pop.department
       and spine.location = pop.location
       and spine.employment_type = pop.employment_type
    left join terminations term
        on spine.month_start_date = term.month_start_date
       and spine.department = term.department
       and spine.location = term.location
       and spine.employment_type = term.employment_type
)

select
    {{ dbt_utils.generate_surrogate_key([
        'month_start_date',
        'department',
        'location',
        'employment_type'
    ]) }}                                                                    as attrition_monthly_key,
    month_start_date,
    month_end_date,
    department,
    location,
    employment_type,
    {{ k_anonymize('start_cohort_employee_count', 'start_cohort_employee_count', 'number(38, 0)') }}
                                                                               as start_headcount,
    {{ k_anonymize('termination_count', 'start_cohort_employee_count', 'number(38, 0)') }}
                                                                               as terminations,
    {{ k_anonymize('attrition_rate_raw', 'start_cohort_employee_count', 'float') }}
                                                                               as attrition_rate,
    {{ k_cohort_size_bucket('start_cohort_employee_count') }}                  as cohort_size_bucket,
    {{ is_k_anonymous('start_cohort_employee_count') }}                        as is_reportable,
    {{ k_suppression_reason('start_cohort_employee_count') }}                  as suppression_reason,
    {{ k_anonymity_threshold() }}                                              as k_anonymity_threshold,
    '{{ invocation_id }}'                                                      as _dbt_invocation_id
from cohorts
```

### 45. `dbt_project/models/marts/people_analytics/privacy_suppression_summary.sql`

**Purpose:** Observability mart for suppressed vs reportable public metric rows.

**Source:**

```sql
{{
    config(
        materialized='table',
        tags=['marts', 'people_analytics', 'phase_3', 'privacy']
    )
}}

-- =============================================================================
-- privacy_suppression_summary — Phase 3 privacy observability
-- =============================================================================
-- Summarizes how often each public People Analytics mart suppresses metrics.
-- This gives reviewers a quick way to see whether k-anonymity is doing real
-- work and whether a dimension design is creating too many tiny cohorts.
-- =============================================================================

with headcount as (
    select
        'workforce_headcount_daily'                                          as privacy_surface,
        'daily'                                                              as date_grain,
        count(*)                                                             as row_count,
        count_if(is_reportable)                                              as reportable_row_count,
        count_if(not is_reportable)                                          as suppressed_row_count,
        min(k_anonymity_threshold)                                           as k_anonymity_threshold
    from {{ ref('workforce_headcount_daily') }}
),

attrition as (
    select
        'workforce_attrition_monthly'                                        as privacy_surface,
        'monthly'                                                            as date_grain,
        count(*)                                                             as row_count,
        count_if(is_reportable)                                              as reportable_row_count,
        count_if(not is_reportable)                                          as suppressed_row_count,
        min(k_anonymity_threshold)                                           as k_anonymity_threshold
    from {{ ref('workforce_attrition_monthly') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'privacy_surface',
        'date_grain'
    ]) }}                                                                    as privacy_suppression_summary_key,
    privacy_surface,
    date_grain,
    row_count,
    reportable_row_count,
    suppressed_row_count,
    suppressed_row_count::float / nullif(row_count, 0)                       as suppressed_row_rate,
    k_anonymity_threshold,
    current_timestamp()                                                      as generated_at,
    '{{ invocation_id }}'                                                    as _dbt_invocation_id
from headcount

union all

select
    {{ dbt_utils.generate_surrogate_key([
        'privacy_surface',
        'date_grain'
    ]) }}                                                                    as privacy_suppression_summary_key,
    privacy_surface,
    date_grain,
    row_count,
    reportable_row_count,
    suppressed_row_count,
    suppressed_row_count::float / nullif(row_count, 0)                       as suppressed_row_rate,
    k_anonymity_threshold,
    current_timestamp()                                                      as generated_at,
    '{{ invocation_id }}'                                                    as _dbt_invocation_id
from attrition
```

### 46. `dbt_project/models/marts/people_analytics/privacy_audit_log.sql`

**Purpose:** Incremental audit table for future API/dashboard access events.

**Source:**

```sql
{{
    config(
        materialized='incremental',
        unique_key='audit_event_id',
        on_schema_change='sync_all_columns',
        tags=['marts', 'people_analytics', 'phase_3', 'privacy', 'audit']
    )
}}

-- =============================================================================
-- privacy_audit_log — Phase 3 access audit table
-- =============================================================================
-- Empty-on-build audit table for future FastAPI / Streamlit access events.
-- The `insert_privacy_audit_event` macro inserts rows into this table.
--
-- Why incremental with a zero-row select? A normal dbt build creates and
-- preserves the table shape without erasing existing events. A full-refresh
-- would still recreate the table, so do not full-refresh this model in any
-- environment where audit history matters.
-- =============================================================================

select
    cast(null as varchar(36))                                                as audit_event_id,
    cast(null as timestamp_ntz)                                              as audited_at,
    cast(null as varchar(255))                                               as actor,
    cast(null as varchar(255))                                               as query_surface,
    cast(null as varchar(255))                                               as purpose,
    parse_json(null)                                                         as filters_json,
    cast(null as integer)                                                    as k_anonymity_threshold,
    cast(null as integer)                                                    as result_row_count,
    cast(null as integer)                                                    as suppressed_row_count,
    cast(null as varchar(255))                                               as privacy_policy_version,
    cast(null as varchar(255))                                               as dbt_invocation_id
where false
```

### 47. `dbt_project/tests/macros/test_normalize_name.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- =============================================================================
-- test_normalize_name
-- =============================================================================
-- Singular test that exercises the normalize_name macro against the cases
-- enumerated in fixtures/normalize_name.yml. Mirror those cases here
-- exactly — if you add a case there, add it here in the same order.
--
-- Test passes when zero rows are returned. Each returned row is a failing
-- case with case_id, the input, the expected output, and the actual output.
-- =============================================================================

with cases (case_id, input_value, expected) as (
    select * from (values
        (1,  'Robert',       'robert'),
        (2,  '  Robert  ',   'robert'),
        (3,  'Édouard',      'edouard'),
        (4,  'Anaïs',        'anais'),
        (5,  'Mary-Jane',    'maryjane'),
        (6,  'O''Brien',     'obrien'),
        (7,  'Jean Paul',    'jeanpaul'),
        (8,  'Núñez',        'nunez'),
        (9,  'François',     'francois'),
        (10, '',             ''),
        (11, cast(null as varchar), cast(null as varchar)),
        (12, 'Robert3',      'robert'),
        (13, '张伟',         ''),
        (14, 'محمد',         ''),
        (15, 'Łukasz',       'lukasz')
    ) as t(case_id, input_value, expected)
),

results as (
    select
        case_id,
        input_value,
        expected,
        {{ normalize_name('input_value') }} as actual
    from cases
)

select *
from results
where coalesce(actual,   '__NULL__') != coalesce(expected, '__NULL__')
```

### 48. `dbt_project/tests/macros/test_first_name_root.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- =============================================================================
-- test_first_name_root
-- =============================================================================
-- Singular test that exercises the first_name_root macro against the cases
-- enumerated in fixtures/first_name_root.yml.
--
-- The macro requires the caller to LEFT JOIN to the nickname_map seed; this
-- test does that join inline so the macro contract is exercised end-to-end.
--
-- Test passes when zero rows are returned. Each returned row is a failing
-- case with case_id, the input, the expected output, and the actual output.
-- =============================================================================

with cases (case_id, input_value, expected) as (
    select * from (values
        (1,  'Robert',     'robert'),
        (2,  'Bob',        'robert'),
        (3,  'Bobby',      'robert'),
        (4,  'Rob',        'robert'),
        (5,  'Liz',        'elizabeth'),
        (6,  'Beth',       'elizabeth'),
        (7,  'Raj',        'rajesh'),
        (8,  'Paco',       'francisco'),
        (9,  'Aiden',      'aiden'),
        (10, 'Steve',      'steve'),
        (11, '  BoB  ',    'robert'),
        (12, 'Mária',      'maria'),
        (13, '',           ''),
        (14, cast(null as varchar), cast(null as varchar))
    ) as t(case_id, input_value, expected)
),

cases_with_root as (
    select
        cases.case_id,
        cases.input_value,
        cases.expected,
        {{ first_name_root('cases.input_value') }} as actual
    from cases
    left join {{ ref('nickname_map') }} nm
        on nm.nickname = {{ normalize_name('cases.input_value') }}
)

select *
from cases_with_root
where coalesce(actual,   '__NULL__') != coalesce(expected, '__NULL__')
```

### 49. `dbt_project/tests/macros/test_privacy_macros.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- =============================================================================
-- test_privacy_macros
-- =============================================================================
-- Exercises the k-anonymity macros against the configured threshold.
-- Test passes when zero rows are returned.
-- =============================================================================

with cases (case_id, cohort_count, metric_value, expected_is_reportable) as (
    select * from (values
        (1, {{ var('k_anonymity_threshold') }} - 1, 100, false),
        (2, {{ var('k_anonymity_threshold') }}, 100, true),
        (3, {{ var('k_anonymity_threshold') }} + 1, 100, true)
    ) as t(case_id, cohort_count, metric_value, expected_is_reportable)
),

results as (
    select
        case_id,
        cohort_count,
        expected_is_reportable,
        {{ is_k_anonymous('cohort_count') }} as actual_is_reportable,
        {{ k_anonymize('metric_value', 'cohort_count', 'number(38, 0)') }} as anonymized_metric,
        {{ k_suppression_reason('cohort_count') }} as suppression_reason,
        {{ k_cohort_size_bucket('cohort_count') }} as cohort_size_bucket
    from cases
)

select *
from results
where actual_is_reportable != expected_is_reportable
   or (expected_is_reportable and anonymized_metric is null)
   or (not expected_is_reportable and anonymized_metric is not null)
   or (expected_is_reportable and suppression_reason is not null)
   or (not expected_is_reportable and suppression_reason != 'K_ANONYMITY_THRESHOLD')
```

### 50. `dbt_project/tests/intermediate/int_identity_source_nodes__covers_expected_grain.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- int_identity_source_nodes must preserve the intended matching grain for each
-- upstream source/prep model. This catches accidental filters before the
-- canonical/queue coverage test runs.

with expected as (
    select 'HRIS' as source_system, count(*) as expected_count
    from {{ ref('int_hris_persons') }}

    union all

    select 'ATS' as source_system, count(*) as expected_count
    from {{ ref('stg_ats__candidates') }}

    union all

    select 'PAYROLL' as source_system, count(*) as expected_count
    from {{ ref('int_payroll_spells') }}

    union all

    select 'CRM' as source_system, count(*) as expected_count
    from {{ ref('stg_crm__sales_reps') }}

    union all

    select 'DMS_ERP' as source_system, count(*) as expected_count
    from {{ ref('int_dms_erp_unified') }}
),

actual as (
    select source_system, count(*) as actual_count
    from {{ ref('int_identity_source_nodes') }}
    group by source_system
)

select
    coalesce(expected.source_system, actual.source_system) as source_system,
    expected.expected_count,
    actual.actual_count
from expected
full outer join actual
    on expected.source_system = actual.source_system
where coalesce(expected.expected_count, -1) != coalesce(actual.actual_count, -1)
```

### 51. `dbt_project/tests/intermediate/int_hris_persons__no_orphan_spells.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- Every row in stg_hris__employees with a non-null DOB and personal_email_local_part
-- must contribute to exactly one int_hris_persons row. This proves the rehire-collapse
-- preserves all input rows (no orphans, no double-counting).
with stg_count as (
    select count(*) as n
    from {{ ref('stg_hris__employees') }}
    where date_of_birth is not null
      and personal_email_local_part is not null
),

persons_spell_sum as (
    select sum(spell_count) as n
    from {{ ref('int_hris_persons') }}
)

select
    stg_count.n  as stg_hris_employees_row_count,
    persons_spell_sum.n  as int_hris_persons_total_spell_count,
    stg_count.n - persons_spell_sum.n as discrepancy
from stg_count
cross join persons_spell_sum
where stg_count.n != persons_spell_sum.n
```

### 52. `dbt_project/tests/intermediate/int_payroll_spells__period_range_valid.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- Every spell's first_pay_period_start must be on or before its
-- most_recent_pay_period_end. A spell with first > most_recent is a
-- collapse logic bug.
select
    payroll_spell_key,
    employee_payroll_id,
    first_pay_period_start,
    most_recent_pay_period_end
from {{ ref('int_payroll_spells') }}
where first_pay_period_start > most_recent_pay_period_end
```

### 53. `dbt_project/tests/intermediate/int_payroll_spells__no_orphan_periods.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- Every row in stg_payroll__records with a non-null employee_payroll_id must
-- contribute to exactly one int_payroll_spells row. This proves the spell
-- collapse preserves all 153K monthly pay-period rows.
with stg_count as (
    select count(*) as n
    from {{ ref('stg_payroll__records') }}
    where employee_payroll_id is not null
),

spells_period_sum as (
    select sum(pay_period_count) as n
    from {{ ref('int_payroll_spells') }}
)

select
    stg_count.n  as stg_payroll_records_row_count,
    spells_period_sum.n  as int_payroll_spells_total_period_count,
    stg_count.n - spells_period_sum.n as discrepancy
from stg_count
cross join spells_period_sum
where stg_count.n != spells_period_sum.n
```

### 54. `dbt_project/tests/intermediate/int_dms_erp_unified__broken_link_implies_erp.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- has_broken_link should only fire when has_erp = TRUE. The "broken link"
-- topology is specifically an ERP user with NULL linked_dms_user_id; it
-- cannot exist for a DMS-only or paired row.
select dms_erp_person_key, has_dms, has_erp, has_broken_link, merge_topology
from {{ ref('int_dms_erp_unified') }}
where has_broken_link = true and has_erp = false
```

### 55. `dbt_project/tests/intermediate/int_dms_erp_unified__dms_user_id_unique_where_not_null.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- dms_user_id must be unique across the unified set when present. This
-- enforces the bipartite-1:0..1 topology assumption documented in
-- int_dms_erp_unified.sql — if a real-world dataset ever introduces 1:N
-- (multiple ERP rows pointing to the same DMS), this test fails loudly
-- and we know to add aggregation logic before continuing.
select dms_user_id, count(*) as rows_with_this_dms_user_id
from {{ ref('int_dms_erp_unified') }}
where dms_user_id is not null
group by dms_user_id
having count(*) > 1
```

### 56. `dbt_project/tests/intermediate/int_dms_erp_unified__erp_user_id_unique_where_not_null.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- erp_user_id must be unique across the unified set when present. ERP rows
-- always have a unique erp_user_id; the FULL OUTER JOIN should preserve that
-- uniqueness. Failure here means the join produced fan-out, which violates
-- the assumption that each ERP row points to at most one DMS row.
select erp_user_id, count(*) as rows_with_this_erp_user_id
from {{ ref('int_dms_erp_unified') }}
where erp_user_id is not null
group by erp_user_id
having count(*) > 1
```

### 57. `dbt_project/tests/intermediate/int_dms_erp_unified__every_row_has_source_node.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- Every row in the unified DMS+ERP set must have at least one source PK.
-- A row with both dms_user_id and erp_user_id NULL would be a FULL OUTER JOIN
-- artifact and indicates a bug in the merge.
select dms_erp_person_key, dms_user_id, erp_user_id, merge_topology
from {{ ref('int_dms_erp_unified') }}
where dms_user_id is null and erp_user_id is null
```

### 58. `dbt_project/tests/intermediate/int_canonical_person__hris_work_email_local_stability.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- No two HRIS employment spells may resolve to different canonical_person_ids
-- when they share DOB + work_email_local_part. This protects the rehire and
-- contractor-to-FTE invariants: new HRIS IDs should not fragment one person.

with resolved_hris_spells as (
    select
        hris.hris_employee_id,
        hris.date_of_birth,
        hris.work_email_local_part,
        canonical.canonical_person_id
    from {{ ref('stg_hris__employees') }} hris
    inner join {{ ref('int_hris_persons') }} persons
        on hris.date_of_birth = persons.date_of_birth
       and hris.personal_email_local_part = persons.personal_email_local_part
    inner join {{ ref('int_canonical_person') }} canonical
        on persons.hris_person_key = canonical.hris_person_key
    where hris.date_of_birth is not null
      and hris.work_email_local_part is not null
)

select
    date_of_birth,
    work_email_local_part,
    count(distinct hris_employee_id) as hris_employee_id_count,
    count(distinct canonical_person_id) as canonical_person_id_count
from resolved_hris_spells
group by date_of_birth, work_email_local_part
having count(distinct canonical_person_id) > 1
```

### 59. `dbt_project/tests/intermediate/int_canonical_person__no_duplicate_source_auto_matches.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- A non-HRIS source identity can auto-merge to at most one HRIS person across
-- all deterministic passes. Ambiguous candidates should be stewarded.

with auto_matches as (
    select source_record_key, hris_person_key
    from {{ ref('int_identity_pass_1_hard_anchors') }}
    where auto_merge_qualified

    union all

    select source_record_key, hris_person_key
    from {{ ref('int_identity_pass_2_name_dob_hire') }}
    where auto_merge_qualified

    union all

    select source_record_key, hris_person_key
    from {{ ref('int_identity_pass_3_email_domain') }}
    where auto_merge_qualified
)

select
    source_record_key,
    count(distinct hris_person_key) as canonical_candidate_count
from auto_matches
group by source_record_key
having count(distinct hris_person_key) > 1
```

### 60. `dbt_project/tests/intermediate/int_canonical_person__no_orphan_source_nodes.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- Every source identity node must end in exactly one of two places:
--   * HRIS nodes appear in int_canonical_person
--   * non-HRIS nodes are either auto-matched into canonical output or queued
--     for stewardship

with auto_matches as (
    select distinct source_record_key
    from {{ ref('int_identity_pass_1_hard_anchors') }}
    where auto_merge_qualified

    union

    select distinct source_record_key
    from {{ ref('int_identity_pass_2_name_dob_hire') }}
    where auto_merge_qualified

    union

    select distinct source_record_key
    from {{ ref('int_identity_pass_3_email_domain') }}
    where auto_merge_qualified
),

queue as (
    select distinct source_record_key
    from {{ ref('int_stewardship_queue') }}
),

hris_orphans as (
    select
        nodes.source_system,
        nodes.source_record_key,
        'HRIS_MISSING_FROM_CANONICAL' as orphan_reason
    from {{ ref('int_identity_source_nodes') }} nodes
    left join {{ ref('int_canonical_person') }} canonical
        on nodes.hris_person_key = canonical.hris_person_key
    where nodes.source_system = 'HRIS'
      and canonical.hris_person_key is null
),

non_hris_orphans as (
    select
        nodes.source_system,
        nodes.source_record_key,
        'NON_HRIS_MISSING_FROM_CANONICAL_AND_QUEUE' as orphan_reason
    from {{ ref('int_identity_source_nodes') }} nodes
    left join auto_matches
        on nodes.source_record_key = auto_matches.source_record_key
    left join queue
        on nodes.source_record_key = queue.source_record_key
    where nodes.source_system != 'HRIS'
      and auto_matches.source_record_key is null
      and queue.source_record_key is null
),

double_assigned as (
    select
        nodes.source_system,
        nodes.source_record_key,
        'NON_HRIS_IN_BOTH_CANONICAL_AND_QUEUE' as orphan_reason
    from {{ ref('int_identity_source_nodes') }} nodes
    inner join auto_matches
        on nodes.source_record_key = auto_matches.source_record_key
    inner join queue
        on nodes.source_record_key = queue.source_record_key
    where nodes.source_system != 'HRIS'
)

select * from hris_orphans
union all
select * from non_hris_orphans
union all
select * from double_assigned
```

### 61. `dbt_project/tests/intermediate/int_stewardship_queue__no_resolved_source_overlap.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- Stewardship should contain only source identities that failed deterministic
-- auto-merge. If a source_record_key appears here and in auto matches, the
-- matcher has created two conflicting outcomes for one input.

with auto_matches as (
    select distinct source_record_key
    from {{ ref('int_identity_pass_1_hard_anchors') }}
    where auto_merge_qualified

    union

    select distinct source_record_key
    from {{ ref('int_identity_pass_2_name_dob_hire') }}
    where auto_merge_qualified

    union

    select distinct source_record_key
    from {{ ref('int_identity_pass_3_email_domain') }}
    where auto_merge_qualified
)

select queue.source_record_key
from {{ ref('int_stewardship_queue') }} queue
inner join auto_matches
    on queue.source_record_key = auto_matches.source_record_key
```

### 62. `dbt_project/tests/marts/dim_employee__covers_hris_spells.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- Every HRIS employment spell should appear exactly once in dim_employee.

with hris as (
    select count(*) as spell_count
    from {{ ref('stg_hris__employees') }}
),

dim as (
    select count(*) as spell_count
    from {{ ref('dim_employee') }}
)

select
    hris.spell_count as hris_spell_count,
    dim.spell_count as dim_spell_count
from hris
cross join dim
where hris.spell_count != dim.spell_count
```

### 63. `dbt_project/tests/marts/dim_employee__no_overlapping_effective_dates.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- A canonical person can have multiple employment spells, but those effective
-- date ranges must not overlap. Otherwise a point-in-time query could resolve
-- one person to multiple HRIS spell rows on the same day.

with ordered_spells as (
    select
        canonical_person_id,
        hris_employee_id,
        effective_start_date,
        effective_end_date,
        lead(effective_start_date) over (
            partition by canonical_person_id
            order by effective_start_date, hris_employee_id
        ) as next_effective_start_date
    from {{ ref('dim_employee') }}
)

select *
from ordered_spells
where next_effective_start_date is not null
  and effective_end_date >= next_effective_start_date
```

### 64. `dbt_project/tests/marts/fct_workforce_daily__active_one_row_per_person_day.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- Active headcount should never double-count one canonical person on one date.

select
    snapshot_date,
    canonical_person_id,
    count(*) as active_rows
from {{ ref('fct_workforce_daily') }}
where is_active_on_date
group by snapshot_date, canonical_person_id
having count(*) > 1
```

### 65. `dbt_project/tests/marts/fct_workforce_daily__date_bounds_valid.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- The daily fact should not emit dates before hire or after the configured
-- snapshot as-of date/current_date.

{% if var('snapshot_as_of_date') %}
    {% set snapshot_as_of_expr = "to_date('" ~ var('snapshot_as_of_date') ~ "')" %}
{% else %}
    {% set snapshot_as_of_expr = "current_date()" %}
{% endif %}

select *
from {{ ref('fct_workforce_daily') }}
where snapshot_date < effective_start_date
   or snapshot_date > {{ snapshot_as_of_expr }}
```

### 66. `dbt_project/tests/marts/workforce_headcount_daily__suppressed_metrics_null.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- Suppressed headcount rows must not expose exact metrics.

select *
from {{ ref('workforce_headcount_daily') }}
where not is_reportable
  and (
      headcount is not null
      or reportable_cohort_employee_count is not null
      or suppression_reason != 'K_ANONYMITY_THRESHOLD'
  )
```

### 67. `dbt_project/tests/marts/workforce_headcount_daily__suppresses_small_cohorts.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- Recompute raw daily headcount cohorts from the core fact. Any below-k cohort
-- in the public mart must be marked non-reportable.

with raw_cohorts as (
    select
        snapshot_date,
        coalesce(department, 'UNKNOWN') as department,
        coalesce(location, 'UNKNOWN') as location,
        coalesce(employment_type, 'UNKNOWN') as employment_type,
        count(distinct canonical_person_id) as raw_cohort_count
    from {{ ref('fct_workforce_daily') }}
    where is_active_on_date
    group by
        snapshot_date,
        coalesce(department, 'UNKNOWN'),
        coalesce(location, 'UNKNOWN'),
        coalesce(employment_type, 'UNKNOWN')
)

select public.*
from {{ ref('workforce_headcount_daily') }} public
inner join raw_cohorts raw
    on public.snapshot_date = raw.snapshot_date
   and public.department = raw.department
   and public.location = raw.location
   and public.employment_type = raw.employment_type
where raw.raw_cohort_count < {{ var('k_anonymity_threshold') }}
  and public.is_reportable
```

### 68. `dbt_project/tests/marts/workforce_attrition_monthly__suppressed_metrics_null.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- Suppressed attrition rows must not expose exact numerator, denominator, or
-- rate values.

select *
from {{ ref('workforce_attrition_monthly') }}
where not is_reportable
  and (
      start_headcount is not null
      or terminations is not null
      or attrition_rate is not null
      or suppression_reason != 'K_ANONYMITY_THRESHOLD'
  )
```

### 69. `dbt_project/tests/marts/workforce_attrition_monthly__suppresses_small_cohorts.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- Recompute month-start cohort sizes from the core fact. Any below-k cohort in
-- the public attrition mart must be marked non-reportable.

with month_first_snapshot as (
    select
        date_trunc('month', snapshot_date)::date as month_start_date,
        min(snapshot_date) as first_snapshot_date
    from {{ ref('fct_workforce_daily') }}
    group by date_trunc('month', snapshot_date)::date
),

raw_cohorts as (
    select
        date_trunc('month', fct.snapshot_date)::date as month_start_date,
        coalesce(fct.department, 'UNKNOWN') as department,
        coalesce(fct.location, 'UNKNOWN') as location,
        coalesce(fct.employment_type, 'UNKNOWN') as employment_type,
        count(distinct fct.canonical_person_id) as raw_start_cohort_count
    from {{ ref('fct_workforce_daily') }} fct
    inner join month_first_snapshot
        on date_trunc('month', fct.snapshot_date)::date = month_first_snapshot.month_start_date
       and fct.snapshot_date = month_first_snapshot.first_snapshot_date
    where fct.is_active_on_date
    group by
        date_trunc('month', fct.snapshot_date)::date,
        coalesce(fct.department, 'UNKNOWN'),
        coalesce(fct.location, 'UNKNOWN'),
        coalesce(fct.employment_type, 'UNKNOWN')
)

select public.*
from {{ ref('workforce_attrition_monthly') }} public
left join raw_cohorts raw
    on public.month_start_date = raw.month_start_date
   and public.department = raw.department
   and public.location = raw.location
   and public.employment_type = raw.employment_type
where coalesce(raw.raw_start_cohort_count, 0) < {{ var('k_anonymity_threshold') }}
  and public.is_reportable
```

### 70. `dbt_project/tests/marts/privacy_suppression_summary__matches_public_surfaces.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- The privacy suppression summary should agree with the public marts it
-- summarizes.

with expected as (
    select
        'workforce_headcount_daily' as privacy_surface,
        count(*) as row_count,
        count_if(is_reportable) as reportable_row_count,
        count_if(not is_reportable) as suppressed_row_count
    from {{ ref('workforce_headcount_daily') }}

    union all

    select
        'workforce_attrition_monthly' as privacy_surface,
        count(*) as row_count,
        count_if(is_reportable) as reportable_row_count,
        count_if(not is_reportable) as suppressed_row_count
    from {{ ref('workforce_attrition_monthly') }}
),

actual as (
    select
        privacy_surface,
        row_count,
        reportable_row_count,
        suppressed_row_count
    from {{ ref('privacy_suppression_summary') }}
)

select
    expected.privacy_surface,
    expected.row_count as expected_row_count,
    actual.row_count as actual_row_count,
    expected.reportable_row_count as expected_reportable_row_count,
    actual.reportable_row_count as actual_reportable_row_count,
    expected.suppressed_row_count as expected_suppressed_row_count,
    actual.suppressed_row_count as actual_suppressed_row_count
from expected
inner join actual
    on expected.privacy_surface = actual.privacy_surface
where expected.row_count != actual.row_count
   or expected.reportable_row_count != actual.reportable_row_count
   or expected.suppressed_row_count != actual.suppressed_row_count
```

### 71. `dbt_project/tests/marts/privacy__no_direct_employee_identifiers_in_people_analytics.sql`

**Purpose:** Custom dbt data test. A passing test returns zero rows; any returned row is a data-quality failure.

**Source:**

```sql
-- Public People Analytics marts must not expose direct employee identifiers or
-- sensitive identity attributes. Core/intermediate models may keep these for
-- controlled joins; business-facing privacy marts may not.

select
    table_schema,
    table_name,
    column_name
from {{ target.database }}.information_schema.columns
where table_schema = upper('{{ target.schema }}_people_analytics')
  and lower(column_name) in (
      'canonical_person_id',
      'hris_person_key',
      'hris_employee_id',
      'employee_sk',
      'daily_workforce_key',
      'date_of_birth',
      'sin_last_4',
      'legal_first_name',
      'legal_last_name',
      'preferred_name',
      'legal_first_name_original',
      'legal_last_name_original',
      'preferred_name_original'
  )
```

### 72. `airflow/dags/atlas_people_analytics.py`

**Purpose:** Production-shaped DAG showing refresh order across dbt dependencies, staging, identity, core, and privacy marts.

**Source:**

```python
"""Airflow DAG for the Atlas synthetic People Analytics pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_DIR = PROJECT_ROOT / "dbt_project"
ENV_FILE = PROJECT_ROOT / ".env"

DBT_COMMAND_PREFIX = f"""
set -euo pipefail
cd "{DBT_DIR}"
if [ -f "{ENV_FILE}" ]; then
  set -a
  source "{ENV_FILE}"
  set +a
fi
export DBT_PROFILES_DIR="${{DBT_PROFILES_DIR:-{DBT_DIR}}}"
export DBT_TARGET="${{DBT_TARGET:-dev}}"
"""


try:
    from airflow.operators.bash import BashOperator

    from airflow import DAG
except ImportError:  # pragma: no cover - Airflow is an optional local dependency.
    dag = None
else:
    default_args = {
        "owner": "atlas",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }

    with DAG(
        dag_id="atlas_people_analytics",
        description="Build Atlas synthetic People Analytics marts from raw data through privacy surfaces.",
        default_args=default_args,
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
        max_active_runs=1,
        tags=["atlas", "people_analytics", "synthetic"],
    ) as dag:
        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"{DBT_COMMAND_PREFIX}\ndbt deps",
        )

        build_staging = BashOperator(
            task_id="build_staging",
            bash_command=f"{DBT_COMMAND_PREFIX}\ndbt build --select staging",
        )

        build_identity = BashOperator(
            task_id="build_identity_resolution",
            bash_command=(
                f"{DBT_COMMAND_PREFIX}\n"
                "dbt build --select +int_canonical_person+ int_stewardship_queue"
            ),
        )

        build_core = BashOperator(
            task_id="build_core_marts",
            bash_command=f"{DBT_COMMAND_PREFIX}\ndbt build --select +dim_employee+ fct_workforce_daily",
        )

        build_privacy = BashOperator(
            task_id="build_privacy_marts",
            bash_command=(
                f"{DBT_COMMAND_PREFIX}\n"
                "dbt build --select +privacy_suppression_summary+ privacy_audit_log "
                "test_privacy_macros privacy__no_direct_employee_identifiers_in_people_analytics"
            ),
        )

        dbt_deps >> build_staging >> build_identity >> build_core >> build_privacy


__all__ = ["dag"]
```

### 73. `api/__init__.py`

**Purpose:** Python implementation module in the project runtime.

**Source:**

```python
"""FastAPI service package for Atlas People Analytics."""
```

### 74. `api/settings.py`

**Purpose:** Environment-driven API/Snowflake configuration with identifier validation to prevent unsafe SQL composition.

**Source:**

```python
"""Runtime configuration for the Atlas metrics service."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any

from dotenv import find_dotenv, load_dotenv

_SNOWFLAKE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def normalize_snowflake_identifier(value: str, label: str) -> str:
    """Return a safe unquoted Snowflake identifier.

    The metrics API reads database/schema names from environment variables so
    local demos can target different dbt schemas. We still keep table SQL
    deterministic and identifier-safe because these values are interpolated
    into fully qualified relation names rather than passed as query parameters.
    """

    cleaned = value.strip()
    if not _SNOWFLAKE_IDENTIFIER_RE.match(cleaned):
        raise ValueError(f"{label} must be a simple Snowflake identifier")
    return cleaned.upper()


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


@dataclass(frozen=True)
class AtlasSettings:
    """Environment-backed settings shared by the API and dashboard tests."""

    snowflake_account: str | None
    snowflake_user: str | None
    snowflake_password: str | None
    snowflake_role: str
    snowflake_warehouse: str
    snowflake_database: str
    snowflake_schema: str
    snowflake_region: str | None
    dbt_schema: str
    people_analytics_schema: str
    api_host: str
    api_port: int
    k_anonymity_min: int
    privacy_policy_version: str = "phase_3_k_anonymity_v1"

    @classmethod
    def from_env(cls) -> AtlasSettings:
        dotenv_path = find_dotenv(usecwd=True)
        if dotenv_path:
            load_dotenv(dotenv_path, override=False)

        dbt_schema = os.getenv("SNOWFLAKE_DBT_SCHEMA", "DBT_DEV").strip()
        people_schema = os.getenv("ATLAS_PEOPLE_ANALYTICS_SCHEMA")
        if not people_schema:
            people_schema = f"{dbt_schema}_PEOPLE_ANALYTICS"

        return cls(
            snowflake_account=os.getenv("SNOWFLAKE_ACCOUNT"),
            snowflake_user=os.getenv("SNOWFLAKE_USER"),
            snowflake_password=os.getenv("SNOWFLAKE_PASSWORD"),
            snowflake_role=os.getenv("SNOWFLAKE_ROLE", "ATLAS_DEVELOPER"),
            snowflake_warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "ATLAS_WH"),
            snowflake_database=os.getenv("SNOWFLAKE_DATABASE", "ATLAS"),
            snowflake_schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
            snowflake_region=os.getenv("SNOWFLAKE_REGION"),
            dbt_schema=dbt_schema,
            people_analytics_schema=people_schema,
            api_host=os.getenv("ATLAS_API_HOST", "127.0.0.1"),
            api_port=_env_int("ATLAS_API_PORT", 8000),
            k_anonymity_min=_env_int("ATLAS_K_ANONYMITY_MIN", 5),
        )

    @property
    def database_identifier(self) -> str:
        return normalize_snowflake_identifier(self.snowflake_database, "SNOWFLAKE_DATABASE")

    @property
    def people_analytics_schema_identifier(self) -> str:
        return normalize_snowflake_identifier(
            self.people_analytics_schema,
            "ATLAS_PEOPLE_ANALYTICS_SCHEMA",
        )

    def public_table(self, table_name: str) -> str:
        table_identifier = normalize_snowflake_identifier(table_name, "table_name")
        return (
            f"{self.database_identifier}."
            f"{self.people_analytics_schema_identifier}."
            f"{table_identifier}"
        )

    def missing_connection_values(self) -> list[str]:
        required = {
            "SNOWFLAKE_ACCOUNT": self.snowflake_account,
            "SNOWFLAKE_USER": self.snowflake_user,
            "SNOWFLAKE_PASSWORD": self.snowflake_password,
            "SNOWFLAKE_ROLE": self.snowflake_role,
            "SNOWFLAKE_WAREHOUSE": self.snowflake_warehouse,
            "SNOWFLAKE_DATABASE": self.snowflake_database,
        }
        return [name for name, value in required.items() if not value]

    def snowflake_connect_kwargs(self) -> dict[str, Any]:
        missing = self.missing_connection_values()
        if missing:
            joined = ", ".join(missing)
            raise RuntimeError(f"Missing Snowflake connection settings: {joined}")

        kwargs = {
            "account": self.snowflake_account or "",
            "user": self.snowflake_user or "",
            "password": self.snowflake_password or "",
            "role": self.snowflake_role,
            "warehouse": self.snowflake_warehouse,
            "database": self.snowflake_database,
            "schema": self.people_analytics_schema,
            "client_session_keep_alive": False,
        }
        return kwargs
```

### 75. `api/snowflake_client.py`

**Purpose:** Thin Snowflake client abstraction used by the FastAPI metrics service.

**Source:**

```python
"""Small Snowflake access wrapper for the Atlas metrics API."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from api.settings import AtlasSettings

QueryParams = tuple[Any, ...]


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return value


class AtlasSnowflakeClient:
    """Executes parameterized queries against Snowflake.

    The service intentionally keeps this wrapper small: Phase 4 is an
    operational facade over dbt-built privacy marts, not a second transformation
    layer with its own business logic.
    """

    def __init__(self, settings: AtlasSettings):
        self._settings = settings

    def _connect(self):
        import snowflake.connector

        return snowflake.connector.connect(**self._settings.snowflake_connect_kwargs())

    def fetch_all(self, sql: str, params: QueryParams = ()) -> list[dict[str, Any]]:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(sql, params)
                columns = [column[0].lower() for column in cursor.description or []]
                return [
                    {column: _json_ready(value) for column, value in zip(columns, row, strict=True)}
                    for row in cursor.fetchall()
                ]
            finally:
                cursor.close()
        finally:
            connection.close()

    def execute(self, sql: str, params: QueryParams = ()) -> None:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(sql, params)
            finally:
                cursor.close()
        finally:
            connection.close()
```

### 76. `api/metrics_service.py`

**Purpose:** Privacy-aware FastAPI service exposing only aggregate public marts and writing best-effort audit events.

**Source:**

```python
"""Privacy-aware FastAPI metrics service for Atlas."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import date
from typing import Annotated, Any, Protocol

from fastapi import Depends, FastAPI, Header, HTTPException, Query

from api.settings import AtlasSettings
from api.snowflake_client import AtlasSnowflakeClient, QueryParams

LOGGER = logging.getLogger(__name__)

MAX_LIMIT = 5_000
PUBLIC_SURFACES = {
    "workforce_headcount_daily",
    "workforce_attrition_monthly",
    "privacy_suppression_summary",
}


class MetricsClient(Protocol):
    def fetch_all(self, sql: str, params: QueryParams = ()) -> list[dict[str, Any]]: ...

    def execute(self, sql: str, params: QueryParams = ()) -> None: ...


@dataclass(frozen=True)
class MetricFilters:
    start_date: date | None = None
    end_date: date | None = None
    department: str | None = None
    location: str | None = None
    employment_type: str | None = None
    privacy_surface: str | None = None
    limit: int = 1_000

    def audit_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        return {
            key: value.isoformat() if isinstance(value, date) else value
            for key, value in payload.items()
            if value is not None
        }


@dataclass(frozen=True)
class SqlStatement:
    sql: str
    params: QueryParams = ()


app = FastAPI(
    title="Atlas People Analytics Metrics API",
    version="0.4.0",
    description=("Privacy-aware metric service over synthetic Atlas People Analytics marts."),
)


def get_settings() -> AtlasSettings:
    return AtlasSettings.from_env()


SettingsDep = Annotated[AtlasSettings, Depends(get_settings)]


def get_client(settings: SettingsDep) -> MetricsClient:
    return AtlasSnowflakeClient(settings)


ClientDep = Annotated[MetricsClient, Depends(get_client)]


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "atlas-metrics-api",
        "docs": "/docs",
        "health": "/health",
        "metadata": "/metadata",
        "metrics": {
            "daily_headcount": "/headcount/daily",
            "monthly_attrition": "/attrition/monthly",
            "suppression_summary": "/privacy/suppression-summary",
        },
        "privacy_note": "Metric endpoints read only from k-anonymous People Analytics marts.",
    }


def _where_clause(predicates: list[str]) -> str:
    if not predicates:
        return ""
    return "where " + "\n  and ".join(predicates)


def _bounded_limit(limit: int) -> int:
    return max(1, min(limit, MAX_LIMIT))


def _add_dimension_filters(
    predicates: list[str],
    params: list[Any],
    filters: MetricFilters,
) -> None:
    if filters.department:
        predicates.append("department = %s")
        params.append(filters.department)
    if filters.location:
        predicates.append("location = %s")
        params.append(filters.location)
    if filters.employment_type:
        predicates.append("employment_type = %s")
        params.append(filters.employment_type)


def build_headcount_query(settings: AtlasSettings, filters: MetricFilters) -> SqlStatement:
    table = settings.public_table("workforce_headcount_daily")
    predicates: list[str] = []
    params: list[Any] = []

    if filters.start_date:
        predicates.append("snapshot_date >= %s")
        params.append(filters.start_date)
    if filters.end_date:
        predicates.append("snapshot_date <= %s")
        params.append(filters.end_date)
    _add_dimension_filters(predicates, params, filters)

    sql = f"""
select
    snapshot_date,
    department,
    location,
    employment_type,
    headcount,
    reportable_cohort_employee_count,
    cohort_size_bucket,
    is_reportable,
    suppression_reason,
    k_anonymity_threshold
from {table}
{_where_clause(predicates)}
order by snapshot_date, department, location, employment_type
limit {_bounded_limit(filters.limit)}
""".strip()
    return SqlStatement(sql=sql, params=tuple(params))


def build_attrition_query(settings: AtlasSettings, filters: MetricFilters) -> SqlStatement:
    table = settings.public_table("workforce_attrition_monthly")
    predicates: list[str] = []
    params: list[Any] = []

    if filters.start_date:
        predicates.append("month_start_date >= %s")
        params.append(filters.start_date)
    if filters.end_date:
        predicates.append("month_start_date <= %s")
        params.append(filters.end_date)
    _add_dimension_filters(predicates, params, filters)

    sql = f"""
select
    month_start_date,
    month_end_date,
    department,
    location,
    employment_type,
    start_headcount,
    terminations,
    attrition_rate,
    cohort_size_bucket,
    is_reportable,
    suppression_reason,
    k_anonymity_threshold
from {table}
{_where_clause(predicates)}
order by month_start_date, department, location, employment_type
limit {_bounded_limit(filters.limit)}
""".strip()
    return SqlStatement(sql=sql, params=tuple(params))


def build_suppression_summary_query(
    settings: AtlasSettings,
    filters: MetricFilters,
) -> SqlStatement:
    table = settings.public_table("privacy_suppression_summary")
    predicates: list[str] = []
    params: list[Any] = []
    if filters.privacy_surface:
        predicates.append("privacy_surface = %s")
        params.append(filters.privacy_surface)

    sql = f"""
select
    privacy_surface,
    date_grain,
    row_count,
    reportable_row_count,
    suppressed_row_count,
    suppressed_row_rate,
    k_anonymity_threshold,
    generated_at
from {table}
{_where_clause(predicates)}
order by privacy_surface
limit {_bounded_limit(filters.limit)}
""".strip()
    return SqlStatement(sql=sql, params=tuple(params))


def build_metadata_query(settings: AtlasSettings) -> SqlStatement:
    table = settings.public_table("workforce_headcount_daily")
    sql = f"""
select
    min(snapshot_date) as min_snapshot_date,
    max(snapshot_date) as max_snapshot_date,
    count(*) as headcount_row_count,
    count_if(not is_reportable) as suppressed_headcount_row_count,
    listagg(distinct department, '||') within group (order by department) as departments,
    listagg(distinct location, '||') within group (order by location) as locations,
    listagg(distinct employment_type, '||') within group (order by employment_type) as employment_types,
    min(k_anonymity_threshold) as k_anonymity_threshold
from {table}
""".strip()
    return SqlStatement(sql=sql)


def build_audit_insert(
    settings: AtlasSettings,
    *,
    actor: str,
    query_surface: str,
    purpose: str,
    filters: dict[str, Any],
    result_row_count: int,
    suppressed_row_count: int,
) -> SqlStatement:
    table = settings.public_table("privacy_audit_log")
    sql = f"""
insert into {table} (
    audit_event_id,
    audited_at,
    actor,
    query_surface,
    purpose,
    filters_json,
    k_anonymity_threshold,
    result_row_count,
    suppressed_row_count,
    privacy_policy_version,
    dbt_invocation_id
)
select
    uuid_string(),
    current_timestamp(),
    %s,
    %s,
    %s,
    try_parse_json(%s),
    %s,
    %s,
    %s,
    %s,
    null
""".strip()
    params = (
        actor,
        query_surface,
        purpose,
        json.dumps(filters, sort_keys=True),
        settings.k_anonymity_min,
        result_row_count,
        suppressed_row_count,
        settings.privacy_policy_version,
    )
    return SqlStatement(sql=sql, params=params)


def _suppressed_row_count(rows: list[dict[str, Any]]) -> int:
    return sum(1 for row in rows if row.get("is_reportable") is False)


def _split_dimension_list(value: Any) -> list[str]:
    if not value:
        return []
    return [item for item in str(value).split("||") if item]


def _query_rows(client: MetricsClient, statement: SqlStatement) -> list[dict[str, Any]]:
    try:
        return client.fetch_all(statement.sql, statement.params)
    except Exception as exc:  # pragma: no cover - exercised by live service failures
        LOGGER.exception("Metrics warehouse query failed")
        raise HTTPException(status_code=503, detail="Metrics warehouse query failed") from exc


def _audit_access(
    client: MetricsClient,
    settings: AtlasSettings,
    *,
    actor: str,
    query_surface: str,
    purpose: str,
    filters: MetricFilters,
    rows: list[dict[str, Any]],
) -> bool:
    statement = build_audit_insert(
        settings,
        actor=actor,
        query_surface=query_surface,
        purpose=purpose,
        filters=filters.audit_payload(),
        result_row_count=len(rows),
        suppressed_row_count=_suppressed_row_count(rows),
    )
    try:
        client.execute(statement.sql, statement.params)
    except Exception:  # pragma: no cover - audit failure should not break reads
        LOGGER.exception("Privacy audit insert failed")
        return False
    return True


def _metric_response(
    client: MetricsClient,
    settings: AtlasSettings,
    statement: SqlStatement,
    *,
    query_surface: str,
    purpose: str,
    filters: MetricFilters,
    actor: str | None,
) -> dict[str, Any]:
    rows = _query_rows(client, statement)
    audit_logged = _audit_access(
        client,
        settings,
        actor=actor or "anonymous",
        query_surface=query_surface,
        purpose=purpose,
        filters=filters,
        rows=rows,
    )
    return {
        "data": rows,
        "row_count": len(rows),
        "suppressed_row_count": _suppressed_row_count(rows),
        "audit_logged": audit_logged,
    }


@app.get("/health")
def health(settings: SettingsDep) -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "atlas-metrics-api",
        "warehouse_configured": not settings.missing_connection_values(),
        "database": settings.database_identifier,
        "people_analytics_schema": settings.people_analytics_schema_identifier,
        "public_surfaces": sorted(PUBLIC_SURFACES),
    }


@app.get("/metadata")
def metadata(
    client: ClientDep,
    settings: SettingsDep,
) -> dict[str, Any]:
    rows = _query_rows(client, build_metadata_query(settings))
    row = rows[0] if rows else {}
    return {
        "min_snapshot_date": row.get("min_snapshot_date"),
        "max_snapshot_date": row.get("max_snapshot_date"),
        "headcount_row_count": row.get("headcount_row_count", 0),
        "suppressed_headcount_row_count": row.get("suppressed_headcount_row_count", 0),
        "departments": _split_dimension_list(row.get("departments")),
        "locations": _split_dimension_list(row.get("locations")),
        "employment_types": _split_dimension_list(row.get("employment_types")),
        "k_anonymity_threshold": row.get("k_anonymity_threshold", settings.k_anonymity_min),
        "public_surfaces": sorted(PUBLIC_SURFACES),
    }


@app.get("/headcount/daily")
def headcount_daily(
    client: ClientDep,
    settings: SettingsDep,
    start_date: date | None = None,
    end_date: date | None = None,
    department: str | None = None,
    location: str | None = None,
    employment_type: str | None = None,
    limit: int = Query(default=1_000, ge=1, le=MAX_LIMIT),
    purpose: str = Query(default="dashboard_view", min_length=1, max_length=255),
    actor: str | None = Header(default=None, alias="X-Atlas-Actor"),
) -> dict[str, Any]:
    filters = MetricFilters(
        start_date=start_date,
        end_date=end_date,
        department=department,
        location=location,
        employment_type=employment_type,
        limit=limit,
    )
    return _metric_response(
        client,
        settings,
        build_headcount_query(settings, filters),
        query_surface="workforce_headcount_daily",
        purpose=purpose,
        filters=filters,
        actor=actor,
    )


@app.get("/attrition/monthly")
def attrition_monthly(
    client: ClientDep,
    settings: SettingsDep,
    start_date: date | None = None,
    end_date: date | None = None,
    department: str | None = None,
    location: str | None = None,
    employment_type: str | None = None,
    limit: int = Query(default=1_000, ge=1, le=MAX_LIMIT),
    purpose: str = Query(default="dashboard_view", min_length=1, max_length=255),
    actor: str | None = Header(default=None, alias="X-Atlas-Actor"),
) -> dict[str, Any]:
    filters = MetricFilters(
        start_date=start_date,
        end_date=end_date,
        department=department,
        location=location,
        employment_type=employment_type,
        limit=limit,
    )
    return _metric_response(
        client,
        settings,
        build_attrition_query(settings, filters),
        query_surface="workforce_attrition_monthly",
        purpose=purpose,
        filters=filters,
        actor=actor,
    )


@app.get("/privacy/suppression-summary")
def privacy_suppression_summary(
    client: ClientDep,
    settings: SettingsDep,
    privacy_surface: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=MAX_LIMIT),
    purpose: str = Query(default="dashboard_view", min_length=1, max_length=255),
    actor: str | None = Header(default=None, alias="X-Atlas-Actor"),
) -> dict[str, Any]:
    if privacy_surface and privacy_surface not in PUBLIC_SURFACES:
        raise HTTPException(status_code=422, detail="Unsupported privacy_surface")
    filters = MetricFilters(privacy_surface=privacy_surface, limit=limit)
    return _metric_response(
        client,
        settings,
        build_suppression_summary_query(settings, filters),
        query_surface="privacy_suppression_summary",
        purpose=purpose,
        filters=filters,
        actor=actor,
    )
```

### 77. `dashboard/__init__.py`

**Purpose:** Python implementation module in the project runtime.

**Source:**

```python
"""Streamlit dashboard package for Atlas People Analytics."""
```

### 78. `dashboard/app.py`

**Purpose:** Streamlit HRBP dashboard over the API, including executive overview, filters, charts, and privacy tab.

**Source:**

```python
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
    default_start = max(min_snapshot, max_snapshot - timedelta(days=7))

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


def reportable_row_count(df: pd.DataFrame, metric_column: str) -> int:
    if df.empty or metric_column not in df:
        return 0
    return int(df[metric_column].notna().sum())


def render_executive_overview(
    metadata: dict[str, Any],
    headcount_payload: dict[str, Any],
    attrition_payload: dict[str, Any],
    suppression: pd.DataFrame,
    headcount: pd.DataFrame,
    attrition: pd.DataFrame,
) -> None:
    st.subheader("Executive Overview")
    st.caption("Governed people metrics from the canonical employee spine")

    latest_headcount = current_headcount(headcount)
    latest_attrition = latest_attrition_rate(attrition)
    summary_cols = st.columns(4)
    summary_cols[0].metric(
        "Reportable headcount",
        "Suppressed" if latest_headcount is None else f"{latest_headcount:,}",
    )
    summary_cols[1].metric(
        "Latest attrition",
        "Suppressed" if latest_attrition is None else f"{latest_attrition:.1%}",
    )
    summary_cols[2].metric(
        "Suppressed rows",
        f"{headcount_payload.get('suppressed_row_count', 0):,}",
    )
    summary_cols[3].metric(
        "Data through",
        str(metadata.get("max_snapshot_date", "Unknown")),
    )

    chart_cols = st.columns([2, 1])
    with chart_cols[0]:
        st.markdown("#### Headcount Trend")
        if headcount.empty or reportable_row_count(headcount, "headcount") == 0:
            st.info("No reportable headcount rows returned.")
        else:
            chart_data = (
                headcount.dropna(subset=["headcount"])
                .groupby("snapshot_date", as_index=False)["headcount"]
                .sum()
                .set_index("snapshot_date")
            )
            st.line_chart(chart_data)

    with chart_cols[1]:
        st.markdown("#### Privacy Status")
        if suppression.empty:
            st.info("No privacy summary rows returned.")
        else:
            privacy_chart = suppression.set_index("privacy_surface")[
                ["reportable_row_count", "suppressed_row_count"]
            ]
            st.bar_chart(privacy_chart)

    detail_cols = st.columns(3)
    detail_cols[0].metric(
        "Reportable headcount rows",
        f"{reportable_row_count(headcount, 'headcount'):,}",
    )
    detail_cols[1].metric(
        "Reportable attrition rows",
        f"{reportable_row_count(attrition, 'attrition_rate'):,}",
    )
    detail_cols[2].metric(
        "k threshold",
        metadata.get("k_anonymity_threshold", 5),
    )


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

    tab_executive, tab_overview, tab_headcount, tab_attrition, tab_privacy = st.tabs(
        ["Executive Overview", "Lineage", "Headcount", "Attrition", "Privacy"]
    )

    with tab_executive:
        render_executive_overview(
            metadata,
            headcount_payload,
            attrition_payload,
            suppression,
            headcount,
            attrition,
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
```

### 79. `identity_engine/__init__.py`

**Purpose:** Python implementation module in the project runtime.

**Source:**

```python
"""Residual identity-review tools for Atlas."""

from identity_engine.evaluation import (
    ResidualEvaluationSummary,
    ResidualProxyEvaluationSummary,
    evaluate_against_deterministic_hints,
    render_proxy_evaluation_report,
    render_residual_report,
    summarize_residual_candidates,
)
from identity_engine.residual_matcher import (
    CanonicalIdentity,
    ResidualCandidate,
    SourceIdentity,
    rank_residual_candidates,
    score_residual_candidate,
)

__all__ = [
    "CanonicalIdentity",
    "ResidualCandidate",
    "ResidualEvaluationSummary",
    "ResidualProxyEvaluationSummary",
    "SourceIdentity",
    "evaluate_against_deterministic_hints",
    "rank_residual_candidates",
    "render_proxy_evaluation_report",
    "render_residual_report",
    "score_residual_candidate",
    "summarize_residual_candidates",
]
```

### 80. `identity_engine/residual_matcher.py`

**Purpose:** Explainable residual candidate scorer for stewardship review, intentionally outside the canonical write path.

**Source:**

```python
"""Explainable residual matching for stewardship review.

Phase 5 deliberately keeps this engine out of the automatic canonical-person
path. It ranks possible candidates for HR/data-steward review after the
deterministic dbt matcher has already decided a source row is not safe to merge.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from rapidfuzz import fuzz

HIGH_CONFIDENCE_THRESHOLD = 0.92
POSSIBLE_MATCH_THRESHOLD = 0.75


@dataclass(frozen=True)
class SourceIdentity:
    source_system: str
    source_record_key: str
    source_primary_id: str | None = None
    source_first_name_root: str | None = None
    source_last_name_norm: str | None = None
    source_hire_date: date | None = None
    source_email_local_part: str | None = None
    source_email_domain: str | None = None
    suggested_canonical_person_id: str | None = None
    stewardship_reason: str | None = None

    @classmethod
    def from_mapping(cls, row: dict[str, Any]) -> SourceIdentity:
        return cls(
            source_system=str(row.get("source_system") or ""),
            source_record_key=str(row.get("source_record_key") or ""),
            source_primary_id=_optional_str(row.get("source_primary_id")),
            source_first_name_root=_optional_str(row.get("source_first_name_root")),
            source_last_name_norm=_optional_str(row.get("source_last_name_norm")),
            source_hire_date=_parse_date(row.get("source_hire_date")),
            source_email_local_part=_optional_str(row.get("source_email_local_part")),
            source_email_domain=_optional_str(row.get("source_email_domain")),
            suggested_canonical_person_id=_optional_str(row.get("suggested_canonical_person_id")),
            stewardship_reason=_optional_str(row.get("stewardship_reason")),
        )


@dataclass(frozen=True)
class CanonicalIdentity:
    canonical_person_id: str
    hris_person_key: str | None = None
    first_name_norm: str | None = None
    last_name_norm: str | None = None
    canonical_hire_date: date | None = None
    work_email_local_part: str | None = None
    personal_email_local_part: str | None = None
    current_department: str | None = None
    current_location: str | None = None
    current_employment_type: str | None = None

    @classmethod
    def from_mapping(cls, row: dict[str, Any]) -> CanonicalIdentity:
        return cls(
            canonical_person_id=str(row.get("canonical_person_id") or ""),
            hris_person_key=_optional_str(row.get("hris_person_key")),
            first_name_norm=_optional_str(
                row.get("first_name_norm") or row.get("canonical_legal_first_name")
            ),
            last_name_norm=_optional_str(
                row.get("last_name_norm") or row.get("canonical_legal_last_name")
            ),
            canonical_hire_date=_parse_date(row.get("canonical_hire_date")),
            work_email_local_part=_optional_str(row.get("work_email_local_part")),
            personal_email_local_part=_optional_str(row.get("personal_email_local_part")),
            current_department=_optional_str(row.get("current_department")),
            current_location=_optional_str(row.get("current_location")),
            current_employment_type=_optional_str(row.get("current_employment_type")),
        )


@dataclass(frozen=True)
class ResidualCandidate:
    source_record_key: str
    source_system: str
    canonical_person_id: str
    residual_score: float
    recommendation: str
    positive_anchor_count: int
    evidence_weight: float
    feature_scores: dict[str, float | None] = field(default_factory=dict)
    reasons: tuple[str, ...] = ()

    def as_export_row(self) -> dict[str, Any]:
        return {
            "source_record_key": self.source_record_key,
            "source_system": self.source_system,
            "candidate_canonical_person_id": self.canonical_person_id,
            "residual_score": round(self.residual_score, 4),
            "recommendation": self.recommendation,
            "positive_anchor_count": self.positive_anchor_count,
            "evidence_weight": round(self.evidence_weight, 4),
            "reasons": "; ".join(self.reasons),
            "first_name_root_score": self.feature_scores.get("first_name_root"),
            "last_name_score": self.feature_scores.get("last_name"),
            "email_local_score": self.feature_scores.get("email_local"),
            "hire_date_score": self.feature_scores.get("hire_date"),
            "deterministic_hint_score": self.feature_scores.get("deterministic_hint"),
        }


def score_residual_candidate(
    source: SourceIdentity,
    candidate: CanonicalIdentity,
) -> ResidualCandidate:
    feature_scores = {
        "first_name_root": _exact_score(source.source_first_name_root, candidate.first_name_norm),
        "last_name": _similarity_score(source.source_last_name_norm, candidate.last_name_norm),
        "email_local": _best_email_score(source, candidate),
        "hire_date": _hire_date_score(source.source_hire_date, candidate.canonical_hire_date),
        "deterministic_hint": _deterministic_hint_score(source, candidate),
    }
    weights = {
        "first_name_root": 0.20,
        "last_name": 0.30,
        "email_local": 0.20,
        "hire_date": 0.20,
        "deterministic_hint": 0.10,
    }

    observed_weight = sum(
        weight for key, weight in weights.items() if feature_scores[key] is not None
    )
    weighted_score = sum(
        (feature_scores[key] or 0.0) * weight
        for key, weight in weights.items()
        if feature_scores[key] is not None
    )
    residual_score = weighted_score / observed_weight if observed_weight else 0.0
    positive_anchor_count = sum(
        1 for score in feature_scores.values() if score is not None and score >= 0.85
    )
    evidence_weight = observed_weight / sum(weights.values())
    recommendation = _recommendation(residual_score, evidence_weight, positive_anchor_count)

    return ResidualCandidate(
        source_record_key=source.source_record_key,
        source_system=source.source_system,
        canonical_person_id=candidate.canonical_person_id,
        residual_score=residual_score,
        recommendation=recommendation,
        positive_anchor_count=positive_anchor_count,
        evidence_weight=evidence_weight,
        feature_scores=feature_scores,
        reasons=_reasons(feature_scores, recommendation),
    )


def rank_residual_candidates(
    source_rows: list[SourceIdentity],
    canonical_rows: list[CanonicalIdentity],
    *,
    top_n: int = 3,
    minimum_score: float = POSSIBLE_MATCH_THRESHOLD,
) -> list[ResidualCandidate]:
    canonical_by_id = {row.canonical_person_id: row for row in canonical_rows}
    canonical_by_last_name: dict[str, list[CanonicalIdentity]] = {}
    for row in canonical_rows:
        if row.last_name_norm:
            canonical_by_last_name.setdefault(row.last_name_norm, []).append(row)

    ranked: list[ResidualCandidate] = []
    for source in source_rows:
        pool = _candidate_pool(source, canonical_rows, canonical_by_id, canonical_by_last_name)
        source_candidates = [
            score_residual_candidate(source, candidate)
            for candidate in pool
            if candidate.canonical_person_id
        ]
        source_candidates = [
            candidate
            for candidate in source_candidates
            if candidate.residual_score >= minimum_score
            and candidate.recommendation != "do_not_suggest"
        ]
        ranked.extend(
            sorted(
                source_candidates,
                key=lambda candidate: (
                    candidate.residual_score,
                    candidate.positive_anchor_count,
                    candidate.evidence_weight,
                    candidate.canonical_person_id,
                ),
                reverse=True,
            )[:top_n]
        )
    return ranked


def _candidate_pool(
    source: SourceIdentity,
    canonical_rows: list[CanonicalIdentity],
    canonical_by_id: dict[str, CanonicalIdentity],
    canonical_by_last_name: dict[str, list[CanonicalIdentity]],
) -> list[CanonicalIdentity]:
    pool: dict[str, CanonicalIdentity] = {}
    if (
        source.suggested_canonical_person_id
        and source.suggested_canonical_person_id in canonical_by_id
    ):
        candidate = canonical_by_id[source.suggested_canonical_person_id]
        pool[candidate.canonical_person_id] = candidate

    if source.source_last_name_norm:
        for candidate in canonical_by_last_name.get(source.source_last_name_norm, []):
            pool[candidate.canonical_person_id] = candidate

    if not pool:
        for candidate in canonical_rows:
            pool[candidate.canonical_person_id] = candidate

    return list(pool.values())


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def _exact_score(left: str | None, right: str | None) -> float | None:
    if not left or not right:
        return None
    return 1.0 if left == right else 0.0


def _similarity_score(left: str | None, right: str | None) -> float | None:
    if not left or not right:
        return None
    return fuzz.ratio(left, right) / 100


def _best_email_score(source: SourceIdentity, candidate: CanonicalIdentity) -> float | None:
    if not source.source_email_local_part:
        return None
    candidate_locals = [
        value
        for value in [candidate.work_email_local_part, candidate.personal_email_local_part]
        if value
    ]
    if not candidate_locals:
        return None
    return max(
        fuzz.ratio(source.source_email_local_part, local_part) / 100
        for local_part in candidate_locals
    )


def _hire_date_score(source_date: date | None, candidate_date: date | None) -> float | None:
    if not source_date or not candidate_date:
        return None
    diff_days = abs((source_date - candidate_date).days)
    if diff_days <= 7:
        return 1.0
    if diff_days <= 30:
        return 0.90
    if diff_days <= 90:
        return 0.60
    if diff_days <= 365:
        return 0.25
    return 0.0


def _deterministic_hint_score(
    source: SourceIdentity,
    candidate: CanonicalIdentity,
) -> float | None:
    if not source.suggested_canonical_person_id:
        return None
    return 1.0 if source.suggested_canonical_person_id == candidate.canonical_person_id else 0.0


def _recommendation(
    residual_score: float,
    evidence_weight: float,
    positive_anchor_count: int,
) -> str:
    if (
        residual_score >= HIGH_CONFIDENCE_THRESHOLD
        and evidence_weight >= 0.70
        and positive_anchor_count >= 3
    ):
        return "high_confidence_review"
    if (
        residual_score >= POSSIBLE_MATCH_THRESHOLD
        and evidence_weight >= 0.50
        and positive_anchor_count >= 2
    ):
        return "possible_review"
    return "do_not_suggest"


def _reasons(
    feature_scores: dict[str, float | None],
    recommendation: str,
) -> tuple[str, ...]:
    reasons: list[str] = [f"recommendation={recommendation}"]
    if feature_scores["last_name"] is not None:
        reasons.append(f"last_name_similarity={feature_scores['last_name']:.2f}")
    if feature_scores["first_name_root"] == 1.0:
        reasons.append("first_name_root_exact")
    if feature_scores["email_local"] is not None and feature_scores["email_local"] >= 0.85:
        reasons.append(f"email_local_similarity={feature_scores['email_local']:.2f}")
    if feature_scores["hire_date"] is not None and feature_scores["hire_date"] >= 0.90:
        reasons.append("hire_date_within_30_days")
    if feature_scores["deterministic_hint"] == 1.0:
        reasons.append("deterministic_candidate_hint")
    return tuple(reasons)
```

### 81. `identity_engine/evaluation.py`

**Purpose:** Residual review reporting and optional proxy evaluation helpers.

**Source:**

```python
"""Evaluation and reporting helpers for residual identity review."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field

from identity_engine.residual_matcher import ResidualCandidate, SourceIdentity

FEATURE_COLUMNS = (
    "first_name_root",
    "last_name",
    "email_local",
    "hire_date",
    "deterministic_hint",
)


@dataclass(frozen=True)
class ResidualEvaluationSummary:
    source_record_count: int
    candidate_count: int
    source_records_with_candidates: int
    recommendation_counts: dict[str, int] = field(default_factory=dict)
    source_system_counts: dict[str, int] = field(default_factory=dict)
    mean_score_by_recommendation: dict[str, float] = field(default_factory=dict)
    mean_evidence_by_recommendation: dict[str, float] = field(default_factory=dict)
    feature_coverage: dict[str, float] = field(default_factory=dict)

    @property
    def review_yield_rate(self) -> float:
        if self.source_record_count == 0:
            return 0.0
        return self.source_records_with_candidates / self.source_record_count


@dataclass(frozen=True)
class ResidualProxyEvaluationSummary:
    evaluated_source_count: int
    source_records_with_candidates: int
    top_1_alignment_count: int
    top_k_alignment_count: int
    missing_candidate_count: int
    mean_proxy_label_rank: float
    candidate_counts_by_recommendation: dict[str, int] = field(default_factory=dict)
    proxy_alignment_by_recommendation: dict[str, float] = field(default_factory=dict)

    @property
    def candidate_coverage_rate(self) -> float:
        if self.evaluated_source_count == 0:
            return 0.0
        return self.source_records_with_candidates / self.evaluated_source_count

    @property
    def top_1_alignment_rate(self) -> float:
        if self.evaluated_source_count == 0:
            return 0.0
        return self.top_1_alignment_count / self.evaluated_source_count

    @property
    def top_k_alignment_rate(self) -> float:
        if self.evaluated_source_count == 0:
            return 0.0
        return self.top_k_alignment_count / self.evaluated_source_count

    @property
    def missing_candidate_rate(self) -> float:
        if self.evaluated_source_count == 0:
            return 0.0
        return self.missing_candidate_count / self.evaluated_source_count


def summarize_residual_candidates(
    candidates: list[ResidualCandidate],
    *,
    source_record_count: int | None = None,
) -> ResidualEvaluationSummary:
    unique_sources = {candidate.source_record_key for candidate in candidates}
    denominator = source_record_count if source_record_count is not None else len(unique_sources)

    recommendation_counts = Counter(candidate.recommendation for candidate in candidates)
    source_system_counts = Counter(candidate.source_system for candidate in candidates)

    scores_by_recommendation: dict[str, list[float]] = defaultdict(list)
    evidence_by_recommendation: dict[str, list[float]] = defaultdict(list)
    feature_present_counts = Counter[str]()
    for candidate in candidates:
        scores_by_recommendation[candidate.recommendation].append(candidate.residual_score)
        evidence_by_recommendation[candidate.recommendation].append(candidate.evidence_weight)
        for feature in FEATURE_COLUMNS:
            if candidate.feature_scores.get(feature) is not None:
                feature_present_counts[feature] += 1

    return ResidualEvaluationSummary(
        source_record_count=denominator,
        candidate_count=len(candidates),
        source_records_with_candidates=len(unique_sources),
        recommendation_counts=dict(sorted(recommendation_counts.items())),
        source_system_counts=dict(sorted(source_system_counts.items())),
        mean_score_by_recommendation={
            recommendation: _mean(scores)
            for recommendation, scores in sorted(scores_by_recommendation.items())
        },
        mean_evidence_by_recommendation={
            recommendation: _mean(values)
            for recommendation, values in sorted(evidence_by_recommendation.items())
        },
        feature_coverage={
            feature: (feature_present_counts[feature] / len(candidates) if candidates else 0.0)
            for feature in FEATURE_COLUMNS
        },
    )


def evaluate_against_deterministic_hints(
    source_rows: list[SourceIdentity],
    candidates: list[ResidualCandidate],
) -> ResidualProxyEvaluationSummary:
    """Evaluate candidate ranking against stewardship hints, not true labels.

    `int_stewardship_queue.suggested_canonical_person_id` is the best
    deterministic candidate that failed auto-merge controls. It is useful as a
    weak proxy for walkthrough evaluation, but it is not ground truth and must
    not be used to auto-resolve employee identity.
    """

    proxy_labels: dict[str, str] = {}
    for source in source_rows:
        if source.suggested_canonical_person_id:
            proxy_labels[source.source_record_key] = source.suggested_canonical_person_id
    candidates_by_source: dict[str, list[ResidualCandidate]] = defaultdict(list)
    for candidate in candidates:
        candidates_by_source[candidate.source_record_key].append(candidate)

    sources_with_candidates = 0
    top_1_alignment_count = 0
    top_k_alignment_count = 0
    missing_candidate_count = 0
    proxy_label_ranks: list[int] = []

    for source_record_key, proxy_label in proxy_labels.items():
        source_candidates = candidates_by_source.get(source_record_key, [])
        if not source_candidates:
            missing_candidate_count += 1
            continue

        sources_with_candidates += 1
        if source_candidates[0].canonical_person_id == proxy_label:
            top_1_alignment_count += 1

        for rank, candidate in enumerate(source_candidates, start=1):
            if candidate.canonical_person_id == proxy_label:
                top_k_alignment_count += 1
                proxy_label_ranks.append(rank)
                break

    recommendation_counts = Counter[str]()
    recommendation_alignments = Counter[str]()
    for candidate in candidates:
        proxy_hint = proxy_labels.get(candidate.source_record_key)
        if not proxy_hint:
            continue
        recommendation_counts[candidate.recommendation] += 1
        if candidate.canonical_person_id == proxy_hint:
            recommendation_alignments[candidate.recommendation] += 1

    return ResidualProxyEvaluationSummary(
        evaluated_source_count=len(proxy_labels),
        source_records_with_candidates=sources_with_candidates,
        top_1_alignment_count=top_1_alignment_count,
        top_k_alignment_count=top_k_alignment_count,
        missing_candidate_count=missing_candidate_count,
        mean_proxy_label_rank=_mean([float(rank) for rank in proxy_label_ranks]),
        candidate_counts_by_recommendation=dict(sorted(recommendation_counts.items())),
        proxy_alignment_by_recommendation={
            recommendation: recommendation_alignments[recommendation] / count
            for recommendation, count in sorted(recommendation_counts.items())
            if count
        },
    )


def render_residual_report(
    summary: ResidualEvaluationSummary,
    candidates: list[ResidualCandidate],
    *,
    top_n: int = 10,
) -> str:
    lines = [
        "# Residual Review Report",
        "",
        "This report summarizes review-only residual identity candidates produced",
        "after deterministic dbt matching. These rows are steward-review aids,",
        "not canonical truth and not automatic merges.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Stewardship rows sampled | {summary.source_record_count:,} |",
        f"| Rows with at least one suggested candidate | {summary.source_records_with_candidates:,} |",
        f"| Candidate rows emitted | {summary.candidate_count:,} |",
        f"| Review yield rate | {_pct(summary.review_yield_rate)} |",
        "",
        "## How To Read This Report",
        "",
        "A row in this report means the residual engine found a reviewable",
        "candidate for a source identity that deterministic dbt matching left",
        "in stewardship. It does not mean the candidate is correct, and it does",
        "not change `int_canonical_person`.",
        "",
        "The highest-value operating metric is not raw match rate. It is whether",
        "the queue gives stewards enough ranked evidence to make safer manual",
        "decisions without creating false-positive merges.",
        "",
        "## Recommendation Mix",
        "",
        "| Recommendation | Candidates | Mean score | Mean evidence weight |",
        "|---|---:|---:|---:|",
    ]
    for recommendation, count in summary.recommendation_counts.items():
        lines.append(
            "| "
            f"{recommendation} | "
            f"{count:,} | "
            f"{summary.mean_score_by_recommendation.get(recommendation, 0.0):.3f} | "
            f"{summary.mean_evidence_by_recommendation.get(recommendation, 0.0):.3f} |"
        )

    lines.extend(
        [
            "",
            "## Source-System Mix",
            "",
            "| Source system | Candidates |",
            "|---|---:|",
        ]
    )
    for source_system, count in summary.source_system_counts.items():
        lines.append(f"| {source_system} | {count:,} |")

    lines.extend(
        [
            "",
            "## Feature Coverage",
            "",
            "| Feature | Candidate coverage |",
            "|---|---:|",
        ]
    )
    for feature, coverage in summary.feature_coverage.items():
        lines.append(f"| {feature} | {_pct(coverage)} |")

    lines.extend(
        [
            "",
            "## Top Review Candidates",
            "",
            "| Source record | Candidate canonical person | Recommendation | Score | Anchors | Reasons |",
            "|---|---|---|---:|---:|---|",
        ]
    )
    for candidate in sorted(
        candidates,
        key=lambda item: (
            item.residual_score,
            item.positive_anchor_count,
            item.evidence_weight,
            item.source_record_key,
        ),
        reverse=True,
    )[:top_n]:
        lines.append(
            "| "
            f"{candidate.source_record_key} | "
            f"{candidate.canonical_person_id} | "
            f"{candidate.recommendation} | "
            f"{candidate.residual_score:.3f} | "
            f"{candidate.positive_anchor_count} | "
            f"{_markdown_cell('; '.join(candidate.reasons))} |"
        )

    lines.extend(
        [
            "",
            "## Recommended Stewardship Workflow",
            "",
            "1. Start with `high_confidence_review` rows and verify the evidence",
            "   against authorized source-system context.",
            "2. Use `possible_review` rows to reduce search effort, not to approve",
            "   automatically.",
            "3. Record reviewer, decision, reason, timestamp, and source evidence",
            "   in the future stewardship workflow before any canonical update.",
            "4. Re-run downstream marts only after an approved identity decision is",
            "   applied through a governed write path.",
            "",
            "## Risk Controls",
            "",
            "- No SIN_LAST_4, full email, or DOB is exported by the residual engine.",
            "- Recommendations are outside the canonical-person write path.",
            "- Review suggestions are intentionally biased toward false negatives",
            "  over false positives.",
            "- Low-evidence candidates remain invisible rather than appearing as",
            "  weak suggestions.",
            "",
            "## Control Boundary",
            "",
            "The residual engine is deliberately outside the canonical-person write path.",
            "Any suggested match must still be adjudicated by a human steward before",
            "it can influence canonical employee records or downstream marts.",
            "",
        ]
    )
    return "\n".join(lines)


def render_proxy_evaluation_report(
    summary: ResidualProxyEvaluationSummary,
    *,
    top_n: int,
    minimum_score: float,
) -> str:
    lines = [
        "# Residual Proxy Evaluation",
        "",
        "This optional evaluation checks residual-review candidate ranking against",
        "the stewardship queue's `suggested_canonical_person_id` when that hint",
        "exists.",
        "",
        "This is **not** ground-truth model evaluation. The hint is the best",
        "deterministic candidate that failed auto-merge controls, so it is useful",
        "for walkthrough diagnostics but must not be treated as an approved match.",
        "",
        "## Run Configuration",
        "",
        "| Setting | Value |",
        "|---|---:|",
        f"| Top candidates per source | {top_n:,} |",
        f"| Minimum residual score | {minimum_score:.2f} |",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Source rows with proxy hint | {summary.evaluated_source_count:,} |",
        f"| Proxy-hint rows with at least one candidate | {summary.source_records_with_candidates:,} |",
        f"| Candidate coverage rate | {_pct(summary.candidate_coverage_rate)} |",
        f"| Top-1 proxy alignment count | {summary.top_1_alignment_count:,} |",
        f"| Top-1 proxy alignment rate | {_pct(summary.top_1_alignment_rate)} |",
        f"| Top-{top_n} proxy alignment count | {summary.top_k_alignment_count:,} |",
        f"| Top-{top_n} proxy alignment rate | {_pct(summary.top_k_alignment_rate)} |",
        f"| Missing candidate count | {summary.missing_candidate_count:,} |",
        f"| Missing candidate rate | {_pct(summary.missing_candidate_rate)} |",
        f"| Mean proxy-label rank when found | {summary.mean_proxy_label_rank:.2f} |",
        "",
        "## Alignment By Recommendation",
        "",
        "| Recommendation | Candidate rows | Proxy alignment rate |",
        "|---|---:|---:|",
    ]
    for recommendation, count in summary.candidate_counts_by_recommendation.items():
        lines.append(
            "| "
            f"{recommendation} | "
            f"{count:,} | "
            f"{_pct(summary.proxy_alignment_by_recommendation.get(recommendation, 0.0))} |"
        )

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- High top-1 alignment means the residual scorer tends to put the",
            "  deterministic hint first when it emits a candidate.",
            "- Low coverage means the scorer is preserving the conservative control",
            "  boundary and leaving weak-evidence rows for manual search.",
            "- Alignment below 100% is not automatically bad: the deterministic hint",
            "  itself is not a steward-approved label.",
            "- This report should guide threshold tuning and reviewer workload",
            "  planning, not canonical identity updates.",
            "",
            "## Control Boundary",
            "",
            "Proxy evaluation is a diagnostic artifact. It does not approve matches,",
            "write `int_canonical_person`, or change downstream People Analytics",
            "marts.",
            "",
        ]
    )
    return "\n".join(lines)


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _pct(value: float) -> str:
    return f"{value:.1%}"


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|")
```

### 82. `identity_engine/snowflake_io.py`

**Purpose:** Queries and CSV export helpers for residual review inputs/outputs.

**Source:**

```python
"""Snowflake IO for Phase 5 residual-review candidate exports."""

from __future__ import annotations

from typing import Any

from api.settings import AtlasSettings, normalize_snowflake_identifier
from api.snowflake_client import AtlasSnowflakeClient
from identity_engine.residual_matcher import CanonicalIdentity, SourceIdentity


def intermediate_schema(settings: AtlasSettings) -> str:
    schema = f"{settings.dbt_schema}_INTERMEDIATE"
    return normalize_snowflake_identifier(schema, "intermediate_schema")


def intermediate_table(settings: AtlasSettings, table_name: str) -> str:
    return (
        f"{settings.database_identifier}."
        f"{intermediate_schema(settings)}."
        f"{normalize_snowflake_identifier(table_name, 'table_name')}"
    )


def build_stewardship_query(settings: AtlasSettings, *, limit: int) -> str:
    table = intermediate_table(settings, "int_stewardship_queue")
    return f"""
select
    source_system,
    source_record_key,
    source_primary_id,
    source_first_name_root,
    source_last_name_norm,
    source_hire_date,
    source_email_local_part,
    source_email_domain,
    suggested_canonical_person_id,
    stewardship_reason
from {table}
order by source_system, source_record_key
limit {max(1, limit)}
""".strip()


def build_canonical_query(settings: AtlasSettings) -> str:
    table = intermediate_table(settings, "int_canonical_person")
    return f"""
select
    canonical_person_id,
    hris_person_key,
    canonical_legal_first_name,
    canonical_legal_last_name,
    canonical_hire_date,
    work_email_local_part,
    personal_email_local_part,
    current_department,
    current_location,
    current_employment_type
from {table}
""".strip()


def load_residual_inputs(
    settings: AtlasSettings,
    *,
    limit: int,
) -> tuple[list[SourceIdentity], list[CanonicalIdentity]]:
    client = AtlasSnowflakeClient(settings)
    stewardship_rows = client.fetch_all(build_stewardship_query(settings, limit=limit))
    canonical_rows = client.fetch_all(build_canonical_query(settings))
    return (
        [SourceIdentity.from_mapping(row) for row in stewardship_rows],
        [CanonicalIdentity.from_mapping(row) for row in canonical_rows],
    )


def export_rows_to_csv(rows: list[dict[str, Any]], output_path: str) -> None:
    import csv

    fieldnames = [
        "source_record_key",
        "source_system",
        "candidate_canonical_person_id",
        "residual_score",
        "recommendation",
        "positive_anchor_count",
        "evidence_weight",
        "reasons",
        "first_name_root_score",
        "last_name_score",
        "email_local_score",
        "hire_date_score",
        "deterministic_hint_score",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
```

### 83. `identity_engine/cli.py`

**Purpose:** CLI commands for residual candidates, residual reports, and proxy evaluation reports.

**Source:**

```python
"""Command line interface for Atlas residual identity review."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from api.settings import AtlasSettings
from identity_engine.evaluation import (
    evaluate_against_deterministic_hints,
    render_proxy_evaluation_report,
    render_residual_report,
    summarize_residual_candidates,
)
from identity_engine.residual_matcher import (
    ResidualCandidate,
    SourceIdentity,
    rank_residual_candidates,
)
from identity_engine.snowflake_io import export_rows_to_csv, load_residual_inputs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Atlas identity-engine utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    residual = subparsers.add_parser(
        "residual-candidates",
        help="Export review-only residual candidate matches from the stewardship queue.",
    )
    residual.add_argument("--limit", type=int, default=500)
    residual.add_argument("--top-n", type=int, default=3)
    residual.add_argument("--minimum-score", type=float, default=0.75)
    residual.add_argument("--output", type=Path)
    residual.set_defaults(func=run_residual_candidates)

    report = subparsers.add_parser(
        "residual-report",
        help="Render a markdown summary of residual candidate review coverage.",
    )
    report.add_argument("--limit", type=int, default=500)
    report.add_argument("--top-n", type=int, default=3)
    report.add_argument("--minimum-score", type=float, default=0.75)
    report.add_argument("--output", type=Path)
    report.add_argument("--top-candidates", type=int, default=10)
    report.set_defaults(func=run_residual_report)

    evaluate = subparsers.add_parser(
        "residual-evaluate",
        help="Render optional proxy evaluation against stewardship deterministic hints.",
    )
    evaluate.add_argument("--limit", type=int, default=500)
    evaluate.add_argument("--top-n", type=int, default=3)
    evaluate.add_argument("--minimum-score", type=float, default=0.75)
    evaluate.add_argument("--output", type=Path)
    evaluate.set_defaults(func=run_residual_evaluate)
    return parser


def run_residual_candidates(args: argparse.Namespace) -> int:
    _source_rows, candidates = _rank_candidates(args)
    export_rows = [candidate.as_export_row() for candidate in candidates]

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        export_rows_to_csv(export_rows, str(args.output))
        print(f"Wrote {len(export_rows)} residual review candidates to {args.output}")
    else:
        print(json.dumps(export_rows, indent=2, sort_keys=True))

    return 0


def run_residual_report(args: argparse.Namespace) -> int:
    source_rows, candidates = _rank_candidates(args)
    summary = summarize_residual_candidates(candidates, source_record_count=len(source_rows))
    report = render_residual_report(summary, candidates, top_n=args.top_candidates)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
        print(f"Wrote residual review report to {args.output}")
    else:
        print(report)

    return 0


def run_residual_evaluate(args: argparse.Namespace) -> int:
    source_rows, candidates = _rank_candidates(args)
    summary = evaluate_against_deterministic_hints(source_rows, candidates)
    report = render_proxy_evaluation_report(
        summary,
        top_n=args.top_n,
        minimum_score=args.minimum_score,
    )

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
        print(f"Wrote residual proxy evaluation to {args.output}")
    else:
        print(report)

    return 0


def _rank_candidates(
    args: argparse.Namespace,
) -> tuple[list[SourceIdentity], list[ResidualCandidate]]:
    settings = AtlasSettings.from_env()
    source_rows, canonical_rows = load_residual_inputs(settings, limit=args.limit)
    candidates = rank_residual_candidates(
        source_rows,
        canonical_rows,
        top_n=args.top_n,
        minimum_score=args.minimum_score,
    )
    return source_rows, candidates


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    func: Any = args.func
    return int(func(args))


if __name__ == "__main__":
    raise SystemExit(main())
```

### 84. `tests/test_phase4_api.py`

**Purpose:** Python unit test file. It validates behavior without needing the full Snowflake/dbt runtime.

**Source:**

```python
from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from api.metrics_service import (
    MetricFilters,
    build_audit_insert,
    build_headcount_query,
    headcount_daily,
    root,
)
from api.settings import AtlasSettings


def test_public_table_rejects_injected_schema() -> None:
    settings = AtlasSettings(
        snowflake_account="acct",
        snowflake_user="user",
        snowflake_password="password",
        snowflake_role="ATLAS_DEVELOPER",
        snowflake_warehouse="ATLAS_WH",
        snowflake_database="ATLAS",
        snowflake_schema="RAW",
        snowflake_region=None,
        dbt_schema="DBT_DEV",
        people_analytics_schema="DBT_DEV_PEOPLE_ANALYTICS;DROP_TABLE",
        api_host="127.0.0.1",
        api_port=8000,
        k_anonymity_min=5,
    )

    with pytest.raises(ValueError, match="simple Snowflake identifier"):
        settings.public_table("workforce_headcount_daily")


def test_root_exposes_safe_service_index() -> None:
    body = root()

    assert body["health"] == "/health"
    assert body["docs"] == "/docs"
    assert body["metrics"]["daily_headcount"] == "/headcount/daily"
    assert "k-anonymous" in body["privacy_note"]


def test_headcount_query_uses_privacy_mart_and_bound_filters() -> None:
    settings = _settings()
    filters = MetricFilters(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
        department="Engineering",
        location="Toronto",
        employment_type="FTE",
        limit=50,
    )

    statement = build_headcount_query(settings, filters)

    assert "ATLAS.DBT_DEV_PEOPLE_ANALYTICS.WORKFORCE_HEADCOUNT_DAILY" in statement.sql
    assert "canonical_person_id" not in statement.sql.lower()
    assert "sin_last_4" not in statement.sql.lower()
    assert statement.params == (
        date(2026, 1, 1),
        date(2026, 1, 31),
        "Engineering",
        "Toronto",
        "FTE",
    )
    assert "limit 50" in statement.sql.lower()


def test_audit_insert_targets_audit_log_with_json_filters() -> None:
    settings = _settings()
    statement = build_audit_insert(
        settings,
        actor="demo_hrbp",
        query_surface="workforce_headcount_daily",
        purpose="dashboard_view",
        filters={"department": "Engineering"},
        result_row_count=10,
        suppressed_row_count=2,
    )

    assert "ATLAS.DBT_DEV_PEOPLE_ANALYTICS.PRIVACY_AUDIT_LOG" in statement.sql
    assert "try_parse_json(%s)" in statement.sql
    assert statement.params[0:3] == (
        "demo_hrbp",
        "workforce_headcount_daily",
        "dashboard_view",
    )
    assert statement.params[3] == '{"department": "Engineering"}'
    assert statement.params[5:7] == (10, 2)


def test_headcount_endpoint_returns_data_and_writes_audit() -> None:
    fake_client = FakeMetricsClient(
        rows=[
            {
                "snapshot_date": "2026-01-01",
                "department": "Engineering",
                "location": "Toronto",
                "employment_type": "FTE",
                "headcount": 12,
                "reportable_cohort_employee_count": 12,
                "cohort_size_bucket": "12",
                "is_reportable": True,
                "suppression_reason": None,
                "k_anonymity_threshold": 5,
            },
            {
                "snapshot_date": "2026-01-01",
                "department": "People",
                "location": "Toronto",
                "employment_type": "FTE",
                "headcount": None,
                "reportable_cohort_employee_count": None,
                "cohort_size_bucket": "<5",
                "is_reportable": False,
                "suppression_reason": "K_ANONYMITY_THRESHOLD",
                "k_anonymity_threshold": 5,
            },
        ],
    )
    body = headcount_daily(
        client=fake_client,
        settings=_settings(),
        department="Engineering",
        limit=1000,
        purpose="dashboard_view",
        actor="demo_hrbp",
    )

    assert body["row_count"] == 2
    assert body["suppressed_row_count"] == 1
    assert body["audit_logged"] is True
    assert len(fake_client.executed) == 1
    assert fake_client.executed[0][1][0:3] == (
        "demo_hrbp",
        "workforce_headcount_daily",
        "dashboard_view",
    )


class FakeMetricsClient:
    def __init__(self, rows: list[dict[str, Any]]):
        self.rows = rows
        self.queries: list[tuple[str, tuple[Any, ...]]] = []
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        self.queries.append((sql, params))
        return self.rows

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        self.executed.append((sql, params))


def _settings() -> AtlasSettings:
    return AtlasSettings(
        snowflake_account="acct",
        snowflake_user="user",
        snowflake_password="password",
        snowflake_role="ATLAS_DEVELOPER",
        snowflake_warehouse="ATLAS_WH",
        snowflake_database="ATLAS",
        snowflake_schema="RAW",
        snowflake_region=None,
        dbt_schema="DBT_DEV",
        people_analytics_schema="DBT_DEV_PEOPLE_ANALYTICS",
        api_host="127.0.0.1",
        api_port=8000,
        k_anonymity_min=5,
    )
```

### 85. `tests/test_phase4_dag_dashboard.py`

**Purpose:** Python unit test file. It validates behavior without needing the full Snowflake/dbt runtime.

**Source:**

```python
from __future__ import annotations

import importlib.util
from pathlib import Path

from dashboard.app import records_frame, reportable_row_count, suppression_rate


def test_airflow_dag_module_imports_without_airflow_installed() -> None:
    dag_path = Path("airflow/dags/atlas_people_analytics.py")
    spec = importlib.util.spec_from_file_location("atlas_people_analytics_dag", dag_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert hasattr(module, "dag")
    if module.dag is not None:
        assert module.dag.dag_id == "atlas_people_analytics"
        assert {task.task_id for task in module.dag.tasks} == {
            "dbt_deps",
            "build_staging",
            "build_identity_resolution",
            "build_core_marts",
            "build_privacy_marts",
        }


def test_dashboard_records_frame_uses_api_data_key() -> None:
    frame = records_frame({"data": [{"department": "Engineering", "headcount": 12}]})

    assert list(frame.columns) == ["department", "headcount"]
    assert frame.iloc[0].to_dict() == {"department": "Engineering", "headcount": 12}


def test_dashboard_suppression_rate_handles_empty_payload() -> None:
    assert suppression_rate({"row_count": 0, "suppressed_row_count": 10}) == 0.0
    assert suppression_rate({"row_count": 10, "suppressed_row_count": 2}) == 0.2


def test_dashboard_reportable_row_count_ignores_suppressed_metrics() -> None:
    frame = records_frame(
        {
            "data": [
                {"department": "Engineering", "headcount": 12},
                {"department": "People", "headcount": None},
                {"department": "Finance", "headcount": 8},
            ]
        }
    )

    assert reportable_row_count(frame, "headcount") == 2
    assert reportable_row_count(frame, "attrition_rate") == 0
```

### 86. `tests/test_phase5_residual_matcher.py`

**Purpose:** Python unit test file. It validates behavior without needing the full Snowflake/dbt runtime.

**Source:**

```python
from __future__ import annotations

from datetime import date

from api.settings import AtlasSettings
from identity_engine.evaluation import (
    evaluate_against_deterministic_hints,
    render_proxy_evaluation_report,
    render_residual_report,
    summarize_residual_candidates,
)
from identity_engine.residual_matcher import (
    CanonicalIdentity,
    ResidualCandidate,
    SourceIdentity,
    rank_residual_candidates,
    score_residual_candidate,
)
from identity_engine.snowflake_io import build_canonical_query, build_stewardship_query


def test_residual_candidate_can_be_ranked_for_review_without_auto_merge() -> None:
    source = SourceIdentity(
        source_system="CRM",
        source_record_key="CRM::123",
        source_first_name_root="robert",
        source_last_name_norm="smith",
        source_hire_date=date(2024, 1, 8),
        source_email_local_part="rsmith",
        suggested_canonical_person_id="cp_abc",
    )
    candidate = CanonicalIdentity(
        canonical_person_id="cp_abc",
        first_name_norm="robert",
        last_name_norm="smith",
        canonical_hire_date=date(2024, 1, 1),
        work_email_local_part="robert.smith",
        personal_email_local_part="rsmith",
    )

    scored = score_residual_candidate(source, candidate)

    assert scored.recommendation == "high_confidence_review"
    assert scored.residual_score >= 0.92
    assert scored.as_export_row()["candidate_canonical_person_id"] == "cp_abc"
    assert "recommendation=high_confidence_review" in scored.reasons


def test_sparse_or_conflicting_evidence_is_not_suggested() -> None:
    source = SourceIdentity(
        source_system="PAYROLL",
        source_record_key="PAYROLL::999",
        source_first_name_root="ana",
        source_last_name_norm="lopez",
    )
    candidate = CanonicalIdentity(
        canonical_person_id="cp_xyz",
        first_name_norm="charles",
        last_name_norm="nguyen",
    )

    scored = score_residual_candidate(source, candidate)

    assert scored.recommendation == "do_not_suggest"
    assert scored.residual_score < 0.75


def test_rank_residual_candidates_keeps_top_reviewable_candidates() -> None:
    source = SourceIdentity(
        source_system="DMS_ERP",
        source_record_key="DMS_ERP::1",
        source_first_name_root="patrick",
        source_last_name_norm="obrien",
        source_hire_date=date(2025, 6, 1),
        source_email_local_part="pobrien",
    )
    candidates = [
        CanonicalIdentity(
            canonical_person_id="cp_good",
            first_name_norm="patrick",
            last_name_norm="obrien",
            canonical_hire_date=date(2025, 6, 5),
            work_email_local_part="pobrien",
        ),
        CanonicalIdentity(
            canonical_person_id="cp_bad",
            first_name_norm="maria",
            last_name_norm="obrien",
            canonical_hire_date=date(2021, 1, 1),
            work_email_local_part="mobrien",
        ),
    ]

    ranked = rank_residual_candidates([source], candidates, top_n=1)

    assert len(ranked) == 1
    assert ranked[0].canonical_person_id == "cp_good"
    assert ranked[0].recommendation in {"high_confidence_review", "possible_review"}


def test_phase5_queries_do_not_select_sensitive_identifiers() -> None:
    settings = _settings()
    sql = (
        f"{build_stewardship_query(settings, limit=25)}\n{build_canonical_query(settings)}".lower()
    )

    assert "sin_last_4" not in sql
    assert "date_of_birth" not in sql
    assert "personal_email," not in sql
    assert "work_email," not in sql


def test_residual_report_summarizes_review_coverage() -> None:
    candidates = [
        ResidualCandidate(
            source_record_key="ATS::1",
            source_system="ATS",
            canonical_person_id="cp_1",
            residual_score=0.96,
            recommendation="high_confidence_review",
            positive_anchor_count=4,
            evidence_weight=1.0,
            feature_scores={
                "first_name_root": 1.0,
                "last_name": 1.0,
                "email_local": 0.9,
                "hire_date": 1.0,
                "deterministic_hint": 1.0,
            },
            reasons=("recommendation=high_confidence_review", "first_name_root_exact"),
        ),
        ResidualCandidate(
            source_record_key="CRM::2",
            source_system="CRM",
            canonical_person_id="cp_2",
            residual_score=0.78,
            recommendation="possible_review",
            positive_anchor_count=2,
            evidence_weight=0.7,
            feature_scores={
                "first_name_root": 1.0,
                "last_name": 0.8,
                "email_local": None,
                "hire_date": 0.9,
                "deterministic_hint": None,
            },
            reasons=("recommendation=possible_review", "hire_date_within_30_days"),
        ),
    ]

    summary = summarize_residual_candidates(candidates, source_record_count=5)
    report = render_residual_report(summary, candidates, top_n=1)

    assert summary.candidate_count == 2
    assert summary.source_records_with_candidates == 2
    assert summary.review_yield_rate == 0.4
    assert summary.recommendation_counts["high_confidence_review"] == 1
    assert "## Recommendation Mix" in report
    assert "ATS::1" in report
    assert "CRM::2" not in report


def test_proxy_evaluation_uses_stewardship_hints_without_claiming_ground_truth() -> None:
    source_rows = [
        SourceIdentity(
            source_system="ATS",
            source_record_key="ATS::1",
            suggested_canonical_person_id="cp_1",
        ),
        SourceIdentity(
            source_system="CRM",
            source_record_key="CRM::2",
            suggested_canonical_person_id="cp_2",
        ),
        SourceIdentity(
            source_system="PAYROLL",
            source_record_key="PAYROLL::3",
        ),
    ]
    candidates = [
        ResidualCandidate(
            source_record_key="ATS::1",
            source_system="ATS",
            canonical_person_id="cp_1",
            residual_score=0.96,
            recommendation="high_confidence_review",
            positive_anchor_count=4,
            evidence_weight=1.0,
        ),
        ResidualCandidate(
            source_record_key="CRM::2",
            source_system="CRM",
            canonical_person_id="cp_wrong",
            residual_score=0.90,
            recommendation="possible_review",
            positive_anchor_count=3,
            evidence_weight=0.8,
        ),
        ResidualCandidate(
            source_record_key="CRM::2",
            source_system="CRM",
            canonical_person_id="cp_2",
            residual_score=0.86,
            recommendation="possible_review",
            positive_anchor_count=3,
            evidence_weight=0.8,
        ),
    ]

    summary = evaluate_against_deterministic_hints(source_rows, candidates)
    report = render_proxy_evaluation_report(summary, top_n=2, minimum_score=0.75)

    assert summary.evaluated_source_count == 2
    assert summary.source_records_with_candidates == 2
    assert summary.top_1_alignment_count == 1
    assert summary.top_k_alignment_count == 2
    assert summary.top_1_alignment_rate == 0.5
    assert summary.top_k_alignment_rate == 1.0
    assert summary.mean_proxy_label_rank == 1.5
    assert summary.candidate_counts_by_recommendation["possible_review"] == 2
    assert summary.proxy_alignment_by_recommendation["possible_review"] == 0.5
    assert "ground-truth model evaluation" in report
    assert "does not approve matches" in report


def _settings() -> AtlasSettings:
    return AtlasSettings(
        snowflake_account="acct",
        snowflake_user="user",
        snowflake_password="password",
        snowflake_role="ATLAS_DEVELOPER",
        snowflake_warehouse="ATLAS_WH",
        snowflake_database="ATLAS",
        snowflake_schema="RAW",
        snowflake_region=None,
        dbt_schema="DBT_DEV",
        people_analytics_schema="DBT_DEV_PEOPLE_ANALYTICS",
        api_host="127.0.0.1",
        api_port=8000,
        k_anonymity_min=5,
    )
```
