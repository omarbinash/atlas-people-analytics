"""
Atlas synthetic data generator.

Generates a realistic, time-evolved population of synthetic employees and
projects them into six operational source systems with deliberate name and
identity drift - exactly the kind of mess a real People Analytics function
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
# Constants - calibrated to roughly match the 401-style dealership population
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
    ("REM", "Remote - Canada"),
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
            # Rehire creates a NEW HRIS_EMPLOYEE_ID - this is realistic and
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

    # If no termination event was emitted, the person is still active -
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
