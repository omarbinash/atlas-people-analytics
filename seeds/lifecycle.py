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
