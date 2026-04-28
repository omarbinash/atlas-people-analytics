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
