# Two-Minute Demo Script

Use this when the interview clock is tight. The goal is to communicate the
project's senior-level judgment before showing details.

## Script

"Atlas is a synthetic People Analytics project I built around the hardest part
of the role description: creating a trusted employee record across fragmented HR
systems."

"The problem is that HRIS, ATS, payroll, CRM, DMS, and ERP can each represent
the same person differently. One system has legal name, another has preferred
name, another has a shortened name, and rehires or contractor-to-FTE conversions
can create new IDs. If that identity layer is wrong, headcount, attrition,
tenure, compensation, and performance reporting all become quietly wrong."

"So I built the foundation first. Synthetic source systems land in raw, dbt
staging normalizes them, and the intermediate layer runs deterministic identity
matching. The matcher starts with hard anchors, then normalized name plus DOB
and hire-date proximity, then weaker email-domain recovery. Anything uncertain
goes to a stewardship queue."

"The key tradeoff is conservative matching. In people data, a false positive
merge is usually worse than a false negative. A missed match creates review
work; a bad merge can pollute downstream metrics and decision-making."

"On top of that identity spine, Atlas builds an SCD2 employee dimension and a
daily workforce fact, so headcount and attrition are point-in-time correct. Then
the people analytics marts apply k-anonymity so small cohorts do not expose
exact counts. The API and Streamlit dashboard only read from those privacy-safe
marts and write audit events."

"The dashboard is not the main deliverable. It is one consumer of the governed
foundation. The real proof is the spine underneath it: identity, history,
definitions, privacy, tests, and operational refresh."

"If I were applying this at Wealthsimple, I would start by mapping source
ownership and metric definitions with HR, Finance, and People Ops, then build
the employee spine and reconcile it before expanding self-serve reporting."

## One-Sentence Close

"Atlas demonstrates how I would build People Analytics from the foundation up:
trusted employee identity first, point-in-time metrics second, stakeholder
surfaces third."
