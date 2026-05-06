# Edward Demo Talk Track

This is the concise version of the Atlas walkthrough for a 30-minute
hiring-manager conversation. It is designed to show senior judgment quickly:
problem framing, architecture, tradeoffs, and how the work would translate to a
real People Analytics function.

## Opening

"I built Atlas because the role description points to a very specific hard
problem: creating a trusted employee record and point-in-time people metrics
from fragmented HR operating systems. I used synthetic data only, but I modeled
the kinds of drift that break real reporting: rehires, name changes,
contractor-to-FTE conversions, preferred names, lagging systems, and mismatched
IDs."

"The project is meant to show how I would think as the person responsible for
the People Analytics foundation, not just as someone building a report."

## Five-Minute Walkthrough

1. **Start with the business problem**

   "Every system can be internally correct while cross-system reporting is
   wrong. If HRIS, ATS, payroll, CRM, and other systems represent the same
   employee differently, headcount, attrition, tenure, compensation, and
   performance attribution can drift silently."

2. **Show the source-to-mart architecture**

   Point to the README architecture diagram or dbt layers:

   - raw synthetic source systems
   - dbt staging models
   - intermediate identity resolution
   - core SCD2 employee dimension and daily workforce fact
   - privacy-safe people analytics marts
   - API and dashboard consumption

3. **Explain the identity matcher**

   "The matcher runs deterministic passes from safest evidence to weaker
   evidence: hard anchors first, normalized name plus DOB and hire-date
   proximity second, then email-domain and last-name recovery third. Anything
   that does not meet the threshold goes to stewardship."

   Key tradeoff:

   "I intentionally prefer false negatives over false positives. In people data,
   an incorrect merge is usually worse than a missed merge because it can pollute
   many metrics downstream."

4. **Explain point-in-time correctness**

   "Once the identity spine exists, `dim_employee` and
   `fct_workforce_daily` let the warehouse answer what was true on a date. That
   is the difference between a current-state employee table and a reliable
   workforce analytics foundation."

5. **Explain privacy and governance**

   "The public people analytics marts suppress exact metrics for small cohorts.
   The API only reads from those marts and writes audit events. Sensitive fields
   such as full DOB and SIN fragments do not propagate into the business-facing
   layer."

6. **Show the operating layer**

   "Airflow gives the refresh order, FastAPI gives controlled metric access, and
   Streamlit gives an HRBP-facing view. The dashboard is intentionally not the
   whole project. It is the surface over a governed data product."

7. **Close with residual review**

   "Phase 5 adds a review assistant for unresolved cases. It ranks likely
   candidates, but it never writes canonical identity. A steward remains the
   decision point."

## If Edward Asks: Why Not ML First?

"Because the highest-risk failure mode in employee identity is a false positive
merge. I would start with deterministic, auditable rules so the organization can
trust why two records were linked. ML or similarity scoring belongs in the
residual review workflow first, where it can reduce manual effort without
becoming an uncontrolled source of truth."

## If Edward Asks: What Would You Do First At Wealthsimple?

"I would start with discovery and source contracts, not dashboards. I would map
the systems that create or modify employee identity, identify the authoritative
fields by use case, document the metric definitions HR and Finance need to
agree on, and build the first version of the employee spine with tests around
the highest-risk joins. After that I would prioritize the first governed metric
surface based on the decisions the team needs to support."

## If Edward Asks: What Was The Hardest Tradeoff?

"The hardest tradeoff is match coverage versus trust. It is tempting to make the
matcher more aggressive so the numbers look cleaner, but in People Analytics a
bad merge is costly. I built the queue so the system is honest about uncertainty
instead of hiding it."

## If Edward Asks: How Would You Validate Impact?

"I would validate it in three layers. First, technical validation: tests for
identity stability, SCD2 continuity, point-in-time counts, and privacy
suppression. Second, reconciliation: compare governed metrics to HRIS, Finance,
and People Ops control totals and explain the differences. Third, stakeholder
validation: confirm that HRBPs and People leaders can answer the decisions they
care about without creating side spreadsheets."

## Questions To Ask Edward

- "What people decisions most need better data right now: headcount planning,
  attrition, hiring funnel, compensation, performance, manager effectiveness, or
  something else?"
- "Where does employee identity currently break across systems?"
- "Which system is treated as authoritative for employee status, manager,
  department, location, and employment type?"
- "How do HR and Finance currently reconcile headcount?"
- "What would make the first six months of this role obviously successful?"
- "How self-serve should People Analytics become, and where do you want stronger
  controls or review?"

## Closing Line

"The reason I built Atlas this way is that a People Analytics function earns
trust by getting the invisible foundation right: identity, history, definitions,
privacy, and operational reliability. The dashboard matters, but only after the
spine underneath it is trustworthy."
