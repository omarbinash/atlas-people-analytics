# SQL Answer Walkthroughs

This document explains how to talk through the SQL prompts in
`sql-practice.md`. The point is not only to produce correct SQL, but to narrate
the business rule, the grain, and the failure mode the query avoids.

## 1. Point-In-Time Headcount

What to say:

"I start from the daily workforce fact because headcount is a point-in-time
question. I filter to one `snapshot_date` and `is_active_on_date`, then count
distinct canonical people by department."

Why it matters:

Using a current employee table would answer "who is active now," not "who was
active on the reporting date."

Common mistake:

Counting HRIS employee IDs can over-count rehires if the business question is
person-level headcount.

## 2. Monthly Attrition Rate

What to say:

"I define the denominator first. Here I use month-start workforce, then count
termination events during the month and divide by that starting population."

Why it matters:

Attrition rates become unstable when the denominator is not explicit.

Common mistake:

Using average monthly headcount without saying so, or dividing by end-of-month
headcount after terminations have already happened.

## 3. Current Employee Row From SCD2

What to say:

"The dimension has versions, so I need to rank rows within the employee spell
and keep the latest effective version."

Why it matters:

SCD2 tables answer both current-state and historical questions, but only when
the query chooses the right version.

Common mistake:

Filtering on a current flag without checking whether the model actually
guarantees one current row per grain.

## 4. Broken SCD2 Contiguity

What to say:

"I use `lead()` to compare each version to the next version in the same employee
spell. If the current `effective_to` reaches or passes the next
`effective_from`, the versions overlap."

Why it matters:

Overlaps create double-counting and ambiguous point-in-time joins.

Common mistake:

Checking only for duplicate keys and missing temporal overlap bugs.

## 5. Rehire Detection

What to say:

"I group by canonical person and count distinct HRIS employment IDs. More than
one HRIS ID for one canonical person is expected for rehire or conversion
patterns."

Why it matters:

The point of the canonical ID is to survive source-system ID changes.

Common mistake:

Treating a new HRIS employee ID as a new person without checking identity
anchors.

## 6. Stewardship Queue Coverage

What to say:

"I summarize unresolved records by source and reason because the queue is an
operational control, not just an exception table."

Why it matters:

High or changing unresolved volume can indicate source drift, missing anchors,
or a matching-rule regression.

Common mistake:

Optimizing only for match rate and hiding uncertainty.

## 7. Privacy-Safe Headcount

What to say:

"I aggregate first, then suppress exact counts when the cohort size is below k.
The public surface should expose the cohort existence and suppression status,
but not the small exact count."

Why it matters:

Privacy should be enforced in the mart or service layer, not only in a chart.

Common mistake:

Filtering small cohorts out entirely, which hides that suppression happened and
makes auditability weaker.

## 8. Match Candidate Ranking

What to say:

"I use a deterministic ranking rule inside each source record. The order includes
score, evidence weight, and a final stable ID tie-breaker so repeated runs return
the same candidate."

Why it matters:

Review workflows need stable ordering. Non-deterministic ties reduce trust.

Common mistake:

Ranking by score only when multiple candidates can have the same score.

## 9. HR And Finance Reconciliation

What to say:

"I compute Atlas' control total at the same reporting grain as Finance, join to
the Finance control table, and surface variance rather than hiding differences."

Why it matters:

People Analytics often fails at the definition boundary between HR and Finance.

Common mistake:

Treating reconciliation differences as SQL bugs before checking definition
differences like contractors, leave status, or effective dates.

## 10. Duplicate Hard Anchor Detection

What to say:

"This is an invariant check. If the same DOB and work-email local part resolve
to multiple canonical people, the identity spine is unstable."

Why it matters:

The hardest bugs in canonical identity are not row-count bugs. They are
stability and uniqueness violations.

Common mistake:

Testing only for nulls and uniqueness on final IDs, but not testing whether
strong anchors split across multiple canonical IDs.

## General Interview Pattern

For each SQL answer, use this order:

1. State the business question.
2. Name the grain.
3. Name the source model.
4. Explain the join/filter/window function.
5. Say what failure mode the query avoids.

Example:

"Headcount is a point-in-time metric at date x department grain, so I use the
daily workforce fact, filter to active rows on the reporting date, and count
distinct canonical people. That avoids using current-state rows for historical
questions."
