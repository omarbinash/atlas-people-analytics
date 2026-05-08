# Phase 5B Residual Review Walkthrough

Phase 5B turns the Phase 5A residual matcher into a richer stewardship package:
candidate exports, a review report, optional proxy evaluation, and a clear
operating path for manual adjudication.

This is still synthetic data only. The residual engine is review-only and does
not update canonical employee records.

## Why Phase 5B Exists

The Phase 2C deterministic matcher is intentionally conservative. It auto-merges
only when evidence is strong enough to survive review. That leaves some source
records in `int_stewardship_queue`.

Phase 5B answers a different question:

> Can we reduce manual review effort without weakening the canonical-person
> control boundary?

The answer is a review-assist workflow:

1. Load unresolved source identities from `int_stewardship_queue`.
2. Load canonical people from `int_canonical_person`.
3. Score candidate canonical people with explainable features.
4. Emit only `high_confidence_review` or `possible_review` recommendations.
5. Keep all suggestions outside the canonical write path.
6. Optionally evaluate ranking behavior against deterministic hints for
   diagnostics, not approval.

## Inputs

| Input | Purpose | Sensitive-field stance |
| --- | --- | --- |
| `int_stewardship_queue` | Source identities requiring review | Excludes SIN_LAST_4, full email, and DOB |
| `int_canonical_person` | Candidate canonical people | Phase 5 export path excludes full DOB and full email |
| `suggested_canonical_person_id` | Weak deterministic hint for diagnostics | Not treated as ground truth |

## Features

| Feature | Meaning | Why it is safe enough for review assistance |
| --- | --- | --- |
| `first_name_root` | Exact match on normalized first-name root | Handles nickname normalization without exposing legal documents |
| `last_name` | Fuzzy similarity on normalized last name | Useful but never sufficient alone |
| `email_local` | Similarity on email local part only | Avoids exporting full email address |
| `hire_date` | Proximity between source and canonical hire dates | Good lifecycle anchor when close |
| `deterministic_hint` | Whether the candidate matches the queue's best dbt hint | Useful for ranking, not a label |

## Recommendation Meaning

| Recommendation | Steward action | Automation boundary |
| --- | --- | --- |
| `high_confidence_review` | Review first; likely fastest to adjudicate | Still requires human approval |
| `possible_review` | Use as search assistance | Never approve automatically |
| `do_not_suggest` | Suppressed from export | No candidate shown |

## Running The Package

Export candidate rows:

```bash
python -m identity_engine.cli residual-candidates \
  --limit 500 \
  --top-n 3 \
  --minimum-score 0.75 \
  --output identity_engine/output/residual_candidates.csv
```

Render the review report:

```bash
python -m identity_engine.cli residual-report \
  --limit 500 \
  --top-n 3 \
  --minimum-score 0.75 \
  --top-candidates 12 \
  --output docs/walkthroughs/residual-review-report.md
```

Render optional proxy evaluation:

```bash
python -m identity_engine.cli residual-evaluate \
  --limit 500 \
  --top-n 3 \
  --minimum-score 0.75 \
  --output docs/walkthroughs/residual-ranking-evaluation.md
```

## Stewardship Workflow

1. Start with `high_confidence_review` rows.
2. Verify candidate evidence in authorized source systems.
3. Record reviewer, decision, reason, timestamp, and evidence.
4. Route ambiguous or conflicting cases to a second reviewer.
5. Apply approved canonical updates through a governed write path.
6. Rebuild downstream marts only after approved identity changes land.

## How To Present This

The strongest explanation:

> Phase 5B does not make the matcher more aggressive. It makes the review
> workflow more usable. The system stays conservative, but stewards get ranked,
> explainable candidates so they spend less time searching.

## What The Optional Evaluation Means

The evaluation report checks whether the residual scorer ranks the stewardship
queue's `suggested_canonical_person_id` highly when that hint exists.

That hint is not ground truth. It is the best deterministic candidate that
failed automatic merge controls. The evaluation is useful for diagnostics:

- Are hints usually present in the top candidate list?
- Does the top candidate tend to align with the deterministic hint?
- Are thresholds too strict for reviewer workload?
- Are some recommendation bands noisier than others?

It is not useful for claiming production accuracy or approving matches.

## Risk Controls

- No full email addresses are exported.
- No SIN_LAST_4 is exported.
- Full DOB is not selected by the residual export path.
- Candidate recommendations never write `int_canonical_person`.
- Proxy evaluation never becomes an identity decision.
- The system prefers false negatives over false positives.

## What Phase 5B Proves

Phase 5B demonstrates that Atlas can extend deterministic identity resolution
with assistive ranking while preserving governance:

- explainable scoring
- review-first recommendations
- sensitive-field minimization
- workload reporting
- diagnostic evaluation without overclaiming accuracy
- clear human stewardship boundary
