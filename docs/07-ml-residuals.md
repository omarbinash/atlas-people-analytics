# Residual Matching Model Card

Atlas Phase 5A adds a residual matching assistant for records that remain in
`int_stewardship_queue` after deterministic identity resolution. Phase 5B adds
the richer walkthrough package around that assistant: review reporting,
stewardship workflow notes, and optional proxy evaluation.

## Intended Use

The residual engine ranks candidate canonical people for manual steward review.
It is designed to reduce review effort, not to replace the deterministic dbt
matcher.

## Non-Goals

- It does not auto-merge records.
- It does not update `int_canonical_person`.
- It does not use SIN_LAST_4, full email addresses, or DOB.
- It does not make employment, compensation, or performance decisions.

## Features

The current review score uses:

- first-name-root exact match
- last-name similarity
- email-local-part similarity
- hire-date proximity
- deterministic candidate hint from the stewardship queue, when available

The score is intentionally explainable and conservative. Recommendations are:

- `high_confidence_review`
- `possible_review`
- `do_not_suggest`

## Threshold Philosophy

False positives in employee identity data are worse than false negatives. A bad
merge can contaminate headcount, attrition, tenure, compensation, and
performance attribution. The residual engine therefore requires multiple
positive anchors before it recommends a candidate for review.

## Running

```bash
python -m identity_engine.cli residual-candidates \
  --limit 500 \
  --top-n 3 \
  --output identity_engine/output/residual_candidates.csv
```

The output directory is gitignored. Exports are review artifacts, not canonical
truth.

Generate a markdown review report:

```bash
python -m identity_engine.cli residual-report \
  --limit 500 \
  --top-n 3 \
  --minimum-score 0.75 \
  --top-candidates 12 \
  --output docs/walkthroughs/residual-review-report.md
```

Generate optional proxy evaluation:

```bash
python -m identity_engine.cli residual-evaluate \
  --limit 500 \
  --top-n 3 \
  --minimum-score 0.75 \
  --output docs/walkthroughs/residual-model-evaluation.md
```

The proxy evaluation uses `int_stewardship_queue.suggested_canonical_person_id`
only as a weak diagnostic hint. It is not ground truth and must not be used to
approve canonical identity updates.

## Phase 5B Walkthroughs

- [Residual Review Walkthrough](walkthroughs/residual-review-walkthrough.md)
- [Residual Review Report](walkthroughs/residual-review-report.md)
- [Residual Proxy Evaluation](walkthroughs/residual-model-evaluation.md)

## Validation

The unit tests cover:

- high-confidence review candidate scoring
- sparse/conflicting evidence suppression
- ranking behavior
- review-coverage report rendering
- proxy-evaluation summary rendering
- SQL guards against sensitive identity fields
