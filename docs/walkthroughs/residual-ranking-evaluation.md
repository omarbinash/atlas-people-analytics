# Residual Proxy Evaluation

This optional evaluation checks residual-review candidate ranking against
the stewardship queue's `suggested_canonical_person_id` when that hint
exists.

This is **not** ground-truth accuracy evaluation. The hint is the best
deterministic candidate that failed auto-merge controls, so it is useful
for walkthrough diagnostics but must not be treated as an approved match.

## Run Configuration

| Setting | Value |
|---|---:|
| Top candidates per source | 3 |
| Minimum residual score | 0.75 |

## Summary

| Metric | Value |
|---|---:|
| Source rows with proxy hint | 470 |
| Proxy-hint rows with at least one candidate | 271 |
| Candidate coverage rate | 57.7% |
| Top-1 proxy alignment count | 224 |
| Top-1 proxy alignment rate | 47.7% |
| Top-3 proxy alignment count | 268 |
| Top-3 proxy alignment rate | 57.0% |
| Missing candidate count | 199 |
| Missing candidate rate | 42.3% |
| Mean proxy-label rank when found | 1.16 |

## Alignment By Recommendation

| Recommendation | Candidate rows | Proxy alignment rate |
|---|---:|---:|
| high_confidence_review | 146 | 100.0% |
| possible_review | 204 | 59.8% |

## Interpretation

- High top-1 alignment means the residual scorer tends to put the
  deterministic hint first when it emits a candidate.
- Low coverage means the scorer is preserving the conservative control
  boundary and leaving weak-evidence rows for manual search.
- Alignment below 100% is not automatically bad: the deterministic hint
  itself is not a steward-approved label.
- This report should guide threshold tuning and reviewer workload
  planning, not canonical identity updates.

## Control Boundary

Proxy evaluation is a diagnostic artifact. It does not approve matches,
write `int_canonical_person`, or change downstream People Analytics
marts.
