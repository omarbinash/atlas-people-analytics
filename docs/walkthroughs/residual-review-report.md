# Residual Review Report

This report summarizes review-only residual identity candidates produced
after deterministic dbt matching. These rows are steward-review aids,
not canonical truth and not automatic merges.

## Summary

| Metric | Value |
|---|---:|
| Stewardship rows sampled | 500 |
| Rows with at least one suggested candidate | 273 |
| Candidate rows emitted | 352 |
| Review yield rate | 54.6% |

## How To Read This Report

A row in this report means the residual engine found a reviewable
candidate for a source identity that deterministic dbt matching left
in stewardship. It does not mean the candidate is correct, and it does
not change `int_canonical_person`.

The highest-value operating metric is not raw match rate. It is whether
the queue gives stewards enough ranked evidence to make safer manual
decisions without creating false-positive merges.

## Recommendation Mix

| Recommendation | Candidates | Mean score | Mean evidence weight |
|---|---:|---:|---:|
| high_confidence_review | 146 | 0.982 | 0.970 |
| possible_review | 206 | 0.866 | 0.886 |

## Source-System Mix

| Source system | Candidates |
|---|---:|
| ATS | 219 |
| CRM | 98 |
| DMS_ERP | 35 |

## Feature Coverage

| Feature | Candidate coverage |
|---|---:|
| first_name_root | 86.9% |
| last_name | 86.9% |
| email_local | 93.5% |
| hire_date | 100.0% |
| deterministic_hint | 99.4% |

## Top Review Candidates

| Source record | Candidate canonical person | Recommendation | Score | Anchors | Reasons |
|---|---|---|---:|---:|---|
| DMS_ERP::0c4c4606076a637c3ef47547b985250a | cp_d233c1fef9ef358105fd4c9f1c879e6f | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| DMS_ERP::0abe752313dc1f288247af4b516604c6 | cp_03c98fcf1bf1c36443852e87dce135dd | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| DMS_ERP::01c1d0bf50e1390ad60cf4fcdf885340 | cp_95469f6140cdd78aac00423ba2a10894 | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| CRM::crm_user_706 | cp_d7a46acab701b9eeedafdf6c75b27631 | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| CRM::crm_user_704 | cp_3bd055746c66f9a8a0087aeabfba5361 | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| CRM::crm_user_658 | cp_544eb1f5f50fe24ba7d8fd72167e091c | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| CRM::crm_user_4951 | cp_828bbf56128ebac85d862d812932e76c | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| CRM::crm_user_4898 | cp_e100eb6bd5a7365629e4a28452fdf0e4 | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| CRM::crm_user_4608 | cp_11edd42272797221c8960b3f6a166deb | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| CRM::crm_user_4514 | cp_857724e95446bd7ab72404124e5ea203 | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| CRM::crm_user_4340 | cp_4adc2d7a7b7275aa1acaf0ffa98f6dfd | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |
| CRM::crm_user_4271 | cp_03c98fcf1bf1c36443852e87dce135dd | high_confidence_review | 1.000 | 5 | recommendation=high_confidence_review; last_name_similarity=1.00; first_name_root_exact; email_local_similarity=1.00; hire_date_within_30_days; deterministic_candidate_hint |

## Recommended Stewardship Workflow

1. Start with `high_confidence_review` rows and verify the evidence
   against authorized source-system context.
2. Use `possible_review` rows to reduce search effort, not to approve
   automatically.
3. Record reviewer, decision, reason, timestamp, and source evidence
   in the future stewardship workflow before any canonical update.
4. Re-run downstream marts only after an approved identity decision is
   applied through a governed write path.

## Risk Controls

- No SIN_LAST_4, full email, or DOB is exported by the residual engine.
- Recommendations are outside the canonical-person write path.
- Review suggestions are intentionally biased toward false negatives
  over false positives.
- Low-evidence candidates remain invisible rather than appearing as
  weak suggestions.

## Control Boundary

The residual engine is deliberately outside the canonical-person write path.
Any suggested match must still be adjudicated by a human steward before
it can influence canonical employee records or downstream marts.
